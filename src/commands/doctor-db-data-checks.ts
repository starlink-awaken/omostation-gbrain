import type { BrainEngine } from '../core/engine.ts';
import type { Check } from './doctor-types.ts';
import type { ProgressReporter } from '../core/progress.ts';
import { startHeartbeat } from '../core/progress.ts';
import * as db from '../core/db.ts';
import { join } from 'path';
import { readFileSync, readdirSync, statSync } from 'fs';
import {
  childTableOrphansCheck,
  checkAbandonedThreads,
  checkCalibrationFreshness,
  checkGradeConfidenceDrift,
  checkVoiceGateHealth,
  checkRerankerHealth,
  checkBrainstormHealth,
  checkZeEmbeddingHealth,
  checkEmbeddingWidthConsistency,
  checkSourceRoutingHealth,
  checkOauthConfidentialHealth,
  checkAutopilotLockScope,
  checkSearchMode,
  checkEvalDrift,
  checkSubagentCapability,
  checkSyncFreshness,
  takesWeightGridCheck,
  whoknowsHealthCheck,
} from './doctor-checks.ts';

/**
 * DB data-plane checks (graph/integrity/markdown/queue) — extracted from
 * runDoctor (BET-Y1Q3-T6-04). Pure check-collection: never process.exit.
 */
export async function runDoctorDbDataChecks(
  engine: BrainEngine,
  checks: Check[],
  progress: ProgressReporter,
  args: string[],
): Promise<void> {
  // 9. Graph health (link + timeline coverage on entity pages).
  // dead_links removed in v0.10.1: ON DELETE CASCADE on link FKs makes it always 0.
  //
  // Skip when the brain has 0 entity pages (markdown-only wikis, journals,
  // notes brains). The coverage formula divides by entity-page count, so it's
  // structurally undefined when no entities exist — emitting WARN under that
  // condition is a false positive. Closes #530.
  progress.heartbeat('graph_coverage');
  try {
    const health = await engine.getHealth();
    const entityCount = (await engine.executeRaw<{ count: number }>(
      "SELECT COUNT(*)::int AS count FROM pages WHERE type IN ('entity', 'person', 'company', 'organization')",
    ))[0]?.count ?? 0;

    const linkPct = ((health.link_coverage ?? 0) * 100).toFixed(0);
    const timelinePct = ((health.timeline_coverage ?? 0) * 100).toFixed(0);
    if (entityCount === 0) {
      // Markdown-only / journal / wiki brain — no entity pages to compute
      // coverage against. Coverage formula is structurally inapplicable.
      checks.push({
        name: 'graph_coverage',
        status: 'ok',
        message: 'No entity pages — graph_coverage not applicable (markdown-only brain)',
      });
    } else if ((health.link_coverage ?? 0) >= 0.5 && (health.timeline_coverage ?? 0) >= 0.5) {
      checks.push({ name: 'graph_coverage', status: 'ok', message: `Entity link coverage ${linkPct}%, timeline ${timelinePct}%` });
    } else {
      checks.push({
        name: 'graph_coverage',
        status: 'warn',
        message: `Entity link coverage ${linkPct}%, timeline ${timelinePct}% (${entityCount} entity pages). Run: gbrain extract all`,
      });
    }

    // Bug 11 — brain_score breakdown. When the total is < 100, show which
    // components contributed the deficit so users know what to fix.
    // Uses distinct *_score field names (not overloading link_coverage /
    // timeline_coverage, which are entity-scoped).
    if (health.brain_score < 100) {
      const parts = [
        `embed ${health.embed_coverage_score}/35`,
        `links ${health.link_density_score}/25`,
        `timeline ${health.timeline_coverage_score}/15`,
        `orphans ${health.no_orphans_score}/15`,
        `dead-links ${health.no_dead_links_score}/10`,
      ];
      checks.push({
        name: 'brain_score',
        status: health.brain_score >= 70 ? 'ok' : 'warn',
        message: `Brain score ${health.brain_score}/100 (${parts.join(', ')})`,
      });
    } else {
      checks.push({ name: 'brain_score', status: 'ok', message: `Brain score 100/100` });
    }
  } catch {
    checks.push({ name: 'graph_coverage', status: 'warn', message: 'Could not check graph coverage' });
  }

  // 10. Integrity sample scan (v0.13 knowledge runtime).
  // Read-only — no network, no writes, no resolver calls. Samples the first
  // 500 pages by slug order and surfaces bare-tweet + dead-link counts as a
  // warning. Full-brain scan: `gbrain integrity check`.
  progress.heartbeat('integrity_sample');
  const integrityHb = startHeartbeat(progress, 'scanning 500-page integrity sample…');
  try {
    const { scanIntegrity } = await import('./integrity.ts');
    const res = await scanIntegrity(engine, { limit: 500 });
    const total = res.bareHits.length + res.externalHits.length;
    if (total === 0) {
      checks.push({
        name: 'integrity',
        status: 'ok',
        message: `Sampled ${res.pagesScanned} pages; no bare-tweet phrases or external links.`,
      });
    } else if (res.bareHits.length > 0) {
      checks.push({
        name: 'integrity',
        status: 'warn',
        message: `Sampled ${res.pagesScanned} pages; ${res.bareHits.length} bare-tweet phrase(s), ${res.externalHits.length} external link(s). Run: gbrain integrity check (or integrity auto to repair).`,
      });
    } else {
      checks.push({
        name: 'integrity',
        status: 'ok',
        message: `Sampled ${res.pagesScanned} pages; ${res.externalHits.length} external link(s) (no bare tweets).`,
      });
    }
  } catch (e) {
    checks.push({ name: 'integrity', status: 'warn', message: `integrity scan skipped: ${e instanceof Error ? e.message : String(e)}` });
  } finally {
    integrityHb();
  }

  // 10. JSONB integrity (v0.12.3 reliability wave).
  // v0.12.0's JSON.stringify()::jsonb pattern stored JSONB string literals
  // instead of objects on real Postgres. PGLite masked this; Supabase did not.
  // Scan 5 known write sites for rows whose top-level jsonb_typeof is
  // 'string'. `page_versions.frontmatter` added in v0.15.2 so doctor's
  // surface matches `repair-jsonb` (the previous 4-target scan missed a
  // repair target, per #254/Codex review).
  progress.heartbeat('jsonb_integrity');
  try {
    const sql = db.getConnection();
    const targets: Array<{ table: string; col: string; expected: 'object' | 'array' }> = [
      { table: 'pages',         col: 'frontmatter',    expected: 'object' },
      { table: 'raw_data',      col: 'data',           expected: 'object' },
      { table: 'ingest_log',    col: 'pages_updated',  expected: 'array'  },
      { table: 'files',         col: 'metadata',       expected: 'object' },
      { table: 'page_versions', col: 'frontmatter',    expected: 'object' },
    ];
    let totalBad = 0;
    const breakdown: string[] = [];
    for (const { table, col } of targets) {
      progress.heartbeat(`jsonb_integrity.${table}.${col}`);
      const rows = await sql.unsafe(
        `SELECT count(*)::int AS n FROM ${table} WHERE jsonb_typeof(${col}) = 'string'`,
      );
      const n = Number((rows as any)[0]?.n ?? 0);
      if (n > 0) { totalBad += n; breakdown.push(`${table}.${col}=${n}`); }
    }
    if (totalBad === 0) {
      checks.push({ name: 'jsonb_integrity', status: 'ok', message: 'All JSONB columns store objects/arrays' });
    } else {
      checks.push({
        name: 'jsonb_integrity',
        status: 'warn',
        message: `${totalBad} row(s) double-encoded (${breakdown.join(', ')}). Fix: gbrain repair-jsonb`,
      });
    }
  } catch {
    checks.push({ name: 'jsonb_integrity', status: 'warn', message: 'Could not check JSONB integrity' });
  }

  // 10b. Takes weight grid integrity (v0.32 — EXP-2).
  //
  // Cross-modal eval over 100K production takes flagged 0.74, 0.82-style
  // weights as false precision. v0.31's engine layer rounds to 0.05 on
  // insert (PR #795); v0.32's migration v48 backfills pre-existing data.
  // This check is the post-backfill drift detector — if a downstream
  // extraction agent or hand-edit re-introduces off-grid values, we want
  // the warning to surface before it pollutes scorecard / calibration math.
  //
  // Pure helper so the test surface targets `takesWeightGridCheck(engine)`
  // directly rather than the full `runDoctor` pipeline (codex review #7).
  progress.heartbeat('takes_weight_grid');
  checks.push(await takesWeightGridCheck(engine));

  // 10c. Child-table orphan detection (closes #1063).
  // The autopilot `orphans` phase scans for orphan pages (no inbound links)
  // but does NOT detect orphan rows in FK-child tables. After a bulk page
  // delete, child rows can persist if cascade didn't fire (pre-FK rows,
  // race during bulk cascade, code path that bypassed cascade). This
  // surfaces them with paste-ready cleanup SQL.
  progress.heartbeat('child_table_orphans');
  checks.push(await childTableOrphansCheck(engine));

  // v0.33: whoknows_health — fixture presence + row count. The eval
  // gate itself runs via `gbrain eval whoknows`; this check is the
  // "did you do the assignment?" signal.
  progress.heartbeat('whoknows_health');
  checks.push(await whoknowsHealthCheck(engine));

  // v0.36 cross-modal wave: modality column cleanup.
  //
  // Historical brains that imported image assets before v0.27.1's
  // `modality='image'` default-set may have image chunks where
  // embedding_image is populated but modality wasn't tagged. The cross-modal
  // search routing in v0.36 depends on `modality` for keyword filtering;
  // surface the gap so operators can run `gbrain backfill modality`.
  progress.heartbeat('cross_modal_modality_backfill');
  try {
    const mismatchRows = await engine.executeRaw<{ count: string | number }>(
      `SELECT COUNT(*)::text AS count FROM content_chunks
       WHERE embedding_image IS NOT NULL
         AND chunk_source = 'image_asset'
         AND (modality IS NULL OR modality != 'image')`,
    );
    const mismatch = parseInt(String(mismatchRows[0]?.count ?? '0'), 10);
    if (mismatch === 0) {
      checks.push({
        name: 'cross_modal_modality_backfill',
        status: 'ok',
        message: 'All image-asset chunks have modality=image',
      });
    } else {
      checks.push({
        name: 'cross_modal_modality_backfill',
        status: 'warn',
        message:
          `${mismatch} image-asset chunk(s) have embedding_image populated but modality != 'image'. ` +
          `Fix: \`gbrain backfill modality\``,
      });
    }
  } catch {
    // Engine probably doesn't have the modality column (pre-v0.27.1 brain) —
    // skip silently. Auto-migration will land it on next upgrade.
    checks.push({
      name: 'cross_modal_modality_backfill',
      status: 'ok',
      message: 'modality column not present (pre-v0.27.1 brain); skipped',
    });
  }

  // v0.36 Phase 3 — unified_multimodal coverage (D21 source-aware).
  //
  // Only meaningful when search.unified_multimodal is on. Reports the
  // percentage of content_chunks with embedding_multimodal populated.
  // Source-aware: a global 95% can hide 0% coverage for a specific source.
  progress.heartbeat('unified_multimodal_coverage');
  try {
    const unifiedFlag = await engine.getConfig('search.unified_multimodal').catch(() => null);
    const unifiedOnlyFlag = await engine.getConfig('search.unified_multimodal_only').catch(() => null);
    const unifiedOn = unifiedFlag === 'true' || unifiedFlag === '1';
    const unifiedOnlyOn = unifiedOnlyFlag === 'true' || unifiedOnlyFlag === '1';

    if (!unifiedOn) {
      checks.push({
        name: 'unified_multimodal_coverage',
        status: 'ok',
        message: 'search.unified_multimodal is off; coverage check N/A',
      });
    } else {
      // D21 source-aware: report per-source coverage so multi-source brains
      // can't hide 0% on one source behind a high global average.
      const rows = await engine.executeRaw<{ source_id: string | null; total: string; covered: string }>(
        `SELECT
           COALESCE(p.source_id, 'default') AS source_id,
           COUNT(*)::text AS total,
           SUM(CASE WHEN cc.embedding_multimodal IS NOT NULL THEN 1 ELSE 0 END)::text AS covered
         FROM content_chunks cc
         JOIN pages p ON p.id = cc.page_id
         GROUP BY p.source_id`,
      );
      const perSource = rows.map(r => ({
        source: r.source_id || 'default',
        total: parseInt(String(r.total), 10),
        covered: parseInt(String(r.covered), 10),
      }));
      const lowestCoverage = perSource.reduce(
        (acc, r) => Math.min(acc, r.total > 0 ? r.covered / r.total : 1),
        1,
      );
      const summary = perSource.map(r => {
        const pct = r.total > 0 ? Math.round((r.covered / r.total) * 100) : 0;
        return `${r.source}:${pct}%`;
      }).join(', ');

      if (unifiedOnlyOn && lowestCoverage < 0.99) {
        checks.push({
          name: 'unified_multimodal_coverage',
          status: 'fail',
          message:
            `unified_multimodal_only is ON but lowest source coverage is ${(lowestCoverage * 100).toFixed(1)}% (${summary}). ` +
            `Run \`gbrain reindex --multimodal\` to bring coverage to 99%+ or disable strict mode.`,
        });
      } else if (lowestCoverage < 0.95) {
        checks.push({
          name: 'unified_multimodal_coverage',
          status: 'warn',
          message:
            `unified_multimodal is on but lowest source coverage is ${(lowestCoverage * 100).toFixed(1)}% (${summary}). ` +
            `Run \`gbrain reindex --multimodal\` to fill the gap.`,
        });
      } else {
        checks.push({
          name: 'unified_multimodal_coverage',
          status: 'ok',
          message: `unified_multimodal coverage: ${summary}`,
        });
      }
    }
  } catch {
    // Column probably not present (pre-v0.36 brain pre-migration); skip silently.
    checks.push({
      name: 'unified_multimodal_coverage',
      status: 'ok',
      message: 'embedding_multimodal column not present yet; skipped',
    });
  }

  // 11. Markdown body completeness (v0.12.3 reliability wave).
  // v0.12.0's splitBody ate everything after the first `---` horizontal rule,
  // truncating wiki-style pages. Heuristic: pages whose body is <30% of the
  // raw source content length when raw has multiple H2/H3 boundaries.
  //
  // No total on this check: the regex scan over rd.data -> 'content' is a
  // sequential scan that LIMIT 100 bounds only the output, not the scan
  // work. We heartbeat every second so agents see life, no fake totals.
  progress.heartbeat('markdown_body_completeness');
  const mbcHb = startHeartbeat(progress, 'scanning pages for truncation…');
  try {
    const sql = db.getConnection();
    const rows = await sql`
      SELECT p.slug,
             length(p.compiled_truth) AS body_len,
             length(rd.data ->> 'content') AS raw_len
      FROM pages p
      JOIN raw_data rd ON rd.page_id = p.id
      WHERE rd.data ? 'content'
        AND length(rd.data ->> 'content') > 1000
        AND length(p.compiled_truth) < length(rd.data ->> 'content') * 0.3
        AND (rd.data ->> 'content') ~ '(^|\n)##+ '
      LIMIT 100
    `;
    if (rows.length === 0) {
      checks.push({ name: 'markdown_body_completeness', status: 'ok', message: 'No truncated bodies detected' });
    } else {
      const sample = rows.slice(0, 3).map((r: any) => r.slug).join(', ');
      checks.push({
        name: 'markdown_body_completeness',
        status: 'warn',
        message: `${rows.length} page(s) appear truncated (sample: ${sample}). Re-import with: gbrain sync --force`,
      });
    }
  } catch {
    // pages_raw.raw_data may not exist on older schemas; best-effort.
    checks.push({ name: 'markdown_body_completeness', status: 'ok', message: 'Skipped (raw_data unavailable)' });
  } finally {
    mbcHb();
  }

  // 11a. Frontmatter integrity (v0.22.4, hardened in v0.38.2.0).
  // scanBrainSources walks every registered source's local_path on disk
  // (not from the DB), invoking parseMarkdown(..., {validate:true}) per
  // file. Reports per-source counts grouped by error code. The fix path is
  // `gbrain frontmatter validate <source-path> --fix`, which writes .bak
  // backups so it works for both git and non-git brain repos.
  //
  // v0.38.2.0 wave (this PR supersedes PR #1287):
  //  - `pruneDir` now applies at descent inside brain-writer.ts:walkDir so
  //    the scan no longer recurses into node_modules / .git / .obsidian /
  //    *.raw / ops. That alone takes the 216K-page user from "hangs
  //    forever" to "completes in seconds" on the typical brain.
  //  - `deadline` (per-file Date.now() check inside the sync loop) is the
  //    load-bearing wall-clock bound. AbortSignal.timeout (kept for
  //    between-source aborts) cannot interrupt sync readdirSync /
  //    readFileSync — codex outside-voice C1 caught the original plan's
  //    assumption that it could.
  //  - Partial-result surfacing: per-source status ('scanned' | 'partial' |
  //    'skipped'), files_scanned numerator, and an honest "scanned ~N files
  //    (source has ~M pages in DB)" message when the deadline fires. The
  //    `partial` and `aborted_at_source` fields on AuditReport feed the
  //    JSON consumer.
  //  - Configurable via GBRAIN_DOCTOR_FM_TIMEOUT_MS (default 30000ms).
  progress.heartbeat('frontmatter_integrity');
  const fmHb = startHeartbeat(progress, 'scanning frontmatter…');
  const fmTimeoutMs = (() => {
    const raw = process.env.GBRAIN_DOCTOR_FM_TIMEOUT_MS;
    const n = raw ? parseInt(raw, 10) : NaN;
    return Number.isFinite(n) && n > 0 ? n : 30000;
  })();
  try {
    const { scanBrainSources } = await import('../core/brain-writer.ts');
    const fmDeadline = Date.now() + fmTimeoutMs;
    const fmAbort = AbortSignal.timeout(fmTimeoutMs);
    // Per-source DB denominator. Coarse — DB pages and on-disk syncable
    // files are overlapping but not identical (unsynced disk files,
    // soft-deleted DB rows, auto-generated pages). Wording in the partial
    // message makes the mismatch honest. Failure of the COUNT degrades to
    // null and the message falls back to bare numerator.
    const dbPageCountForSource = async (sourceId: string): Promise<number | null> => {
      try {
        const rows = await engine.executeRaw<{ n: string }>(
          `SELECT COUNT(*)::text AS n FROM pages WHERE source_id = $1 AND deleted_at IS NULL`,
          [sourceId],
        );
        if (rows.length === 0) return null;
        const parsed = parseInt(rows[0].n, 10);
        return Number.isFinite(parsed) ? parsed : null;
      } catch {
        return null;
      }
    };
    const report = await scanBrainSources(engine, {
      signal: fmAbort,
      deadline: fmDeadline,
      dbPageCountForSource,
    });

    if (report.total === 0 && !report.partial) {
      const sources = report.per_source.length;
      checks.push({
        name: 'frontmatter_integrity',
        status: 'ok',
        message: sources === 0
          ? 'No registered sources to scan'
          : `${sources} source(s) clean — no frontmatter issues`,
      });
    } else {
      // Build per-source breakdown that distinguishes scanned / partial /
      // skipped so the user can tell which sources weren't checked.
      const sourceMessages: string[] = [];
      for (const src of report.per_source) {
        if (src.status === 'skipped') {
          // Codex adversarial #1: `gbrain frontmatter validate` takes a
          // filesystem PATH, not a source id. Pre-fix the hint pointed users
          // at a command that would fail with "no such directory" — breaking
          // the very remediation path this PR ships to give them.
          sourceMessages.push(
            `${src.source_id}: NOT SCANNED (timeout — run \`gbrain frontmatter validate ${src.source_path}\`)`,
          );
          continue;
        }
        if (src.status === 'partial') {
          const denom = src.db_page_count != null ? ` (source has ~${src.db_page_count} pages in DB)` : '';
          const codes = src.total > 0
            ? `, ${Object.entries(src.errors_by_code).map(([k, v]) => `${k}=${v}`).join(', ')}`
            : '';
          sourceMessages.push(
            `${src.source_id}: PARTIAL — scanned ~${src.files_scanned} files${denom}, ${src.total} issue(s) so far${codes}`,
          );
          continue;
        }
        // status === 'scanned'
        if (src.total === 0) continue; // clean source — don't clutter the message
        const codes = Object.entries(src.errors_by_code)
          .map(([k, v]) => `${k}=${v}`)
          .join(', ');
        sourceMessages.push(`${src.source_id}: ${src.total} (${codes})`);
      }
      const fixHint = report.partial
        ? `Raise GBRAIN_DOCTOR_FM_TIMEOUT_MS or run \`gbrain frontmatter validate <source>\` directly. Fix issues: \`gbrain frontmatter validate <source> --fix\``
        : `Fix: gbrain frontmatter validate <source-path> --fix`;
      checks.push({
        name: 'frontmatter_integrity',
        status: 'warn',
        message:
          `${report.total} frontmatter issue(s)` +
          (report.partial ? ` (PARTIAL SCAN — timeout after ${fmTimeoutMs / 1000}s)` : '') +
          `. ${sourceMessages.join('; ')}. ${fixHint}`,
      });
    }
  } catch (e) {
    // Codex outside-voice D4: the abort path returns cleanly via partial
    // state — this catch is purely for unexpected errors (FS permission,
    // OOM, disk full, etc.). Pre-v0.38.2.0 (PR #1287) had an unreachable
    // abort-classifier branch here; removed because timer-based aborts
    // in a sync walker can't surface as a thrown error anyway.
    checks.push({
      name: 'frontmatter_integrity',
      status: 'warn',
      message: `Could not scan frontmatter: ${e instanceof Error ? e.message : String(e)}`,
    });
  } finally {
    fmHb();
  }

  // 11a-bis. Eval-capture health (v0.25.0). Capture is a fire-and-forget
  // side-effect that logs failures to a persistent table so this check
  // can see drops cross-process (the MCP server captures; `gbrain doctor`
  // runs in a separate process). Counts failures in the last 24h and
  // warns when non-zero. Pre-v31 brains: the table doesn't exist yet;
  // swallow the error and report skipped.
  progress.heartbeat('eval_capture');
  try {
    const since = new Date(Date.now() - 24 * 3600 * 1000);
    const failures = await engine.listEvalCaptureFailures({ since });
    if (failures.length === 0) {
      checks.push({ name: 'eval_capture', status: 'ok', message: 'No capture failures in the last 24h' });
    } else {
      const byReason = new Map<string, number>();
      for (const f of failures) {
        byReason.set(f.reason, (byReason.get(f.reason) ?? 0) + 1);
      }
      const breakdown = [...byReason.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([r, n]) => `${n} ${r}`)
        .join(', ');
      checks.push({
        name: 'eval_capture',
        status: 'warn',
        message: `${failures.length} capture failure(s) in the last 24h (${breakdown}). ` +
          `If you care about replay fidelity, investigate. If not, set eval.capture: false ` +
          `in ~/.gbrain/config.json to silence.`,
      });
    }
  } catch (err) {
    // Distinguish "table doesn't exist yet" (pre-v31, ok skip) from real
    // problems like RLS denying SELECT — the latter masks the very condition
    // this check is supposed to surface (capture INSERTs almost certainly
    // also fail).
    const code = (err as { code?: string } | null)?.code;
    if (code === '42P01') {
      checks.push({ name: 'eval_capture', status: 'ok', message: 'Skipped (eval_capture_failures table unavailable — apply migrations or upgrade)' });
    } else if (code === '42501') {
      checks.push({
        name: 'eval_capture',
        status: 'warn',
        message: 'RLS denies SELECT on eval_capture_failures. Capture INSERTs are almost certainly failing too. Run as a role with BYPASSRLS or grant SELECT on this table.',
      });
    } else {
      checks.push({
        name: 'eval_capture',
        status: 'warn',
        message: `Could not read eval_capture_failures: ${(err as Error)?.message ?? String(err)}`,
      });
    }
  }

  // 11a-bis-3. contradictions probe summary (v0.32.6 — M1).
  //
  // Reads the most recent eval_contradictions_runs row and surfaces:
  //   - headline count + severity breakdown
  //   - paste-ready resolution commands per HIGH-severity finding
  //   - Wilson CI band so the user knows whether the headline is trustworthy
  // Skipped (status: 'ok') when the table is empty — the probe simply hasn't
  // run yet, which is normal on a fresh install.
  progress.heartbeat('contradictions');
  try {
    const recent = await engine.loadContradictionsTrend(7);
    if (recent.length === 0) {
      checks.push({
        name: 'contradictions',
        status: 'ok',
        message: 'No probe runs in the last 7 days. Run `gbrain eval suspected-contradictions --query "..." --top-k 5` to populate.',
      });
    } else {
      const latest = recent[0];
      const report = latest.report_json as Record<string, unknown> | null;
      const perQuery = (report?.per_query as Array<{
        contradictions: Array<{
          severity: 'low' | 'medium' | 'high';
          axis: string;
          a: { slug: string };
          b: { slug: string };
          resolution_command: string;
        }>;
      }> | undefined) ?? [];
      let high = 0, medium = 0, low = 0;
      const highFindings: Array<{ a: string; b: string; axis: string; cmd: string }> = [];
      for (const q of perQuery) {
        for (const c of q.contradictions) {
          if (c.severity === 'high') {
            high++;
            highFindings.push({ a: c.a.slug, b: c.b.slug, axis: c.axis, cmd: c.resolution_command });
          } else if (c.severity === 'medium') medium++;
          else low++;
        }
      }
      const total = high + medium + low;
      if (total === 0) {
        checks.push({
          name: 'contradictions',
          status: 'ok',
          message: `Latest probe run (${latest.ran_at.slice(0, 10)}) found no suspected contradictions across ${latest.queries_evaluated} queries.`,
        });
      } else {
        const ciLow = (latest.wilson_ci_lower * 100).toFixed(0);
        const ciHigh = (latest.wilson_ci_upper * 100).toFixed(0);
        const lines = [
          `${total} suspected contradictions (high=${high} medium=${medium} low=${low}) detected by latest probe — Wilson CI 95%: ${ciLow}-${ciHigh}%.`,
        ];
        for (const f of highFindings.slice(0, 3)) {
          lines.push(`  HIGH: ${f.a} vs ${f.b}${f.axis ? ' — ' + f.axis : ''}`);
          lines.push(`    → ${f.cmd}`);
        }
        if (highFindings.length > 3) {
          lines.push(`  …and ${highFindings.length - 3} more — see \`gbrain eval suspected-contradictions review\``);
        }
        checks.push({
          name: 'contradictions',
          status: high > 0 ? 'warn' : 'ok',
          message: lines.join('\n  '),
        });
      }
    }
  } catch (err) {
    const code = (err as { code?: string } | null)?.code;
    if (code === '42P01') {
      checks.push({ name: 'contradictions', status: 'ok', message: 'Skipped (eval_contradictions_runs table unavailable — apply migrations to enable)' });
    } else {
      checks.push({
        name: 'contradictions',
        status: 'warn',
        message: `Could not read contradictions trend: ${(err as Error)?.message ?? String(err)}`,
      });
    }
  }

  // 11a-bis-2. facts_extraction_health (v0.31.2 — codex P1 #3).
  //
  // Mirrors the eval_capture check shape but reads facts:absorb rows
  // (written by writeFactsAbsorbLog from src/core/facts/absorb-log.ts).
  // Iterates over EVERY source so multi-source brains see per-source
  // failure rates instead of only 'default'. Threshold configurable via
  // `facts.absorb_warn_threshold` (default 10 over the last 24h, per
  // source, per reason). When the threshold is exceeded for any
  // (source, reason) pair, status flips to warn and the message names
  // the breakdown.
  progress.heartbeat('facts_extraction_health');
  try {
    const thresholdRaw = await engine.getConfig('facts.absorb_warn_threshold');
    const parsed = parseInt(thresholdRaw ?? '', 10);
    const threshold = Number.isFinite(parsed) && parsed > 0 ? parsed : 10;

    // Single SQL grouping by (source_id, reason) over the last 24h. The
    // composite index v50 added (idx_ingest_log_source_type_created on
    // source_id, source_type, created_at DESC) covers this query's
    // filter + sort path.
    const rows = await engine.executeRaw<{
      source_id: string;
      reason: string;
      n: string | number;
    }>(
      `SELECT
         source_id,
         split_part(summary, ':', 1) AS reason,
         COUNT(*)::text AS n
       FROM ingest_log
       WHERE source_type = 'facts:absorb'
         AND created_at >= now() - INTERVAL '24 hours'
       GROUP BY source_id, split_part(summary, ':', 1)
       ORDER BY source_id, COUNT(*) DESC`,
    );

    if (rows.length === 0) {
      checks.push({
        name: 'facts_extraction_health',
        status: 'ok',
        message: 'No facts:absorb failures in the last 24h.',
      });
    } else {
      // Group per source so the breakdown is operator-friendly.
      const bySource = new Map<string, Array<{ reason: string; n: number }>>();
      let anyOverThreshold = false;
      for (const r of rows) {
        const n = typeof r.n === 'number' ? r.n : parseInt(r.n, 10);
        if (!Number.isFinite(n)) continue;
        if (n >= threshold) anyOverThreshold = true;
        if (!bySource.has(r.source_id)) bySource.set(r.source_id, []);
        bySource.get(r.source_id)!.push({ reason: r.reason, n });
      }
      const summary = [...bySource.entries()]
        .map(([sid, reasons]) =>
          `${sid}: ${reasons.map(x => `${x.n} ${x.reason}`).join(', ')}`,
        )
        .join(' | ');
      checks.push({
        name: 'facts_extraction_health',
        status: anyOverThreshold ? 'warn' : 'ok',
        message: anyOverThreshold
          ? `Facts:absorb failures over the threshold (${threshold}) in the last 24h: ${summary}. ` +
            `Run \`gbrain recall --since 24h --json\` to inspect what landed; ` +
            `tune the gate via \`gbrain config set facts.absorb_warn_threshold N\`.`
          : `Facts:absorb activity in last 24h (under threshold ${threshold}): ${summary}.`,
      });
    }
  } catch (err) {
    const code = (err as { code?: string } | null)?.code;
    if (code === '42P01' || code === '42703') {
      // ingest_log missing entirely (extreme legacy) or source_id column
      // missing (pre-v50 brain that hasn't run apply-migrations yet).
      checks.push({
        name: 'facts_extraction_health',
        status: 'ok',
        message: 'Skipped (ingest_log.source_id unavailable — run `gbrain apply-migrations --yes`).',
      });
    } else if (code === '42501') {
      checks.push({
        name: 'facts_extraction_health',
        status: 'warn',
        message: 'RLS denies SELECT on ingest_log. The check can\'t see facts:absorb rows. Run as a BYPASSRLS role or grant SELECT on this table.',
      });
    } else {
      checks.push({
        name: 'facts_extraction_health',
        status: 'warn',
        message: `Could not read ingest_log for facts:absorb: ${(err as Error)?.message ?? String(err)}`,
      });
    }
  }

  // 11a-2. effective_date_health (v0.29.1).
  //
  // Detects pages where computeEffectiveDate fell back to updated_at even
  // though parseable frontmatter dates are present (codex pass-1 #5
  // resolution: the sentinel column lets us catch "wrong but populated"
  // rows that look healthy at first glance).
  //
  // Sample 1000 random rows by default to keep the check fast on 200K-page
  // brains. The expression index pages_coalesce_date_idx makes the future-
  // date and pre-1990 scans cheap; the parseable-fm-date scan reads
  // frontmatter JSONB and is the slow path.
  progress.heartbeat('effective_date_health');
  try {
    const result = await engine.executeRaw<{ kind: string; count: string }>(
      `WITH sample AS (
         SELECT slug, frontmatter, effective_date, effective_date_source
           FROM pages
          ORDER BY id DESC
          LIMIT 1000
       )
       SELECT 'fallback_with_fm_date' AS kind, COUNT(*)::text AS count
         FROM sample
        WHERE effective_date_source = 'fallback'
          AND (frontmatter ? 'event_date' OR frontmatter ? 'date' OR frontmatter ? 'published')
       UNION ALL
       SELECT 'future_dated', COUNT(*)::text FROM sample
        WHERE effective_date IS NOT NULL AND effective_date > NOW() + INTERVAL '1 year'
       UNION ALL
       SELECT 'pre_1990', COUNT(*)::text FROM sample
        WHERE effective_date IS NOT NULL AND effective_date < TIMESTAMPTZ '1990-01-01'`,
    );
    const counts = new Map(result.map(r => [r.kind, Number(r.count)]));
    const fallbackWithFm = counts.get('fallback_with_fm_date') ?? 0;
    const future = counts.get('future_dated') ?? 0;
    const pre1990 = counts.get('pre_1990') ?? 0;
    if (fallbackWithFm > 0 || future > 0 || pre1990 > 0) {
      const parts: string[] = [];
      if (fallbackWithFm > 0) parts.push(`${fallbackWithFm} fell back to updated_at despite parseable frontmatter date`);
      if (future > 0) parts.push(`${future} dated > NOW() + 1y`);
      if (pre1990 > 0) parts.push(`${pre1990} pre-1990`);
      checks.push({
        name: 'effective_date_health',
        status: 'warn',
        message: `${parts.join('; ')} (sample of last 1000 pages). Run \`gbrain reindex-frontmatter\` to recompute.`,
      });
    } else {
      checks.push({
        name: 'effective_date_health',
        status: 'ok',
        message: 'Sample of last 1000 pages clean (no fallback-with-parseable-fm-date, no future-dated, no pre-1990)',
      });
    }
  } catch (err) {
    const code = (err as { code?: string } | null)?.code;
    if (code === '42703') {
      // column doesn't exist — pre-v0.29.1 brain
      checks.push({ name: 'effective_date_health', status: 'ok', message: 'Skipped (effective_date column unavailable — run gbrain apply-migrations)' });
    } else {
      checks.push({ name: 'effective_date_health', status: 'warn', message: `Could not read pages: ${(err as Error)?.message ?? String(err)}` });
    }
  }

  // 11a-3. salience_health (v0.29.1).
  //
  // Detects pages with active takes (so emotional_weight should be > 0)
  // whose recompute_emotional_weight phase hasn't yet run, plus the
  // brain-average emotional_weight as an informational signal.
  progress.heartbeat('salience_health');
  try {
    const result = await engine.executeRaw<{ kind: string; n: string }>(
      `SELECT 'zero_weight_with_takes' AS kind, COUNT(DISTINCT p.id)::text AS n
         FROM pages p
         JOIN takes t ON t.page_id = p.id AND t.active = TRUE
        WHERE COALESCE(p.emotional_weight, 0) = 0
       UNION ALL
       SELECT 'nonzero_weight', COUNT(*)::text FROM pages WHERE COALESCE(emotional_weight, 0) > 0`,
    );
    const counts = new Map(result.map(r => [r.kind, Number(r.n)]));
    const zeroWithTakes = counts.get('zero_weight_with_takes') ?? 0;
    const nonzero = counts.get('nonzero_weight') ?? 0;
    if (zeroWithTakes > 0) {
      checks.push({
        name: 'salience_health',
        status: 'warn',
        message: `${zeroWithTakes} pages with active takes have emotional_weight=0. Run \`gbrain dream --phase recompute_emotional_weight\` to populate. Brain has ${nonzero} pages with non-zero emotional_weight.`,
      });
    } else if (nonzero === 0) {
      checks.push({
        name: 'salience_health',
        status: 'ok',
        message: 'Skipped (no pages have emotional_weight > 0; either fresh install or recompute hasn\'t run yet)',
      });
    } else {
      checks.push({
        name: 'salience_health',
        status: 'ok',
        message: `${nonzero} pages have non-zero emotional_weight; no take/weight mismatches detected`,
      });
    }
  } catch (err) {
    const code = (err as { code?: string } | null)?.code;
    if (code === '42703' || code === '42P01') {
      checks.push({ name: 'salience_health', status: 'ok', message: 'Skipped (emotional_weight or takes table unavailable — pre-v0.29 brain)' });
    } else {
      checks.push({ name: 'salience_health', status: 'warn', message: `Could not read pages: ${(err as Error)?.message ?? String(err)}` });
    }
  }

  // 11b. Queue health (v0.19.1 queue-resilience wave).
  // Postgres-only because PGLite has no multi-process worker surface. Two
  // subchecks, both cheap (single SELECT each, status-index-covered):
  //
  //   1. stalled-forever: any active job whose started_at is > 1h old. The
  //      incident that motivated this release ran 90+ min before surfacing.
  //      Surface the ID so the operator can `gbrain jobs get <id>` to inspect
  //      or `gbrain jobs cancel <id>` to force-kill.
  //
  //   2. backpressure-missed: per-name waiting depth exceeds the threshold
  //      (default 10, override via GBRAIN_QUEUE_WAITING_THRESHOLD env). Signal
  //      that a submitter probably needs maxWaiting set. Bounded by per-name
  //      aggregation so a single name's pile shows up clearly instead of
  //      getting lost in the total.
  //
  // Not included in v0.19.1 (tracked as B7 follow-up): worker-heartbeat
  // staleness. It needs a minion_workers table; the lock_until-on-active-jobs
  // proxy can't distinguish "no worker" from "worker idle," and a check that
  // cries wolf erodes trust in every other doctor check.
  progress.heartbeat('queue_health');
  if (engine.kind === 'pglite') {
    checks.push({
      name: 'queue_health',
      status: 'ok',
      message: 'Skipped (PGLite — no multi-process worker surface)',
    });
  } else {
    const queueHealthHb = startHeartbeat(progress, 'scanning queue health…');
    try {
      const sql = db.getConnection();
      // Subcheck 1: stalled-forever active jobs (>1h wall-clock).
      const stalledRows: Array<{ id: number; name: string; started_at: string }> = await sql`
        SELECT id, name, started_at::text AS started_at
          FROM minion_jobs
         WHERE status = 'active'
           AND started_at IS NOT NULL
           AND started_at < now() - interval '1 hour'
         ORDER BY started_at ASC
         LIMIT 5
      `;
      // Subcheck 2: per-name waiting depth exceeds threshold.
      const rawThreshold = process.env.GBRAIN_QUEUE_WAITING_THRESHOLD;
      const parsedThreshold = rawThreshold ? parseInt(rawThreshold, 10) : 10;
      const threshold = Number.isFinite(parsedThreshold) && parsedThreshold >= 1
        ? parsedThreshold
        : 10;
      const depthRows: Array<{ name: string; queue: string; depth: number }> = await sql`
        SELECT name, queue, count(*)::int AS depth
          FROM minion_jobs
         WHERE status = 'waiting'
         GROUP BY name, queue
        HAVING count(*) > ${threshold}
         ORDER BY depth DESC
         LIMIT 5
      `;
      // Subcheck 3 (v0.22.14): RSS-watchdog kills in the last 24h. Bare workers
      // newly default to --max-rss 2048 (was 0); operators who run large embed
      // or import jobs may see kills that didn't happen pre-v0.22.14. We surface
      // a hint when this signature appears so the upgrade path is obvious.
      // Signature: when the watchdog trips, gracefulShutdown('watchdog') aborts
      // in-flight jobs with `new Error('watchdog')`. The worker's failJob path
      // (worker.ts:660-664) writes `error_text = 'aborted: watchdog'` for any
      // job in-flight at the moment of the kill.
      //
      // We deliberately DO NOT do a loose `ILIKE '%watchdog%'`:
      //   1. Parent jobs that inherit `on_child_fail='fail_parent'` get
      //      `"child job N failed: aborted: watchdog"` — counting that
      //      double-counts (child + parent) for one watchdog event.
      //   2. Any user error_text containing the word "watchdog" matches.
      // Match the exact prefix `'aborted: watchdog'` to scope this purely to
      // the worker's own kill signature.
      const rssKillRows: Array<{ cnt: number }> = await sql`
        SELECT count(*)::int AS cnt
          FROM minion_jobs
         WHERE status IN ('dead', 'failed')
           AND finished_at > now() - interval '24 hours'
           AND error_text = 'aborted: watchdog'
      `;
      const rssKillCount = rssKillRows[0]?.cnt ?? 0;

      // Subcheck 4 (v0.30.2): prompt_too_long terminal failures on subagent
      // jobs in the last 24h. The dream/synthesize phase classifies Anthropic
      // 400 "prompt is too long" responses as UnrecoverableError so they
      // dead-letter on first attempt instead of clogging the queue with
      // max_stalled retries. Surface count + fix hint when present.
      const promptTooLongRows: Array<{ cnt: number }> = await sql`
        SELECT count(*)::int AS cnt
          FROM minion_jobs
         WHERE name = 'subagent'
           AND status = 'dead'
           AND finished_at > now() - interval '24 hours'
           AND error_text LIKE 'prompt_too_long:%'
      `;
      const promptTooLongCount = promptTooLongRows[0]?.cnt ?? 0;

      const problems: string[] = [];
      if (stalledRows.length > 0) {
        const sample = stalledRows
          .map(r => `#${r.id}(${r.name})`)
          .join(', ');
        problems.push(
          `${stalledRows.length} stalled-forever job(s): ${sample}. ` +
          `Fix: gbrain jobs get <id> to inspect; gbrain jobs cancel <id> to force-kill.`
        );
      }
      if (depthRows.length > 0) {
        const sample = depthRows
          .map(r => `${r.name}@${r.queue}=${r.depth}`)
          .join(', ');
        problems.push(
          `waiting-queue depth exceeds ${threshold} for: ${sample}. ` +
          `Fix: set maxWaiting on the submitter (or raise GBRAIN_QUEUE_WAITING_THRESHOLD).`
        );
      }
      if (rssKillCount > 0) {
        problems.push(
          `${rssKillCount} job(s) dead-lettered for RSS-watchdog memory-limit kills in last 24h. ` +
          `v0.22.14 changed the bare-worker --max-rss default from 0 (off) to 2048 MB. ` +
          `Fix: raise the limit (e.g. \`gbrain jobs work --max-rss 4096\`) or opt out (\`--max-rss 0\`). ` +
          `See skills/migrations/v0.22.14.md.`
        );
      }
      if (promptTooLongCount > 0) {
        problems.push(
          `${promptTooLongCount} subagent job(s) dead-lettered with prompt_too_long in last 24h. ` +
          `Dream/synthesize transcripts exceeded the model's input context. ` +
          `Fix: \`gbrain dream --phase synthesize --dry-run --json\` to identify fat transcripts; ` +
          `set \`dream.synthesize.max_prompt_tokens\` to bound the per-chunk budget, or use a ` +
          `larger-context model (Opus 4.7 = 1M tokens vs Sonnet 4.6 = 200K).`
        );
      }

      if (problems.length === 0) {
        checks.push({
          name: 'queue_health',
          status: 'ok',
          message: `No stalled-forever jobs; no queue over depth ${threshold}.`,
        });
      } else {
        checks.push({
          name: 'queue_health',
          status: 'warn',
          message: problems.join(' '),
        });
      }
    } catch (e) {
      checks.push({
        name: 'queue_health',
        status: 'warn',
        message: `queue_health scan skipped: ${e instanceof Error ? e.message : String(e)}`,
      });
    } finally {
      queueHealthHb();
    }
  }

  // 11.4 subagent_capability (v0.38 — D7; was subagent_provider in v0.31.12). Surfaces a
  // warn when models.tier.subagent or models.default points at a non-Anthropic
  // provider. Layers 1 (queue.ts submit-time) and 2 (handler runtime) also
  // enforce; this is the surfacing layer so users see the config drift before
  // a job is submitted.
  progress.heartbeat('subagent_capability');
  checks.push(await checkSubagentCapability(engine));

  // 11.5 facts_health (v0.31 hot memory). Surfaces per-source counters so
  // operators can see the extraction pipeline's pulse without raw SQL.
  // Lightweight: one COUNT-with-filters query + a top-5 aggregate. Only
  // runs when the facts table exists (post-v40 brains); pre-v40 the
  // probe is a no-op.
  progress.heartbeat('facts_health');
  try {
    const factsExists = await engine.executeRaw<{ exists: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'facts') AS exists`,
    );
    if (factsExists[0]?.exists) {
      const health = await engine.getFactsHealth('default');
      const status: 'ok' | 'warn' = health.total_active >= 0 ? 'ok' : 'warn';
      const top = health.top_entities
        .slice(0, 3)
        .map(t => `${t.entity_slug}:${t.count}`)
        .join(', ') || '—';
      checks.push({
        name: 'facts_health',
        status,
        message:
          `facts_health(default): ${health.total_active} active, ` +
          `${health.total_today} today, ${health.total_week} this week, ` +
          `${health.total_consolidated} consolidated, ` +
          `top entities ${top}`,
      });
    } else {
      checks.push({
        name: 'facts_health',
        status: 'ok',
        message: 'facts table not present (pre-v0.31 brain or migration pending)',
      });
    }
  } catch (e) {
    checks.push({
      name: 'facts_health',
      status: 'warn',
      message: `facts_health probe failed: ${e instanceof Error ? e.message : String(e)}`,
    });
  }

  // 12. Index audit (opt-in via --index-audit). v0.13.1 follow-up to #170.
  // Reports indexes with zero recorded scans on Postgres. Informational only;
  // we DO NOT auto-drop. On #170's brain, idx_pages_frontmatter and
  // idx_pages_trgm showed 0 scans — the suggestion there is "consider
  // investigating on YOUR brain," not "drop these globally." Zero scans on a
  // fresh install is also normal (nothing has queried yet); the real signal
  // is zero scans on a long-running active brain.
  if (args.includes('--index-audit')) {
    progress.heartbeat('index_audit');
    if (engine.kind === 'pglite') {
      checks.push({
        name: 'index_audit',
        status: 'ok',
        message: 'Skipped (PGLite — pg_stat_user_indexes is a Postgres extension)',
      });
    } else {
      try {
        const sql = db.getConnection();
        const rows = await sql`
          SELECT schemaname, relname AS table, indexrelname AS index,
                 idx_scan, pg_size_pretty(pg_relation_size(indexrelid)) AS size
            FROM pg_stat_user_indexes
           WHERE schemaname = 'public'
             AND idx_scan = 0
           ORDER BY pg_relation_size(indexrelid) DESC
           LIMIT 20
        `;
        if (rows.length === 0) {
          checks.push({ name: 'index_audit', status: 'ok', message: 'All public indexes have recorded scans' });
        } else {
          const list = rows.map((r: any) => `${r.index}(${r.size})`).join(', ');
          checks.push({
            name: 'index_audit',
            status: 'warn',
            message: `${rows.length} zero-scan index(es): ${list}. ` +
                     `Consider investigating whether they're used on YOUR workload (fresh brains naturally show zero scans until queries accumulate). ` +
                     `Do not drop without confirming.`,
          });
        }
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        checks.push({ name: 'index_audit', status: 'warn', message: `Index audit failed: ${msg}` });
      }
    }
  }

  // v0.27.1: image_assets — vanished images (files row exists but file
  // missing on disk). Cherry-4b. Engine-agnostic; uses listFilesForPage's
  // sibling SQL via raw query for cross-engine compatibility.
  if (engine) {
    progress.heartbeat('image_assets');
    try {
      const rows = await engine.executeRaw<{ storage_path: string }>(
        `SELECT storage_path FROM files WHERE mime_type LIKE 'image/%' LIMIT 1000`
      );
      let vanished = 0;
      const vanishedPaths: string[] = [];
      const fs = await import('node:fs');
      for (const r of rows) {
        try {
          fs.statSync(r.storage_path);
        } catch {
          vanished++;
          if (vanishedPaths.length < 5) vanishedPaths.push(r.storage_path);
        }
      }
      if (rows.length === 0) {
        checks.push({ name: 'image_assets', status: 'ok', message: 'No image assets indexed yet' });
      } else if (vanished === 0) {
        checks.push({ name: 'image_assets', status: 'ok', message: `${rows.length} image(s) all present on disk` });
      } else {
        checks.push({
          name: 'image_assets',
          status: 'warn',
          message: `${vanished} of ${rows.length} image(s) missing from disk (e.g. ${vanishedPaths.join(', ')}). ` +
                   `Fix: restore from git, or \`gbrain sync --skip-failed\` to acknowledge.`,
        });
      }
    } catch {
      // Pre-v36 brains may not have the files table on PGLite — quiet skip.
    }

    // v0.27.1 Eng-1B: ocr_health — counters incremented by importImageFile.
    // Warns when OCR is opted-in (attempted > 0) but never succeeds.
    progress.heartbeat('ocr_health');
    try {
      const attempted = parseInt((await engine.getConfig('ocr_attempted')) ?? '0', 10);
      const succeeded = parseInt((await engine.getConfig('ocr_succeeded')) ?? '0', 10);
      const failedNoKey = parseInt((await engine.getConfig('ocr_failed_no_key')) ?? '0', 10);
      const failedOther = parseInt((await engine.getConfig('ocr_failed_other')) ?? '0', 10);
      if (attempted === 0) {
        checks.push({ name: 'ocr_health', status: 'ok', message: 'OCR not in use (or no images ingested with OCR opt-in)' });
      } else if (succeeded === 0 && (failedNoKey > 0 || failedOther > 0)) {
        const reasons: string[] = [];
        if (failedNoKey > 0) reasons.push(`${failedNoKey} no-key`);
        if (failedOther > 0) reasons.push(`${failedOther} other`);
        checks.push({
          name: 'ocr_health',
          status: 'warn',
          message: `OCR is opted-in but no calls succeeded (${attempted} attempted, ${reasons.join(', ')}). ` +
                   `Fix: verify OPENAI_API_KEY is set, or set embedding_image_ocr=false to disable.`,
        });
      } else {
        checks.push({
          name: 'ocr_health',
          status: 'ok',
          message: `OCR healthy (${succeeded}/${attempted} succeeded; ${failedNoKey} no-key, ${failedOther} other failures)`,
        });
      }
    } catch { /* config table missing on a very old brain — skip */ }
  }

  // Sync freshness check (v0.32 — Check that sources are synced recently)
  if (engine !== null) {
    progress.heartbeat('sync_freshness');
    checks.push(await checkSyncFreshness(engine));
  }

  // v0.32.3 search-lite — mode + eval_drift surfaces. Status stays 'ok' per
  // [CDX-20]; hint lives in `message`.
  if (engine !== null) {
    progress.heartbeat('search_mode');
    checks.push(await checkSearchMode(engine));
    progress.heartbeat('eval_drift');
    checks.push(await checkEvalDrift(engine));
    // v0.35.0.0+ reranker_health — read JSONL audit; warn on auth or volume.
    progress.heartbeat('reranker_health');
    checks.push(await checkRerankerHealth(engine));
    // v0.37.0 brainstorm_health — migration v79, track_retrieval, calibration cold-start.
    progress.heartbeat('brainstorm_health');
    checks.push(await checkBrainstormHealth(engine));
    // v0.36.0.0 (A5): ZE embedding key health + schema/config width consistency.
    progress.heartbeat('ze_embedding_health');
    checks.push(await checkZeEmbeddingHealth(engine));
    progress.heartbeat('embedding_width_consistency');
    checks.push(await checkEmbeddingWidthConsistency(engine));

    // v0.37.7.0 doctor checks (#1167, #1166, #1226) — fast-mode skipped
    // since these touch DB queries with cost on large brains.
    // 5K — source_routing_health (D5 lock: 200-page total cap)
    progress.heartbeat('source_routing_health');
    checks.push(await checkSourceRoutingHealth(engine));
    // 5L — oauth_confidential_client_health (success-path probe per codex CF8)
    progress.heartbeat('oauth_confidential_client_health');
    checks.push(await checkOauthConfidentialHealth(engine));
    // 5M — autopilot_lock_scope (PID-safe hint per codex CF11)
    progress.heartbeat('autopilot_lock_scope');
    checks.push(checkAutopilotLockScope());
  }

}
