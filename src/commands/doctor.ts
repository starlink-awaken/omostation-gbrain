import type { BrainEngine } from '../core/engine.ts';
import * as db from '../core/db.ts';
import { LATEST_VERSION, getIdleBlockers } from '../core/migrate.ts';
import { checkResolvable } from '../core/check-resolvable.ts';
import { autoFixDryViolations, type AutoFixReport, type FixOutcome } from '../core/dry-fix.ts';
import { autoDetectSkillsDirReadOnly } from '../core/repo-root.ts';
import { loadOrDeriveManifest } from '../core/skill-manifest.ts';
import { parseSkillFrontmatter } from '../core/skill-frontmatter.ts';
import {
  analyzeSkillBrainFirst,
  buildBrainFirstSummaryLine,
  type BrainFirstAnalysis,
} from '../core/skill-brain-first.ts';
import {
  loadSnapshot,
  writeSnapshotAtomically,
  diffAgainstSnapshot,
  appendAuditEventsForTransitions,
} from '../core/audit-skill-brain-first.ts';
import { loadCompletedMigrations } from '../core/preferences.ts';
import { compareVersions } from './migrations/index.ts';
import { createProgress, startHeartbeat, type ProgressReporter } from '../core/progress.ts';
import { getCliOptions, cliOptsToProgressOptions } from '../core/cli-options.ts';
import type { DbUrlSource } from '../core/config.ts';
import { gbrainPath } from '../core/config.ts';
import { dirname, isAbsolute, join, resolve as resolvePath } from 'path';
import { fileURLToPath } from 'url';
import { existsSync, readFileSync, readdirSync, statSync } from 'fs';
import { resolveWhoknowsFixturePath, whoknowsHealthCheck, takesWeightGridCheck } from './doctor-checks.ts';
export { resolveWhoknowsFixturePath, whoknowsHealthCheck, takesWeightGridCheck };
export { runRemediationPlan, runRemediate } from './doctor-remediate.ts';
import {
  checkSchemaPackActive,
  checkSchemaPackConsistency,
  checkSchemaPackSourceDrift,
} from './doctor-remediate.ts';
export {
  checkSchemaPackActive,
  checkSchemaPackConsistency,
  checkSchemaPackSourceDrift,
};
import {
  type Check,
  type DoctorReport,
  computeDoctorReport,
  outputResults,
} from './doctor-types.ts';
export {
  type Check,
  type DoctorReport,
  computeDoctorReport,
  outputResults,
};
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
} from './doctor-checks.ts';
import { runDoctorDbConnChecks } from './doctor-db-conn-checks.ts';
import { runDoctorDbDataChecks } from './doctor-db-data-checks.ts';
export { runDoctorDbConnChecks, runDoctorDbDataChecks };
export {
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
};



/**
 * Focused doctor for `run_doctor` MCP op + `gbrain remote doctor` CLI.
 *
 * Runs five checks scoped to "what does a remote operator need to know about
 * this brain right now?":
 *   - connection (engine reachable + page count)
 *   - schema_version (current vs latest)
 *   - brain_score (the 5-component health composite)
 *   - sync_failures (unacked parse failures)
 *   - queue_health (Postgres-only: stalled-forever active jobs)
 *
 * Deliberately a focused subset of the local doctor surface, NOT a full
 * mirror. Generalizing to lint/integrity/orphans is filed as follow-up work
 * pending demand. Local doctor is unchanged — operators on the host machine
 * still get the full check set.
 */
/**
 * Doctor check: takes.weight grid integrity (v0.32 — EXP-2).
 *
 * Pure helper — no `process.exit`, no side effects beyond the SQL probe.
 * `runDoctor` calls this and pushes the result onto its check list.
 * Tests can target this directly with a stubbed engine (codex review #7).
 *
 * Branches:
 *   - takes table doesn't exist (fresh brain pre-v37) → warn, "skipped"
 *   - 0 takes total → ok, "no takes yet" (avoids divide-by-zero)
 *   - off_grid / total > 10% → fail
 *   - off_grid / total > 1%  → warn
 *   - else → ok
 *
 * Tolerance matches migration v48: any value with abs(weight - on_grid) > 1e-3
 * is genuinely off-grid (the 0.05 grid is 5e-2; float32 noise is ~1e-7).
 */


export async function doctorReportRemote(engine: BrainEngine): Promise<DoctorReport> {
  const checks: Check[] = [];

  // 1. Connection
  let pageCount = 0;
  try {
    const stats = await engine.getStats();
    pageCount = stats.page_count ?? 0;
    checks.push({
      name: 'connection',
      status: 'ok',
      message: `Connected, ${pageCount} pages`,
    });
  } catch (e) {
    checks.push({
      name: 'connection',
      status: 'fail',
      message: e instanceof Error ? e.message : String(e),
    });
    // Without a connection, every other check is meaningless — short-circuit.
    return computeDoctorReport(checks);
  }

  // 2. Schema version. Uses engine.getConfig('version') — the same engine-
  // agnostic API the local doctor uses, works on both Postgres and PGLite.
  try {
    const versionStr = await engine.getConfig('version');
    const version = parseInt(versionStr || '0', 10);
    if (version >= LATEST_VERSION) {
      checks.push({ name: 'schema_version', status: 'ok', message: `Version ${version} (latest: ${LATEST_VERSION})` });
    } else if (version === 0) {
      checks.push({
        name: 'schema_version',
        status: 'fail',
        message: `No schema version recorded. Migrations never ran. Run \`gbrain apply-migrations --yes\` on the host.`,
      });
    } else {
      checks.push({
        name: 'schema_version',
        status: 'warn',
        message: `Version ${version}, latest is ${LATEST_VERSION}. Run \`gbrain apply-migrations --yes\` on the host.`,
      });
    }
  } catch {
    checks.push({ name: 'schema_version', status: 'warn', message: 'Could not check schema version' });
  }

  // 3. Brain score
  try {
    const health = await engine.getHealth();
    const score = health.brain_score ?? 0;
    checks.push({
      name: 'brain_score',
      status: score >= 70 ? 'ok' : score >= 50 ? 'warn' : 'fail',
      message: `Brain score ${score}/100`,
    });
  } catch (e) {
    checks.push({
      name: 'brain_score',
      status: 'warn',
      message: `Could not compute: ${e instanceof Error ? e.message : String(e)}`,
    });
  }

  // 3b. Migration wedge hint (v0.31.8 — D14 + D19). The brain server's
  // filesystem holds the migration ledger; the wedge condition (>=3 consecutive
  // partials with no later complete) needs the force-retry hint, not plain
  // --yes. Same shape as the local doctor at line ~336.
  try {
    const completed = loadCompletedMigrations();
    const byVersion = new Map<string, { complete: boolean; partial: boolean }>();
    for (const entry of completed) {
      const seen = byVersion.get(entry.version) ?? { complete: false, partial: false };
      if (entry.status === 'complete') seen.complete = true;
      if (entry.status === 'partial') seen.partial = true;
      byVersion.set(entry.version, seen);
    }
    const completedVersions = Array.from(byVersion.entries()).filter(([, s]) => s.complete).map(([v]) => v);
    const stuck = Array.from(byVersion.entries())
      .filter(([v, s]) => {
        if (!s.partial || s.complete) return false;
        const supersededBy = completedVersions.find(cv => compareVersions(cv, v) >= 0);
        return supersededBy === undefined;
      })
      .map(([v]) => v);
    const wedged: string[] = [];
    for (const v of stuck) {
      const partialCount = completed.filter(e => e.version === v && e.status === 'partial').length;
      if (partialCount >= 3) wedged.push(v);
    }
    if (wedged.length > 0) {
      const cmd = wedged.map(v => `gbrain apply-migrations --force-retry ${v}`).join(' && ');
      checks.push({
        name: 'minions_migration',
        status: 'fail',
        message: `WEDGED MIGRATION(s) on brain host: ${wedged.join(', ')}. Run on the host: ${cmd}`,
      });
    } else if (stuck.length > 0) {
      checks.push({
        name: 'minions_migration',
        status: 'fail',
        message: `MINIONS HALF-INSTALLED on brain host: ${stuck.join(', ')}. Run on the host: gbrain apply-migrations --yes`,
      });
    }
  } catch {
    // Best-effort. A broken JSONL on the brain server should not stop the
    // remote doctor.
  }

  // 4. Sync failures (file-plane state, not in-DB; see src/core/sync.ts).
  // Read the JSONL file directly at the canonical path; cheap and engine-agnostic.
  try {
    const { readFileSync, existsSync } = await import('fs');
    const { gbrainPath } = await import('../core/config.ts');
    const path = gbrainPath('sync-failures.jsonl');
    let unacked = 0;
    if (existsSync(path)) {
      const lines = readFileSync(path, 'utf-8').split('\n').filter(l => l.trim());
      for (const line of lines) {
        try {
          const entry = JSON.parse(line) as { acknowledged_at?: string | null };
          if (!entry.acknowledged_at) unacked++;
        } catch { /* skip malformed line */ }
      }
    }
    checks.push({
      name: 'sync_failures',
      status: unacked === 0 ? 'ok' : 'warn',
      message: unacked === 0
        ? 'No unacked failures'
        : `${unacked} unacked failure(s) — run \`gbrain sync --skip-failed\` on the host to acknowledge`,
    });
  } catch {
    checks.push({ name: 'sync_failures', status: 'ok', message: 'No failures recorded' });
  }

  // 4b. Multi-source drift (v0.31.8 — D8 + D14). Same shape as the local
  // doctor's check at the same name. Runs server-side; the result is
  // returned to the thin-client over MCP.
  try {
    const { findMisroutedPages } = await import('../core/multi-source-drift.ts');
    const sources = await engine.executeRaw<{ id: string; local_path: string | null }>(
      `SELECT id, local_path FROM sources`,
    );
    const nonDefaultWithPath = sources.filter(s => s.id !== 'default' && s.local_path);
    if (sources.length > 1 && nonDefaultWithPath.length > 0) {
      const result = await findMisroutedPages(
        engine,
        nonDefaultWithPath.map(s => ({ id: s.id, local_path: s.local_path as string })),
      );
      if (result.walk_truncated) {
        checks.push({
          name: 'multi_source_drift',
          status: 'warn',
          message: 'Multi-source drift check skipped — FS walk hit limit/timeout on the brain server.',
        });
      } else if (result.count > 0) {
        const sampleStr = result.sample.map(s => `${s.slug} (intended=${s.intended_source})`).join(', ');
        checks.push({
          name: 'multi_source_drift',
          status: 'warn',
          message:
            `${result.count} page slug(s) appear at 'default' but NOT at the intended source ` +
            `(e.g., ${sampleStr}). Likely pre-v0.30.3 misroutes OR an incomplete initial sync. ` +
            `Verify on the brain host: \`gbrain sources status\` then \`gbrain sync --source <id> --full\`.`,
        });
      } else {
        checks.push({
          name: 'multi_source_drift',
          status: 'ok',
          message: 'No cross-source slug drift detected.',
        });
      }
    }
  } catch {
    // Best-effort, like the rest of doctorReportRemote.
  }

  // 5. Queue health (Postgres-only). PGLite has no minion_jobs in the same
  // shape; skip the check there with an informational message.
  if (engine.kind === 'postgres') {
    try {
      const rows = await engine.executeRaw<{ stalled: string | number }>(
        `SELECT COUNT(*) AS stalled FROM minion_jobs
          WHERE state = 'active'
            AND started_at IS NOT NULL
            AND started_at < NOW() - INTERVAL '1 hour'`,
      );
      const stalled = Number(rows[0]?.stalled ?? 0);
      checks.push({
        name: 'queue_health',
        status: stalled === 0 ? 'ok' : 'warn',
        message: stalled === 0
          ? 'No stalled active jobs'
          : `${stalled} active job(s) stalled > 1h — \`gbrain jobs cancel <id>\` or \`gbrain jobs retry <id>\` on the host`,
      });
    } catch {
      checks.push({ name: 'queue_health', status: 'ok', message: 'No queue activity' });
    }
  } else {
    checks.push({ name: 'queue_health', status: 'ok', message: 'PGLite — no queue to check' });
  }

  // v0.31.12 subagent runtime enforcement (Layer 3 of 3 — Codex F13).
  // The subagent loop is Anthropic-only. If models.tier.subagent or
  // models.default is explicitly set to a non-Anthropic provider, warn here
  // so the user sees it at the next `gbrain doctor` run instead of at the
  // next subagent job submission. (Layers 1+2 also enforce — this is the
  // surfacing layer.)
  checks.push(await checkSubagentCapability(engine));

  // 6. Sync freshness check
  checks.push(await checkSyncFreshness(engine));

  // v0.39 T7 + T9 — schema-pack health checks (3 checks per v0.38 plan):
  //   schema_pack_active        — active pack resolves cleanly
  //   schema_pack_consistency   — % of pages typed against active pack
  //   schema_pack_source_drift  — per-source pack divergence
  checks.push(await checkSchemaPackActive(engine));
  checks.push(await checkSchemaPackConsistency(engine));
  checks.push(await checkSchemaPackSourceDrift(engine));

  // 7. v0.32.3 search-lite mode + per-key drift surface.
  checks.push(await checkSearchMode(engine));

  // 8. v0.32.3 eval_drift: retrieval-affecting files changed since last
  // eval run? Non-blocking — surfaces as ok + hint.
  checks.push(await checkEvalDrift(engine));

  // 9. v0.35.0.0+ reranker_health: surfaces rerank-audit failures from
  // ~/.gbrain/audit/rerank-failures-*.jsonl. Failure-only (no success
  // logging on the search hot path per CDX2-F22). Reads
  // search.reranker.enabled FIRST so absence-of-failures means different
  // things when reranker is on vs off.
  checks.push(await checkRerankerHealth(engine));

  // 9b. v0.37.0 brainstorm_health: surfaces three brainstorm/lsd readiness
  // signals: (a) migration v79 applied (last_retrieved_at column exists),
  // (b) calibration cold-start status (active_bias_tags empty), (c)
  // search.track_retrieval enabled/disabled. Each surfaces a paste-ready
  // fix hint.
  checks.push(await checkBrainstormHealth(engine));

  // 10. v0.36.1.0 Hindsight calibration wave (T12) — four new checks:
  //   - abandoned_threads: high-conviction takes never revisited
  //   - calibration_freshness: profile is older than 7 days
  //   - grade_confidence_drift: judge self-reported confidence vs actual accuracy (CDX-11 mitigation)
  //   - voice_gate_health: voice gate failure rate over the last 7 days
  checks.push(await checkAbandonedThreads(engine));
  checks.push(await checkCalibrationFreshness(engine));
  checks.push(await checkGradeConfidenceDrift(engine));
  checks.push(await checkVoiceGateHealth(engine));

  return computeDoctorReport(checks);
}

// --- v0.36.1.0 calibration doctor checks (T12) ---

/**
 * abandoned_threads: surfaces active high-conviction takes (weight >= 0.7)
 * older than 12 months that have neither been superseded nor linked to a
 * follow-up page. These are commitments the user made and never revisited.
 * Status 'ok' with a count; never warns/fails (this is signal, not error).
 */
export async function runDoctor(engine: BrainEngine | null, args: string[], dbSource?: DbUrlSource) {
  const jsonOutput = args.includes('--json');
  const fastMode = args.includes('--fast');
  const doFix = args.includes('--fix');
  const dryRun = args.includes('--dry-run');
  const locksMode = args.includes('--locks');

  // --locks is a focused diagnostic: it runs the same pg_stat_activity
  // query that `runMigrations` pre-flight uses, prints any idle-in-tx
  // backends, and exits. Used by a user (or the migrate.ts error 57014
  // message) who just hit a statement_timeout and needs to find the
  // blocker. Referenced from migrate.ts's 57014 diagnostic — that
  // message promised this flag exists.
  if (locksMode) {
    await runLocksCheck(engine, jsonOutput);
    return;
  }

  const checks: Check[] = [];
  let autoFixReport: AutoFixReport | null = null;

  // Progress reporter. `--json` is doctor's own JSON output (list of checks);
  // progress events stay on stderr regardless, gated by the global --quiet /
  // --progress-json flags. On a 52K-page brain the DB checks can take minutes,
  // and without a heartbeat agents can't tell doctor from a hang.
  const progress = createProgress(cliOptsToProgressOptions(getCliOptions()));

  // --- Filesystem checks (always run, no DB needed) ---

  // 1. Resolver health
  // Use the same auto-detect as `check-resolvable` so doctor sees a
  // workspace/skills dir reachable via $OPENCLAW_WORKSPACE or
  // ~/.openclaw/workspace, not just a `skills/` walked up from cwd.
  // Read-only variant adds the install-path fallback so a hosted-CLI install
  // run from `~` (e.g., `bun install -g github:garrytan/gbrain && cd ~ &&
  // gbrain doctor`) can still find the bundled skills/ dir without warning.
  const detected = autoDetectSkillsDirReadOnly();
  const skillsDir = detected.dir;
  if (skillsDir) {

    // --fix: run auto-repair BEFORE checkResolvable so the post-fix scan
    // reflects the new state. Auto-fix only targets DRY violations today;
    // other resolver issues are left to human repair.
    //
    // SAFETY GATE (v0.31.7 follow-up to D5): refuse --fix when the skills
    // dir came from the install-path fallback. autoFixDryViolations writes
    // to SKILL.md files; a user running `cd ~ && gbrain doctor --fix`
    // without an explicit signal would have install_path resolve to the
    // bundled gbrain repo and silently rewrite the install-tree skills.
    // Codex caught this leak in the v0.31.7 ship review (D6 lock).
    if (doFix) {
      if (detected.source === 'install_path') {
        process.stderr.write(
          'gbrain doctor --fix refused: skills dir resolved via install-path fallback (read-only).\n' +
          'The --fix flag writes to SKILL.md files; running it against the bundled install\n' +
          'tree would silently mutate gbrain itself. Set $GBRAIN_SKILLS_DIR, $OPENCLAW_WORKSPACE,\n' +
          'or pass --skills-dir <path> to point at the workspace you actually want to fix.\n',
        );
      } else {
        autoFixReport = autoFixDryViolations(skillsDir, { dryRun });
        printAutoFixReport(autoFixReport, dryRun, jsonOutput);
      }
    }

    const report = checkResolvable(skillsDir);
    if (report.errors.length === 0 && report.warnings.length === 0) {
      checks.push({
        name: 'resolver_health',
        status: 'ok',
        message: `${report.summary.total_skills} skills, all reachable`,
      });
    } else {
      const status = report.errors.length > 0 ? 'fail' as const : 'warn' as const;
      const total = report.errors.length + report.warnings.length;
      const check: Check = {
        name: 'resolver_health',
        status,
        message: `${total} issue(s): ${report.errors.length} error(s), ${report.warnings.length} warning(s)`,
        issues: [...report.errors, ...report.warnings].map(i => ({
          type: i.type,
          skill: i.skill,
          action: i.action,
          fix: i.fix,
        })),
      };
      checks.push(check);
    }
  } else {
    checks.push({ name: 'resolver_health', status: 'warn', message: 'Could not find skills directory' });
  }

  // 2. Skill conformance
  if (skillsDir) {
    const conformanceResult = checkSkillConformance(skillsDir);
    checks.push(conformanceResult);
  }

  // 2b. Skill brain-first compliance (v0.36.x, supersedes PR #1206).
  // Scans every SKILL.md for external-lookup tools (web_search, exa,
  // perplexity, etc.) and warns when the skill doesn't declare
  // `brain_first: exempt` AND doesn't carry a canonical Convention
  // callout / Phase 1 brain heading / position-relative brain-first
  // reference. Motivated by the 2026-05-19 tweet-shield incident.
  //
  // Audit trail: snapshot+diff at ~/.gbrain/audit/skill-brain-first-
  // snapshot.json. Writes one detected/resolved JSONL line per state
  // transition + one fixed line per applied --fix. Stable brain → zero
  // audit writes per doctor run.
  if (skillsDir) {
    checks.push(skillBrainFirstCheck(skillsDir));
  }

  // 3. Half-migrated Minions detection (filesystem-only).
  // If completed.jsonl has any status:"partial" entry with no later
  // status:"complete" for the same version, the install is mid-migration.
  // Typical cause: v0.11.0 stopgap wrote a partial record but nobody ran
  // `gbrain apply-migrations --yes` afterward. This check fires on every
  // `gbrain doctor` invocation so your OpenClaw's health skill catches it.
  //
  // Forward-progress override: a partial entry for vX.Y.Z is treated as
  // stale (not stuck) if there is a `complete` entry for any vA.B.C >= vX.Y.Z
  // anywhere in the file. The reasoning: if a newer migration successfully
  // landed, the install moved past the older partial — the old record is
  // historical noise from a stopgap that never finished cleanly, but the
  // schema clearly advanced. Without this, every install that went through
  // a v0.11.0 stopgap and then upgraded carries the "MINIONS HALF-INSTALLED"
  // flag forever, even on installs that have been at v0.22+ for months.
  try {
    const completed = loadCompletedMigrations();
    const byVersion = new Map<string, { complete: boolean; partial: boolean }>();
    for (const entry of completed) {
      const seen = byVersion.get(entry.version) ?? { complete: false, partial: false };
      if (entry.status === 'complete') seen.complete = true;
      if (entry.status === 'partial') seen.partial = true;
      byVersion.set(entry.version, seen);
    }
    const completedVersions = Array.from(byVersion.entries())
      .filter(([, s]) => s.complete)
      .map(([v]) => v);
    const stuck = Array.from(byVersion.entries())
      .filter(([v, s]) => {
        if (!s.partial || s.complete) return false;
        // Forward-progress override: if any version >= v has completed, the
        // partial is stale. compareVersions returns 1 when first arg is newer.
        const supersededBy = completedVersions.find(cv => compareVersions(cv, v) >= 0);
        return supersededBy === undefined;
      })
      .map(([v]) => v);

    // v0.31.8 (D19): detect 3-consecutive-partials shape (the apply-migrations
    // wedge condition). The `stuck` filter above already excludes
    // forward-progress-superseded versions, so we only count actual unresolved
    // partials per version. A version with >=3 trailing partials needs
    // `gbrain apply-migrations --force-retry <v>` once before plain --yes
    // will succeed (the 3-consecutive-partials guard in apply-migrations.ts
    // is still active). Without this hint, operators wedged on v0.29.1 (and
    // any future migration that hits the same guard) get "run --yes" advice
    // that won't unstick them.
    const wedged: string[] = [];
    for (const v of stuck) {
      const partialCount = completed.filter(
        e => e.version === v && e.status === 'partial',
      ).length;
      if (partialCount >= 3) wedged.push(v);
    }

    if (wedged.length > 0) {
      // The wedged set is a STRICT subset of the stuck set, so a wedged
      // version is also stuck. Surface the force-retry hint instead of the
      // generic --yes hint; chained with `&&` when multiple versions are
      // wedged so the operator can copy-paste a single line.
      const cmd = wedged.map(v => `gbrain apply-migrations --force-retry ${v}`).join(' && ');
      checks.push({
        name: 'minions_migration',
        status: 'fail',
        message: `WEDGED MIGRATION(s): ${wedged.join(', ')} (>=3 consecutive partials). Run: ${cmd}`,
      });
    } else if (stuck.length > 0) {
      checks.push({
        name: 'minions_migration',
        status: 'fail',
        message: `MINIONS HALF-INSTALLED (partial migration: ${stuck.join(', ')}). Run: gbrain apply-migrations --yes`,
      });
    }
    // Note: the "no preferences.json but schema is v7+" case is detected
    // in the DB section below (needs schema version).
  } catch (e) {
    // completed.jsonl read/parse failure is non-fatal — probably a fresh
    // install with no record yet. Don't warn here; the DB check below
    // handles the "schema v7+ but no prefs" case.
  }

  // 3b. Upgrade-error trail (v0.13+). `gbrain upgrade` silently swallows
  // best-effort failures in `gbrain post-upgrade`; the failure record is
  // appended to ~/.gbrain/upgrade-errors.jsonl so we can surface it here
  // with a paste-ready recovery hint. Without this, users end up with
  // half-upgraded brains and no signal.
  try {
    const home = process.env.HOME || '';
    const errPath = join(home, '.gbrain', 'upgrade-errors.jsonl');
    if (existsSync(errPath)) {
      const lines = readFileSync(errPath, 'utf-8').split('\n').filter(l => l.trim());
      if (lines.length > 0) {
        const latest = JSON.parse(lines[lines.length - 1]) as {
          ts: string; phase: string; from_version: string; to_version: string; hint: string;
        };
        const date = latest.ts.slice(0, 10);
        checks.push({
          name: 'upgrade_errors',
          status: 'warn',
          message: `Post-upgrade failure on ${date} (${latest.from_version} → ${latest.to_version}, phase: ${latest.phase}). Recovery: ${latest.hint}`,
        });
      }
    }
  } catch {
    // Read/parse failure is itself best-effort; skip silently.
  }

  // 3b-bis. Supervisor health (filesystem-only: PID liveness + audit log).
  // Reads the default PID file (`~/.gbrain/supervisor.pid` unless the user
  // overrode with GBRAIN_SUPERVISOR_PID_FILE) and the latest audit file
  // written by src/core/minions/handlers/supervisor-audit.ts. Surfaces
  // supervisor_running / last_start / crashes_24h / max_crashes_exceeded.
  // Does NOT run the supervisor itself — this is a read-only health check.
  try {
    const { DEFAULT_PID_FILE } = await import('../core/minions/supervisor.ts');
    const { readSupervisorEvents, summarizeCrashes } = await import('../core/minions/handlers/supervisor-audit.ts');

    let supervisorPid: number | null = null;
    let running = false;
    if (existsSync(DEFAULT_PID_FILE)) {
      try {
        const line = readFileSync(DEFAULT_PID_FILE, 'utf8').trim().split('\n')[0];
        const parsed = parseInt(line, 10);
        if (!isNaN(parsed) && parsed > 0) {
          supervisorPid = parsed;
          try { process.kill(parsed, 0); running = true; } catch { running = false; }
        }
      } catch { /* unreadable */ }
    }

    const events = readSupervisorEvents({ sinceMs: 24 * 60 * 60 * 1000 });
    const lastStart = events.filter(e => e.event === 'started').pop()?.ts ?? null;
    // Shared classifier — same code path runs in `gbrain jobs supervisor
    // status` (src/commands/jobs.ts). Counts only events whose `likely_cause`
    // is NOT in the clean denylist (clean_exit, graceful_shutdown). Pre-v0.34
    // entries lacking `likely_cause` fall back to `code !== 0`. Supersedes
    // v0.35.4.0's binary `classifyWorkerExit({code})` on this surface: the
    // `likely_cause` read correctly classifies SIGTERM (code=null,
    // likely_cause='graceful_shutdown') as clean, and produces per-cause
    // buckets so operators triage memory pressure (oom) vs code bugs
    // (runtime) without grep'ing JSONL. `classifyWorkerExit` is still
    // used by the supervisor's internal restart policy where the binary
    // shape is the right contract.
    const summary = summarizeCrashes(events);
    const crashes24h = summary.total;
    const causeStr = `runtime=${summary.by_cause.runtime_error} oom=${summary.by_cause.oom_or_external_kill} unknown=${summary.by_cause.unknown} legacy=${summary.by_cause.legacy}`;
    const maxCrashesEvent = events.filter(e => e.event === 'max_crashes_exceeded').pop() ?? null;

    // Only surface a Check if the supervisor was ever observed (stops the
    // "never used the supervisor" install from getting a warn about it).
    if (supervisorPid !== null || events.length > 0) {
      if (maxCrashesEvent) {
        checks.push({
          name: 'supervisor',
          status: 'fail',
          message: `Supervisor gave up at ${maxCrashesEvent.ts} (max_crashes_exceeded). Restart with: gbrain jobs supervisor start --detach`,
        });
      } else if (!running && events.length > 0) {
        checks.push({
          name: 'supervisor',
          status: 'warn',
          message: `Supervisor not running (last_start=${lastStart ?? 'unknown'}). Restart with: gbrain jobs supervisor start --detach`,
        });
      } else if (crashes24h >= 1) {
        // Threshold dropped from `>3` (pre-fix, inflated by clean exits being
        // miscounted) to `>=1` (any real crash is signal). Per-cause breakdown
        // gives operators triage context without grep'ing the JSONL.
        checks.push({
          name: 'supervisor',
          status: 'warn',
          message: `Worker crashed ${crashes24h}x in last 24h (${causeStr}). Check ~/.gbrain/audit/supervisor-*.jsonl for context.`,
        });
      } else {
        checks.push({
          name: 'supervisor',
          status: 'ok',
          message: `running=true pid=${supervisorPid} last_start=${lastStart ?? 'unknown'} crashes_24h=${crashes24h} clean_exits_24h=${summary.clean_exits}`,
        });
      }
    }
  } catch {
    // Audit read / import failure is best-effort; skip silently.
  }

  // 3b-tris. Stub-guard fire count (last 24h). The v0.34.5 stub guard in
  // fence-write.ts refuses to spawn unprefixed entity pages (e.g. bare
  // `alice.md` at brain root). Each fire is appended to
  // ~/.gbrain/audit/stub-guard-YYYY-Www.jsonl. This check is the operator
  // visibility surface for the guard's v0.36 sunset criterion: when the
  // 24h count is consistently low, the prefix-expansion in
  // resolveEntitySlug is doing its job and the guard can be removed.
  //
  // WARN at >10 fires/24h — at that rate the resolver is probably missing
  // a case (typo prefix, alias, non-Latin script). Operators should grep
  // the audit log for the slugs that hit it and either add the missing
  // resolver branch or document them as legitimate bare-slug ingestion.
  try {
    const { readRecentStubGuardEvents } = await import('../core/facts/stub-guard-audit.ts');
    const events = readRecentStubGuardEvents({ sinceMs: 24 * 60 * 60 * 1000 });
    if (events.length > 10) {
      // Surface the top 3 slugs that hit it so operators have somewhere to start.
      const slugCounts = new Map<string, number>();
      for (const e of events) slugCounts.set(e.slug, (slugCounts.get(e.slug) ?? 0) + 1);
      const topSlugs = [...slugCounts.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 3)
        .map(([slug, n]) => `${slug}(${n})`)
        .join(', ');
      checks.push({
        name: 'stub_guard_24h',
        status: 'warn',
        message:
          `Stub guard fired ${events.length}x in last 24h (top: ${topSlugs}). ` +
          `If this stays elevated, the prefix-expansion in resolveEntitySlug is ` +
          `missing a case. Check ~/.gbrain/audit/stub-guard-*.jsonl for the slugs ` +
          `that hit it.`,
      });
    } else if (events.length > 0) {
      checks.push({
        name: 'stub_guard_24h',
        status: 'ok',
        message: `Stub guard fired ${events.length}x in last 24h (below WARN threshold of 10).`,
      });
    }
    // Zero hits is the goal — emit no check at all so the doctor output stays clean.
  } catch {
    // Audit read failure is best-effort; skip silently.
  }

  // 3c. Sync failure trail (Bug 9). sync.ts gates the `sync.last_commit`
  // bookmark when per-file parse errors happen, and appends each failure
  // to ~/.gbrain/sync-failures.jsonl with the commit hash + exact error.
  // Without this doctor check, users see "sync blocked" and have no
  // surface showing which files to fix.
  try {
    const { unacknowledgedSyncFailures, loadSyncFailures, summarizeFailuresByCode } = await import('../core/sync.ts');
    const unacked = unacknowledgedSyncFailures();
    const all = loadSyncFailures();
    if (unacked.length > 0) {
      const codeSummary = summarizeFailuresByCode(unacked);
      const codeBreakdown = codeSummary.map(s => `${s.code}=${s.count}`).join(', ');
      const preview = unacked.slice(0, 3).map(f => `${f.path} (${f.error.slice(0, 60)})`).join('; ');
      checks.push({
        name: 'sync_failures',
        status: 'warn',
        message:
          `${unacked.length} unacknowledged sync failure(s) [${codeBreakdown}]. ${preview}` +
          `${unacked.length > 3 ? `, and ${unacked.length - 3} more` : ''}. ` +
          `Fix the file(s) and re-run 'gbrain sync', or use 'gbrain sync --skip-failed' to acknowledge.`,
      });
    } else if (all.length > 0) {
      // Acknowledged-only: show code breakdown for visibility.
      const ackedSummary = summarizeFailuresByCode(all);
      const ackedBreakdown = ackedSummary.map(s => `${s.code}=${s.count}`).join(', ');
      checks.push({
        name: 'sync_failures',
        status: 'ok',
        message: `${all.length} historical sync failure(s), all acknowledged [${ackedBreakdown}].`,
      });
    }
  } catch {
    // Best-effort. A broken JSONL should not stop doctor.
  }

  // 3d. Slug-fallback audit (v0.32.7 CJK wave, codex C7). Informational
  // count of pages where importFromFile fell back to a frontmatter slug
  // because the path slugified empty (emoji / Thai / Arabic / exotic-script
  // filenames). NOT routed through sync-failures.jsonl — that surface
  // gates bookmark advancement, info rows don't fit there.
  try {
    const { readRecentSlugFallbacks } = await import('../core/audit-slug-fallback.ts');
    const fallbacks = readRecentSlugFallbacks(7);
    if (fallbacks.length > 0) {
      checks.push({
        name: 'slug_fallback_audit',
        status: 'ok',
        message: `info: ${fallbacks.length} slug fallback${fallbacks.length === 1 ? '' : 's'} in the last 7 days (SLUG_FALLBACK_FRONTMATTER).`,
      });
    }
  } catch {
    // Best-effort; audit-log read failure shouldn't stop doctor.
  }

  // 3e. home_dir_in_worktree (v0.35.8.0). Walks up from `gbrainPath()`
  // looking for a `.git` directory OR file. If found, warns: `~/.gbrain/`
  // lives inside a git worktree, so an accidental `git add` from the
  // worktree root could stage the brain. Pairs with the retroactive
  // `~/.gbrain/.gitignore` (single-line `*`) laid down by saveConfig +
  // post-upgrade. Honest scope: the .gitignore covers casual `git add`
  // but NOT already-tracked files, screenshots, backups, or `git add -f`.
  //
  // Walk termination: stops at $HOME (don't keep walking into / on a user
  // who set GBRAIN_HOME=/tmp/something). Handles `.git` as both a directory
  // (main repo) and a file (linked worktree pointing at parent's worktrees/).
  // Honors GBRAIN_HOME via gbrainPath().
  try {
    const gbrainHome = gbrainPath();
    const home = process.env.HOME || '';
    let worktreeRoot: string | null = null;
    if (gbrainHome && home && gbrainHome.startsWith(home + '/')) {
      // Walk up from gbrainHome's parent toward $HOME, stopping at $HOME.
      // We don't check gbrainHome itself: a `.git` directly inside ~/.gbrain
      // isn't a containing-worktree, it would be a brain repo cloned there.
      let cur = dirname(gbrainHome);
      while (cur && cur.length >= home.length) {
        const gitPath = join(cur, '.git');
        try {
          const st = statSync(gitPath);
          // Either a directory (main repo) or a file (linked worktree pointer).
          if (st.isDirectory() || st.isFile()) {
            worktreeRoot = cur;
            break;
          }
        } catch {
          // No .git at this level; continue.
        }
        if (cur === home) break;
        const parent = dirname(cur);
        if (parent === cur) break;
        cur = parent;
      }
    }
    if (worktreeRoot) {
      const homeEnvHint = process.env.GBRAIN_HOME
        ? `# Or move \`~/.gbrain\` outside the worktree by setting GBRAIN_HOME elsewhere.`
        : `# Fix: \`export GBRAIN_HOME=/some/path/outside/the/worktree\` (gbrain appends \`.gbrain\`).`;
      checks.push({
        name: 'home_dir_in_worktree',
        status: 'warn',
        message:
          `~/.gbrain lives inside git worktree at ${worktreeRoot}. ` +
          `Config + brain DB could be committed by accident. ` +
          `A retroactive ~/.gbrain/.gitignore blocks casual \`git add\`, but does NOT cover ` +
          `already-tracked files, screenshots, backups, or \`git add -f\`. ${homeEnvHint}`,
      });
    } else {
      checks.push({
        name: 'home_dir_in_worktree',
        status: 'ok',
        message: 'gbrain home is outside any enclosing git worktree.',
      });
    }
  } catch {
    // Best-effort filesystem-hygiene check; never block doctor.
  }

  // 3b-multi-source. Multi-source drift (v0.31.8 — D8 + D17 + OV12 + OV13).
  // Pre-v0.30.3 putPage misrouted multi-source writes to (default, slug).
  // For each non-default source with local_path set, walk the FS and surface
  // slugs that exist at default but NOT at the intended source. Only runs
  // on multi-source brains (sources count > 1). Single-source brains skip.
  // Engine is nullable in runDoctor (--fast / DB-down skip the DB phase);
  // bail silently here when engine is null since the check needs DB access.
  if (engine !== null) try {
    const { findMisroutedPages } = await import('../core/multi-source-drift.ts');
    const sources = await engine!.executeRaw<{ id: string; local_path: string | null }>(
      `SELECT id, local_path FROM sources`,
    );
    const nonDefaultWithPath = sources.filter(s => s.id !== 'default' && s.local_path);
    if (sources.length > 1 && nonDefaultWithPath.length > 0) {
      const result = await findMisroutedPages(
        engine!,
        nonDefaultWithPath.map(s => ({ id: s.id, local_path: s.local_path as string })),
      );
      if (result.walk_truncated) {
        checks.push({
          name: 'multi_source_drift',
          status: 'warn',
          message:
            `Multi-source drift check skipped — FS walk hit limit/timeout. ` +
            `Re-run on a quieter brain or shorter walk via GBRAIN_DRIFT_LIMIT/GBRAIN_DRIFT_TIMEOUT_MS.`,
        });
      } else if (result.count > 0) {
        const sampleStr = result.sample.map(s => `${s.slug} (intended=${s.intended_source})`).join(', ');
        checks.push({
          name: 'multi_source_drift',
          status: 'warn',
          message:
            `${result.count} page slug(s) appear at 'default' but NOT at the intended source ` +
            `(e.g., ${sampleStr}). Two possible causes: (1) pre-v0.30.3 putPage misroutes; ` +
            `(2) source X never completed initial sync and the default page is unrelated. ` +
            `Verify with 'gbrain sources status', then either re-sync with ` +
            `'gbrain sync --source <id> --full' or 'gbrain delete <slug>' if the default-source ` +
            `row is the misroute. (A 'gbrain sources rehome' cleanup command is tracked for v0.32.0.)`,
        });
      } else {
        checks.push({
          name: 'multi_source_drift',
          status: 'ok',
          message: 'No cross-source slug drift detected.',
        });
      }
    }
  } catch {
    // Best-effort. A broken sources table or unreadable local_path should
    // not stop doctor. The walk itself catches per-directory errors; this
    // outer try covers the executeRaw path.
  }

  // 3c. Orphan clone temp dirs (v0.28 P1). `gbrain sources add --url` clones
  // into $GBRAIN_HOME/clones/.tmp/<id>-<rand>/ and renames atomically; if the
  // process is SIGKILL'd between clone-finish and rename, the temp dir
  // orphans. Surface entries older than 24h so operators notice before the
  // disk fills. The autopilot purge phase nukes these on its cadence; this
  // check just makes the state visible.
  try {
    const fs = await import('fs');
    const cfg = await import('../core/config.ts');
    const tmpRoot = cfg.gbrainPath('clones', '.tmp');
    if (fs.existsSync(tmpRoot)) {
      const STALE_MS = 24 * 3600 * 1000;
      const now = Date.now();
      const stale: { name: string; ageHours: number }[] = [];
      for (const ent of fs.readdirSync(tmpRoot, { withFileTypes: true })) {
        const full = join(tmpRoot, ent.name);
        try {
          const st = fs.lstatSync(full);
          const age = now - st.mtimeMs;
          if (age > STALE_MS) {
            stale.push({ name: ent.name, ageHours: Math.floor(age / 3600_000) });
          }
        } catch {
          /* skip unreadable */
        }
      }
      if (stale.length === 0) {
        checks.push({
          name: 'orphan_clones',
          status: 'ok',
          message: `No stale clone temp dirs in ${tmpRoot}.`,
        });
      } else {
        checks.push({
          name: 'orphan_clones',
          status: 'warn',
          message:
            `${stale.length} stale clone temp dir(s) in ${tmpRoot}: ` +
            stale.map(s => `${s.name} (${s.ageHours}h)`).join(', ') +
            `. Run \`gbrain sources purge-orphan-clones\` or wait for the autopilot purge phase.`,
        });
      }
    }
  } catch {
    // Filesystem read failure is non-fatal.
  }

  // --- DB checks (skip if --fast or no engine) ---

  if (fastMode || !engine) {
    if (!engine) {
      // Pick the precise message. When dbSource is provided, we know
      // whether a URL exists (env or config-file) — the caller simply
      // skipped the connection. When null, there really is no config
      // anywhere.
      let msg: string;
      if (fastMode && dbSource) {
        msg = `Skipping DB checks (--fast mode, URL present from ${dbSource})`;
      } else if (!fastMode && dbSource) {
        msg = `Could not connect to configured DB (URL from ${dbSource}); filesystem checks only`;
      } else {
        msg = 'No database configured (filesystem checks only). Set GBRAIN_DATABASE_URL or run `gbrain init`.';
      }
      checks.push({ name: 'connection', status: 'warn', message: msg });
    }
    const earlyFail1 = outputResults(checks, jsonOutput);
    process.exit(earlyFail1 ? 1 : 0);
    return;
  }

  // DB checks phase — start a single reporter phase so agents see which
  // check is running (several take seconds on 50K-page brains; without a
  // heartbeat the binary looks hung when stdout is piped).
  progress.start('doctor.db_checks');

  await runDoctorDbConnChecks(engine, checks, progress, jsonOutput);
  await runDoctorDbDataChecks(engine, checks, progress, args);

  progress.finish();

  const hasFail = outputResults(checks, jsonOutput);

  // Features teaser (non-JSON, non-failing only)
  if (!jsonOutput && !hasFail && engine) {
    try {
      const { featuresTeaserForDoctor } = await import('./features.ts');
      const teaser = await featuresTeaserForDoctor(engine);
      if (teaser) console.log(`\n${teaser}`);
    } catch { /* best-effort */ }
  }

  process.exit(hasFail ? 1 : 0);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Print the auto-fix report in human-readable form. JSON output goes through
 *  outputResults alongside the check list; this is the pretty-print path. */
function printAutoFixReport(report: AutoFixReport, dryRun: boolean, jsonOutput: boolean): void {
  if (jsonOutput) return; // JSON consumers read autoFixReport via the check issues / caller
  const verb = dryRun ? 'PROPOSED' : 'APPLIED';
  for (const outcome of report.fixed) {
    console.log(`[${verb}] ${outcome.skillPath} (${outcome.patternLabel})`);
    if (outcome.before) {
      console.log('--- before');
      console.log(outcome.before);
      console.log('--- after');
      console.log(outcome.after ?? '');
      console.log('');
    }
  }
  const n = report.fixed.length;
  const s = report.skipped.length;
  if (n === 0 && s === 0) {
    console.log('Doctor --fix: no DRY violations to repair.');
    return;
  }
  const label = dryRun ? 'fixes proposed' : 'fixes applied';
  console.log(`${n} ${label}${s > 0 ? `, ${s} skipped:` : '.'}`);
  for (const sk of report.skipped) {
    const hint = sk.reason === 'working_tree_dirty' ? ' (run `git stash` first)' : '';
    console.log(`  - ${sk.skillPath}: ${sk.reason}${hint}`);
  }
  if (dryRun && n > 0) console.log('\nRun without --dry-run to apply.');
}


/** Quick skill conformance check — frontmatter + required sections */
function checkSkillConformance(skillsDir: string): Check {
  const manifestPath = join(skillsDir, 'manifest.json');
  if (!existsSync(manifestPath)) {
    return { name: 'skill_conformance', status: 'warn', message: 'manifest.json not found' };
  }

  try {
    const manifest = JSON.parse(readFileSync(manifestPath, 'utf-8'));
    const skills = manifest.skills || [];
    let passing = 0;
    const failing: string[] = [];

    for (const skill of skills) {
      const skillPath = join(skillsDir, skill.path);
      if (!existsSync(skillPath)) {
        failing.push(`${skill.name}: file missing`);
        continue;
      }
      const content = readFileSync(skillPath, 'utf-8');
      // Check frontmatter exists
      if (!content.startsWith('---')) {
        failing.push(`${skill.name}: no frontmatter`);
        continue;
      }
      passing++;
    }

    if (failing.length === 0) {
      return { name: 'skill_conformance', status: 'ok', message: `${passing}/${skills.length} skills pass` };
    }
    return {
      name: 'skill_conformance',
      status: 'warn',
      message: `${passing}/${skills.length} pass. Failing: ${failing.join(', ')}`,
    };
  } catch {
    return { name: 'skill_conformance', status: 'warn', message: 'Could not parse manifest.json' };
  }
}

/**
 * v0.36.x skill_brain_first doctor check (supersedes PR #1206).
 *
 * Walks the skills manifest, runs the pure `analyzeSkillBrainFirst()`
 * helper on each, surfaces violators with structured issues[]. Snapshot-
 * diff against the previous run drives audit JSONL writes (transition-
 * only) — stable brains produce zero audit churn per doctor invocation.
 *
 * Exit shape:
 *   - 0 violators → status: 'ok', message: '<n> skills compliant or exempt'
 *   - any violator → status: 'warn', message + per-skill summary lines +
 *     formerly-EXEMPT_SKILLS hint when applicable (CMT1 replaces the
 *     dropped upgrade migration with a guided opt-in)
 *
 * Test seam: pure function, no `process.exit`. Direct call from tests
 * with a synthetic skills dir under tempdir.
 */
export function skillBrainFirstCheck(skillsDir: string): Check {
  let manifest: ReturnType<typeof loadOrDeriveManifest>;
  try {
    manifest = loadOrDeriveManifest(skillsDir);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return {
      name: 'skill_brain_first',
      status: 'warn',
      message: `Could not load skills manifest from ${skillsDir} (${msg})`,
    };
  }
  if (manifest.skills.length === 0) {
    return {
      name: 'skill_brain_first',
      status: 'ok',
      message: 'No skills found — skill_brain_first not applicable',
    };
  }

  const violators: BrainFirstAnalysis[] = [];
  const typoSkills: BrainFirstAnalysis[] = [];

  for (const entry of manifest.skills) {
    const skillPath = join(skillsDir, entry.path);
    if (!existsSync(skillPath)) continue; // resolver_health already reports
    let content: string;
    try {
      content = readFileSync(skillPath, 'utf-8');
    } catch {
      continue; // best-effort; permissions etc.
    }
    const fm = parseSkillFrontmatter(content);
    const result = analyzeSkillBrainFirst(content, entry.name, fm);
    if (result.typo_hint) typoSkills.push(result);
    if (result.status === 'warn') violators.push(result);
  }

  // --- Snapshot + diff audit (A2 contract) ---------------------------------
  // Best-effort: snapshot/audit failures don't poison the check result.
  const violatorSlugs = new Set(violators.map(v => v.skill));
  const patternsBySlug = new Map<string, string[]>();
  for (const v of violators) {
    patternsBySlug.set(v.skill, v.external_patterns_matched);
  }
  let priorSnapshotPresent = true;
  try {
    const snapshot = loadSnapshot();
    priorSnapshotPresent = snapshot.present;
    const diff = diffAgainstSnapshot(violatorSlugs, snapshot.violators);
    const doctorRunId = `${process.pid}-${Date.now()}`;
    if (snapshot.present) {
      // Steady-state path: write events only for transitions.
      appendAuditEventsForTransitions(diff, patternsBySlug, doctorRunId);
    } else {
      // First run / corrupt snapshot: bootstrap by writing one
      // `detected` line per current violator. This is the only path
      // that writes more than `diff.added.length` lines in a single
      // doctor invocation.
      const bootstrapDiff = { added: Array.from(violatorSlugs).sort(), removed: [], unchanged: [] };
      appendAuditEventsForTransitions(bootstrapDiff, patternsBySlug, doctorRunId);
    }
    writeSnapshotAtomically(violatorSlugs);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    process.stderr.write(`[gbrain] skill_brain_first audit step failed (${msg}); check continues\n`);
  }

  // --- Build the check result ---------------------------------------------
  if (violators.length === 0) {
    const typoNote = typoSkills.length > 0
      ? ` (note: ${typoSkills.length} skill(s) have brain_first typo hints: ${typoSkills.map(t => t.skill).join(', ')})`
      : '';
    return {
      name: 'skill_brain_first',
      status: 'ok',
      message: `${manifest.skills.length} skill(s) compliant or exempt${typoNote}`,
    };
  }

  // Sort for deterministic message + issues order.
  violators.sort((a, b) => a.skill.localeCompare(b.skill));

  const formerlyExempt = violators.filter(v => v.formerly_hardcoded_exempt);
  const summary: string[] = [];
  summary.push(
    `${violators.length} skill(s) do external lookups without a brain-first compliance signal. ` +
    `Fix via 'gbrain doctor --fix' (adds canonical Convention callout) ` +
    `or set 'brain_first: exempt' in skill frontmatter for genuine infra skills.`,
  );
  if (formerlyExempt.length > 0) {
    summary.push(
      `Of these, ${formerlyExempt.length} were hardcoded-exempt in PR #1206 (${formerlyExempt.map(v => v.skill).slice(0, 6).join(', ')}${formerlyExempt.length > 6 ? ', ...' : ''}). ` +
      `These need explicit opt-out now: run 'gbrain doctor --fix' to add the canonical callout, ` +
      `or add 'brain_first: exempt' to frontmatter for skills that genuinely shouldn't consult the brain.`,
    );
  }
  if (typoSkills.length > 0) {
    summary.push(
      `${typoSkills.length} skill(s) have brain_first typo hints: ` +
      typoSkills.slice(0, 6).map(t => `${t.skill} — ${t.typo_hint}`).join('; ') +
      (typoSkills.length > 6 ? '; ...' : ''),
    );
  }

  return {
    name: 'skill_brain_first',
    status: 'warn',
    message: summary.join(' '),
    issues: violators.map(v => ({
      type: 'skill_missing_brain_first',
      skill: v.skill,
      action: v.formerly_hardcoded_exempt
        ? `Add canonical Convention callout OR set 'brain_first: exempt' (was hardcoded-exempt in PR #1206)`
        : `Add canonical Convention callout OR set 'brain_first: exempt'`,
      fix: {
        kind: 'add-convention-callout',
        external_patterns: v.external_patterns_matched,
        typo_hint: v.typo_hint,
        formerly_hardcoded_exempt: v.formerly_hardcoded_exempt,
        summary_line: buildBrainFirstSummaryLine(v),
      },
    })),
  };
}


/**
 * `gbrain doctor --locks` — list idle-in-transaction backends older
 * than 5 minutes that could block DDL. Exits 0 on clean, 1 on blockers.
 *
 * Agents hitting a statement_timeout (SQLSTATE 57014) during migration
 * need a one-command path to find and kill the blocker. migrate.ts's
 * 57014 diagnostic references this flag by name; keep the two in sync.
 *
 * Postgres-only. PGLite has no pool, no idle-in-tx concept, so the
 * check prints a one-liner and exits 0.
 */
async function runLocksCheck(engine: BrainEngine | null, jsonOutput: boolean): Promise<void> {
  if (!engine) {
    if (jsonOutput) {
      console.log(JSON.stringify({ status: 'unavailable', reason: 'no_engine' }));
    } else {
      console.log('gbrain doctor --locks requires a database connection. Configure a URL and retry.');
    }
    process.exit(1);
  }

  if (engine.kind !== 'postgres') {
    if (jsonOutput) {
      console.log(JSON.stringify({ status: 'not_applicable', engine: engine.kind }));
    } else {
      console.log(`gbrain doctor --locks is Postgres-only. Current engine: ${engine.kind}. No blockers possible (no connection pool).`);
    }
    return;
  }

  const blockers = await getIdleBlockers(engine);

  if (jsonOutput) {
    console.log(JSON.stringify({ status: blockers.length === 0 ? 'ok' : 'blockers_found', blockers }, null, 2));
    if (blockers.length > 0) process.exit(1);
    return;
  }

  if (blockers.length === 0) {
    console.log('✓ No idle-in-transaction backends older than 5 minutes.');
    return;
  }

  console.log(`Found ${blockers.length} idle-in-transaction backend(s) older than 5 minutes:\n`);
  for (const b of blockers) {
    console.log(`  PID ${b.pid}  (idle since ${b.query_start})`);
    console.log(`    Query: ${b.query}`);
    console.log(`    Kill:  SELECT pg_terminate_backend(${b.pid});`);
    console.log('');
  }
  console.log('These connections may block ALTER TABLE DDL during migration.');
  console.log('After terminating, retry: gbrain apply-migrations --yes');
  process.exit(1);
}
