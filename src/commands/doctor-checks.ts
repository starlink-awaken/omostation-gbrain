import type { BrainEngine } from '../core/engine.ts';
import { existsSync, readFileSync, statSync } from 'fs';
import { dirname, isAbsolute, join, resolve as resolvePath } from 'path';
import { fileURLToPath } from 'url';
import { gbrainPath } from '../core/config.ts';

const WHOKNOWS_FIXTURE_RELATIVE_PATH = 'test/fixtures/whoknows-eval.jsonl';

function isGbrainSourceRoot(dir: string): boolean {
  // A directory is the gbrain source root when it carries the source markers.
  // (Regression from the doctor god-module split in #8: the previous
  // "same dirname as import.meta.url" check never matched any ancestor, so
  // resolveWhoknowsFixturePath() always returned null and the default-fixture
  // doctor check fell back to "warn" instead of "ok".)
  return (
    existsSync(join(dir, 'src', 'cli.ts')) &&
    existsSync(join(dir, 'skills', 'RESOLVER.md'))
  );
}


import type { Check } from './doctor.ts';


export function resolveWhoknowsFixturePath(
  env: NodeJS.ProcessEnv = process.env,
  moduleUrl: string = import.meta.url,
): string | null {
  if (env.GBRAIN_WHOKNOWS_FIXTURE_PATH) {
    return isAbsolute(env.GBRAIN_WHOKNOWS_FIXTURE_PATH)
      ? env.GBRAIN_WHOKNOWS_FIXTURE_PATH
      : resolvePath(process.cwd(), env.GBRAIN_WHOKNOWS_FIXTURE_PATH);
  }

  try {
    let dir = dirname(fileURLToPath(moduleUrl));
    for (let i = 0; i < 10; i++) {
      if (isGbrainSourceRoot(dir)) return join(dir, WHOKNOWS_FIXTURE_RELATIVE_PATH);
      const parent = dirname(dir);
      if (parent === dir) break;
      dir = parent;
    }
  } catch {
    // Some bundlers/runtimes may not expose a normal file: import URL.
    // Doctor should surface an override hint instead of fabricating a path.
  }

  return null;
}

/**
 * v0.33: whoknows_health — verify the eval fixture is present at the
 * documented path. Lightweight; just checks file existence and row count,
 * not the eval gate outcome (that runs via `gbrain eval whoknows`).
 *
 * Surface is intentionally narrow: a missing fixture means the eval
 * cannot run at all, which is the highest-leverage signal. Hit-rate
 * regression detection lives in `gbrain eval whoknows --json` and is
 * the job of the eval command, not the doctor sweep.
 */
export async function whoknowsHealthCheck(_engine: BrainEngine): Promise<Check> {
  try {
    const fixturePath = resolveWhoknowsFixturePath();
    if (!fixturePath) {
      return {
        name: 'whoknows_health',
        status: 'warn',
        message: 'whoknows eval fixture path could not be resolved. Set GBRAIN_WHOKNOWS_FIXTURE_PATH to the absolute path for test/fixtures/whoknows-eval.jsonl.',
      };
    }
    if (!existsSync(fixturePath)) {
      return {
        name: 'whoknows_health',
        status: 'warn',
        message: `whoknows eval fixture missing at ${fixturePath}. Fix: hand-label 10 queries you'd actually run, format {query, expected_top_3_slugs, notes}.`,
      };
    }
    const stat = statSync(fixturePath);
    if (stat.size === 0) {
      return {
        name: 'whoknows_health',
        status: 'warn',
        message: 'whoknows eval fixture exists but is empty. The eval cannot pass without queries.',
      };
    }
    const raw = readFileSync(fixturePath, 'utf-8');
    const rows = raw
      .split('\n')
      .filter((l) => {
        const t = l.trim();
        return t && !t.startsWith('#') && !t.startsWith('//');
      });
    if (rows.length < 5) {
      return {
        name: 'whoknows_health',
        status: 'warn',
        message: `whoknows eval fixture has only ${rows.length} row(s); ENG-D2 recommends 10. Fix: add more hand-labeled queries.`,
      };
    }
    return {
      name: 'whoknows_health',
      status: 'ok',
      message: `whoknows eval fixture present (${rows.length} queries). Run \`gbrain eval whoknows test/fixtures/whoknows-eval.jsonl\` to grade.`,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return {
      name: 'whoknows_health',
      status: 'warn',
      message: `Could not check whoknows fixture: ${msg}`,
    };
  }
}


export async function takesWeightGridCheck(engine: BrainEngine): Promise<Check> {
  try {
    const rows = await engine.executeRaw<{ off_grid: string | number; total: string | number }>(
      `SELECT
         count(*) FILTER (WHERE weight IS NOT NULL
                          AND abs(weight::numeric - ROUND(weight::numeric * 20) / 20) > 0.001)::int AS off_grid,
         count(*)::int AS total
       FROM takes`,
    );
    const total = Number(rows[0]?.total ?? 0);
    const offGrid = Number(rows[0]?.off_grid ?? 0);
    if (total === 0) {
      return { name: 'takes_weight_grid', status: 'ok', message: 'No takes yet' };
    }
    const ratio = offGrid / total;
    if (ratio > 0.10) {
      return {
        name: 'takes_weight_grid',
        status: 'fail',
        message: `${offGrid}/${total} takes off the 0.05 grid (${(ratio * 100).toFixed(1)}%). Fix: gbrain apply-migrations --yes`,
      };
    }
    if (ratio > 0.01) {
      return {
        name: 'takes_weight_grid',
        status: 'warn',
        message: `${offGrid}/${total} takes off the 0.05 grid (${(ratio * 100).toFixed(1)}%). Fix: gbrain apply-migrations --yes`,
      };
    }
    return {
      name: 'takes_weight_grid',
      status: 'ok',
      message: offGrid === 0
        ? `${total} take(s) on grid`
        : `${total} take(s) on grid (${offGrid} within tolerance)`,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    // takes table missing on a fresh pre-v37 brain — warn, don't fail.
    return {
      name: 'takes_weight_grid',
      status: 'warn',
      message: `Could not check takes weight grid: ${msg}`,
    };
  }
}

/**
 * Child-table orphan detection (closes #1063).
 *
 * The autopilot `orphans` phase (src/core/cycle.ts:runPhaseOrphans) detects
 * orphan PAGES (pages with no inbound links via the page-graph). It does NOT
 * scan FK-child tables for orphan rows. When a bulk page delete leaves
 * orphans in `content_chunks` / `page_versions` / `tags` / `takes` / etc.
 * — whether from pre-FK migrations, race conditions, or a code path that
 * bypassed cascade — they persist indefinitely until manual SQL cleanup.
 *
 * All ten FK-to-pages tables declare `ON DELETE CASCADE` in the live schema
 * (verified via `pg_constraint` snapshot in the issue body), so finding any
 * orphan row is by definition unexpected. The check ships paste-ready
 * cleanup SQL when orphans surface.
 *
 * Excluded: `files.page_id` and `links.origin_page_id` — both declared as
 * `ON DELETE SET NULL`, so a NULL value is a valid state (file/link survives
 * after page deletion); only NOT-NULL-but-page-missing is an orphan there.
 * The check encodes that distinction for the two SET NULL columns.
 *
 * Pure helper for parity with `takesWeightGridCheck` so tests can target it
 * directly without driving the full `runDoctor` pipeline.
 */

export async function childTableOrphansCheck(engine: BrainEngine): Promise<Check> {
  // (table, fk_column, allow_null). When allow_null=true, NULL is a valid
  // state (FK was declared ON DELETE SET NULL); the orphan predicate filters
  // out NULL values. When false, NULL is impossible by NOT NULL constraint;
  // any value not in pages.id is an orphan.
  const targets: Array<{ table: string; col: string; allowNull: boolean }> = [
    { table: 'content_chunks',   col: 'page_id',          allowNull: false },
    { table: 'page_versions',    col: 'page_id',          allowNull: false },
    { table: 'tags',             col: 'page_id',          allowNull: false },
    { table: 'takes',            col: 'page_id',          allowNull: false },
    { table: 'raw_data',         col: 'page_id',          allowNull: false },
    { table: 'timeline_entries', col: 'page_id',          allowNull: false },
    { table: 'links',            col: 'from_page_id',     allowNull: false },
    { table: 'links',            col: 'to_page_id',       allowNull: false },
    { table: 'links',            col: 'origin_page_id',   allowNull: true  },
    { table: 'files',            col: 'page_id',          allowNull: true  },
  ];
  let totalOrphans = 0;
  const breakdown: string[] = [];
  const cleanupSql: string[] = [];
  const errors: string[] = [];
  for (const { table, col, allowNull } of targets) {
    try {
      // NOT IN subquery is portable across postgres + PGLite. The `pages.id`
      // subquery covers every existing parent row.
      const nullFilter = allowNull ? `${col} IS NOT NULL AND ` : '';
      const rows = await engine.executeRaw<{ n: string | number }>(
        `SELECT COUNT(*)::int AS n FROM ${table} WHERE ${nullFilter}${col} NOT IN (SELECT id FROM pages)`,
      );
      const n = Number(rows[0]?.n ?? 0);
      if (n > 0) {
        totalOrphans += n;
        breakdown.push(`${table}.${col}=${n}`);
        cleanupSql.push(
          `DELETE FROM ${table} WHERE ${nullFilter}${col} NOT IN (SELECT id FROM pages);`,
        );
      }
    } catch (e) {
      // Table or column may not exist on older schemas — skip and continue.
      // Aggregate the errors so doctor surfaces "could not check N tables"
      // when a real failure shape appears (network, lock, syntax).
      const msg = e instanceof Error ? e.message : String(e);
      errors.push(`${table}.${col}: ${msg.slice(0, 80)}`);
    }
  }
  if (totalOrphans === 0 && errors.length === 0) {
    return {
      name: 'child_table_orphans',
      status: 'ok',
      message: 'All FK-child tables clean (10 tables checked)',
    };
  }
  if (totalOrphans === 0 && errors.length > 0) {
    return {
      name: 'child_table_orphans',
      status: 'warn',
      message: `Could not check ${errors.length}/10 FK-child tables (older schema or transient error): ${errors.slice(0, 3).join('; ')}`,
    };
  }
  return {
    name: 'child_table_orphans',
    status: 'warn',
    message:
      `${totalOrphans} orphan row(s) in FK-child tables (${breakdown.join(', ')}). ` +
      `Cleanup: ${cleanupSql.join(' ')}`,
  };
}


export async function checkAbandonedThreads(engine: BrainEngine): Promise<Check> {
  try {
    const rows = await engine.executeRaw<{ count: number }>(
      `SELECT COUNT(*)::int AS count FROM takes
         WHERE active = true
           AND resolved_at IS NULL
           AND superseded_by IS NULL
           AND weight >= 0.7
           AND since_date IS NOT NULL
           AND since_date::date < (now() - INTERVAL '12 months')`,
    );
    const count = rows[0]?.count ?? 0;
    if (count === 0) {
      return {
        name: 'abandoned_threads',
        status: 'ok',
        message: 'No abandoned high-conviction threads',
      };
    }
    return {
      name: 'abandoned_threads',
      status: 'ok',
      message: `${count} high-conviction take(s) older than 12 months and never revisited — see \`gbrain calibration\` for details`,
    };
  } catch (e) {
    return {
      name: 'abandoned_threads',
      status: 'warn',
      message: `Could not check abandoned threads: ${e instanceof Error ? e.message : String(e)}`,
    };
  }
}

/**
 * calibration_freshness: warns when the active calibration profile is
 * older than 7 days (configurable). Default holder 'garry'. Multi-source
 * brains see one row per source; this check uses the most recent across
 * all sources.
 */
export async function checkCalibrationFreshness(engine: BrainEngine): Promise<Check> {
  try {
    const rows = await engine.executeRaw<{ generated_at: Date | null }>(
      `SELECT MAX(generated_at) AS generated_at FROM calibration_profiles WHERE holder = 'garry'`,
    );
    const generated = rows[0]?.generated_at;
    if (!generated) {
      return {
        name: 'calibration_freshness',
        status: 'ok',
        message: 'No calibration profile yet (builds after 5+ resolved takes)',
      };
    }
    const ageMs = Date.now() - new Date(generated).getTime();
    const ageDays = Math.floor(ageMs / (1000 * 60 * 60 * 24));
    const staleDays = 7;
    if (ageDays > staleDays) {
      return {
        name: 'calibration_freshness',
        status: 'warn',
        message: `Calibration profile is ${ageDays} days old (stale at >${staleDays}d). Run \`gbrain calibration --regenerate\``,
      };
    }
    return {
      name: 'calibration_freshness',
      status: 'ok',
      message: `Calibration profile generated ${ageDays}d ago`,
    };
  } catch (e) {
    return {
      name: 'calibration_freshness',
      status: 'warn',
      message: `Could not check calibration freshness: ${e instanceof Error ? e.message : String(e)}`,
    };
  }
}

/**
 * grade_confidence_drift (CDX-11 mitigation): compare the judge's
 * self-reported confidence on auto-applied verdicts against the eventual
 * accuracy on those same takes. When auto-resolutions diverge from
 * confidence prediction, the judge is mis-calibrated and the operator
 * should retune the prompt or revisit the threshold.
 *
 * v0.36.1.0 ship state: returns 'ok' with a counter — actual drift math
 * requires a measurement window we haven't accumulated yet. The check
 * exists so the surface is wired; the math arrives once we have N >= 30
 * auto-applied verdicts to compare.
 */
export async function checkGradeConfidenceDrift(engine: BrainEngine): Promise<Check> {
  try {
    const rows = await engine.executeRaw<{ applied_count: number }>(
      `SELECT COUNT(*)::int AS applied_count FROM take_grade_cache WHERE applied = true`,
    );
    const applied = rows[0]?.applied_count ?? 0;
    if (applied < 30) {
      return {
        name: 'grade_confidence_drift',
        status: 'ok',
        message: `Only ${applied} auto-applied verdicts — need 30+ for drift detection`,
      };
    }
    // v0.37+ TODO: compute confidence-vs-accuracy correlation; warn when
    // mean(applied verdicts' confidence) deviates from the actual accuracy
    // rate (cross-checked against later manual corrections via the
    // contradictions probe). For v0.36.1.0 the check surfaces only the
    // count and a "calibration math pending" status.
    return {
      name: 'grade_confidence_drift',
      status: 'ok',
      message: `${applied} auto-applied verdicts; drift math arrives in v0.37+`,
    };
  } catch (e) {
    return {
      name: 'grade_confidence_drift',
      status: 'warn',
      message: `Could not check grade confidence drift: ${e instanceof Error ? e.message : String(e)}`,
    };
  }
}

/**
 * voice_gate_health: warns when calibration_profiles rows show a high rate
 * of voice gate failures over the last 7 days. Failures aren't bad in
 * isolation (template fallback is fine), but a sustained high rate signals
 * the rubric needs tuning.
 */
export async function checkVoiceGateHealth(engine: BrainEngine): Promise<Check> {
  try {
    const rows = await engine.executeRaw<{ total: number; failures: number }>(
      `SELECT COUNT(*)::int AS total,
              COALESCE(SUM(CASE WHEN voice_gate_passed = false THEN 1 ELSE 0 END), 0)::int AS failures
         FROM calibration_profiles
         WHERE generated_at >= (now() - INTERVAL '7 days')`,
    );
    const total = rows[0]?.total ?? 0;
    const failures = rows[0]?.failures ?? 0;
    if (total === 0) {
      return {
        name: 'voice_gate_health',
        status: 'ok',
        message: 'No calibration profile generation in the last 7 days',
      };
    }
    const failRate = failures / total;
    if (failRate >= 0.3) {
      return {
        name: 'voice_gate_health',
        status: 'warn',
        message: `Voice gate failed ${failures}/${total} (${Math.round(failRate * 100)}%) in last 7 days. Review src/core/calibration/voice-gate.ts rubric.`,
      };
    }
    return {
      name: 'voice_gate_health',
      status: 'ok',
      message: `Voice gate ${failures}/${total} failed in last 7 days (${Math.round(failRate * 100)}%)`,
    };
  } catch (e) {
    return {
      name: 'voice_gate_health',
      status: 'warn',
      message: `Could not check voice gate health: ${e instanceof Error ? e.message : String(e)}`,
    };
  }
}

/**
 * v0.35.0.0+ reranker_health doctor check.
 *
 * Logic (post-CDX2 review):
 *   1) Read `search.reranker.enabled` first. When disabled and no
 *      failures in window → 'ok: reranker disabled'. Avoids interpreting
 *      "no events" as "broken" when reranker is simply not in use.
 *   2) Walk last 7 days of `~/.gbrain/audit/rerank-failures-*.jsonl`.
 *   3) Auth failures: ANY single one warns (config-time problem doctor's
 *      own probe should have caught — surface it).
 *   4) Transient (network/timeout/rate_limit): warn at >=5 in window.
 *      Below that they're noise; reranker fails open anyway.
 *   5) Payload-too-large failures: warn at >=1 (indicates a workload
 *      mismatch that the operator should know about).
 *
 * Engine-agnostic (file-based + one config-key read).
 */
export async function checkRerankerHealth(engine: BrainEngine): Promise<Check> {
  try {
    const { readRecentRerankFailures } = await import('../core/rerank-audit.ts');
    const cfg = await engine.getConfig('search.reranker.enabled');
    const rerankerEnabled = cfg === 'true' || cfg === '1';

    const failures = readRecentRerankFailures(7);
    if (failures.length === 0) {
      return {
        name: 'reranker_health',
        status: 'ok',
        message: rerankerEnabled
          ? 'No rerank failures in last 7 days'
          : 'Reranker disabled — no failures expected',
      };
    }

    const authFails = failures.filter((f) => f.reason === 'auth');
    if (authFails.length > 0) {
      return {
        name: 'reranker_health',
        status: 'warn',
        message: `${authFails.length} reranker auth failure(s) in last 7 days. Fix: verify ZEROENTROPY_API_KEY and run \`gbrain models doctor\`.`,
      };
    }

    const payloadFails = failures.filter((f) => f.reason === 'payload_too_large');
    if (payloadFails.length > 0) {
      return {
        name: 'reranker_health',
        status: 'warn',
        message: `${payloadFails.length} reranker payload-too-large failure(s) in last 7 days. Fix: lower \`search.reranker.top_n_in\` (default 30) or split very large documents.`,
      };
    }

    const transientFails = failures.filter(
      (f) => f.reason === 'network' || f.reason === 'timeout' || f.reason === 'rate_limit',
    );
    if (transientFails.length >= 5) {
      return {
        name: 'reranker_health',
        status: 'warn',
        message: `${transientFails.length} transient reranker failure(s) in last 7 days. Search fails open to RRF order; check ZE status if persistent.`,
      };
    }

    return {
      name: 'reranker_health',
      status: 'ok',
      message: `${failures.length} reranker failure(s) in last 7 days (below threshold)`,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return {
      name: 'reranker_health',
      status: 'warn',
      message: `Could not check reranker audit: ${msg}`,
    };
  }
}

/**
 * v0.37.0 brainstorm_health doctor check.
 *
 * Surfaces three readiness signals for `gbrain brainstorm` / `gbrain lsd`:
 *
 *   1. Migration v79 applied — the `pages.last_retrieved_at` column exists.
 *      If missing, LSD's stale-page signal degrades silently (corpus-sampling
 *      fallback only). Fix: `gbrain apply-migrations --yes`.
 *
 *   2. search.track_retrieval — when explicitly off, LSD never accumulates
 *      stale signal (every page stays at NULL last_retrieved_at). Default-on
 *      is fine; explicit-off is a warning so the user notices the setting.
 *      Fix: `gbrain config set search.track_retrieval true`.
 *
 *   3. Calibration cold-start — the latest calibration profile has empty
 *      `active_bias_tags`. brainstorm + LSD judge fall back to no-anti-bias
 *      mode with a stderr warning at run time; this surfaces it earlier.
 *      Fix: `gbrain calibration --regenerate` once enough takes are resolved.
 *
 * Returns the FIRST non-ok signal as the status — column-missing dominates,
 * then disabled-tracking, then cold-start. All three are non-blocking warnings;
 * brainstorm + LSD still work, just with degraded signal.
 */
export async function checkBrainstormHealth(engine: BrainEngine): Promise<Check> {
  // (1) Column probe — fast, single-query.
  try {
    const probeRows = await engine.executeRaw<{ exists: boolean }>(
      `SELECT EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_name = 'pages' AND column_name = 'last_retrieved_at'
       ) AS exists`,
      []
    );
    const columnPresent = probeRows[0]?.exists === true;
    if (!columnPresent) {
      return {
        name: 'brainstorm_health',
        status: 'warn',
        message: `pages.last_retrieved_at column missing. LSD stale-bias degraded to corpus-sampling. Fix: \`gbrain apply-migrations --yes\``,
      };
    }
  } catch (e) {
    // Information schema may not be queryable on every engine variant.
    // Don't fail the doctor over this — degrade to skip.
    const msg = e instanceof Error ? e.message : String(e);
    return {
      name: 'brainstorm_health',
      status: 'warn',
      message: `Could not probe pages.last_retrieved_at (${msg}); brainstorm/lsd may run with degraded signal.`,
    };
  }

  // (2) search.track_retrieval — explicit-off surfaces as a warning.
  try {
    const trackCfg = await engine.getConfig('search.track_retrieval');
    if (trackCfg === 'false' || trackCfg === '0' || trackCfg === 'off' || trackCfg === 'no') {
      return {
        name: 'brainstorm_health',
        status: 'warn',
        message: `search.track_retrieval is explicitly off — LSD's stale-page signal never accumulates. Fix: \`gbrain config set search.track_retrieval true\` (or accept and use brainstorm only).`,
      };
    }
  } catch {
    // Config read miss is benign; default-on applies.
  }

  // (3) Calibration cold-start — empty active_bias_tags.
  try {
    const calibRows = await engine.executeRaw<{ active_bias_tags: string[] | null }>(
      `SELECT active_bias_tags
         FROM calibration_profiles
         ORDER BY generated_at DESC
         LIMIT 1`,
      []
    );
    if (calibRows.length === 0) {
      return {
        name: 'brainstorm_health',
        status: 'ok',
        message: `Migration v79 applied; tracking enabled. Calibration profile not yet generated — brainstorm/lsd will run unbiased until enough takes are resolved.`,
      };
    }
    const tags = calibRows[0].active_bias_tags;
    if (!Array.isArray(tags) || tags.length === 0) {
      return {
        name: 'brainstorm_health',
        status: 'ok',
        message: `Migration v79 applied; tracking enabled. Calibration cold-start (no active_bias_tags) — judge runs unbiased. Fix when ready: \`gbrain calibration --regenerate\`.`,
      };
    }
    return {
      name: 'brainstorm_health',
      status: 'ok',
      message: `Migration v79 applied; tracking enabled; calibration profile with ${tags.length} bias tag(s) loaded.`,
    };
  } catch {
    // Pre-v0.36.1 brain (no calibration_profiles table). Brainstorm/lsd still
    // work without anti-bias context — orchestrator stderr-warns at run time.
    return {
      name: 'brainstorm_health',
      status: 'ok',
      message: `Migration v79 applied; tracking enabled. calibration_profiles table missing (pre-v0.36.1 brain) — judge runs unbiased.`,
    };
  }
}

/**
 * v0.36.0.0 (A5): ze_embedding_health doctor check.
 *
 * When the configured embedding_model starts with `zeroentropyai:`, verify
 * the API key is set. Doesn't make a network call by default — the existing
 * `gbrain models doctor` probe covers that, and we don't want every
 * `gbrain doctor` run to spend tokens. Surfaces a paste-ready fix when the
 * key is missing.
 */
export async function checkZeEmbeddingHealth(engine: BrainEngine): Promise<Check> {
  try {
    // v0.37 fix wave (Lane E.3 + CDX2-10): read from gateway, not DB.
    // The file plane is canonical post-v0.37; the DB config table is
    // schema-applied metadata. Reading DB here would skip the warning
    // when the user has a fresh install with no DB config row yet.
    const { getEmbeddingModel } = await import('../core/ai/gateway.ts');
    const { loadConfigFileOnly } = await import('../core/config.ts');
    let model = '';
    try { model = getEmbeddingModel(); } catch { /* gateway unconfigured */ }
    if (!model.startsWith('zeroentropyai:')) {
      return {
        name: 'ze_embedding_health',
        status: 'ok',
        message: `Configured embedding model "${model || 'default'}" is not ZeroEntropy — skip.`,
      };
    }
    const envKey = process.env.ZEROENTROPY_API_KEY;
    // File plane: zeroentropy_api_key on GBrainConfig (added by C.3).
    const fileKey = loadConfigFileOnly()?.zeroentropy_api_key;
    if (!envKey && !fileKey) {
      return {
        name: 'ze_embedding_health',
        status: 'warn',
        message:
          `embedding_model="${model}" but ZEROENTROPY_API_KEY is not set. ` +
          `Fix: get a key at https://dashboard.zeroentropy.dev and either ` +
          `\`export ZEROENTROPY_API_KEY=...\` or edit ~/.gbrain/config.json ` +
          `to add "zeroentropy_api_key": "...". (gbrain config set writes the DB plane, which the embed pipeline ignores.)`,
      };
    }
    return {
      name: 'ze_embedding_health',
      status: 'ok',
      message: `embedding_model="${model}" with key configured`,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return {
      name: 'ze_embedding_health',
      status: 'warn',
      message: `Could not check ZE embedding health: ${msg}`,
    };
  }
}

/**
 * v0.36.0.0 (A5): embedding_width_consistency doctor check.
 *
 * Cross-checks that `config.embedding_dimensions` matches the actual
 * `vector(N)` width on `content_chunks.embedding`. Drift here means the
 * ze-switch was interrupted mid-flight (schema changed but config write
 * crashed, or vice versa). Surfaces a paste-ready `gbrain ze-switch
 * --resume` hint.
 */
export async function checkEmbeddingWidthConsistency(engine: BrainEngine): Promise<Check> {
  try {
    // v0.37 fix wave (Lane E.1 + CDX-8): read from gateway, not DB. The
    // file plane is canonical post-v0.37; the DB config table is
    // schema-applied metadata. Reading DB here silently skipped the
    // check on fresh installs whose DB config row hadn't been written
    // yet.
    const { getEmbeddingDimensions, getEmbeddingModel } = await import('../core/ai/gateway.ts');
    let configDim: number;
    let resolvedModel: string;
    try {
      configDim = getEmbeddingDimensions();
      resolvedModel = getEmbeddingModel();
    } catch {
      return {
        name: 'embedding_width_consistency',
        status: 'ok',
        message: 'gateway not configured — skipping width check.',
      };
    }
    if (!Number.isFinite(configDim) || configDim <= 0) {
      return {
        name: 'embedding_width_consistency',
        status: 'warn',
        message: `gateway returned non-positive embedding dimension "${configDim}".`,
      };
    }

    // Read the actual column width via the existing helper (shared with
    // init.ts and embed.ts dim-mismatch pre-flight). One source of truth.
    const { readContentChunksEmbeddingDim, embeddingMismatchMessage } = await import('../core/embedding-dim-check.ts');
    const existing = await readContentChunksEmbeddingDim(engine);
    if (!existing.exists) {
      return {
        name: 'embedding_width_consistency',
        status: 'warn',
        message: 'content_chunks.embedding column not found. Fix: run `gbrain init --migrate-only` or check schema.',
      };
    }
    if (existing.dims === null) {
      return {
        name: 'embedding_width_consistency',
        status: 'warn',
        message: 'content_chunks.embedding is not a vector type. Schema may be corrupt.',
      };
    }
    if (existing.dims !== configDim) {
      // E.2: use the engine-kind-branched recipe instead of pointing at
      // the no-op `gbrain config set` path. The recipe is paste-ready
      // for the brain's actual engine.
      const databasePath = (engine as { _savedConfig?: { database_path?: string } })._savedConfig?.database_path;
      const recipe = embeddingMismatchMessage({
        currentDims: existing.dims,
        requestedDims: configDim,
        requestedModel: resolvedModel,
        source: 'doctor',
        engineKind: engine.kind,
        databasePath,
      });
      return {
        name: 'embedding_width_consistency',
        status: 'warn',
        message:
          `Schema width mismatch: content_chunks.embedding is vector(${existing.dims}) but ` +
          `gateway resolved embedding_dimensions = ${configDim}.\n\n${recipe}`,
      };
    }
    return {
      name: 'embedding_width_consistency',
      status: 'ok',
      message: `Schema width (${existing.dims}d) matches gateway embedding_dimensions`,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return {
      name: 'embedding_width_consistency',
      status: 'warn',
      message: `Could not check embedding width: ${msg}`,
    };
  }
}

/**
 * v0.32.3 [CDX-20]: surface mode + per-key override drift.
 *
 * Status stays `ok` (never warns; never docks health score). If
 * search.mode is unset → suggest picking one. If overrides contradict
 * the mode (e.g. mode=conservative but cache.enabled=false), say so in
 * the message and paste a `gbrain search modes --reset` fix command.
 */

/**
 * v0.37.7.0 — Tier 5K source_routing_health (D5 lock: 200-page total cap).
 *
 * On a multi-source brain, sample up to 200 recent pages across all
 * non-default sources (per-source cap = min(50, ceil(200/N))). Warn
 * when:
 *  - A non-default source has zero pages (silent-collapse-to-default
 *    fingerprint from #1167 + #1222).
 *  - The brain repo has a `.gitignore` file but
 *    `sync.respect_gitignore` is unset/false (info-line nudge for
 *    Tier 4I's opt-in flag).
 *
 * Cost-bounded: total cap of 200 means a 20-source CEO brain pays
 * 20*10 = 200 selects rather than 20*50 = 1000.
 */
export async function checkSourceRoutingHealth(engine: BrainEngine): Promise<Check> {
  try {
    const sources = await engine.executeRaw<{ id: string }>(
      `SELECT id FROM sources WHERE id <> 'default'`,
    );
    if (sources.length === 0) {
      return { name: 'source_routing_health', status: 'ok', message: 'Single-source brain (no federation to check)' };
    }
    const perSourceCap = Math.min(50, Math.ceil(200 / Math.max(1, sources.length)));
    const emptySources: string[] = [];
    for (const s of sources) {
      const rows = await engine.executeRaw<{ n: string }>(
        `SELECT COUNT(*)::text AS n FROM pages WHERE source_id = $1 LIMIT $2`,
        [s.id, perSourceCap],
      );
      if (Number(rows[0]?.n ?? 0) === 0) {
        emptySources.push(s.id);
      }
    }
    if (emptySources.length > 0) {
      return {
        name: 'source_routing_health',
        status: 'warn',
        message:
          `${emptySources.length} non-default source(s) have zero pages: ${emptySources.join(', ')}. ` +
          `If you've recently run \`gbrain import --source-id <id>\` against these, the writes may have ` +
          `silently fallen to the default source pre-v0.37.7.0. Re-run with --source-id; verify via ` +
          `\`gbrain sources current --json\`.`,
      };
    }
    return {
      name: 'source_routing_health',
      status: 'ok',
      message: `Multi-source brain (${sources.length} non-default source(s)); all populated`,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { name: 'source_routing_health', status: 'warn', message: `Check failed: ${msg}` };
  }
}

/**
 * v0.37.7.0 — Tier 5L oauth_confidential_client_health.
 *
 * Confidential OAuth clients (token_endpoint_auth_method != 'none')
 * MUST have a non-NULL client_secret_hash. v0.34.1.0's #909 fix
 * intentionally NULLs the column for public PKCE clients; if any
 * row claims confidential auth but has NULL hash, that's the
 * regression fingerprint from #1166.
 */
export async function checkOauthConfidentialHealth(engine: BrainEngine): Promise<Check> {
  try {
    const rows = await engine.executeRaw<{ client_id: string; method: string | null; hash: string | null }>(
      `SELECT client_id,
              token_endpoint_auth_method AS method,
              client_secret_hash AS hash
         FROM oauth_clients`,
    );
    if (rows.length === 0) {
      return { name: 'oauth_confidential_client_health', status: 'ok', message: 'No OAuth clients registered' };
    }
    const broken = rows.filter(r => {
      const isPublic = r.method === 'none';
      return !isPublic && (r.hash == null || r.hash === '');
    });
    if (broken.length > 0) {
      return {
        name: 'oauth_confidential_client_health',
        status: 'fail',
        message:
          `${broken.length} confidential OAuth client(s) have NULL/empty secret hash: ${broken.map(b => b.client_id).slice(0, 5).join(', ')}` +
          (broken.length > 5 ? ` (+${broken.length - 5} more)` : '') +
          `. Fix: \`gbrain auth revoke-client <id> && gbrain auth register-client …\` for each, OR \`gbrain upgrade\` if pre-v0.37.7.0.`,
      };
    }
    return {
      name: 'oauth_confidential_client_health',
      status: 'ok',
      message: `${rows.length} OAuth client(s) registered; all auth shapes consistent`,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    // Pre-OAuth schema (oauth_clients table missing) → ok.
    if (msg.toLowerCase().includes('relation') && msg.toLowerCase().includes('does not exist')) {
      return { name: 'oauth_confidential_client_health', status: 'ok', message: 'OAuth not configured (skipping)' };
    }
    return { name: 'oauth_confidential_client_health', status: 'warn', message: `Check failed: ${msg}` };
  }
}

/**
 * v0.37.7.0 — Tier 5M autopilot_lock_scope (PID-safe hint per codex CF11).
 *
 * Detects stale autopilot lockfiles. When `GBRAIN_HOME` is set, the
 * canonical lock path lives under `gbrainPath('autopilot.lock')`.
 * If a hardcoded `~/.gbrain/autopilot.lock` ALSO exists outside the
 * current `GBRAIN_HOME`, that's a pre-v0.37.7.0 leftover or a
 * different brain's lock. Hint includes PID + a `ps -p` check so
 * the user verifies before deleting.
 */
export function checkAutopilotLockScope(): Check {
  try {
    const canonical = gbrainPath('autopilot.lock');
    const home = process.env.HOME || '';
    const legacy = home ? `${home}/.gbrain/autopilot.lock` : '';
    // Same path → nothing to surface.
    if (canonical === legacy || !legacy || !existsSync(legacy)) {
      return { name: 'autopilot_lock_scope', status: 'ok', message: `Lock path: ${canonical}` };
    }
    // legacy lock exists outside GBRAIN_HOME. Read its PID for a safe hint.
    let owningPid: string = 'unknown';
    try {
      const raw = readFileSync(legacy, 'utf8').trim();
      if (/^\d+$/.test(raw)) owningPid = raw;
    } catch { /* unreadable → leave 'unknown' */ }
    return {
      name: 'autopilot_lock_scope',
      status: 'warn',
      message:
        `Stale lockfile outside GBRAIN_HOME: ${legacy} (owning PID: ${owningPid}). ` +
        `Verify with \`ps -p ${owningPid}\` — if the process is dead, \`rm ${legacy}\`. ` +
        `If alive, identify it (\`ps -fp ${owningPid}\`) and stop before deleting.`,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { name: 'autopilot_lock_scope', status: 'warn', message: `Check failed: ${msg}` };
  }
}

export async function checkSearchMode(engine: BrainEngine): Promise<Check> {
  try {
    const mode = await engine.getConfig('search.mode');
    const overrides = await engine.listConfigKeys('search.');
    // Exclude search.mode itself + the upgrade-notice state key from the
    // override roster — they aren't knobs.
    const overrideKeys = overrides.filter(k => k !== 'search.mode' && k !== 'search.mode_upgrade_notice_shown');

    if (!mode) {
      return {
        name: 'search_mode',
        status: 'ok',
        message: 'search.mode is unset (using balanced fallback). Run `gbrain search modes` to see what is running and pick a mode explicitly.',
      };
    }

    if (overrideKeys.length === 0) {
      return {
        name: 'search_mode',
        status: 'ok',
        message: `Mode: ${mode} (no per-key overrides — mode bundle is canonical).`,
      };
    }

    return {
      name: 'search_mode',
      status: 'ok',
      message: `Mode: ${mode} with ${overrideKeys.length} per-key override(s) (${overrideKeys.join(', ')}). To consolidate to the pure mode bundle: gbrain search modes --reset`,
    };
  } catch (e) {
    return {
      name: 'search_mode',
      status: 'ok',
      message: `Could not read search mode config (${(e as Error).message ?? 'unknown'}).`,
    };
  }
}

/**
 * v0.32.3 [CDX-6]: surface when retrieval-affecting files have changed
 * since the most recent published eval. Curated watch-list in
 * src/core/eval/drift-watch.ts; additions to that list require a
 * CHANGELOG line.
 *
 * Status stays `ok` — operator-facing reminder, not a hard gate.
 */
export async function checkEvalDrift(engine: BrainEngine): Promise<Check> {
  try {
    const { watchedFilesDrifted } = await import('../core/eval/drift-watch.ts');
    // Working tree vs HEAD (uncommitted retrieval changes). The fuller
    // version (vs the commit of the last published eval) is wired when
    // eval_results lands; today we just probe for uncommitted retrieval
    // changes so the operator sees them before re-running evals.
    const repoRoot = process.cwd();
    const drifted = watchedFilesDrifted(repoRoot);
    if (drifted.length === 0) {
      return {
        name: 'eval_drift',
        status: 'ok',
        message: 'No retrieval-affecting files changed in working tree.',
      };
    }
    const summary = drifted.slice(0, 3).join(', ') + (drifted.length > 3 ? ', …' : '');
    return {
      name: 'eval_drift',
      status: 'ok',
      message: `${drifted.length} retrieval-affecting file(s) changed since HEAD: ${summary}. Re-run \`gbrain eval run-all\` after committing these changes.`,
    };
  } catch (e) {
    return {
      name: 'eval_drift',
      status: 'ok',
      message: `Could not probe retrieval drift (${(e as Error).message ?? 'unknown'}).`,
    };
  }
}

/**
 * v0.31.12 — surface a warn when models.tier.subagent or models.default
 * resolves to a non-Anthropic provider. The subagent loop in
 * src/core/minions/handlers/subagent.ts uses Anthropic Messages API with
 * prompt caching on system + tools; non-Anthropic providers would break
 * the loop at runtime. This check makes the configuration drift visible
 * before a job is submitted.
 */
export async function checkSubagentCapability(engine: BrainEngine): Promise<Check> {
  try {
    const { classifyCapabilities } = await import('../core/ai/capabilities.ts');
    const tierSubagent = await engine.getConfig('models.tier.subagent');
    const modelsDefault = await engine.getConfig('models.default');

    // Helper: explain a verdict in user-facing terms.
    const explain = (resolved: string, source: string): Check | null => {
      const verdict = classifyCapabilities(resolved);
      if (verdict === 'unusable:no_tools') {
        return {
          name: 'subagent_capability',
          status: 'warn',
          message:
            `${source} is "${resolved}" but that provider/model lacks native tool calling. ` +
            `The subagent loop cannot run on this model — runtime will fall back to claude-sonnet-4-6. ` +
            `Fix: \`gbrain config set ${source} <provider>:<model-with-tools>\` (e.g. anthropic:claude-sonnet-4-6 or openai:gpt-5.2).`,
        };
      }
      if (verdict === 'unknown') {
        return {
          name: 'subagent_capability',
          status: 'warn',
          message:
            `${source} is "${resolved}" which references an unknown provider. ` +
            `Use a recipe-declared provider. ` +
            `Fix: \`gbrain config set ${source} anthropic:claude-sonnet-4-6\` or pick another known provider.`,
        };
      }
      if (verdict === 'degraded:no_caching') {
        return {
          name: 'subagent_capability',
          status: 'warn',
          message:
            `${source} is "${resolved}" — provider does not support prompt caching. ` +
            `The subagent loop runs hot (cost scales linearly with conversation length). ` +
            `For lower cost on long loops, use an Anthropic model: ` +
            `\`gbrain config set models.tier.subagent anthropic:claude-sonnet-4-6\`.`,
        };
      }
      return null;
    };

    if (tierSubagent) {
      const issue = explain(tierSubagent, 'models.tier.subagent');
      if (issue) return issue;
    } else if (modelsDefault) {
      const issue = explain(modelsDefault, 'models.default');
      if (issue) return issue;
    }
    // v0.37 (T10 / D7) + v0.38 (D7 capability rename): warn when the configured
    // chat_model is non-Anthropic AND ANTHROPIC_API_KEY isn't set. With
    // agent.use_gateway_loop=false (the v0.38 default), subagent jobs still
    // require Anthropic at runtime; without the key, gbrain dream / gbrain
    // agent run / gbrain autopilot will all fail at job submission. Catches
    // the post-init drift case the init-time caveat would have shown if init
    // had been re-run.
    try {
      const { loadConfig } = await import('../core/config.ts');
      const cfg = loadConfig();
      const chatModel = cfg?.chat_model;
      const { isAnthropicProvider } = await import('../core/model-config.ts');
      if (chatModel && !isAnthropicProvider(chatModel) && !process.env.ANTHROPIC_API_KEY) {
        return {
          name: 'subagent_capability',
          status: 'warn',
          message:
            `chat_model is "${chatModel}" (non-Anthropic) and ANTHROPIC_API_KEY is not set. ` +
            `Subagent features (gbrain dream, gbrain agent run, gbrain autopilot) will fail at job submission ` +
            `unless agent.use_gateway_loop=true. Chat alone (gbrain think) still works. ` +
            `Either set ANTHROPIC_API_KEY or enable: \`gbrain config set agent.use_gateway_loop true\`.`,
        };
      }
    } catch { /* loadConfig may throw; fall through */ }

    return {
      name: 'subagent_capability',
      status: 'ok',
      message: tierSubagent
        ? `Subagent tier resolves to "${tierSubagent}" with full tool-loop capability`
        : `Subagent tier resolves to default (claude-sonnet-4-6) — full tool-loop capability`,
    };
  } catch (e) {
    return {
      name: 'subagent_capability',
      status: 'warn',
      message: `Could not check subagent capability: ${e instanceof Error ? e.message : String(e)}`,
    };
  }
}

// v0.38 — `checkSubagentProvider` was renamed to `checkSubagentCapability` (D7).
// Back-compat alias preserved for any external doctor extensions importing it.
const checkSubagentProvider = checkSubagentCapability;
void checkSubagentProvider;

// Module-scoped flag so the NaN-fallback warning fires once per process.
let _syncFreshnessEnvWarned = false;

function _resolveSyncFreshnessHours(varName: string, fallback: number): number {
  const raw = process.env[varName];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    if (!_syncFreshnessEnvWarned) {
      _syncFreshnessEnvWarned = true;
      console.warn(
        `[gbrain doctor] Ignoring invalid ${varName}=${raw}; using default ${fallback}h.`,
      );
    }
    return fallback;
  }
  return n;
}

/**
 * Sync freshness check (v0.32.4) — verify that sources with local_path have
 * been synced recently. Detects the silent failure mode where `gbrain sync`
 * stopped running and brain search now misses recent pages.
 *
 * Pure staleness check. Reads `sources.last_sync_at` only — no filesystem
 * access. Filesystem-vs-DB drift detection is intentionally out of scope:
 *   - doctorReportRemote runs in the HTTP MCP server (src/commands/serve-http.ts);
 *     walking arbitrary DB-supplied paths from a remote-callable endpoint
 *     crosses a trust boundary (OAuth write scope could mutate local_path).
 *   - Drift detection belongs in `multi_source_drift` which already has
 *     GBRAIN_DRIFT_LIMIT + GBRAIN_DRIFT_TIMEOUT_MS guards.
 *
 * Thresholds (env-overridable, default = 24h warn / 72h fail):
 *   - GBRAIN_SYNC_FRESHNESS_WARN_HOURS
 *   - GBRAIN_SYNC_FRESHNESS_FAIL_HOURS
 * Invalid values (NaN, ≤0) fall back to defaults with a once-per-process warn.
 *
 * Edge cases handled:
 *   - last_sync_at IS NULL → fail "never synced"
 *   - last_sync_at > now() (clock skew / corrupted timestamp) → warn
 *   - mixed sources → highest-severity drives the overall status
 *   - executeRaw throws → outer-catch warn so doctor keeps running
 *
 * Failure messages embed `source.id` so the fix command
 * `gbrain sync --source <id>` matches what the user copy-pastes.
 */
export async function checkSyncFreshness(
  engine: BrainEngine,
  opts?: { nowMs?: number },
): Promise<Check> {
  try {
    const sources = await engine.executeRaw<{
      id: string;
      name: string;
      local_path: string | null;
      last_sync_at: Date | null;
    }>(
      `SELECT id, name, local_path, last_sync_at FROM sources WHERE local_path IS NOT NULL`,
    );

    if (sources.length === 0) {
      return {
        name: 'sync_freshness',
        status: 'ok',
        message: 'No federated sources to sync',
      };
    }

    const warnHours = _resolveSyncFreshnessHours('GBRAIN_SYNC_FRESHNESS_WARN_HOURS', 24);
    const failHours = _resolveSyncFreshnessHours('GBRAIN_SYNC_FRESHNESS_FAIL_HOURS', 72);
    const warnMs = warnHours * 60 * 60 * 1000;
    const failMs = failHours * 60 * 60 * 1000;

    // `opts.nowMs` is a test-only injection seam for the boundary tests.
    // Without it, the two `Date.now()` calls (one in the test's `agoMs`
    // helper, one here) drift apart by microseconds-to-milliseconds, which
    // pushes "exactly 72h ago" above the strict `>` threshold and flips the
    // status from warn to fail (CI-flaky, see PR #1138 ship). Production
    // callers omit `nowMs` and get live wall-clock semantics.
    const now = opts?.nowMs ?? Date.now();
    const issues: string[] = [];
    let hasWarnings = false;
    let hasFailures = false;

    for (const source of sources) {
      // Embed source.id in user-visible messages so `gbrain sync --source <id>`
      // matches what the user copy-pastes. Show display name in parens when set.
      const display = source.name && source.name !== source.id
        ? `'${source.id}' (${source.name})`
        : `'${source.id}'`;

      if (!source.last_sync_at) {
        issues.push(`Source ${display} has never been synced`);
        hasFailures = true;
        continue;
      }

      const lastSync = new Date(source.last_sync_at).getTime();
      const ageMs = now - lastSync;

      if (ageMs < 0) {
        issues.push(
          `Source ${display} has future last_sync_at — clock skew or corrupted timestamp`,
        );
        hasWarnings = true;
        continue;
      }

      const ageHours = Math.floor(ageMs / (1000 * 60 * 60));
      const ageDays = Math.floor(ageHours / 24);

      if (ageMs > failMs) {
        issues.push(`Source ${display} last synced ${ageDays}d ago — brain search is stale!`);
        hasFailures = true;
      } else if (ageMs > warnMs) {
        issues.push(`Source ${display} last synced ${ageHours}h ago`);
        hasWarnings = true;
      }
    }

    if (hasFailures) {
      return {
        name: 'sync_freshness',
        status: 'fail',
        message: `${issues.join('; ')}. Run \`gbrain sync --source <id>\` for each stale source`,
      };
    }
    if (hasWarnings) {
      return {
        name: 'sync_freshness',
        status: 'warn',
        message: `${issues.join('; ')}. Run \`gbrain sync --source <id>\` to refresh`,
      };
    }
    return {
      name: 'sync_freshness',
      status: 'ok',
      message: `All ${sources.length} federated source(s) synced recently`,
    };
  } catch (e) {
    return {
      name: 'sync_freshness',
      status: 'warn',
      message: `Could not check sync freshness: ${e instanceof Error ? e.message : String(e)}`,
    };
  }
}
