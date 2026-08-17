import type { BrainEngine } from '../core/engine.ts';
import { existsSync, readFileSync, statSync } from 'fs';
import { dirname, isAbsolute, join, resolve as resolvePath } from 'path';
import { fileURLToPath } from 'url';

const WHOKNOWS_FIXTURE_RELATIVE_PATH = 'test/fixtures/whoknows-eval.jsonl';

function isGbrainSourceRoot(dir: string): boolean {
  return dirname(dir) === dirname(fileURLToPath(import.meta.url)) && dirname(dir) !== dir;
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