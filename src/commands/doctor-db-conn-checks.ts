import type { BrainEngine } from '../core/engine.ts';
import type { Check } from './doctor-types.ts';
import { outputResults } from './doctor-types.ts';
import type { ProgressReporter } from '../core/progress.ts';
import { startHeartbeat } from '../core/progress.ts';
import * as db from '../core/db.ts';
import { LATEST_VERSION } from '../core/migrate.ts';
import { join } from 'path';

/**
 * DB connection + embedding checks — extracted from runDoctor (BET-Y1Q3-T6-04).
 * Returns after pushing checks; process.exit on unrecoverable connection failure.
 */
export async function runDoctorDbConnChecks(
  engine: BrainEngine,
  checks: Check[],
  progress: ProgressReporter,
  jsonOutput: boolean,
): Promise<void> {
  // 3. Connection
  progress.heartbeat('connection');
  try {
    const stats = await engine.getStats();
    checks.push({ name: 'connection', status: 'ok', message: `Connected, ${stats.page_count} pages` });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    checks.push({ name: 'connection', status: 'fail', message: msg });
    progress.finish();
    const earlyFail2 = outputResults(checks, jsonOutput);
    process.exit(earlyFail2 ? 1 : 0);
    return;
  }

  // 4. pgvector extension
  progress.heartbeat('pgvector');
  try {
    const sql = db.getConnection();
    const ext = await sql`SELECT extname FROM pg_extension WHERE extname = 'vector'`;
    if (ext.length > 0) {
      checks.push({ name: 'pgvector', status: 'ok', message: 'Extension installed' });
    } else {
      checks.push({ name: 'pgvector', status: 'fail', message: 'Extension not found. Run: CREATE EXTENSION vector;' });
    }
  } catch {
    checks.push({ name: 'pgvector', status: 'warn', message: 'Could not check pgvector extension' });
  }

  // 4b. PgBouncer / prepared-statement compatibility.
  // URL-only inspection — no DB roundtrip — so this is cheap and works
  // regardless of whether the caller is the module singleton or a
  // worker-instance engine.
  progress.heartbeat('pgbouncer_prepare');
  try {
    const { resolvePrepare } = await import('../core/db.ts');
    const { loadConfig } = await import('../core/config.ts');
    const config = loadConfig();
    const url = config?.database_url || '';
    const prepare = resolvePrepare(url);
    if (prepare === false) {
      checks.push({
        name: 'pgbouncer_prepare',
        status: 'ok',
        message: 'Prepared statements disabled (PgBouncer-safe)',
      });
    } else {
      try {
        const parsed = new URL(url.replace(/^postgres(ql)?:\/\//, 'http://'));
        if (parsed.port === '6543') {
          checks.push({
            name: 'pgbouncer_prepare',
            status: 'warn',
            message:
              'Port 6543 (PgBouncer transaction mode) detected but prepared statements are enabled. ' +
              'This causes "prepared statement does not exist" errors under concurrent load. ' +
              'Fix: unset GBRAIN_PREPARE (or set =false), or add ?prepare=false to the connection URL.',
          });
        }
      } catch {
        // URL parse failure — skip, nothing actionable
      }
    }
  } catch {
    // best-effort; never fail doctor on this check
  }

  // 5. RLS — check ALL public tables, not just gbrain's own.
  // Any table without RLS in the public schema is a security risk:
  // Supabase exposes the public schema via PostgREST, so tables without
  // RLS are readable/writable by anyone with the anon key.
  //
  // Escape hatch ("write it in blood"): if a user or plugin deliberately
  // wants a public-schema table readable by the anon key (analytics,
  // materialized views the anon key needs), they can exempt it with a
  // Postgres COMMENT whose value starts with:
  //
  //     GBRAIN:RLS_EXEMPT reason=<non-empty reason>
  //
  // The comment lives in pg_description, survives pg_dump, is visible in
  // schema diffs, and requires raw SQL in psql to set — there is no
  // `gbrain rls-exempt add` CLI on purpose. Doctor re-enumerates the
  // exemption list on every successful run so exempt tables never go
  // invisible. See docs/guides/rls-and-you.md.
  progress.heartbeat('rls');
  if (engine.kind === 'pglite') {
    // PGLite is embedded and single-user — no PostgREST exposure,
    // RLS is not a meaningful security boundary here.
    checks.push({
      name: 'rls',
      status: 'ok',
      message: 'Skipped (PGLite — no PostgREST exposure, RLS not applicable)',
    });
  } else {
    try {
      const sql = db.getConnection();
      // Left-join pg_description so we get the (optional) COMMENT ON TABLE
      // value alongside rowsecurity in a single round-trip. Filter to
      // base tables in the public schema.
      const tables = await sql`
        SELECT
          t.tablename,
          t.rowsecurity,
          COALESCE(
            obj_description(format('public.%I', t.tablename)::regclass, 'pg_class'),
            ''
          ) AS comment
        FROM pg_tables t
        WHERE t.schemaname = 'public'
      `;
      const EXEMPT_RE = /^GBRAIN:RLS_EXEMPT\s+reason=\S.{3,}/;
      const exempt: string[] = [];
      const gaps: string[] = [];
      for (const t of tables as Array<any>) {
        if (t.rowsecurity) continue;
        if (EXEMPT_RE.test(t.comment || '')) {
          exempt.push(t.tablename);
        } else {
          gaps.push(t.tablename);
        }
      }
      if (gaps.length === 0) {
        const suffix = exempt.length > 0
          ? ` (${exempt.length} explicitly exempt: ${exempt.join(', ')})`
          : '';
        checks.push({
          name: 'rls',
          status: 'ok',
          message: `RLS enabled on ${tables.length - exempt.length}/${tables.length} public tables${suffix}`,
        });
      } else {
        const names = gaps.join(', ');
        // Double-escape " inside identifiers so a pathological table name
        // like `weird"table` renders as `"weird""table"` in the remediation
        // SQL (matches how Postgres parses quoted identifiers). Doubling
        // any existing " is the minimum needed to keep the output valid
        // copy-paste SQL. Extremely rare in practice but cheap to get right.
        const fixes = gaps
          .map(n => `ALTER TABLE "public"."${n.replace(/"/g, '""')}" ENABLE ROW LEVEL SECURITY;`)
          .join(' ');
        const exemptInfo = exempt.length > 0
          ? ` (${exempt.length} other table(s) explicitly exempt.)`
          : '';
        checks.push({
          name: 'rls',
          status: 'fail',
          message:
            `${gaps.length} table(s) WITHOUT Row Level Security: ${names}.${exemptInfo} ` +
            `Fix: ${fixes} ` +
            `If a table should stay readable by the anon key on purpose, see docs/guides/rls-and-you.md for the GBRAIN:RLS_EXEMPT comment escape hatch.`,
        });
      }
    } catch {
      checks.push({ name: 'rls', status: 'warn', message: 'Could not check RLS status' });
    }
  }

  // 6. Schema version — also surfaces the #218 "postinstall silently failed"
  // state: if schema_version is 0/missing but the DB connected, migrations
  // never ran. That's the same class as a half-migrated install, just from a
  // different root cause (Bun blocked our top-level postinstall on global
  // install). Message is actionable either way.
  progress.heartbeat('schema_version');
  let schemaVersion = 0;
  try {
    const version = await engine.getConfig('version');
    schemaVersion = parseInt(version || '0', 10);
    if (schemaVersion >= LATEST_VERSION) {
      checks.push({ name: 'schema_version', status: 'ok', message: `Version ${schemaVersion} (latest: ${LATEST_VERSION})` });
    } else if (schemaVersion === 0) {
      checks.push({
        name: 'schema_version',
        status: 'fail',
        message: `No schema version recorded. Migrations never ran. Fix: gbrain apply-migrations --yes. ` +
                 `If you installed via 'bun install -g github:...', see https://github.com/garrytan/gbrain/issues/218.`,
      });
    } else {
      checks.push({
        name: 'schema_version',
        status: 'warn',
        message: `Version ${schemaVersion}, latest is ${LATEST_VERSION}. Fix: gbrain apply-migrations --yes`,
      });
    }
  } catch {
    checks.push({ name: 'schema_version', status: 'warn', message: 'Could not check schema version' });
  }

  // Note: we intentionally DO NOT fail on "schema v7+ but no preferences.json".
  // That's a valid fresh-install state after `gbrain init` — the migration
  // orchestrator writes preferences, but `init` alone doesn't run it. The
  // partial-completed.jsonl check in the filesystem section (step 3) is
  // the canonical half-migration signal and fires when the stopgap ran
  // but `apply-migrations` didn't follow up.

  // 7. RLS event trigger (post-install drift detector for v35 auto-RLS).
  // Catches the case where an operator manually drops the trigger to debug
  // something and forgets to recreate it. Does NOT catch install-time silent
  // failure — runMigrations rethrows on SQL failure and only bumps
  // config.version after success, so a failed v35 install means version
  // stays at 34 and check #6 (schema_version) fires loudly.
  //
  // Healthy evtenabled values: 'O' (origin) and 'A' (always). 'R' is
  // replica-only and would NOT fire in normal origin sessions; 'D' is
  // disabled. Both of those are warn states.
  progress.heartbeat('rls_event_trigger');
  if (engine.kind === 'pglite') {
    checks.push({
      name: 'rls_event_trigger',
      status: 'ok',
      message: 'Skipped (PGLite — no event trigger support)',
    });
  } else {
    try {
      const sql = db.getConnection();
      const rows = await sql`
        SELECT evtname, evtenabled FROM pg_event_trigger
        WHERE evtname = 'auto_rls_on_create_table'
      `;
      if (rows.length === 0) {
        checks.push({
          name: 'rls_event_trigger',
          status: 'warn',
          message:
            'Auto-RLS event trigger missing. New tables created outside gbrain may not get RLS. ' +
            'Fix: gbrain apply-migrations --force-retry 35',
        });
      } else if (rows[0].evtenabled !== 'O' && rows[0].evtenabled !== 'A') {
        checks.push({
          name: 'rls_event_trigger',
          status: 'warn',
          message:
            `Auto-RLS event trigger present but evtenabled=${rows[0].evtenabled} ` +
            `(not origin/always). Trigger will not fire in normal sessions. ` +
            `Fix: ALTER EVENT TRIGGER auto_rls_on_create_table ENABLE;`,
        });
      } else {
        checks.push({
          name: 'rls_event_trigger',
          status: 'ok',
          message: 'Auto-RLS event trigger installed',
        });
      }
    } catch {
      checks.push({
        name: 'rls_event_trigger',
        status: 'warn',
        message: 'Could not check RLS event trigger',
      });
    }
  }

  // 8. Embedding health
  progress.heartbeat('embeddings');
  try {
    const health = await engine.getHealth();
    const pct = (health.embed_coverage * 100).toFixed(0);
    if (health.embed_coverage >= 0.9) {
      checks.push({ name: 'embeddings', status: 'ok', message: `${pct}% coverage, ${health.missing_embeddings} missing` });
    } else if (health.embed_coverage > 0) {
      checks.push({ name: 'embeddings', status: 'warn', message: `${pct}% coverage, ${health.missing_embeddings} missing. Run: gbrain embed --stale` });
    } else {
      checks.push({ name: 'embeddings', status: 'warn', message: 'No embeddings yet. Run: gbrain embed --stale' });
    }
  } catch {
    checks.push({ name: 'embeddings', status: 'warn', message: 'Could not check embedding health' });
  }

  // 8b. Embedding provider eval — live smoke test of the configured provider.
  //     Verifies: correct model, API key works, dimensions match config, DB column matches.
  progress.heartbeat('embedding_provider');
  try {
    const {
      getEmbeddingModel,
      getEmbeddingDimensions,
      embedOne,
      isAvailable,
    } = await import('../core/ai/gateway.ts');

    const configuredModel = getEmbeddingModel();
    const configuredDims = getEmbeddingDimensions();
    const available = isAvailable('embedding');

    // v0.37 (T9, codex #7 nuance): catch the v0.36 silent-default case where
    // config has no embedding_model but the schema column exists at a dim
    // that doesn't match the gateway's resolved default. Empty-brain vs
    // non-empty-brain branching determines the repair hint:
    //   - empty brain (no embedded chunks) → `gbrain init --force --embedding-model …`
    //   - non-empty brain → `gbrain retrieval-upgrade --to … --reindex`
    // The bug-reporter's `rm -rf ~/.gbrain` recovery is never the right answer.
    let surfacedUnconfiguredDrift = false;
    try {
      const { loadConfig } = await import('../core/config.ts');
      const cfg = loadConfig();
      const fileEmbeddingSet = !!cfg?.embedding_model;
      const deferredSetup = cfg?.embedding_disabled === true;
      if (!fileEmbeddingSet && !deferredSetup) {
        // Read column dim + chunk count
        const { readContentChunksEmbeddingDim } = await import('../core/embedding-dim-check.ts');
        const colDim = await readContentChunksEmbeddingDim(engine);
        if (colDim.exists && colDim.dims !== null && colDim.dims !== configuredDims) {
          // Determine if the brain has any content — drift is only a real
          // user-facing problem once the user has imported anything. A
          // pristine brain (0 total chunks) is still in fresh-install state;
          // first import will hit the loud preflight before any column
          // write, so doctor doesn't need to pre-warn.
          let totalChunks = 0;
          let embeddedCount = 0;
          try {
            const rows = await engine.executeRaw<{ total: number | string; embedded: number | string }>(
              `SELECT COUNT(*)::int AS total, COUNT(*) FILTER (WHERE embedding IS NOT NULL)::int AS embedded FROM content_chunks`,
            );
            totalChunks = Number(rows?.[0]?.total ?? 0);
            embeddedCount = Number(rows?.[0]?.embedded ?? 0);
          } catch { /* table may be missing or fresh; treat as empty */ }

          if (totalChunks > 0) {
            const fix = embeddedCount === 0
              ? `No embeddings yet — drop the empty schema and re-init at the right dim:\n        gbrain init --force --pglite --embedding-model ${configuredModel} --embedding-dimensions ${configuredDims}`
              : `Non-empty brain (${embeddedCount} embedded chunks). Migrate cleanly:\n        gbrain retrieval-upgrade --to ${configuredModel} --reindex`;

            checks.push({
              name: 'embedding_provider',
              status: 'warn',
              message:
                `Schema column is vector(${colDim.dims}) but gateway default resolves to ${configuredModel} (${configuredDims}d). ` +
                `Persist your provider choice with \`gbrain config set embedding_model ${configuredModel}\` AND fix the schema:\n      ${fix}`,
            });
            surfacedUnconfiguredDrift = true;
          }
        }
      }
    } catch {
      // loadConfig may throw on a malformed config; let the existing
      // available/probe branch surface the issue.
    }

    if (surfacedUnconfiguredDrift) {
      // Bail out — the warn above is more actionable than the live probe.
    } else if (!available) {
      // Per v0.28.5 plan P1: silently skipped when no API key is configured.
      // Doctor must stay green on CI / local-only / offline environments where
      // a full provider probe isn't possible. The skipped status is still
      // visible in --json output so operators can see it ran.
      checks.push({
        name: 'embedding_provider',
        status: 'ok',
        message: `Skipped (no provider credentials). Model: ${configuredModel}.`,
      });
    } else {
      // Live embed test
      const start = Date.now();
      const vec = await embedOne('gbrain doctor embedding smoke test');
      const ms = Date.now() - start;
      const actualDims = vec.length;

      const issues: string[] = [];

      // Check dimensions match config
      if (actualDims !== configuredDims) {
        issues.push(`Dimension mismatch: provider returned ${actualDims} but config expects ${configuredDims}`);
      }

      // Check DB column dimensions match (engine-portable; works on both
      // Postgres and PGLite via the shared dim-check helper added in v0.28.5).
      try {
        const { readContentChunksEmbeddingDim } = await import('../core/embedding-dim-check.ts');
        const colDim = await readContentChunksEmbeddingDim(engine);
        if (colDim.exists && colDim.dims !== null && colDim.dims !== actualDims) {
          issues.push(`DB dimension mismatch: column is vector(${colDim.dims}) but provider returns ${actualDims}-dim. See docs/embedding-migrations.md for the manual ALTER recipe.`);
        }
      } catch { /* column or table missing — fresh brain, fine */ }

      if (issues.length > 0) {
        checks.push({
          name: 'embedding_provider',
          status: 'warn',
          message: `${configuredModel} responds (${ms}ms, ${actualDims} dims) but: ${issues.join('; ')}`,
        });
      } else {
        checks.push({
          name: 'embedding_provider',
          status: 'ok',
          message: `${configuredModel} ✓ ${ms}ms, ${actualDims} dims, DB aligned`,
        });
      }
    }
  } catch (e: any) {
    // Per v0.28.5 plan P1: non-fatal on network failure. The probe surfaces
    // the issue but doesn't fail doctor — common cases (rate limit, transient
    // 5xx, DNS blip, expired key) shouldn't take down a CI run.
    checks.push({
      name: 'embedding_provider',
      status: 'warn',
      message: `Embedding provider probe failed: ${e.message?.slice(0, 200) ?? e}`,
    });
  }

  // 8c. Alternative provider advisory (v0.32 D11=C / Codex finding #2 wire-through).
  // Walks listRecipes() and surfaces any recipe whose required env vars are ALL
  // set in the process env but is not the currently configured provider. Helps
  // users discover that, e.g., OPENAI_API_KEY=x DASHSCOPE_API_KEY=y means they
  // have a Chinese-region alternative ready to go without setup.
  progress.heartbeat('alternative_providers');
  try {
    const { listRecipes } = await import('../core/ai/recipes/index.ts');
    const { getEmbeddingModel } = await import('../core/ai/gateway.ts');
    const configuredId = (getEmbeddingModel() || '').split(':')[0];
    const alternatives: string[] = [];
    for (const r of listRecipes()) {
      if (r.id === configuredId) continue;
      const required = r.auth_env?.required ?? [];
      // Skip recipes with no required env (they're "always available" — not a
      // useful signal) and recipes that require env we don't have.
      if (required.length === 0) continue;
      const allPresent = required.every(k => !!process.env[k]);
      if (!allPresent) continue;
      // Skip recipes without an embedding touchpoint (chat-only — not an
      // embedding alternative).
      if (!r.touchpoints.embedding) continue;
      alternatives.push(r.id);
    }
    if (alternatives.length > 0) {
      checks.push({
        name: 'alternative_providers',
        status: 'ok',
        message: `Detected ${alternatives.length} alternative embedding provider${alternatives.length > 1 ? 's' : ''} ready to use: ${alternatives.join(', ')}. Run \`gbrain providers list\` to switch.`,
      });
    }
  } catch { /* listRecipes / gateway not available — silent */ }

  // 8c. Embedding column registry (v0.36 — D5 + D13 + D14).
  //     Validates every column in the merged registry against the real DB
  //     shape: (a) column exists, (b) declared type+dims match actual
  //     format_type(atttypid, atttypmod), (c) HNSW index present on
  //     Postgres, (d) the ACTIVE default column has >= 90% coverage.
  //
  //     Batch probes (D5) so the registry can grow without N+1 round-trips:
  //     one format_type query, one pg_indexes query, one coverage-per-active
  //     column query.
  progress.heartbeat('embedding_column_registry');
  try {
    const { getEmbeddingColumnRegistry, resolveEmbeddingColumn, quoteIdentifier } =
      await import('../core/search/embedding-column.ts');
    const { loadConfig: _loadConfig } = await import('../core/config.ts');
    const fileCfg = _loadConfig();
    const mergedCfg = fileCfg ? await (await import('../core/config.ts')).loadConfigWithEngine(engine, fileCfg).catch(() => fileCfg) : null;
    if (!mergedCfg) {
      checks.push({
        name: 'embedding_column_registry',
        status: 'ok',
        message: 'No brain config loaded — skipped',
      });
    } else {
      const registry = getEmbeddingColumnRegistry(mergedCfg);
      const declaredColumns = Object.keys(registry);
      const activeCol = resolveEmbeddingColumn(undefined, mergedCfg).name;

      // D13 — batch format_type probe via pg_attribute. udt_name only
      // returns 'vector' vs 'halfvec'; format_type(atttypid, atttypmod)
      // returns 'vector(1024)' / 'halfvec(2560)' so dim drift surfaces.
      const formatRows = await engine.executeRaw<{ attname: string; formatted: string }>(
        `SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS formatted
           FROM pg_attribute a
           JOIN pg_class c ON c.oid = a.attrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = 'public'
            AND c.relname = 'content_chunks'
            AND a.attname = ANY($1::text[])
            AND NOT a.attisdropped`,
        [declaredColumns],
      );
      const actualByName = new Map<string, string>();
      for (const r of formatRows) actualByName.set(r.attname, r.formatted);

      // D5 — batch index probe (Postgres only; PGLite indexing is implicit
      // and the partial-index pattern doesn't surface in pg_indexes the
      // same way). Reports informational, not blocking — search still
      // works without an HNSW index, just slow.
      const haveIndex = new Map<string, boolean>();
      if (engine.kind === 'postgres') {
        const indexRows = await engine.executeRaw<{ indexdef: string }>(
          `SELECT indexdef FROM pg_indexes
            WHERE tablename = 'content_chunks'
              AND schemaname = 'public'`,
        );
        for (const col of declaredColumns) {
          const found = indexRows.some(r => /USING\s+hnsw/i.test(r.indexdef) && r.indexdef.includes(`(${col} `));
          haveIndex.set(col, found);
        }
      }

      // Per-column health rollup.
      const issues: string[] = [];
      const okColumns: string[] = [];
      for (const colName of declaredColumns) {
        const entry = registry[colName];
        const actual = actualByName.get(colName);
        if (!actual) {
          issues.push(`${colName}: declared but column does NOT exist in content_chunks`);
          continue;
        }
        // Expected format: `vector(N)` or `halfvec(N)`.
        const m = actual.match(/^(vector|halfvec)\((\d+)\)/i);
        const actualType = m ? m[1].toLowerCase() : actual;
        const actualDims = m ? parseInt(m[2], 10) : null;
        if (actualType !== entry.type) {
          issues.push(
            `${colName}: declared type=${entry.type} but actual is ${actual}. ` +
              `Fix: gbrain config set embedding_columns '<JSON>' OR ` +
              `ALTER TABLE content_chunks ALTER COLUMN ${colName} TYPE ${entry.type}(${entry.dimensions});`,
          );
          continue;
        }
        if (actualDims !== null && actualDims !== entry.dimensions) {
          issues.push(
            `${colName}: declared dims=${entry.dimensions} but actual is ${actual}. ` +
              `Fix one side: update config OR ` +
              `ALTER TABLE content_chunks ALTER COLUMN ${colName} TYPE ${entry.type}(${entry.dimensions});`,
          );
          continue;
        }
        if (engine.kind === 'postgres' && haveIndex.get(colName) === false) {
          issues.push(
            `${colName}: no HNSW index. Search works but uses sequential scan. ` +
              `Fix: CREATE INDEX IF NOT EXISTS idx_chunks_${colName} ON content_chunks USING hnsw (${quoteIdentifier(colName)} ${entry.type}_cosine_ops);`,
          );
          continue;
        }
        okColumns.push(colName);
      }

      // D14 — coverage gate on the ACTIVE default column. Catches the
      // "user switched to a 5%-populated column" silent-degradation case.
      let coverageWarn: string | null = null;
      if (activeCol && actualByName.has(activeCol)) {
        // Codex /ship #5: pull `total` alongside `pct` so a fresh brain
        // (0 chunks → NULLIF makes pct NULL → coalesces to 0) doesn't
        // false-warn "Active column 'embedding' is 0.0% populated".
        const covRows = await engine.executeRaw<{ pct: number; total: number }>(
          `SELECT (
             COUNT(*) FILTER (WHERE ${quoteIdentifier(activeCol)} IS NOT NULL)::float
             / NULLIF(COUNT(*), 0) * 100
           )::float AS pct,
           COUNT(*)::int AS total
           FROM content_chunks`,
        );
        const pct = covRows[0]?.pct ?? 0;
        const total = covRows[0]?.total ?? 0;
        // Only warn when there's a real coverage gap. Empty brain (0 chunks)
        // is a normal state for new installs — skip the gate entirely.
        if (total > 0 && pct < 90) {
          coverageWarn =
            `Active column '${activeCol}' is ${pct.toFixed(1)}% populated. ` +
            `Search quality silently degraded on un-embedded chunks. ` +
            `Fix: gbrain embed --column ${activeCol} --stale (write-side support v2) ` +
            `OR gbrain config set search_embedding_column embedding`;
        }
      }

      if (issues.length === 0 && !coverageWarn) {
        const indexNote = engine.kind === 'postgres' ? ' (all indexed)' : '';
        checks.push({
          name: 'embedding_column_registry',
          status: 'ok',
          message: `Registry healthy: ${okColumns.length} columns (${okColumns.join(', ')})${indexNote}; active='${activeCol}'`,
        });
      } else {
        const allMessages = [
          ...issues,
          ...(coverageWarn ? [coverageWarn] : []),
        ];
        checks.push({
          name: 'embedding_column_registry',
          status: 'warn',
          message: allMessages.join(' | '),
        });
      }
    }
  } catch (err) {
    // Pre-config brains, registry-validation throws, etc. Surfaces the
    // error message but doesn't fail the doctor run.
    checks.push({
      name: 'embedding_column_registry',
      status: 'warn',
      message: `Could not check embedding column registry: ${(err as Error).message}`,
    });
  }
}
