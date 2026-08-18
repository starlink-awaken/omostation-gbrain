import postgres from 'postgres';
import type {
  BrainEngine,
  LinkBatchInput, TimelineBatchInput,
  ReservedConnection,
  DreamVerdict, DreamVerdictInput,
  FileSpec, FileRow,
  TakeBatchInput, Take, TakesListOpts, TakeHit, StaleTakeRow,
  TakeResolution, SynthesisEvidenceInput,
  TakesScorecard, TakesScorecardOpts, CalibrationBucket, CalibrationCurveOpts,
  FactRow, FactKind, FactVisibility, FactInsertStatus,
  NewFact, FactListOpts, FactsHealth,
} from './engine.ts';
import type {
  DomainBankSampleOpts, CorpusSampleOpts, DomainBankRow,
} from './types.ts';
import { MAX_SEARCH_LIMIT, clampSearchLimit } from './engine.ts';
import { deriveResolutionTuple, finalizeScorecard } from './takes-resolution.ts';
import { normalizeWeightForStorage } from './takes-fence.ts';
import { runMigrations } from './migrate.ts';
import { SCHEMA_SQL } from './schema-embedded.ts';
import { verifySchema } from './schema-verify.ts';
import { applyChunkEmbeddingIndexPolicy, dropZombieIndexes } from './vector-index.ts';
import {
  normalizeEngineColumn,
  buildVectorCastFragment,
  quoteIdentifier,
  COLUMN_NAME_REGEX,
  EmbeddingColumnNotRegisteredError,
} from './search/embedding-column.ts';
import type {
  Page, PageInput, PageFilters, PageType,
  Chunk, ChunkInput, StaleChunkRow,
  SearchResult, SearchOpts,
  Link, GraphNode, GraphPath,
  TimelineEntry, TimelineInput, TimelineOpts,
  RawData,
  PageVersion,
  BrainStats, BrainHealth,
  IngestLogEntry, IngestLogInput,
  EngineConfig,
  EvalCandidate, EvalCandidateInput,
  EvalCaptureFailure, EvalCaptureFailureReason,
  SalienceOpts, SalienceResult, AnomaliesOpts, AnomalyResult,
  EmotionalWeightInputRow, EmotionalWeightWriteRow,
} from './types.ts';
import { GBrainError, PAGE_SORT_SQL } from './types.ts';
import { computeAnomaliesFromBuckets } from './cycle/anomaly.ts';
import * as db from './db.ts';
import { ConnectionManager } from './connection-manager.ts';
import { logConnectionEvent } from './connection-audit.ts';
import { validateSlug, contentHash, rowToPage, rowToChunk, rowToSearchResult, parseEmbedding, tryParseEmbedding, takeRowToTake } from './utils.ts';
import { resolveBoostMap, resolveHardExcludes } from './search/source-boost.ts';
import { buildSourceFactorCase, buildHardExcludeClause, buildVisibilityClause, buildRecencyComponentSql } from './search/sql-ranking.ts';
import { DEFAULT_EMBEDDING_MODEL, DEFAULT_EMBEDDING_DIMENSIONS } from './ai/defaults.ts';

function escapeSqlStringLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

export function getPostgresSchema(
  dims: number = DEFAULT_EMBEDDING_DIMENSIONS,
  model: string = DEFAULT_EMBEDDING_MODEL,
): string {
  const parsedDims = Number(dims);
  if (!Number.isInteger(parsedDims) || parsedDims <= 0) {
    throw new Error(`Invalid embedding dimensions: ${dims}`);
  }
  const sanitizedModel = escapeSqlStringLiteral(String(model));
  return applyChunkEmbeddingIndexPolicy(SCHEMA_SQL, parsedDims)
    .replace(/vector\(1536\)/g, `vector(${parsedDims})`)
    .replace(/'text-embedding-3-large'/g, `'${sanitizedModel}'`)
    .replace(/\('embedding_dimensions', '1536'\)/g, `('embedding_dimensions', '${parsedDims}')`);
}

// CONNECTION_ERROR_PATTERNS / isConnectionError were used by the per-call
// executeRaw retry that #406 originally shipped. Eng-review D3 dropped that
// retry as unsound (regex idempotence-boundary doesn't hold for writable
// CTEs or side-effecting SELECTs). Recovery now happens at the supervisor
// level (3-strikes-then-reconnect). The unit tests in
// test/connection-resilience.test.ts retain a self-contained copy of the
// helper so the regression-against-future-reintroduction guard still works.
// See (tracked TODO) item: "err.code-based connection-error matching" for the
// follow-up that will reintroduce a typed retry mechanism.

export class PostgresEngine {
  readonly kind = 'postgres' as const;
  private _sql: ReturnType<typeof postgres> | null = null;
  /** Saved config for reconnection. */
  private _savedConfig: (EngineConfig & { poolSize?: number; parentConnectionManager?: ConnectionManager }) | null = null;
  /** Whether a reconnect is in progress (prevents concurrent reconnects). */
  private _reconnecting = false;
  /**
   * Tracks which connection path this engine is using so disconnect() is
   * idempotent. 'instance' = own _sql pool (poolSize was set);
   * 'module' = the module-level db singleton (backward compat path).
   * null = never connected, or already disconnected. Without this, a second
   * disconnect() on an instance-pool engine would fall through to
   * db.disconnect() and clobber the unrelated module-level connection.
   */
  private _connectionStyle: 'instance' | 'module' | null = null;

  /**
   * v0.30.1 (Fix 1 + X1 + T5): instance-owned ConnectionManager.
   * - INSTANCE-owned: each PostgresEngine constructs its own.
   * - Worker engines (cycle, sync) inherit via opts.parentConnectionManager.
   * - transaction() clones share the parent's via copy.
   * - Module-singleton path (when poolSize unset) wraps the db.ts singleton.
   *
   * Public so callers can access read()/ddl()/bulk()/healthCheck() without
   * threading the manager through every API. doctor's connection_routing
   * check uses it; runMigrations() uses ddl().
   */
  connectionManager: ConnectionManager | null = null;

  // Instance connection (for workers) or fall back to module global (backward compat)
  get sql(): ReturnType<typeof postgres> {
    if (this._sql) return this._sql;
    return db.getConnection();
  }

  // Lifecycle
  async connect(config: EngineConfig & { poolSize?: number; parentConnectionManager?: ConnectionManager }): Promise<void> {
    this._savedConfig = config;
    const url = config.database_url;
    if (config.poolSize) {
      // Instance-level connection for worker isolation. resolvePoolSize lets
      // GBRAIN_POOL_SIZE cap below the caller's requested size when set — the
      // env var is a user escape hatch, so it wins.
      const url = config.database_url;
      if (!url) throw new GBrainError('No database URL', 'database_url is missing', 'Provide --url');
      const size = Math.min(config.poolSize, db.resolvePoolSize(config.poolSize));
      // Honor PgBouncer transaction-mode detection on worker-instance pools too.
      // Without this, `gbrain jobs work` against a Supabase pooler URL hits
      // "prepared statement does not exist" under load just like the module
      // singleton did before v0.15.4.
      const prepare = db.resolvePrepare(url);
      // Session timeouts (statement_timeout + idle_in_transaction_session_timeout)
      // keep orphan pgbouncer backends from holding locks for hours when the
      // postgres.js client disconnects mid-transaction. See resolveSessionTimeouts
      // in db.ts for context + env var overrides.
      const timeouts = db.resolveSessionTimeouts();
      const opts: Record<string, unknown> = {
        max: size,
        idle_timeout: 20,
        connect_timeout: 10,
        types: { bigint: postgres.BigInt },
      };
      if (Object.keys(timeouts).length > 0) {
        opts.connection = timeouts;
      }
      if (typeof prepare === 'boolean') {
        opts.prepare = prepare;
      }
      this._sql = postgres(url, opts);
      await this._sql`SELECT 1`;
      await db.setSessionDefaults(this._sql);
      this._connectionStyle = 'instance';

      // v0.30.1: instance-owned ConnectionManager wraps the read pool we just
      // built. Parent inheritance (T5/X1): worker engines pass their parent's
      // manager so kill-switch state and direct pool are shared.
      this.connectionManager = new ConnectionManager({
        url,
        parent: config.parentConnectionManager,
        readPoolOwnedExternally: true, // we own _sql; manager just routes
      });
      this.connectionManager.setReadPool(this._sql);
    } else {
      // Module-level singleton (backward compat for CLI main engine)
      await db.connect(config);
      this._connectionStyle = 'module';

      // v0.30.1: connection-manager wraps the module singleton.
      if (url) {
        this.connectionManager = new ConnectionManager({
          url,
          parent: config.parentConnectionManager,
          readPoolOwnedExternally: true, // db.ts owns the pool
        });
        this.connectionManager.setReadPool(db.getConnection());
      }
    }
  }

  async disconnect(): Promise<void> {
    // v0.30.1: tear down the direct pool first if the manager owns one.
    if (this.connectionManager) {
      await this.connectionManager.disconnect();
      this.connectionManager = null;
    }
    if (this._sql) {
      await this._sql.end();
      this._sql = null;
      // After this point, _connectionStyle stays 'instance' so a second
      // disconnect() is a no-op rather than falling through and clearing
      // the unrelated module-level db singleton.
      return;
    }
    if (this._connectionStyle === 'module') {
      await db.disconnect();
      this._connectionStyle = null;
    }
    // else: nothing to disconnect (already done or never connected)
  }

  async initSchema(): Promise<void> {
    // v0.30.1 (X1): route DDL through the direct pool when ConnectionManager
    // is in dual-pool mode. The pooler's 2-min statement_timeout truncates
    // SCHEMA_SQL replays + migrations on Supabase; the direct pool gets
    // 30min. Lane B replaces the lock primitive with a TTL+heartbeat table
    // lock; Lane A does the routing and keeps pg_advisory_lock(42) on the
    // SAME connection so the lock is correct.
    const conn = this.connectionManager
      ? await this.connectionManager.ddl()
      : this.sql;

    // Resolve the embedding dim/model from the gateway. v0.37 fix wave:
    // fallbacks track the canonical defaults in `ai/defaults.ts` instead of
    // stale v0.13 OpenAI literals, AND we store the full `provider:model`
    // string in the DB config table — consumers like ze-switch and doctor
    // expect the provider prefix. (Round-1 CDX-4 + A.8.)
    let dims: number = DEFAULT_EMBEDDING_DIMENSIONS;
    let model: string = DEFAULT_EMBEDDING_MODEL;
    try {
      const gw = await import('./ai/gateway.ts');
      dims = gw.getEmbeddingDimensions();
      model = gw.getEmbeddingModel() || model;
    } catch { /* gateway not yet configured — use defaults */ }

    const sqlText = getPostgresSchema(dims, model);

    // Advisory lock prevents concurrent initSchema() calls from deadlocking
    // on DDL statements (DROP TRIGGER + CREATE TRIGGER acquire AccessExclusiveLock).
    //
    // v0.30.1 honest limitation: pg_advisory_lock(42) is session-scoped to
    // `conn`. When dual-pool routing is active, conn is a direct-pool reserved
    // backend, so the lock is held for the duration of initSchema. Lane B
    // replaces this with a TTL+heartbeat table lock that survives pooler-side
    // session resets.
    const t0 = Date.now();
    logConnectionEvent({
      pool: this.connectionManager?.isDualPoolActive() ? 'ddl' : 'read',
      op: 'acquire',
      caller: 'PostgresEngine.initSchema',
    });
    await conn`SELECT pg_advisory_lock(42)`;
    try {
      // Pre-schema bootstrap: add forward-referenced state the embedded schema
      // blob requires but that older brains don't have yet (issues #366/#375/
      // #378/#396 + #266/#357). Idempotent on fresh installs and modern brains.
      // Threads the DDL connection (same one holding the advisory lock above)
      // so bootstrap probes run on the locked connection — without this, the
      // probes ran through `this.sql` (the pooler/instance pool) outside the
      // lock, opening a concurrent-bootstrap race for Supabase users on the
      // transaction pooler. Codex P1 finding from v0.36 dreamy-thompson wave.
      await this.applyForwardReferenceBootstrap(conn);

      await conn.unsafe(sqlText);

      // Run any pending migrations automatically
      const { applied } = await runMigrations(this);
      if (applied > 0) {
        console.log(`  ${applied} migration(s) applied`);
      }

      // Post-migration schema verification: catches columns that migrations
      // defined but PgBouncer transaction-mode silently failed to create.
      // Self-heals missing columns via ALTER TABLE ADD COLUMN IF NOT EXISTS.
      const verify = await verifySchema(this);
      if (verify.healed.length > 0) {
        console.log(`  Schema verify: self-healed ${verify.healed.length} missing column(s)`);
      }

      // v0.30.1 (Fix 5): sweep zombie HNSW indexes (indisvalid=false) from
      // crashed CREATE INDEX CONCURRENTLY calls. Best-effort; errors logged
      // to stderr but never block engine.connect.
      try {
        const result = await dropZombieIndexes(this);
        if (result.dropped.length > 0) {
          console.log(`  HNSW sweep: dropped ${result.dropped.length} zombie index(es)`);
        }
      } catch { /* best-effort */ }
    } finally {
      await conn`SELECT pg_advisory_unlock(42)`;
      logConnectionEvent({
        pool: this.connectionManager?.isDualPoolActive() ? 'ddl' : 'read',
        op: 'release',
        caller: 'PostgresEngine.initSchema',
        duration_ms: Date.now() - t0,
      });
    }
  }

  /**
   * Bootstrap state that SCHEMA_SQL forward-references but that older brains
   * don't have yet. Mirror of `PGLiteEngine#applyForwardReferenceBootstrap`
   * in shape and intent. Currently covers:
   *
   *   - `sources` table + default seed (FK target of pages.source_id) — v0.18
   *   - `pages.source_id` column (indexed by `idx_pages_source_id`) — v0.18
   *   - `links.link_source` column (indexed by `idx_links_source`) — v0.13
   *   - `links.origin_page_id` column (indexed by `idx_links_origin`) — v0.13
   *   - `content_chunks.symbol_name` column (indexed by `idx_chunks_symbol_name`) — v0.19
   *   - `content_chunks.language` column (indexed by `idx_chunks_language`) — v0.19
   *   - `content_chunks.search_vector` + `parent_symbol_path` + `doc_comment`
   *     + `symbol_name_qualified` columns (indexed by `idx_chunks_search_vector`
   *     and `idx_chunks_symbol_qualified`) — v0.20 Cathedral II
   *   - `pages.deleted_at` column (indexed by `pages_deleted_at_purge_idx`) — v0.26.5
   *   - `mcp_request_log.agent_name` + `params` + `error_message` columns
   *     (indexed by `idx_mcp_log_agent_time`) — v0.26.3
   *   - `subagent_messages.provider_id` column (indexed by
   *     `idx_subagent_messages_provider`) — v0.27
   *
   * Keep this in sync with the PGLite version; covered by
   * `test/schema-bootstrap-coverage.test.ts` (PGLite side) and
   * `test/e2e/postgres-bootstrap.test.ts` (Postgres side).
   */
  private async applyForwardReferenceBootstrap(injectedConn?: postgres.Sql): Promise<void> {
    // Use the caller-provided connection (DDL pool, holding the advisory lock
    // from initSchema) when available — falls back to this.sql for backward
    // compatibility with any unit-test path that still calls bootstrap directly.
    // Production path always passes the DDL conn so bootstrap probes run inside
    // the same lock scope as SCHEMA_SQL replay.
    const conn = injectedConn ?? this.sql;

    // Single round-trip probe for every forward-reference target.
    // current_schema() resolves to whatever search_path the connection uses,
    // which matches schema-embedded.ts's `public.` references.
    const probeRows = await conn<{
      pages_exists: boolean;
      source_id_exists: boolean;
      deleted_at_exists: boolean;
      effective_date_exists: boolean;
      links_exists: boolean;
      link_source_exists: boolean;
      origin_page_id_exists: boolean;
      chunks_exists: boolean;
      symbol_name_exists: boolean;
      language_exists: boolean;
      search_vector_exists: boolean;
      embedding_image_exists: boolean;
      mcp_log_exists: boolean;
      agent_name_exists: boolean;
      subagent_messages_exists: boolean;
      subagent_provider_id_exists: boolean;
      ingest_log_exists: boolean;
      ingest_log_source_id_exists: boolean;
      files_exists: boolean;
      files_source_id_exists: boolean;
      files_page_id_exists: boolean;
      oauth_clients_exists: boolean;
      oauth_clients_source_id_exists: boolean;
      oauth_clients_federated_read_exists: boolean;
      sources_exists: boolean;
      sources_archived_exists: boolean;
      sources_archived_at_exists: boolean;
      sources_archive_expires_at_exists: boolean;
      pages_last_retrieved_at_exists: boolean;
      pages_access_count_exists: boolean;
      pages_confidence_score_exists: boolean;
      pages_ingested_via_exists: boolean;
      pages_ingested_at_exists: boolean;
      pages_source_uri_exists: boolean;
      pages_source_kind_exists: boolean;
    }[]>`
      SELECT
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = 'pages') AS pages_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'source_id') AS source_id_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'deleted_at') AS deleted_at_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'effective_date') AS effective_date_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = 'links') AS links_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'links' AND column_name = 'link_source') AS link_source_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'links' AND column_name = 'origin_page_id') AS origin_page_id_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = 'content_chunks') AS chunks_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'content_chunks' AND column_name = 'symbol_name') AS symbol_name_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'content_chunks' AND column_name = 'language') AS language_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'content_chunks' AND column_name = 'search_vector') AS search_vector_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'content_chunks' AND column_name = 'embedding_image') AS embedding_image_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = 'mcp_request_log') AS mcp_log_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'mcp_request_log' AND column_name = 'agent_name') AS agent_name_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = 'subagent_messages') AS subagent_messages_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'subagent_messages' AND column_name = 'provider_id') AS subagent_provider_id_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = 'ingest_log') AS ingest_log_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'ingest_log' AND column_name = 'source_id') AS ingest_log_source_id_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = 'files') AS files_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'files' AND column_name = 'source_id') AS files_source_id_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'files' AND column_name = 'page_id') AS files_page_id_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = 'oauth_clients') AS oauth_clients_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'oauth_clients' AND column_name = 'source_id') AS oauth_clients_source_id_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'oauth_clients' AND column_name = 'federated_read') AS oauth_clients_federated_read_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = 'sources') AS sources_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'sources' AND column_name = 'archived') AS sources_archived_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'sources' AND column_name = 'archived_at') AS sources_archived_at_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'sources' AND column_name = 'archive_expires_at') AS sources_archive_expires_at_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'last_retrieved_at') AS pages_last_retrieved_at_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'access_count') AS pages_access_count_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'confidence_score') AS pages_confidence_score_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'ingested_via') AS pages_ingested_via_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'ingested_at') AS pages_ingested_at_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'source_uri') AS pages_source_uri_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pages' AND column_name = 'source_kind') AS pages_source_kind_exists
    `;
    const probe = probeRows[0]!;

    const needsPagesBootstrap = probe.pages_exists && !probe.source_id_exists;
    const needsLinksBootstrap = probe.links_exists
      && (!probe.link_source_exists || !probe.origin_page_id_exists);
    const needsChunksBootstrap = probe.chunks_exists
      && (!probe.symbol_name_exists || !probe.language_exists || !probe.search_vector_exists);
    // v0.26.5: pages_deleted_at_purge_idx in SCHEMA_SQL crashes if the column
    // doesn't exist yet. Migration v34 also adds it, but bootstrap runs first.
    const needsPagesDeletedAt = probe.pages_exists && !probe.deleted_at_exists;
    // v0.26.3 (v33): idx_mcp_log_agent_time in SCHEMA_SQL needs agent_name col.
    const needsMcpLogBootstrap = probe.mcp_log_exists && !probe.agent_name_exists;
    // v0.27 (v36): idx_subagent_messages_provider in SCHEMA_SQL needs provider_id
    // (the SECOND column in the composite index `(job_id, provider_id)`).
    const needsSubagentProviderId = probe.subagent_messages_exists && !probe.subagent_provider_id_exists;
    // v0.27.1 (v39): idx_chunks_embedding_image partial HNSW in SCHEMA_SQL
    // references embedding_image. Use embedding_image_exists as the proxy for
    // both v39 columns; modality is added in the same migration.
    const needsChunksEmbeddingImage = probe.chunks_exists && !probe.embedding_image_exists;
    // v0.29.1 (v40 + v41): pages_coalesce_date_idx expression index in SCHEMA_SQL
    // references effective_date. Use effective_date_exists as the proxy for the
    // five v40 + v41 pages columns (emotional_weight, effective_date,
    // effective_date_source, import_filename, salience_touched_at).
    const needsPagesRecency = probe.pages_exists && !probe.effective_date_exists;
    // v0.31.2 (v50): idx_ingest_log_source_type_created in SCHEMA_SQL references
    // source_id. Old brains have ingest_log without source_id; bootstrap adds
    // the column before SCHEMA_SQL replay creates the index.
    const needsIngestLogSourceId = probe.ingest_log_exists && !probe.ingest_log_source_id_exists;
    // v0.18 (v18): files.source_id + files.page_id added; idx_files_source_id
    // and idx_files_page_id in SCHEMA_SQL crash without them.
    const needsFilesBootstrap = probe.files_exists
      && (!probe.files_source_id_exists || !probe.files_page_id_exists);
    // v0.34.1 (v60+v61+v65): oauth_clients.source_id + federated_read added;
    // FK to sources(id) + GIN index idx_oauth_clients_federated_read in
    // SCHEMA_SQL crash without them.
    const needsOauthClientsBootstrap = probe.oauth_clients_exists
      && (!probe.oauth_clients_source_id_exists || !probe.oauth_clients_federated_read_exists);
    // v0.26.5 (v34): sources.archived + archived_at + archive_expires_at added
    // for soft-delete lifecycle. SCHEMA_SQL's `CREATE TABLE IF NOT EXISTS sources`
    // is a no-op on pre-existing sources tables (won't add columns), so the
    // visibility filters in search/list_pages trip on old brains. Bootstrap
    // closes the gap before any visibility-filter SQL runs.
    const needsSourcesArchive = probe.sources_exists
      && (!probe.sources_archived_exists
          || !probe.sources_archived_at_exists
          || !probe.sources_archive_expires_at_exists);
    // v0.37.0 (v79): pages_last_retrieved_at_idx in SCHEMA_SQL references
    // last_retrieved_at. Pre-v79 brains crash without the column; bootstrap
    // adds it before SCHEMA_SQL replay creates the index. v79 runs later
    // via runMigrations and is idempotent.
    const needsPagesLastRetrievedAt = probe.pages_exists && !(probe as { pages_last_retrieved_at_exists?: boolean }).pages_last_retrieved_at_exists;
    const needsPagesMemTheta = probe.pages_exists
      && (!(probe as { pages_access_count_exists?: boolean }).pages_access_count_exists
          || !(probe as { pages_confidence_score_exists?: boolean }).pages_confidence_score_exists);
    // v0.38.0 (v80): provenance columns. Not referenced by any SCHEMA_SQL
    // index/FK today; bootstrap exists for the column-only forward-
    // reference class defense-in-depth.
    const probeProv = probe as {
      pages_ingested_via_exists?: boolean;
      pages_ingested_at_exists?: boolean;
      pages_source_uri_exists?: boolean;
      pages_source_kind_exists?: boolean;
    };
    const needsPagesProvenance = probe.pages_exists
      && (!probeProv.pages_ingested_via_exists
          || !probeProv.pages_ingested_at_exists
          || !probeProv.pages_source_uri_exists
          || !probeProv.pages_source_kind_exists);

    if (!needsPagesBootstrap && !needsLinksBootstrap && !needsChunksBootstrap
        && !needsPagesDeletedAt && !needsMcpLogBootstrap && !needsSubagentProviderId
        && !needsChunksEmbeddingImage && !needsPagesRecency
        && !needsIngestLogSourceId && !needsFilesBootstrap
        && !needsOauthClientsBootstrap && !needsSourcesArchive
        && !needsPagesLastRetrievedAt
        && !needsPagesMemTheta
        && !needsPagesProvenance) return;

    console.log('  Pre-v0.21 brain detected, applying forward-reference bootstrap');

    if (needsPagesBootstrap) {
      // Mirror schema-embedded.ts's `sources` shape so the subsequent
      // SCHEMA_SQL CREATE TABLE IF NOT EXISTS is a true no-op.
      // Archive columns (v34) are folded in here so a pre-v18 brain doesn't
      // need needsSourcesArchive to also fire — bootstrap creates a complete
      // v34-shape sources in one go. needsSourcesArchive then only fires on
      // the pre-v34 case (sources exists, archive cols don't).
      await conn.unsafe(`
        CREATE TABLE IF NOT EXISTS sources (
          id                 TEXT PRIMARY KEY,
          name               TEXT NOT NULL UNIQUE,
          local_path         TEXT,
          last_commit        TEXT,
          last_sync_at       TIMESTAMPTZ,
          config             JSONB NOT NULL DEFAULT '{}'::jsonb,
          archived           BOOLEAN NOT NULL DEFAULT FALSE,
          archived_at        TIMESTAMPTZ,
          archive_expires_at TIMESTAMPTZ,
          created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        INSERT INTO sources (id, name, config)
          VALUES ('default', 'default', '{"federated": true}'::jsonb)
          ON CONFLICT (id) DO NOTHING;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS source_id TEXT
          NOT NULL DEFAULT 'default' REFERENCES sources(id) ON DELETE CASCADE;
      `);
    }

    if (needsLinksBootstrap) {
      // v11 (links_provenance_columns) handles the CHECK constraint, the
      // UNIQUE swap, and the backfill. The bootstrap only adds enough state
      // for SCHEMA_SQL's `CREATE INDEX idx_links_source/origin` not to crash.
      // v11 runs later via runMigrations and is idempotent.
      await conn.unsafe(`
        ALTER TABLE links ADD COLUMN IF NOT EXISTS link_source TEXT;
        ALTER TABLE links ADD COLUMN IF NOT EXISTS origin_page_id INTEGER
          REFERENCES pages(id) ON DELETE SET NULL;
      `);
    }

    if (needsChunksBootstrap) {
      // v26 (content_chunks_code_metadata) adds symbol_name + language; v27
      // (Cathedral II) adds parent_symbol_path + doc_comment +
      // symbol_name_qualified + search_vector. The schema blob has indexes
      // (idx_chunks_search_vector line 141, idx_chunks_symbol_qualified
      // line 142) that need the v27 columns to exist before they run.
      // v26 + v27 run later via runMigrations and are idempotent.
      await conn.unsafe(`
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS language TEXT;
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS symbol_name TEXT;
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS parent_symbol_path TEXT[];
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS doc_comment TEXT;
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS symbol_name_qualified TEXT;
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS search_vector TSVECTOR;
      `);
    }

    if (needsPagesDeletedAt) {
      // v34 (destructive_guard_columns) adds the column + sources columns +
      // partial purge index. Bootstrap only adds enough for SCHEMA_SQL's
      // `CREATE INDEX pages_deleted_at_purge_idx ... WHERE deleted_at IS NOT NULL`
      // not to crash. v34 runs later via runMigrations and is idempotent.
      await conn.unsafe(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
      `);
    }

    if (needsMcpLogBootstrap) {
      // v33 (admin_dashboard_columns_v0_26_3) adds agent_name + params +
      // error_message to mcp_request_log. SCHEMA_SQL's
      // `CREATE INDEX idx_mcp_log_agent_time ON mcp_request_log(agent_name,...)`
      // crashes without agent_name. v33 runs later via runMigrations and is
      // idempotent (and also handles backfill).
      await conn.unsafe(`
        ALTER TABLE mcp_request_log ADD COLUMN IF NOT EXISTS agent_name TEXT;
        ALTER TABLE mcp_request_log ADD COLUMN IF NOT EXISTS params JSONB;
        ALTER TABLE mcp_request_log ADD COLUMN IF NOT EXISTS error_message TEXT;
      `);
    }

    if (needsSubagentProviderId) {
      // v36 (subagent_provider_neutral_persistence_v0_27) adds provider_id +
      // schema_version on subagent_messages and subagent_tool_executions.
      // SCHEMA_SQL's `CREATE INDEX idx_subagent_messages_provider ON
      // subagent_messages (job_id, provider_id)` crashes without provider_id
      // (composite-index second column). v36 runs later via runMigrations and
      // is idempotent.
      await conn.unsafe(`
        ALTER TABLE subagent_messages ADD COLUMN IF NOT EXISTS provider_id TEXT;
      `);
    }

    if (needsChunksEmbeddingImage) {
      // v39 (multimodal_dual_column_v0_27_1) adds modality + embedding_image
      // columns to content_chunks plus a partial HNSW index that references
      // embedding_image. Bootstrap mirrors enough state for SCHEMA_SQL's
      // `CREATE INDEX idx_chunks_embedding_image ... WHERE embedding_image IS NOT NULL`
      // not to crash. v39 runs later via runMigrations and is idempotent.
      await conn.unsafe(`
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS modality TEXT NOT NULL DEFAULT 'text';
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS embedding_image vector(1024);
      `);
    }

    if (needsPagesRecency) {
      // v40 (pages_emotional_weight) adds emotional_weight; v41
      // (pages_recency_columns) adds effective_date + effective_date_source +
      // import_filename + salience_touched_at and the
      // `pages_coalesce_date_idx ON pages ((COALESCE(effective_date, updated_at)))`
      // expression index. SCHEMA_SQL's CREATE INDEX for that expression crashes
      // before v41 runs. Bootstrap adds all five additive columns; v40 + v41
      // run later via runMigrations and are idempotent.
      await conn.unsafe(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS emotional_weight      REAL NOT NULL DEFAULT 0.0;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS effective_date        TIMESTAMPTZ;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS effective_date_source TEXT;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS import_filename       TEXT;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS salience_touched_at   TIMESTAMPTZ;
      `);
    }

    if (needsIngestLogSourceId) {
      // v50 (ingest_log_source_id) adds source_id +
      // idx_ingest_log_source_type_created composite index. SCHEMA_SQL's
      // CREATE INDEX (source_id, source_type, created_at) crashes without
      // source_id. Bootstrap adds the column with NOT NULL DEFAULT 'default'
      // so the index can build cleanly.
      await conn.unsafe(`
        ALTER TABLE ingest_log ADD COLUMN IF NOT EXISTS source_id TEXT NOT NULL DEFAULT 'default';
      `);
    }

    if (needsFilesBootstrap) {
      // v18 (files_provenance_columns) adds source_id + page_id to files plus
      // idx_files_source_id and idx_files_page_id in SCHEMA_SQL. Pre-v18 brains
      // crash on the CREATE INDEX. Bootstrap adds both columns; v18 runs later
      // via runMigrations and is idempotent.
      await conn.unsafe(`
        ALTER TABLE files ADD COLUMN IF NOT EXISTS source_id TEXT
          NOT NULL DEFAULT 'default' REFERENCES sources(id) ON DELETE CASCADE;
        ALTER TABLE files ADD COLUMN IF NOT EXISTS page_id INTEGER
          REFERENCES pages(id) ON DELETE SET NULL;
      `);
    }

    if (needsOauthClientsBootstrap) {
      // v60+v61+v65 (oauth_clients_source_id_fk, oauth_clients_federated_read_column,
      // oauth_clients_federated_read_gin_index) add source_id + federated_read
      // and the GIN index idx_oauth_clients_federated_read. SCHEMA_SQL's
      // FK + index references crash on pre-v60 brains. Bootstrap mirrors the
      // v60+v61 column shape; v60-v65 run later via runMigrations and are
      // idempotent (and handle backfill + the v64 RESTRICT-flip).
      await conn.unsafe(`
        ALTER TABLE oauth_clients ADD COLUMN IF NOT EXISTS source_id TEXT
          DEFAULT 'default' REFERENCES sources(id) ON DELETE SET NULL;
        ALTER TABLE oauth_clients ADD COLUMN IF NOT EXISTS federated_read TEXT[]
          NOT NULL DEFAULT '{}';
      `);
    }

    if (needsSourcesArchive) {
      // v34 (destructive_guard_columns) promotes archive lifecycle from JSONB
      // config to real columns on sources. SCHEMA_SQL's `CREATE TABLE IF NOT EXISTS
      // sources` is a no-op against an existing pre-v34 sources table, so the
      // column-add never lands until the v34 migration runs. v34's UPDATE
      // statements + downstream visibility filters (search/query/list_pages)
      // need the columns to exist on the table schema. Bootstrap adds the
      // three columns; v34 runs later via runMigrations and is idempotent
      // (and handles JSONB → column backfill).
      await conn.unsafe(`
        ALTER TABLE sources ADD COLUMN IF NOT EXISTS archived BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE sources ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
        ALTER TABLE sources ADD COLUMN IF NOT EXISTS archive_expires_at TIMESTAMPTZ;
      `);
    }

    if (needsPagesLastRetrievedAt) {
      // v79 (pages_last_retrieved_at): adds the real stale-page signal column
      // + full B-tree index. SCHEMA_SQL's CREATE INDEX
      // pages_last_retrieved_at_idx crashes without the column. v79 runs
      // later via runMigrations and is idempotent.
      await conn.unsafe(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS last_retrieved_at TIMESTAMPTZ;
      `);
    }

    if (needsPagesMemTheta) {
      // v89 (pages_memtheta_columns): keep pre-column brains readable once
      // search begins selecting these ranking attributes directly.
      await conn.unsafe(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS access_count INT NOT NULL DEFAULT 0;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS confidence_score REAL NOT NULL DEFAULT 1.0;
      `);
    }

    if (needsPagesProvenance) {
      // v81 (pages_provenance_columns): four nullable columns added by the
      // v0.38 ingestion cathedral. No SCHEMA_SQL index/FK references them
      // today; bootstrap exists defense-in-depth so future schema work that
      // does reference them doesn't wedge pre-v81 brains.
      await conn.unsafe(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS ingested_via TEXT;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS source_uri TEXT;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS source_kind TEXT;
      `);
    }
  }

  async transaction<T>(fn: (engine: BrainEngine) => Promise<T>): Promise<T> {
    const conn = this._sql || db.getConnection();
    return conn.begin(async (tx) => {
      // Create a scoped engine with tx as its connection, no shared state mutation
      const txEngine = Object.create(this) as PostgresEngine;
      Object.defineProperty(txEngine, 'sql', { get: () => tx });
      Object.defineProperty(txEngine, '_sql', { value: tx as unknown as ReturnType<typeof postgres>, writable: false });
      return fn(txEngine);
    }) as Promise<T>;
  }

  async withReservedConnection<T>(fn: (conn: ReservedConnection) => Promise<T>): Promise<T> {
    const pool = this._sql || db.getConnection();
    const reserved = await pool.reserve();
    try {
      const conn: ReservedConnection = {
        async executeRaw<R = Record<string, unknown>>(query: string, params?: unknown[]): Promise<R[]> {
          const rows = params === undefined
            ? await reserved.unsafe(query)
            : await reserved.unsafe(query, params as Parameters<typeof reserved.unsafe>[1]);
          return rows as unknown as R[];
        },
      };
      return await fn(conn);
    } finally {
      reserved.release();
    }
  }

  // Pages CRUD

  async executeRaw<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]> {
    const conn = this.sql;
    return conn.unsafe(sql, params as Parameters<typeof conn.unsafe>[1]) as unknown as T[];
    // Pre-#406 behavior: throw on any error including connection death.
    // Per-call auto-retry is not safe here because executeRaw is also used
    // for non-transactional mutations (DELETE/UPDATE/INSERT in sources.ts,
    // ALTER TABLE in migrations) where retrying after a connection-mid-statement
    // death can phantom-write a row that already committed on the server.
    // Recovery instead happens at the supervisor level: the watchdog detects
    // 3 consecutive health-check failures and calls engine.reconnect() to
    // swap in a fresh pool. See db.ts setSessionDefaults / supervisor.ts.
  }
}


// Method mixins (BET-Y1Q3-T6-04 split): injected at module load.
import { postgresPagesMethods } from './postgres-engine-pages.ts';
import { postgresLinksMethods } from './postgres-engine-links.ts';
import { postgresTakesMethods } from './postgres-engine-takes.ts';
Object.assign(PostgresEngine.prototype, postgresPagesMethods);
Object.assign(PostgresEngine.prototype, postgresLinksMethods);
Object.assign(PostgresEngine.prototype, postgresTakesMethods);

/**
 * Interface merge: PostgresEngine's full method set is provided by the mixin
 * modules above (Object.assign at load). Declaring the BrainEngine surface
 * here keeps `implements BrainEngine`-style call sites type-correct.
 */
export interface PostgresEngine extends BrainEngine {}
