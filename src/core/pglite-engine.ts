import { PGlite } from '@electric-sql/pglite';
import { vector } from '@electric-sql/pglite/vector';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import type { Transaction } from '@electric-sql/pglite';
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
import { MAX_SEARCH_LIMIT, clampSearchLimit } from './engine.ts';
import { runMigrations } from './migrate.ts';
import { PGLITE_SCHEMA_SQL, getPGLiteSchema } from './pglite-schema.ts';
import { DEFAULT_EMBEDDING_MODEL, DEFAULT_EMBEDDING_DIMENSIONS } from './ai/defaults.ts';
import { acquireLock, releaseLock, type LockHandle } from './pglite-lock.ts';
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
  DomainBankSampleOpts, CorpusSampleOpts, DomainBankRow,
} from './types.ts';
import { validateSlug, contentHash, rowToPage, rowToChunk, rowToSearchResult, takeRowToTake } from './utils.ts';
import { deriveResolutionTuple, finalizeScorecard } from './takes-resolution.ts';
import { normalizeWeightForStorage } from './takes-fence.ts';
import { GBrainError, PAGE_SORT_SQL } from './types.ts';
import { computeAnomaliesFromBuckets } from './cycle/anomaly.ts';
import { resolveBoostMap, resolveHardExcludes } from './search/source-boost.ts';
import { buildSourceFactorCase, buildHardExcludeClause, buildVisibilityClause, buildRecencyComponentSql } from './search/sql-ranking.ts';
import {
  normalizeEngineColumn,
  buildVectorCastFragment,
  quoteIdentifier,
  COLUMN_NAME_REGEX,
  EmbeddingColumnNotRegisteredError,
} from './search/embedding-column.ts';
import { hasCJK, escapeLikePattern } from './cjk.ts';

type PGLiteDB = PGlite;

// Tier 3 snapshot fast-restore. Reads a tar dump produced by
// `bun run scripts/build-pglite-snapshot.ts`. Snapshot is matched against
// the current MIGRATIONS hash via a sidecar `.version` file; on mismatch we
// silently fall through to a normal initSchema (snapshot is just an
// optimization, never authoritative).
let _snapshotWarnLogged = false;
function tryLoadSnapshot(snapshotPath: string): Blob | null {
  try {
    // Lazy require so production builds without these imports don't crash.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const fs = require('node:fs') as typeof import('node:fs');
    const crypto = require('node:crypto') as typeof import('node:crypto');
    const { MIGRATIONS } = require('./migrate.ts') as typeof import('./migrate.ts');
    const { PGLITE_SCHEMA_SQL } = require('./pglite-schema.ts') as typeof import('./pglite-schema.ts');

    if (!fs.existsSync(snapshotPath)) {
      if (!_snapshotWarnLogged) {
        // eslint-disable-next-line no-console
        console.warn(`[pglite] GBRAIN_PGLITE_SNAPSHOT set but file missing: ${snapshotPath} — using normal init.`);
        _snapshotWarnLogged = true;
      }
      return null;
    }
    const versionPath = snapshotPath.replace(/\.tar(?:\.gz)?$/, '.version');
    if (!fs.existsSync(versionPath)) {
      if (!_snapshotWarnLogged) {
        // eslint-disable-next-line no-console
        console.warn(`[pglite] snapshot version file missing: ${versionPath} — using normal init.`);
        _snapshotWarnLogged = true;
      }
      return null;
    }
    const expectedHash = computeSnapshotSchemaHash(MIGRATIONS, PGLITE_SCHEMA_SQL, crypto);
    const actualHash = fs.readFileSync(versionPath, 'utf8').trim();
    if (expectedHash !== actualHash) {
      if (!_snapshotWarnLogged) {
        // eslint-disable-next-line no-console
        console.warn(`[pglite] snapshot stale (schema hash mismatch) — using normal init. Rebuild with: bun run build:pglite-snapshot`);
        _snapshotWarnLogged = true;
      }
      return null;
    }
    const buf = fs.readFileSync(snapshotPath);
    return new Blob([buf]);
  } catch {
    // Any failure -> fall through to normal init. Never block tests.
    return null;
  }
}

export function computeSnapshotSchemaHash(
  migrations: Array<{ version: number; name: string; sql?: string; sqlFor?: { pglite?: string } }>,
  schemaSQL: string,
  crypto: typeof import('node:crypto'),
): string {
  const hash = crypto.createHash('sha256');
  hash.update('schema:');
  hash.update(schemaSQL);
  hash.update('\nmigrations:\n');
  for (const m of migrations) {
    hash.update(String(m.version));
    hash.update('\t');
    hash.update(m.name);
    hash.update('\t');
    hash.update(m.sql ?? '');
    hash.update('\t');
    hash.update(m.sqlFor?.pglite ?? '');
    hash.update('\n');
  }
  return hash.digest('hex');
}

export class PGLiteEngine implements BrainEngine {
  readonly kind = 'pglite' as const;
  private _db: PGLiteDB | null = null;
  private _lock: LockHandle | null = null;
  // Tier 3: when GBRAIN_PGLITE_SNAPSHOT loaded a post-initSchema state into
  // PGlite.create(loadDataDir), initSchema is a no-op (schema is already
  // present + migrations already applied). Saves ~1-3s per fresh test PGLite.
  private _snapshotLoaded = false;

  get db(): PGLiteDB {
    if (!this._db) throw new Error('PGLite not connected. Call connect() first.');
    return this._db;
  }

  // Lifecycle
  async connect(config: EngineConfig): Promise<void> {
    const dataDir = config.database_path || undefined; // undefined = in-memory

    // Acquire file lock to prevent concurrent PGLite access (crashes with Aborted())
    this._lock = await acquireLock(dataDir);

    if (!this._lock.acquired) {
      throw new Error('Could not acquire PGLite lock. Another gbrain process is using the database.');
    }

    // Tier 3: optional snapshot fast-restore. Only applies to in-memory
    // engines (no persistent dataDir). The snapshot was built from a fresh
    // `initSchema()` run; if the version file matches the current MIGRATIONS
    // hash, load the dump and skip the schema replay. Mismatch or missing
    // file silently falls back to normal init.
    let loadDataDir: Blob | undefined;
    if (!dataDir && process.env.GBRAIN_PGLITE_SNAPSHOT) {
      const snapshotResult = tryLoadSnapshot(process.env.GBRAIN_PGLITE_SNAPSHOT);
      if (snapshotResult) {
        loadDataDir = snapshotResult;
        this._snapshotLoaded = true;
      }
    }

    try {
      this._db = await PGlite.create({
        dataDir,
        loadDataDir,
        extensions: { vector, pg_trgm },
      });
    } catch (err) {
      // v0.13.1: any PGLite.create() failure becomes actionable. Most commonly
      // this is the macOS 26.3 WASM bug (#223). We deliberately do NOT suggest
      // "missing migrations" as a cause — migrations run AFTER create(), so a
      // create-time abort has nothing to do with them. Nest the original error
      // message so debugging isn't erased.
      const original = err instanceof Error ? err.message : String(err);
      const wrapped = new Error(
        `PGLite failed to initialize its WASM runtime.\n` +
        `  This is most commonly the macOS 26.3 WASM bug: https://github.com/garrytan/gbrain/issues/223\n` +
        `  Run \`gbrain doctor\` for a full diagnosis.\n` +
        `  Original error: ${original}`
      );
      // Release the lock so a fresh process can try again; leaking the lock
      // here turns a recoverable init error into a stuck-brain state.
      if (this._lock?.acquired) {
        try { await releaseLock(this._lock); } catch { /* ignore cleanup error */ }
        this._lock = null;
      }
      throw wrapped;
    }
  }

  async disconnect(): Promise<void> {
    if (this._db) {
      await this._db.close();
      this._db = null;
    }
    if (this._lock?.acquired) {
      await releaseLock(this._lock);
      this._lock = null;
    }
  }

  async initSchema(): Promise<void> {
    // Tier 3: snapshot was loaded into PGlite — schema + migrations already
    // applied. Nothing to do. Returns immediately.
    if (this._snapshotLoaded) {
      return;
    }
    // Pre-schema bootstrap: add forward-referenced state the embedded schema
    // blob requires but that older brains don't have yet (issues #366/#375/
    // #378/#396 + #266/#357). Bootstrap is idempotent and a no-op on fresh
    // installs and modern brains.
    await this.applyForwardReferenceBootstrap();

    // Resolve embedding dim/model from gateway. v0.37 fix wave: fallbacks
    // track the canonical defaults in `ai/defaults.ts` (zeroentropyai:zembed-1
    // / 1280d) instead of the stale v0.13 OpenAI literals, AND we store the
    // full `provider:model` string in the DB config table — consumers like
    // ze-switch, doctor, and recommendation-context expect the provider
    // prefix. (Round-1 CDX-4 + A.8.)
    let dims: number = DEFAULT_EMBEDDING_DIMENSIONS;
    let model: string = DEFAULT_EMBEDDING_MODEL;
    try {
      const gw = await import('./ai/gateway.ts');
      dims = gw.getEmbeddingDimensions();
      model = gw.getEmbeddingModel() || model;
    } catch { /* gateway not configured — use defaults */ }

    await this.db.exec(getPGLiteSchema(dims, model));

    const { applied } = await runMigrations(this);
    if (applied > 0) {
      console.log(`  ${applied} migration(s) applied`);
    }
  }

  /**
   * Bootstrap state that PGLITE_SCHEMA_SQL forward-references but that older
   * brains don't have yet. Currently covers:
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
   * **Maintenance contract:** when a future migration adds a column-with-index
   * or new-table-with-FK referenced by PGLITE_SCHEMA_SQL, extend this method
   * AND `test/schema-bootstrap-coverage.test.ts`'s `REQUIRED_BOOTSTRAP_COVERAGE`.
   * The coverage test fails loudly if the bootstrap drifts behind the schema.
   */
  private async applyForwardReferenceBootstrap(): Promise<void> {
    // Single round-trip probe for every forward-reference target.
    const { rows } = await this.db.query(`
      SELECT
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='pages') AS pages_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='source_id') AS source_id_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='deleted_at') AS deleted_at_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='links') AS links_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='links' AND column_name='link_source') AS link_source_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='links' AND column_name='origin_page_id') AS origin_page_id_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='content_chunks') AS chunks_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='content_chunks' AND column_name='symbol_name') AS symbol_name_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='content_chunks' AND column_name='language') AS language_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='content_chunks' AND column_name='search_vector') AS search_vector_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='content_chunks' AND column_name='embedding_image') AS embedding_image_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='effective_date') AS effective_date_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='mcp_request_log') AS mcp_log_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='mcp_request_log' AND column_name='agent_name') AS agent_name_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='subagent_messages') AS subagent_messages_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='subagent_messages' AND column_name='provider_id') AS subagent_provider_id_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='ingest_log') AS ingest_log_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='ingest_log' AND column_name='source_id') AS ingest_log_source_id_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='files') AS files_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='files' AND column_name='source_id') AS files_source_id_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='files' AND column_name='page_id') AS files_page_id_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='oauth_clients') AS oauth_clients_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='oauth_clients' AND column_name='source_id') AS oauth_clients_source_id_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='oauth_clients' AND column_name='federated_read') AS oauth_clients_federated_read_exists,
        EXISTS (SELECT 1 FROM information_schema.tables
                WHERE table_schema='public' AND table_name='sources') AS sources_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='sources' AND column_name='archived') AS sources_archived_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='sources' AND column_name='archived_at') AS sources_archived_at_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='sources' AND column_name='archive_expires_at') AS sources_archive_expires_at_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='last_retrieved_at') AS pages_last_retrieved_at_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='access_count') AS pages_access_count_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='confidence_score') AS pages_confidence_score_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='ingested_via') AS pages_ingested_via_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='ingested_at') AS pages_ingested_at_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='source_uri') AS pages_source_uri_exists,
        EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='pages' AND column_name='source_kind') AS pages_source_kind_exists
    `);
    const probe = rows[0] as {
      pages_exists: boolean;
      source_id_exists: boolean;
      deleted_at_exists: boolean;
      links_exists: boolean;
      link_source_exists: boolean;
      origin_page_id_exists: boolean;
      chunks_exists: boolean;
      symbol_name_exists: boolean;
      language_exists: boolean;
      search_vector_exists: boolean;
      embedding_image_exists: boolean;
      effective_date_exists: boolean;
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
    };

    const needsPagesBootstrap = probe.pages_exists && !probe.source_id_exists;
    const needsLinksBootstrap = probe.links_exists
      && (!probe.link_source_exists || !probe.origin_page_id_exists);
    const needsChunksBootstrap = probe.chunks_exists
      && (!probe.symbol_name_exists || !probe.language_exists || !probe.search_vector_exists);
    const needsPagesDeletedAt = probe.pages_exists && !probe.deleted_at_exists;
    // v0.27.1 — partial HNSW idx_chunks_embedding_image references this column.
    const needsChunksEmbeddingImage = probe.chunks_exists && !probe.embedding_image_exists;
    // v0.26.3 (v33): idx_mcp_log_agent_time in PGLITE_SCHEMA_SQL needs agent_name col.
    const needsMcpLogBootstrap = probe.mcp_log_exists && !probe.agent_name_exists;
    // v0.27 (v36): idx_subagent_messages_provider in PGLITE_SCHEMA_SQL needs
    // provider_id (the SECOND column in the composite index `(job_id, provider_id)`).
    const needsSubagentProviderId = probe.subagent_messages_exists && !probe.subagent_provider_id_exists;
    // v0.29.1 (v40 + v41): pages_coalesce_date_idx expression index in
    // PGLITE_SCHEMA_SQL references effective_date. Use effective_date_exists
    // as the proxy for the five v40 + v41 pages columns.
    const needsPagesRecency = probe.pages_exists && !probe.effective_date_exists;
    // v0.31.2 (v50): idx_ingest_log_source_type_created in PGLITE_SCHEMA_SQL
    // references source_id. Old brains have ingest_log without source_id;
    // bootstrap adds the column before SCHEMA_SQL replay creates the index.
    const needsIngestLogSourceId = probe.ingest_log_exists && !probe.ingest_log_source_id_exists;
    // v0.18 (v18): files.source_id + files.page_id added; idx_files_source_id
    // and idx_files_page_id in PGLITE_SCHEMA_SQL crash without them.
    const needsFilesBootstrap = probe.files_exists
      && (!probe.files_source_id_exists || !probe.files_page_id_exists);
    // v0.34.1 (v60+v61+v65): oauth_clients.source_id + federated_read added;
    // FK to sources(id) + GIN index idx_oauth_clients_federated_read in
    // PGLITE_SCHEMA_SQL crash without them.
    const needsOauthClientsBootstrap = probe.oauth_clients_exists
      && (!probe.oauth_clients_source_id_exists || !probe.oauth_clients_federated_read_exists);
    // v0.26.5 (v34): sources.archived + archived_at + archive_expires_at added
    // for soft-delete lifecycle. Not directly referenced by indexes BUT
    // PGLITE_SCHEMA_SQL's `CREATE TABLE IF NOT EXISTS sources` is a no-op on
    // pre-existing sources tables (won't add columns), so visibility filters
    // referencing these columns trip on old brains. The bootstrap closes the
    // gap before any visibility-filter SQL runs.
    const needsSourcesArchive = probe.sources_exists
      && (!probe.sources_archived_exists
          || !probe.sources_archived_at_exists
          || !probe.sources_archive_expires_at_exists);
    // v0.37.0 (v79): pages_last_retrieved_at_idx in PGLITE_SCHEMA_SQL
    // references last_retrieved_at. Pre-v79 brains crash without the column.
    const needsPagesLastRetrievedAt = probe.pages_exists && !probe.pages_last_retrieved_at_exists;
    // v0.38.x MemTheta: search queries project pages.access_count +
    // pages.confidence_score directly. Add both before reads hit old brains.
    const needsPagesMemTheta = probe.pages_exists
      && (!probe.pages_access_count_exists || !probe.pages_confidence_score_exists);
    // v0.38.0 (v80): provenance columns on pages. Not referenced by any
    // SCHEMA_SQL index or FK today, but added defense-in-depth so future
    // schema work that references them doesn't wedge pre-v80 brains.
    const needsPagesProvenance = probe.pages_exists
      && (!probe.pages_ingested_via_exists
          || !probe.pages_ingested_at_exists
          || !probe.pages_source_uri_exists
          || !probe.pages_source_kind_exists);

    // Fresh installs (no tables yet) and modern brains both no-op.
    if (!needsPagesBootstrap && !needsLinksBootstrap && !needsChunksBootstrap
        && !needsPagesDeletedAt && !needsChunksEmbeddingImage
        && !needsMcpLogBootstrap && !needsSubagentProviderId
        && !needsPagesRecency && !needsIngestLogSourceId
        && !needsFilesBootstrap && !needsOauthClientsBootstrap
        && !needsSourcesArchive && !needsPagesLastRetrievedAt
        && !needsPagesMemTheta
        && !needsPagesProvenance) return;

    console.log('  Pre-v0.21 brain detected, applying forward-reference bootstrap');

    if (needsPagesBootstrap) {
      // Mirror schema-embedded.ts shape for `sources` so the subsequent
      // PGLITE_SCHEMA_SQL CREATE TABLE IF NOT EXISTS is a true no-op.
      // Archive columns (v34) are folded in here so a pre-v18 brain doesn't
      // need needsSourcesArchive to also fire — bootstrap creates a complete
      // v34-shape sources in one go. needsSourcesArchive then only fires on
      // the pre-v34 case (sources exists, archive cols don't).
      await this.db.exec(`
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
      // v11 (links_provenance_columns) is responsible for the CHECK constraint
      // and backfill. The bootstrap only adds enough state for SCHEMA_SQL's
      // `CREATE INDEX idx_links_source/origin` not to crash. v11 runs later
      // via runMigrations and is idempotent (`IF NOT EXISTS` everywhere).
      await this.db.exec(`
        ALTER TABLE links ADD COLUMN IF NOT EXISTS link_source TEXT;
        ALTER TABLE links ADD COLUMN IF NOT EXISTS origin_page_id INTEGER
          REFERENCES pages(id) ON DELETE SET NULL;
      `);
    }

    if (needsChunksBootstrap) {
      // v26 (content_chunks_code_metadata) adds symbol_name + language; v27
      // (Cathedral II) adds parent_symbol_path + doc_comment +
      // symbol_name_qualified + search_vector. PGLITE_SCHEMA_SQL has indexes
      // (idx_chunks_search_vector, idx_chunks_symbol_qualified) that need the
      // v27 columns to exist before they run. v26 + v27 run later via
      // runMigrations and are idempotent.
      await this.db.exec(`
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
      // partial purge index. Bootstrap only adds enough for PGLITE_SCHEMA_SQL's
      // `CREATE INDEX pages_deleted_at_purge_idx ... WHERE deleted_at IS NOT NULL`
      // not to crash. v34 runs later via runMigrations and is idempotent.
      await this.db.exec(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
      `);
    }

    if (needsChunksEmbeddingImage) {
      // v39 (multimodal_dual_column_v0_27_1) adds modality + embedding_image
      // columns to content_chunks plus the partial HNSW index that references
      // the column. Bootstrap mirrors enough for PGLITE_SCHEMA_SQL's
      // `CREATE INDEX idx_chunks_embedding_image ... WHERE embedding_image IS NOT NULL`
      // not to crash. v39 runs later via runMigrations and is idempotent.
      await this.db.exec(`
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS modality TEXT NOT NULL DEFAULT 'text';
        ALTER TABLE content_chunks ADD COLUMN IF NOT EXISTS embedding_image vector(1024);
      `);
    }

    if (needsMcpLogBootstrap) {
      // v33 (admin_dashboard_columns_v0_26_3) adds agent_name + params +
      // error_message to mcp_request_log. PGLITE_SCHEMA_SQL's
      // `CREATE INDEX idx_mcp_log_agent_time ON mcp_request_log(agent_name,...)`
      // crashes without agent_name. v33 runs later via runMigrations and is
      // idempotent (and also handles backfill).
      await this.db.exec(`
        ALTER TABLE mcp_request_log ADD COLUMN IF NOT EXISTS agent_name TEXT;
        ALTER TABLE mcp_request_log ADD COLUMN IF NOT EXISTS params JSONB;
        ALTER TABLE mcp_request_log ADD COLUMN IF NOT EXISTS error_message TEXT;
      `);
    }

    if (needsSubagentProviderId) {
      // v36 (subagent_provider_neutral_persistence_v0_27) adds provider_id +
      // schema_version on subagent_messages and subagent_tool_executions.
      // PGLITE_SCHEMA_SQL's `CREATE INDEX idx_subagent_messages_provider ON
      // subagent_messages (job_id, provider_id)` crashes without provider_id
      // (composite-index second column). v36 runs later via runMigrations and
      // is idempotent.
      await this.db.exec(`
        ALTER TABLE subagent_messages ADD COLUMN IF NOT EXISTS provider_id TEXT;
      `);
    }

    if (needsPagesRecency) {
      // v40 (pages_emotional_weight) adds emotional_weight; v41
      // (pages_recency_columns) adds effective_date + effective_date_source +
      // import_filename + salience_touched_at and the
      // `pages_coalesce_date_idx ON pages ((COALESCE(effective_date, updated_at)))`
      // expression index. PGLITE_SCHEMA_SQL's CREATE INDEX for that expression
      // crashes before v41 runs. Bootstrap adds all five additive columns;
      // v40 + v41 run later via runMigrations and are idempotent.
      await this.db.exec(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS emotional_weight      REAL NOT NULL DEFAULT 0.0;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS effective_date        TIMESTAMPTZ;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS effective_date_source TEXT;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS import_filename       TEXT;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS salience_touched_at   TIMESTAMPTZ;
      `);
    }

    if (needsIngestLogSourceId) {
      // v50 (ingest_log_source_id) adds source_id + the
      // idx_ingest_log_source_type_created composite index.
      // PGLITE_SCHEMA_SQL's CREATE INDEX (source_id, source_type, created_at)
      // crashes without source_id. Bootstrap adds the column with NOT NULL
      // DEFAULT 'default' so the index can build cleanly.
      await this.db.exec(`
        ALTER TABLE ingest_log ADD COLUMN IF NOT EXISTS source_id TEXT NOT NULL DEFAULT 'default';
      `);
    }

    if (needsFilesBootstrap) {
      // v18 (files_provenance_columns) adds source_id + page_id to files plus
      // idx_files_source_id and idx_files_page_id in PGLITE_SCHEMA_SQL. Pre-v18
      // brains crash on the CREATE INDEX. Bootstrap adds both columns; v18
      // runs later via runMigrations and is idempotent.
      await this.db.exec(`
        ALTER TABLE files ADD COLUMN IF NOT EXISTS source_id TEXT
          NOT NULL DEFAULT 'default' REFERENCES sources(id) ON DELETE CASCADE;
        ALTER TABLE files ADD COLUMN IF NOT EXISTS page_id INTEGER
          REFERENCES pages(id) ON DELETE SET NULL;
      `);
    }

    if (needsOauthClientsBootstrap) {
      // v60+v61+v65 (oauth_clients_source_id_fk, oauth_clients_federated_read_column,
      // oauth_clients_federated_read_gin_index) add source_id + federated_read
      // and the GIN index idx_oauth_clients_federated_read. PGLITE_SCHEMA_SQL's
      // FK + index references crash on pre-v60 brains. Bootstrap mirrors the
      // v60+v61 column shape; v60-v65 run later via runMigrations and are
      // idempotent (and handle backfill + RESTRICT-flip).
      await this.db.exec(`
        ALTER TABLE oauth_clients ADD COLUMN IF NOT EXISTS source_id TEXT
          DEFAULT 'default' REFERENCES sources(id) ON DELETE SET NULL;
        ALTER TABLE oauth_clients ADD COLUMN IF NOT EXISTS federated_read TEXT[]
          NOT NULL DEFAULT '{}';
      `);
    }

    if (needsSourcesArchive) {
      // v34 (destructive_guard_columns) promotes archive lifecycle from JSONB
      // config to real columns on sources. PGLITE_SCHEMA_SQL's
      // `CREATE TABLE IF NOT EXISTS sources` is a no-op against an existing
      // pre-v34 sources table, so the column-add never lands until the v34
      // migration runs. v34's UPDATE statements + downstream visibility filters
      // (search/query/list_pages) need the columns to exist on the table
      // schema. Bootstrap adds the three columns; v34 runs later via
      // runMigrations and is idempotent (and handles JSONB → column backfill).
      await this.db.exec(`
        ALTER TABLE sources ADD COLUMN IF NOT EXISTS archived BOOLEAN NOT NULL DEFAULT FALSE;
        ALTER TABLE sources ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
        ALTER TABLE sources ADD COLUMN IF NOT EXISTS archive_expires_at TIMESTAMPTZ;
      `);
    }

    if (needsPagesLastRetrievedAt) {
      // v79 (pages_last_retrieved_at): adds the stale-page signal column +
      // full B-tree index. PGLITE_SCHEMA_SQL's CREATE INDEX
      // pages_last_retrieved_at_idx crashes without the column. v79 runs
      // later via runMigrations and is idempotent.
      await this.db.exec(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS last_retrieved_at TIMESTAMPTZ;
      `);
    }

    if (needsPagesMemTheta) {
      // v89 (pages_memtheta_columns): keep old brains queryable once ranking
      // starts reading these columns.
      await this.db.exec(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS access_count INT NOT NULL DEFAULT 0;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS confidence_score REAL NOT NULL DEFAULT 1.0;
      `);
    }

    if (needsPagesProvenance) {
      // v81 (pages_provenance_columns): four nullable columns added by the
      // v0.38 ingestion cathedral. No SCHEMA_SQL index or FK references
      // them today, but bootstrap probes cover the column-only forward-
      // reference class defense-in-depth so future schema work doesn't
      // wedge pre-v81 brains.
      await this.db.exec(`
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS ingested_via TEXT;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS source_uri TEXT;
        ALTER TABLE pages ADD COLUMN IF NOT EXISTS source_kind TEXT;
      `);
    }
  }

  async withReservedConnection<T>(fn: (conn: ReservedConnection) => Promise<T>): Promise<T> {
    // PGLite has no connection pool. The single backing connection is
    // always effectively reserved — pass it through.
    const db = this.db;
    const conn: ReservedConnection = {
      async executeRaw<R = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<R[]> {
        const { rows } = await db.query(sql, params);
        return rows as R[];
      },
    };
    return fn(conn);
  }

  async transaction<T>(fn: (engine: BrainEngine) => Promise<T>): Promise<T> {
    return this.db.transaction(async (tx) => {
      const txEngine = Object.create(this) as PGLiteEngine;
      Object.defineProperty(txEngine, 'db', { get: () => tx });
      return fn(txEngine);
    });
  }

  // Pages CRUD

  async executeRaw<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]> {
    const { rows } = await this.db.query(sql, params);
    return rows as T[];
  }
}

// Method mixins (BET-Y1Q3-T6-04 split): injected at module load.
import { pglitePagesMethods } from './pglite-engine-pages.ts';
import { pgliteLinksMethods } from './pglite-engine-links.ts';
import { pgliteFactsMethods } from './pglite-engine-facts.ts';
import { pgliteTakesMethods } from './pglite-engine-takes.ts';
Object.assign(PGLiteEngine.prototype, pglitePagesMethods);
Object.assign(PGLiteEngine.prototype, pgliteLinksMethods);
Object.assign(PGLiteEngine.prototype, pgliteFactsMethods);
Object.assign(PGLiteEngine.prototype, pgliteTakesMethods);

/**
 * Interface merge: PGLiteEngine's full method set is provided by the mixin
 * modules above (Object.assign at load). Declaring the BrainEngine surface
 * here keeps `implements BrainEngine`-style call sites type-correct.
 */
export interface PGLiteEngine extends BrainEngine {}
