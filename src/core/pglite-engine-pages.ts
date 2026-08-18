/**
 * PGLiteEngine pages methods — split out of pglite-engine.ts (BET-Y1Q3-T6-04).
 * Injected onto PGLiteEngine.prototype via Object.assign.
 */
import { DEFAULT_EMBEDDING_MODEL } from './ai/defaults.ts';
import { hasCJK, escapeLikePattern } from './cjk.ts';
import { MAX_SEARCH_LIMIT, clampSearchLimit } from './engine.ts';
import {
  COLUMN_NAME_REGEX,
  EmbeddingColumnNotRegisteredError,
  buildVectorCastFragment,
  normalizeEngineColumn,
  quoteIdentifier,
} from './search/embedding-column.ts';
import { resolveBoostMap, resolveHardExcludes } from './search/source-boost.ts';
import { buildHardExcludeClause, buildSourceFactorCase, buildVisibilityClause } from './search/sql-ranking.ts';
import {
  PAGE_SORT_SQL,
  type Chunk,
  type ChunkInput,
  type CorpusSampleOpts,
  type DomainBankRow,
  type DomainBankSampleOpts,
  type Page,
  type PageFilters,
  type PageInput,
  type SearchOpts,
  type SearchResult,
  type StaleChunkRow,
} from './types.ts';
import { contentHash, rowToChunk, rowToPage, rowToSearchResult, validateSlug } from './utils.ts';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';
import { vector } from '@electric-sql/pglite/vector';

export interface PGLiteEngineLike {
  db: import('@electric-sql/pglite').PGlite;
  [key: string]: any;
}

export const pglitePagesMethods: Record<string, any> = {
  getPage: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string; includeDeleted?: boolean }): Promise<Page | null> {
    // v0.26.5: hide soft-deleted by default; opt-in via opts.includeDeleted.
    const includeDeleted = opts?.includeDeleted === true;
    const sourceId = opts?.sourceId;
    const where: string[] = ['slug = $1'];
    const params: unknown[] = [slug];
    if (sourceId) {
      params.push(sourceId);
      where.push(`source_id = $${params.length}`);
    }
    if (!includeDeleted) {
      where.push('deleted_at IS NULL');
    }
    const { rows } = await this.db.query(
      `SELECT id, source_id, slug, type, title, compiled_truth, timeline, frontmatter, content_hash, created_at, updated_at, deleted_at
       FROM pages WHERE ${where.join(' AND ')} LIMIT 1`,
      params
    );
    if (rows.length === 0) return null;
    return rowToPage(rows[0] as Record<string, unknown>);
  }
,
  putPage: async function(this: PGLiteEngineLike, slug: string, page: PageInput, opts?: { sourceId?: string }): Promise<Page> {
    slug = validateSlug(slug);
    const hash = page.content_hash || contentHash(page);
    const frontmatter = page.frontmatter || {};
    const sourceId = opts?.sourceId ?? 'default';

    // v0.18.0 Step 5+: source_id is now in the INSERT column list so multi-
    // source callers land on the intended (source_id, slug) row. Omitting it
    // let the schema DEFAULT 'default' apply, fabricating duplicate slugs that
    // later made bare-slug subqueries return multiple rows.
    // ON CONFLICT target is (source_id, slug); global UNIQUE(slug) dropped in v17.
    const pageKind = page.page_kind || 'markdown';
    // v0.29.1 — additive opt-in columns. COALESCE(EXCLUDED.x, pages.x)
    // preserves existing values when caller omits them (auto-link path,
    // code reindex, etc.). Mirrors postgres-engine.ts.
    const effectiveDate = page.effective_date instanceof Date
      ? page.effective_date.toISOString()
      : (page.effective_date ?? null);
    const effectiveDateSource = page.effective_date_source ?? null;
    const importFilename = page.import_filename ?? null;
    // v0.32.7 CJK wave: chunker_version + source_path columns.
    const chunkerVersion = page.chunker_version ?? null;
    const sourcePath = page.source_path ?? null;
    const { rows } = await this.db.query(
      `INSERT INTO pages (source_id, slug, type, page_kind, title, compiled_truth, timeline, frontmatter, content_hash, updated_at, effective_date, effective_date_source, import_filename, chunker_version, source_path)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, now(), $10::timestamptz, $11, $12, COALESCE($13, 1), $14)
       ON CONFLICT (source_id, slug) DO UPDATE SET
         type = EXCLUDED.type,
         page_kind = EXCLUDED.page_kind,
         title = EXCLUDED.title,
         compiled_truth = EXCLUDED.compiled_truth,
         timeline = EXCLUDED.timeline,
         frontmatter = EXCLUDED.frontmatter,
         content_hash = EXCLUDED.content_hash,
         updated_at = now(),
         effective_date        = COALESCE(EXCLUDED.effective_date,        pages.effective_date),
         effective_date_source = COALESCE(EXCLUDED.effective_date_source, pages.effective_date_source),
         import_filename       = COALESCE(EXCLUDED.import_filename,       pages.import_filename),
         chunker_version       = COALESCE(EXCLUDED.chunker_version,       pages.chunker_version),
         source_path           = COALESCE(EXCLUDED.source_path,           pages.source_path)
       RETURNING id, source_id, slug, type, title, compiled_truth, timeline, frontmatter, content_hash, created_at, updated_at, effective_date, effective_date_source, import_filename`,
      [sourceId, slug, page.type, pageKind, page.title, page.compiled_truth, page.timeline || '', JSON.stringify(frontmatter), hash, effectiveDate, effectiveDateSource, importFilename, chunkerVersion, sourcePath]
    );
    return rowToPage(rows[0] as Record<string, unknown>);
  }
,
  deletePage: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<void> {
    const sourceId = opts?.sourceId ?? 'default';
    await this.db.query(
      'DELETE FROM pages WHERE slug = $1 AND source_id = $2',
      [slug, sourceId]
    );
  }
,
  softDeletePage: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<{ slug: string } | null> {
    // Idempotent-as-null: only flip rows currently active. Source filter is
    // optional; without it the first matching row across sources gets soft-deleted.
    const sourceId = opts?.sourceId;
    const where: string[] = ['slug = $1', 'deleted_at IS NULL'];
    const params: unknown[] = [slug];
    if (sourceId) {
      params.push(sourceId);
      where.push(`source_id = $${params.length}`);
    }
    const { rows } = await this.db.query(
      `UPDATE pages SET deleted_at = now() WHERE ${where.join(' AND ')} RETURNING slug`,
      params
    );
    if (rows.length === 0) return null;
    return { slug: (rows[0] as { slug: string }).slug };
  }
,
  restorePage: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<boolean> {
    const sourceId = opts?.sourceId;
    const where: string[] = ['slug = $1', 'deleted_at IS NOT NULL'];
    const params: unknown[] = [slug];
    if (sourceId) {
      params.push(sourceId);
      where.push(`source_id = $${params.length}`);
    }
    const { rows } = await this.db.query(
      `UPDATE pages SET deleted_at = NULL WHERE ${where.join(' AND ')} RETURNING slug`,
      params
    );
    return rows.length > 0;
  }
,
  purgeDeletedPages: async function(this: PGLiteEngineLike, olderThanHours: number): Promise<{ slugs: string[]; count: number }> {
    // Clamp to non-negative integer; cascade through FKs (content_chunks,
    // page_links, chunk_relations) on DELETE.
    const hours = Math.max(0, Math.floor(olderThanHours));
    const { rows } = await this.db.query(
      `DELETE FROM pages
       WHERE deleted_at IS NOT NULL
         AND deleted_at < now() - ($1 || ' hours')::interval
       RETURNING slug`,
      [hours]
    );
    const slugs = (rows as { slug: string }[]).map((r) => r.slug);
    return { slugs, count: slugs.length };
  }
,
  refreshPageBody: async function(this: PGLiteEngineLike, 
    slug: string,
    sourceId: string,
    compiledTruth: string,
    timeline: string,
    contentHash: string,
  ): Promise<void> {
    // Parity with PostgresEngine.refreshPageBody: narrow UPDATE only.
    // The deleted_at filter prevents a redirect retry from reviving a
    // canonical that was already purged.
    await this.db.query(
      `UPDATE pages
         SET compiled_truth = $1,
             timeline = $2,
             content_hash = $3,
             updated_at = now()
       WHERE source_id = $4
         AND slug = $5
         AND deleted_at IS NULL`,
      [compiledTruth, timeline, contentHash, sourceId, slug],
    );
  }
,
  migrateFactsToCanonical: async function(this: PGLiteEngineLike, 
    phantomSlug: string,
    canonicalSlug: string,
    sourceId: string,
  ): Promise<{ migrated: number }> {
    // Parity with PostgresEngine.migrateFactsToCanonical. UPDATE preserves
    // every column except entity_slug + source_markdown_slug. Active rows
    // only (expired_at IS NULL) so we don't disturb the supersession audit
    // trail.
    const { rows } = await this.db.query(
      `UPDATE facts
         SET entity_slug = $1,
             source_markdown_slug = $1
       WHERE source_id = $2
         AND source_markdown_slug = $3
         AND expired_at IS NULL
       RETURNING id`,
      [canonicalSlug, sourceId, phantomSlug],
    );
    return { migrated: rows.length };
  }
,
  listPages: async function(this: PGLiteEngineLike, filters?: PageFilters): Promise<Page[]> {
    const limit = filters?.limit || 100;
    const offset = filters?.offset || 0;

    const where: string[] = [];
    const params: unknown[] = [];
    const tagJoin = filters?.tag ? 'JOIN tags t ON t.page_id = p.id' : '';

    if (filters?.type) {
      params.push(filters.type);
      where.push(`p.type = $${params.length}`);
    }
    if (filters?.tag) {
      params.push(filters.tag);
      where.push(`t.tag = $${params.length}`);
    }
    if (filters?.updated_after) {
      params.push(filters.updated_after);
      where.push(`p.updated_at > $${params.length}::timestamptz`);
    }
    // slugPrefix uses the (source_id, slug) UNIQUE btree for index range scans.
    // Escape LIKE metacharacters so the user prefix is treated as a literal.
    if (filters?.slugPrefix) {
      const escaped = filters.slugPrefix.replace(/[\\%_]/g, (c) => '\\' + c) + '%';
      params.push(escaped);
      where.push(`p.slug LIKE $${params.length} ESCAPE '\\'`);
    }
    // v0.31.12 + v0.34.1 (#876, D9): scope to a single source OR an array
    // of sources. Array form wins (federated subsumes scalar).
    if (filters?.sourceIds && filters.sourceIds.length > 0) {
      params.push(filters.sourceIds);
      where.push(`p.source_id = ANY($${params.length}::text[])`);
    } else if (filters?.sourceId) {
      params.push(filters.sourceId);
      where.push(`p.source_id = $${params.length}`);
    }
    // v0.26.5: hide soft-deleted by default; opt in via filters.includeDeleted.
    if (filters?.includeDeleted !== true) {
      where.push('p.deleted_at IS NULL');
    }

    const whereSql = where.length > 0 ? `WHERE ${where.join(' AND ')}` : '';
    params.push(limit, offset);
    const limitSql = `LIMIT $${params.length - 1} OFFSET $${params.length}`;

    // v0.29: ORDER BY threading via PAGE_SORT_SQL whitelist (no SQL injection).
    const sortKey = filters?.sort && PAGE_SORT_SQL[filters.sort] ? filters.sort : 'updated_desc';
    const orderBy = PAGE_SORT_SQL[sortKey];

    const { rows } = await this.db.query(
      `SELECT p.* FROM pages p ${tagJoin} ${whereSql}
       ORDER BY ${orderBy} ${limitSql}`,
      params
    );

    return (rows as Record<string, unknown>[]).map(rowToPage);
  }
,
  getAllSlugs: async function(this: PGLiteEngineLike, opts?: { sourceId?: string }): Promise<Set<string>> {
    // v0.31.8 (D12): when opts.sourceId is set, return only that source's
    // slugs (used by reconcileLinks so wikilink resolution doesn't span
    // unrelated sources). Without opts, returns the union across sources
    // (pre-v0.31.8 behavior — preserved for callers that still expect the
    // brain-wide slug index, e.g. extract.ts's link resolver).
    if (opts?.sourceId) {
      const { rows } = await this.db.query(
        'SELECT slug FROM pages WHERE source_id = $1',
        [opts.sourceId]
      );
      return new Set((rows as { slug: string }[]).map(r => r.slug));
    }
    const { rows } = await this.db.query('SELECT slug FROM pages');
    return new Set((rows as { slug: string }[]).map(r => r.slug));
  }
,
  listAllPageRefs: async function(this: PGLiteEngineLike, ): Promise<Array<{ slug: string; source_id: string }>> {
    // v0.32.8: see postgres-engine.ts:listAllPageRefs for context. ORDER BY
    // (source_id, slug) for determinism; WHERE deleted_at IS NULL matches
    // default page visibility.
    const { rows } = await this.db.query(
      `SELECT slug, source_id FROM pages
       WHERE deleted_at IS NULL
       ORDER BY source_id, slug`
    );
    return (rows as { slug: string; source_id: string }[]).map(r => ({ slug: r.slug, source_id: r.source_id }));
  }
,
  listPrefixSampledPages: async function(this: PGLiteEngineLike, opts: DomainBankSampleOpts): Promise<DomainBankRow[]> {
    if (opts.prefixes.length === 0) return [];
    const exclude = opts.excludeSlugs ?? [];
    const staleBias = opts.staleBias === true;
    const staleThreshold = opts.staleThresholdDays ?? 90;
    const sourceIds = opts.sourceIds ?? null;
    const sourceId = opts.sourceId ?? null;
    const { rows } = await this.db.query(
      `WITH prefix_pages AS (
         SELECT
           p.id AS page_id,
           p.slug,
           p.source_id,
           p.title,
           p.compiled_truth,
           p.last_retrieved_at,
           substring(p.slug from '^[^/]+/[^/]+') AS prefix,
           COUNT(pl.id) AS connection_count
         FROM pages p
         LEFT JOIN page_links pl ON pl.to_page_id = p.id
         WHERE p.deleted_at IS NULL
           AND substring(p.slug from '^[^/]+/[^/]+') = ANY($1::text[])
           AND (cardinality($2::text[]) = 0 OR NOT (p.slug = ANY($2::text[])))
           AND (
             ($3::text[] IS NOT NULL AND p.source_id = ANY($3::text[]))
             OR ($3::text[] IS NULL AND $4::text IS NOT NULL AND p.source_id = $4)
             OR ($3::text[] IS NULL AND $4::text IS NULL)
           )
         GROUP BY p.id, p.slug, p.source_id, p.title, p.compiled_truth, p.last_retrieved_at
       ),
       ranked AS (
         SELECT
           pp.*,
           (CASE WHEN $5::boolean THEN
             CASE
               WHEN pp.last_retrieved_at IS NULL THEN 2
               WHEN pp.last_retrieved_at < NOW() - ($6::int * INTERVAL '1 day') THEN 1
               ELSE 0
             END
           ELSE 0
           END) AS stale_score,
           ROW_NUMBER() OVER (
             PARTITION BY pp.prefix
             ORDER BY
               (CASE WHEN $5::boolean THEN
                 CASE
                   WHEN pp.last_retrieved_at IS NULL THEN 2
                   WHEN pp.last_retrieved_at < NOW() - ($6::int * INTERVAL '1 day') THEN 1
                   ELSE 0
                 END
               ELSE 0
               END) DESC,
               pp.connection_count DESC,
               pp.slug ASC
           ) AS rn
         FROM prefix_pages pp
       ),
       with_chunk AS (
         SELECT
           r.*,
           (
             SELECT cc.id FROM content_chunks cc
             WHERE cc.page_id = r.page_id AND cc.embedding IS NOT NULL
             ORDER BY cc.chunk_index ASC
             LIMIT 1
           ) AS representative_chunk_id
         FROM ranked r
         WHERE r.rn = 1
       )
       SELECT page_id, slug, source_id, title, compiled_truth, last_retrieved_at,
              prefix, connection_count, representative_chunk_id
       FROM with_chunk
       ORDER BY prefix`,
      [opts.prefixes, exclude, sourceIds, sourceId, staleBias, staleThreshold]
    );
    return (rows as Array<Record<string, unknown>>).map((r): DomainBankRow => ({
      slug: r.slug as string,
      source_id: r.source_id as string,
      prefix: r.prefix as string | null,
      page_id: Number(r.page_id),
      title: r.title as string | null,
      compiled_truth: (r.compiled_truth as string | null) ?? '',
      connection_count: Number(r.connection_count),
      last_retrieved_at: r.last_retrieved_at == null ? null : new Date(r.last_retrieved_at as string),
      representative_chunk_id: r.representative_chunk_id == null ? null : Number(r.representative_chunk_id),
    }));
  }
,
  listCorpusSample: async function(this: PGLiteEngineLike, opts: CorpusSampleOpts): Promise<DomainBankRow[]> {
    if (opts.n <= 0) return [];
    const exclude = opts.excludeSlugs ?? [];
    const sourceIds = opts.sourceIds ?? null;
    const sourceId = opts.sourceId ?? null;
    if (typeof opts.seed === 'number') {
      const clamped = Math.max(-1, Math.min(1, opts.seed));
      await this.db.query('SELECT setseed($1::float8)', [clamped]);
    }
    const { rows } = await this.db.query(
      `WITH sampled AS (
         SELECT
           p.id AS page_id,
           p.slug,
           p.source_id,
           p.title,
           p.compiled_truth,
           p.last_retrieved_at,
           substring(p.slug from '^[^/]+/[^/]+') AS prefix,
           (SELECT COUNT(*) FROM page_links pl WHERE pl.to_page_id = p.id) AS connection_count
         FROM pages p
         WHERE p.deleted_at IS NULL
           AND (cardinality($1::text[]) = 0 OR NOT (p.slug = ANY($1::text[])))
           AND (
             ($2::text[] IS NOT NULL AND p.source_id = ANY($2::text[]))
             OR ($2::text[] IS NULL AND $3::text IS NOT NULL AND p.source_id = $3)
             OR ($2::text[] IS NULL AND $3::text IS NULL)
           )
         ORDER BY RANDOM()
         LIMIT $4
       )
       SELECT
         s.*,
         (
           SELECT cc.id FROM content_chunks cc
           WHERE cc.page_id = s.page_id AND cc.embedding IS NOT NULL
           ORDER BY cc.chunk_index ASC
           LIMIT 1
         ) AS representative_chunk_id
       FROM sampled s`,
      [exclude, sourceIds, sourceId, opts.n]
    );
    return (rows as Array<Record<string, unknown>>).map((r): DomainBankRow => ({
      slug: r.slug as string,
      source_id: r.source_id as string,
      prefix: r.prefix as string | null,
      page_id: Number(r.page_id),
      title: r.title as string | null,
      compiled_truth: (r.compiled_truth as string | null) ?? '',
      connection_count: Number(r.connection_count),
      last_retrieved_at: r.last_retrieved_at == null ? null : new Date(r.last_retrieved_at as string),
      representative_chunk_id: r.representative_chunk_id == null ? null : Number(r.representative_chunk_id),
    }));
  }
,
  resolveSlugs: async function(this: PGLiteEngineLike, partial: string): Promise<string[]> {
    // Try exact match first
    const exact = await this.db.query('SELECT slug FROM pages WHERE slug = $1', [partial]);
    if (exact.rows.length > 0) return [(exact.rows[0] as { slug: string }).slug];

    // Fuzzy match via pg_trgm
    const { rows } = await this.db.query(
      `SELECT slug, similarity(title, $1) AS sim
       FROM pages
       WHERE title % $1 OR slug ILIKE $2
       ORDER BY sim DESC
       LIMIT 5`,
      [partial, '%' + partial + '%']
    );
    return (rows as { slug: string }[]).map(r => r.slug);
  }
,
  searchKeyword: async function(this: PGLiteEngineLike, query: string, opts?: SearchOpts): Promise<SearchResult[]> {
    const limit = clampSearchLimit(opts?.limit);
    const offset = opts?.offset || 0;
    const detailFilter = opts?.detail === 'low' ? `AND cc.chunk_source = 'compiled_truth'` : '';

    if (opts?.limit && opts.limit > MAX_SEARCH_LIMIT) {
      console.warn(`[gbrain] Warning: search limit clamped from ${opts.limit} to ${MAX_SEARCH_LIMIT}`);
    }

    // Fetch 3x to give dedup headroom, then page-dedup + re-limit.
    const innerLimit = Math.min(limit * 3, MAX_SEARCH_LIMIT * 3);

    // Source-aware ranking (v0.22): see postgres-engine.ts for rationale.
    const boostMap = resolveBoostMap();
    const sourceFactorCase = buildSourceFactorCase('p.slug', boostMap, opts?.detail);
    const hardExcludePrefixes = resolveHardExcludes(opts?.exclude_slug_prefixes, opts?.include_slug_prefixes);
    const hardExcludeClause = buildHardExcludeClause('p.slug', hardExcludePrefixes);

    // v0.26.5: visibility filter (soft-deleted + archived-source).
    const visibilityClause = buildVisibilityClause('p', 's');

    // v0.32.7: CJK query branch. PGLite uses websearch_to_tsquery('english')
    // which can't tokenize CJK; queries return empty. Switch to ILIKE on
    // chunk_text with bigram-frequency-count ranking when the query contains
    // CJK characters. ASCII path stays exactly the same below.
    if (hasCJK(query)) {
      return this._searchKeywordCJK(query, {
        limit, offset, innerLimit, sourceFactorCase,
        hardExcludeClause, visibilityClause, detailFilter, opts,
        dedup: true,
      });
    }

    // v0.20.0 Cathedral II Layer 10 C1/C2: language + symbol-kind filters.
    const params: unknown[] = [query, innerLimit, limit, offset];
    let extraFilter = '';
    if (opts?.language) {
      params.push(opts.language);
      extraFilter += ` AND cc.language = $${params.length}`;
    }
    if (opts?.symbolKind) {
      params.push(opts.symbolKind);
      extraFilter += ` AND cc.symbol_type = $${params.length}`;
    }
    // v0.33: multi-type filter for whoknows.
    if (opts?.types && opts.types.length > 0) {
      params.push(opts.types);
      extraFilter += ` AND p.type = ANY($${params.length}::text[])`;
    }
    // v0.29.1 — since/until date filter (Postgres parity, codex pass-1 #10).
    // Reads against COALESCE(effective_date, updated_at) so date filtering
    // matches user intent (a meeting was on its event_date, not when it
    // got reimported). Same param shape as Postgres engine.
    if (opts?.afterDate) {
      params.push(opts.afterDate);
      extraFilter += ` AND COALESCE(p.effective_date, p.updated_at, p.created_at) > $${params.length}::timestamptz`;
    }
    if (opts?.beforeDate) {
      params.push(opts.beforeDate);
      extraFilter += ` AND COALESCE(p.effective_date, p.updated_at, p.created_at) < $${params.length}::timestamptz`;
    }
    // v0.34.1 (#861 — P0 leak seal): source-isolation. Array wins over scalar.
    if (opts?.sourceIds && opts.sourceIds.length > 0) {
      params.push(opts.sourceIds);
      extraFilter += ` AND p.source_id = ANY($${params.length}::text[])`;
    } else if (opts?.sourceId) {
      params.push(opts.sourceId);
      extraFilter += ` AND p.source_id = $${params.length}`;
    }

    const { rows } = await this.db.query(
      `WITH ranked AS (
         SELECT
           p.slug, p.id as page_id, p.title, p.type, p.source_id, p.access_count, p.confidence_score,
           p.effective_date, p.effective_date_source,
           cc.id as chunk_id, cc.chunk_index, cc.chunk_text, cc.chunk_source,
           ts_rank(cc.search_vector, websearch_to_tsquery('english', $1)) * ${sourceFactorCase} AS score,
           CASE WHEN p.updated_at < (
             SELECT MAX(te.created_at) FROM timeline_entries te WHERE te.page_id = p.id
           ) THEN true ELSE false END AS stale
         FROM content_chunks cc
         JOIN pages p ON p.id = cc.page_id
         JOIN sources s ON s.id = p.source_id
         WHERE cc.search_vector @@ websearch_to_tsquery('english', $1) ${detailFilter}${extraFilter} ${hardExcludeClause} ${visibilityClause}
           -- v0.27.1: hide image rows from default text-keyword search so
           -- OCR text doesn't drown text-page hits. Image-similarity queries
           -- run a separate vector path on embedding_image.
           AND cc.modality = 'text'
         ORDER BY score DESC
         LIMIT $2
       ),
       best_per_page AS (
         SELECT DISTINCT ON (slug) *
         FROM ranked
         ORDER BY slug, score DESC
       )
       SELECT * FROM best_per_page
       ORDER BY score DESC
       LIMIT $3 OFFSET $4`,
      params
    );

    return (rows as Record<string, unknown>[]).map(rowToSearchResult);
  }
,
  _searchKeywordCJK: async function(this: PGLiteEngineLike,
    query: string,
    ctx: {
      limit: number;
      offset: number;
      innerLimit: number;
      sourceFactorCase: string;
      hardExcludeClause: string;
      visibilityClause: string;
      detailFilter: string;
      opts: SearchOpts | undefined;
      dedup: boolean;
    },
  ): Promise<SearchResult[]> {
    const { limit, offset, innerLimit, sourceFactorCase, hardExcludeClause, visibilityClause, detailFilter, opts, dedup } = ctx;
    const qRaw = query;
    if (qRaw.length === 0) return [];
    const qLike = escapeLikePattern(qRaw);

    // $1 = qLike (escaped for ILIKE)
    // $2 = qRaw  (raw for position()/replace() ranking arithmetic)
    // $3 = inner limit (dedup path) OR final limit (chunk-grain path)
    // $4 = final limit (dedup path only) — see callers
    // $5 = offset (dedup path)  /  $4 = offset (chunk-grain path)
    const params: unknown[] = dedup
      ? [qLike, qRaw, innerLimit, limit, offset]
      : [qLike, qRaw, limit, offset];

    let extraFilter = '';
    if (opts?.language) {
      params.push(opts.language);
      extraFilter += ` AND cc.language = $${params.length}`;
    }
    if (opts?.symbolKind) {
      params.push(opts.symbolKind);
      extraFilter += ` AND cc.symbol_type = $${params.length}`;
    }
    if (opts?.afterDate) {
      params.push(opts.afterDate);
      extraFilter += ` AND COALESCE(p.effective_date, p.updated_at, p.created_at) > $${params.length}::timestamptz`;
    }
    if (opts?.beforeDate) {
      params.push(opts.beforeDate);
      extraFilter += ` AND COALESCE(p.effective_date, p.updated_at, p.created_at) < $${params.length}::timestamptz`;
    }
    // v0.34.1 (#861 — P0 leak seal): source-isolation on the CJK fallback path.
    if (opts?.sourceIds && opts.sourceIds.length > 0) {
      params.push(opts.sourceIds);
      extraFilter += ` AND p.source_id = ANY($${params.length}::text[])`;
    } else if (opts?.sourceId) {
      params.push(opts.sourceId);
      extraFilter += ` AND p.source_id = $${params.length}`;
    }

    // Bigram-frequency count: count occurrences of $qRaw in chunk_text via
    // (length(chunk) - length(replace(chunk, q, ''))) / length(q). Acts as
    // a ts_rank substitute. position()-tiebreaker so earlier-in-chunk hits
    // outrank later ones at the same occurrence count.
    const scoreExpr = `
      ((LENGTH(cc.chunk_text) - LENGTH(REPLACE(cc.chunk_text, $2, ''))) / NULLIF(LENGTH($2), 0)::real
        + 1.0 / NULLIF(POSITION($2 IN cc.chunk_text), 0)::real)
      * ${sourceFactorCase}
    `;

    if (dedup) {
      const { rows } = await this.db.query(
        `WITH ranked AS (
           SELECT
             p.slug, p.id as page_id, p.title, p.type, p.source_id, p.access_count, p.confidence_score,
             p.effective_date, p.effective_date_source,
             cc.id as chunk_id, cc.chunk_index, cc.chunk_text, cc.chunk_source,
             ${scoreExpr} AS score,
             CASE WHEN p.updated_at < (
               SELECT MAX(te.created_at) FROM timeline_entries te WHERE te.page_id = p.id
             ) THEN true ELSE false END AS stale
           FROM content_chunks cc
           JOIN pages p ON p.id = cc.page_id
           JOIN sources s ON s.id = p.source_id
           WHERE cc.chunk_text ILIKE '%' || $1 || '%' ESCAPE '\\' ${detailFilter}${extraFilter} ${hardExcludeClause} ${visibilityClause}
             AND cc.modality = 'text'
           ORDER BY score DESC
           LIMIT $3
         ),
         best_per_page AS (
           SELECT DISTINCT ON (slug) *
           FROM ranked
           ORDER BY slug, score DESC
         )
         SELECT * FROM best_per_page
         ORDER BY score DESC
         LIMIT $4 OFFSET $5`,
        params,
      );
      return (rows as Record<string, unknown>[]).map(rowToSearchResult);
    } else {
      const { rows } = await this.db.query(
        `SELECT
           p.slug, p.id as page_id, p.title, p.type, p.source_id, p.access_count, p.confidence_score,
           p.effective_date, p.effective_date_source,
           cc.id as chunk_id, cc.chunk_index, cc.chunk_text, cc.chunk_source,
           ${scoreExpr} AS score,
           CASE WHEN p.updated_at < (
             SELECT MAX(te.created_at) FROM timeline_entries te WHERE te.page_id = p.id
           ) THEN true ELSE false END AS stale
         FROM content_chunks cc
         JOIN pages p ON p.id = cc.page_id
         JOIN sources s ON s.id = p.source_id
         WHERE cc.chunk_text ILIKE '%' || $1 || '%' ESCAPE '\\' ${detailFilter}${extraFilter} ${hardExcludeClause} ${visibilityClause}
         ORDER BY score DESC
         LIMIT $3 OFFSET $4`,
        params,
      );
      return (rows as Record<string, unknown>[]).map(rowToSearchResult);
    }
  },

  searchKeywordChunks: async function(this: PGLiteEngineLike, query: string, opts?: SearchOpts): Promise<SearchResult[]> {
    const limit = clampSearchLimit(opts?.limit);
    const offset = opts?.offset || 0;
    const detailFilter = opts?.detail === 'low' ? `AND cc.chunk_source = 'compiled_truth'` : '';

    if (opts?.limit && opts.limit > MAX_SEARCH_LIMIT) {
      console.warn(`[gbrain] Warning: search limit clamped from ${opts.limit} to ${MAX_SEARCH_LIMIT}`);
    }

    // Source-aware ranking applied here too — searchKeywordChunks is the
    // chunk-grain anchor primitive that two-pass retrieval (Layer 7) uses.
    const boostMap = resolveBoostMap();
    const sourceFactorCase = buildSourceFactorCase('p.slug', boostMap, opts?.detail);
    const hardExcludePrefixes = resolveHardExcludes(opts?.exclude_slug_prefixes, opts?.include_slug_prefixes);
    const hardExcludeClause = buildHardExcludeClause('p.slug', hardExcludePrefixes);
    const visibilityClause = buildVisibilityClause('p', 's');

    // v0.32.7: CJK branch (same as searchKeyword but without page-dedup).
    if (hasCJK(query)) {
      return this._searchKeywordCJK(query, {
        limit, offset,
        innerLimit: 0,             // unused on chunk-grain (no inner CTE)
        sourceFactorCase,
        hardExcludeClause, visibilityClause, detailFilter, opts,
        dedup: false,
      });
    }

    const params: unknown[] = [query, limit, offset];
    let extraFilter = '';
    if (opts?.language) {
      params.push(opts.language);
      extraFilter += ` AND cc.language = $${params.length}`;
    }
    if (opts?.symbolKind) {
      params.push(opts.symbolKind);
      extraFilter += ` AND cc.symbol_type = $${params.length}`;
    }
    // v0.29.1 since/until parity (codex pass-1 #10).
    if (opts?.afterDate) {
      params.push(opts.afterDate);
      extraFilter += ` AND COALESCE(p.effective_date, p.updated_at, p.created_at) > $${params.length}::timestamptz`;
    }
    if (opts?.beforeDate) {
      params.push(opts.beforeDate);
      extraFilter += ` AND COALESCE(p.effective_date, p.updated_at, p.created_at) < $${params.length}::timestamptz`;
    }
    // v0.34.1 (#861 — P0 leak seal): source-isolation for the chunk-grain
    // anchor primitive. Layer 7 two-pass walks from these anchors so a
    // foreign-source anchor would let the walk leak into foreign neighbors.
    if (opts?.sourceIds && opts.sourceIds.length > 0) {
      params.push(opts.sourceIds);
      extraFilter += ` AND p.source_id = ANY($${params.length}::text[])`;
    } else if (opts?.sourceId) {
      params.push(opts.sourceId);
      extraFilter += ` AND p.source_id = $${params.length}`;
    }

    // visibilityClause already declared above (v0.32.7: hoisted so CJK branch can reuse).

    const { rows } = await this.db.query(
      `SELECT
         p.slug, p.id as page_id, p.title, p.type, p.source_id, p.access_count, p.confidence_score,
         p.effective_date, p.effective_date_source,
         cc.id as chunk_id, cc.chunk_index, cc.chunk_text, cc.chunk_source,
         ts_rank(cc.search_vector, websearch_to_tsquery('english', $1)) * ${sourceFactorCase} AS score,
         CASE WHEN p.updated_at < (
           SELECT MAX(te.created_at) FROM timeline_entries te WHERE te.page_id = p.id
         ) THEN true ELSE false END AS stale
       FROM content_chunks cc
       JOIN pages p ON p.id = cc.page_id
       JOIN sources s ON s.id = p.source_id
       WHERE cc.search_vector @@ websearch_to_tsquery('english', $1) ${detailFilter}${extraFilter} ${hardExcludeClause} ${visibilityClause}
       ORDER BY score DESC
       LIMIT $2 OFFSET $3`,
      params
    );

    return (rows as Record<string, unknown>[]).map(rowToSearchResult);
  }
,
  searchVector: async function(this: PGLiteEngineLike, embedding: Float32Array, opts?: SearchOpts): Promise<SearchResult[]> {
    const limit = clampSearchLimit(opts?.limit);
    const offset = opts?.offset || 0;
    const vecStr = '[' + Array.from(embedding).join(',') + ']';
    const detailFilter = opts?.detail === 'low' ? `AND cc.chunk_source = 'compiled_truth'` : '';

    if (opts?.limit && opts.limit > MAX_SEARCH_LIMIT) {
      console.warn(`[gbrain] Warning: search limit clamped from ${opts.limit} to ${MAX_SEARCH_LIMIT}`);
    }

    // Two-stage CTE (v0.22): pure-distance ORDER BY in inner CTE preserves
    // HNSW; outer SELECT re-ranks by raw_score * source_factor over the
    // narrow candidate pool. innerLimit scales with offset to preserve the
    // pagination contract. See postgres-engine.ts searchVector for rationale.
    const boostMap = resolveBoostMap();
    // Outer SELECT references the aliased CTE column. Aliasing the CTE as `hc`
    // disambiguates the correlated subquery (`te.page_id = hc.page_id`) from
    // the inner column. Without the alias, an unqualified `page_id` in the
    // subquery's WHERE would lexically resolve back to `te.page_id` itself
    // and degrade to `te.page_id = te.page_id` (always true), making every
    // result stale=true. Codex caught this in adversarial review.
    const sourceFactorCaseOnSlug = buildSourceFactorCase('hc.slug', boostMap, opts?.detail);
    const hardExcludePrefixes = resolveHardExcludes(opts?.exclude_slug_prefixes, opts?.include_slug_prefixes);
    const hardExcludeClause = buildHardExcludeClause('p.slug', hardExcludePrefixes);
    const innerLimit = offset + Math.max(limit * 5, 100);

    const params: unknown[] = [vecStr, innerLimit, limit, offset];
    let extraFilter = '';
    if (opts?.language) {
      params.push(opts.language);
      extraFilter += ` AND cc.language = $${params.length}`;
    }
    if (opts?.symbolKind) {
      params.push(opts.symbolKind);
      extraFilter += ` AND cc.symbol_type = $${params.length}`;
    }
    // v0.33: multi-type filter for whoknows. Applied inside HNSW candidate
    // CTE so the candidate pool consists only of typed pages — limit budget
    // goes to person/company pages instead of being eaten by other types.
    if (opts?.types && opts.types.length > 0) {
      params.push(opts.types);
      extraFilter += ` AND p.type = ANY($${params.length}::text[])`;
    }
    // v0.29.1 since/until parity (codex pass-1 #10). Filter applied INSIDE
    // the inner CTE so HNSW's candidate pool already excludes out-of-range
    // pages — preserves pagination contract.
    if (opts?.afterDate) {
      params.push(opts.afterDate);
      extraFilter += ` AND COALESCE(p.effective_date, p.updated_at, p.created_at) > $${params.length}::timestamptz`;
    }
    if (opts?.beforeDate) {
      params.push(opts.beforeDate);
      extraFilter += ` AND COALESCE(p.effective_date, p.updated_at, p.created_at) < $${params.length}::timestamptz`;
    }
    // v0.34.1 (#861, F2 — P0 leak seal): source-isolation in the INNER CTE
    // so HNSW candidate pool narrows before re-rank. Mirrors postgres-engine
    // placement decision (codex flagged this during plan review).
    if (opts?.sourceIds && opts.sourceIds.length > 0) {
      params.push(opts.sourceIds);
      extraFilter += ` AND p.source_id = ANY($${params.length}::text[])`;
    } else if (opts?.sourceId) {
      params.push(opts.sourceId);
      extraFilter += ` AND p.source_id = $${params.length}`;
    }

    // v0.26.5: visibility filter applied in the inner CTE so HNSW sees the
    // same candidate count it always did. See postgres-engine.ts for rationale.
    const visibilityClause = buildVisibilityClause('p', 's');

    // v0.36 (D11): column routing via resolved descriptor. Engine doesn't
    // read config — caller resolved at hybrid/op boundary. The cast SQL
    // ($1::vector vs $1::halfvec(N)) comes from buildVectorCastFragment.
    //
    // v0.36 Phase 3: 'embedding_multimodal' is the unified column populated
    // by `gbrain reindex --multimodal`. No modality filter — the column
    // itself is the discriminator (only re-embedded rows have non-NULL).
    const resolvedCol = normalizeEngineColumn(opts?.embeddingColumn);
    const { col, castSql } = buildVectorCastFragment(resolvedCol);
    let modalityFilter: string;
    if (resolvedCol.name === 'embedding_image') {
      modalityFilter = `AND cc.modality = 'image'`;
    } else if (resolvedCol.name === 'embedding_multimodal') {
      modalityFilter = '';
    } else {
      modalityFilter = `AND cc.modality = 'text'`;
    }

    const { rows } = await this.db.query(
      `WITH hnsw_candidates AS (
         SELECT
           p.slug, p.id as page_id, p.title, p.type, p.source_id, p.access_count, p.confidence_score, p.updated_at,
           p.effective_date, p.effective_date_source,
           cc.id as chunk_id, cc.chunk_index, cc.chunk_text, cc.chunk_source,
           1 - (cc.${col} <=> ${castSql}) AS raw_score
         FROM content_chunks cc
         JOIN pages p ON p.id = cc.page_id
         JOIN sources s ON s.id = p.source_id
         WHERE cc.${col} IS NOT NULL ${modalityFilter} ${detailFilter}${extraFilter} ${hardExcludeClause} ${visibilityClause}
         ORDER BY cc.${col} <=> ${castSql}
         LIMIT $2
       )
       SELECT
         hc.slug, hc.page_id, hc.title, hc.type, hc.source_id,
         hc.effective_date, hc.effective_date_source,
         hc.chunk_id, hc.chunk_index, hc.chunk_text, hc.chunk_source,
         hc.raw_score * ${sourceFactorCaseOnSlug} AS score,
         CASE WHEN hc.updated_at < (
           SELECT MAX(te.created_at) FROM timeline_entries te WHERE te.page_id = hc.page_id
         ) THEN true ELSE false END AS stale
       FROM hnsw_candidates hc
       ORDER BY score DESC
       LIMIT $3
       OFFSET $4`,
      params
    );

    return (rows as Record<string, unknown>[]).map(rowToSearchResult);
  }
,
  getEmbeddingsByChunkIds: async function(this: PGLiteEngineLike, 
    ids: number[],
    column: string = 'embedding',
  ): Promise<Map<number, Float32Array>> {
    if (ids.length === 0) return new Map();
    // v0.36 (D9): column parameter so hybrid.cosineReScore can rehydrate
    // from the active embedding space (Voyage 1024d, ZE halfvec 2560d,
    // etc.). Identifier-quoted (D12 layer 2) plus strict regex on the
    // column name (D12 layer 1) before interpolation.
    if (!COLUMN_NAME_REGEX.test(column)) {
      throw new EmbeddingColumnNotRegisteredError(column, []);
    }
    const quotedCol = quoteIdentifier(column);
    const { rows } = await this.db.query(
      `SELECT id, ${quotedCol} AS embedding FROM content_chunks WHERE id = ANY($1::int[]) AND ${quotedCol} IS NOT NULL`,
      [ids]
    );
    const result = new Map<number, Float32Array>();
    for (const row of rows as Record<string, unknown>[]) {
      if (row.embedding) {
        const emb = typeof row.embedding === 'string'
          ? new Float32Array(JSON.parse(row.embedding))
          : row.embedding as Float32Array;
        result.set(row.id as number, emb);
      }
    }
    return result;
  }
,
  upsertChunks: async function(this: PGLiteEngineLike, slug: string, chunks: ChunkInput[], opts?: { sourceId?: string }): Promise<void> {
    const sourceId = opts?.sourceId ?? 'default';

    // Source-scope the page-id lookup so duplicate slugs in different sources
    // do not return multiple rows or target the wrong page.
    const pageResult = await this.db.query(
      'SELECT id FROM pages WHERE slug = $1 AND source_id = $2',
      [slug, sourceId]
    );
    if (pageResult.rows.length === 0) throw new Error(`Page not found: ${slug} (source=${sourceId})`);
    const pageId = (pageResult.rows[0] as { id: number }).id;

    // Remove chunks that no longer exist
    const newIndices = chunks.map(c => c.chunk_index);
    if (newIndices.length > 0) {
      // PGLite doesn't auto-serialize arrays, so use ANY with explicit array cast
      await this.db.query(
        `DELETE FROM content_chunks WHERE page_id = $1 AND chunk_index != ALL($2::int[])`,
        [pageId, newIndices]
      );
    } else {
      await this.db.query('DELETE FROM content_chunks WHERE page_id = $1', [pageId]);
      return;
    }

    // Batch upsert: build dynamic multi-row INSERT.
    // v0.19.0: includes language/symbol_name/symbol_type/start_line/end_line
    // so code chunks carry their tree-sitter metadata into the DB. Markdown
    // chunks pass NULL for all five. Order must match the column list.
    // v0.20.0 Cathedral II Layer 6: adds parent_symbol_path / doc_comment /
    // symbol_name_qualified so nested-chunk emission (A3) and eventual A1
    // edge resolution can round-trip metadata through upserts.
    // v0.27.1 (Phase 8): added `modality` + `embedding_image` to the column
    // list. Image chunks pass embedding=null + embedding_image=Float32Array
    // (1024-dim Voyage). Text/code chunks pass embedding=Float32Array +
    // embedding_image=null. Default modality='text' when omitted.
    const cols = '(page_id, chunk_index, chunk_text, chunk_source, embedding, model, token_count, embedded_at, language, symbol_name, symbol_type, start_line, end_line, parent_symbol_path, doc_comment, symbol_name_qualified, modality, embedding_image)';
    const rowParts: string[] = [];
    const params: unknown[] = [];
    let paramIdx = 1;

    for (const chunk of chunks) {
      const embeddingStr = chunk.embedding
        ? '[' + Array.from(chunk.embedding).join(',') + ']'
        : null;
      const embeddingImageStr = chunk.embedding_image
        ? '[' + Array.from(chunk.embedding_image).join(',') + ']'
        : null;
      const parentPath = chunk.parent_symbol_path && chunk.parent_symbol_path.length > 0
        ? chunk.parent_symbol_path
        : null;
      const modality = chunk.modality ?? 'text';

      // Inline ::vector NULL literals to avoid a per-branch placeholder.
      const embeddingPh = embeddingStr ? `$${paramIdx++}::vector` : 'NULL';
      const embeddedAtPh = embeddingStr ? 'now()' : 'NULL';
      const embeddingImagePh = embeddingImageStr ? `$${paramIdx++}::vector` : 'NULL';

      rowParts.push(
        `($${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, ` +
        `${embeddingPh}, $${paramIdx++}, $${paramIdx++}, ${embeddedAtPh}, ` +
        `$${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, ` +
        `$${paramIdx++}::text[], $${paramIdx++}, $${paramIdx++}, ` +
        `$${paramIdx++}, ${embeddingImagePh})`,
      );

      // Param push order MUST match placeholder allocation order. Both
      // embedding placeholders (when present) are allocated BEFORE the
      // bulk row placeholders, so their values must be pushed first.
      if (embeddingStr) params.push(embeddingStr);
      if (embeddingImageStr) params.push(embeddingImageStr);
      params.push(
        pageId, chunk.chunk_index, chunk.chunk_text, chunk.chunk_source,
        chunk.model || DEFAULT_EMBEDDING_MODEL, chunk.token_count || null,
        chunk.language || null, chunk.symbol_name || null, chunk.symbol_type || null,
        chunk.start_line ?? null, chunk.end_line ?? null,
        parentPath, chunk.doc_comment || null, chunk.symbol_name_qualified || null,
        modality,
      );
    }

    // CONSISTENCY: when chunk_text changes and no new embedding is supplied, BOTH embedding AND
    // embedded_at must reset to NULL so `embed --stale` correctly picks up the row for re-embedding.
    // See postgres-engine.ts upsertChunks for the full rationale — pglite mirrors it for parity.
    await this.db.query(
      `INSERT INTO content_chunks ${cols} VALUES ${rowParts.join(', ')}
       ON CONFLICT (page_id, chunk_index) DO UPDATE SET
         chunk_text = EXCLUDED.chunk_text,
         chunk_source = EXCLUDED.chunk_source,
         embedding = CASE WHEN EXCLUDED.chunk_text != content_chunks.chunk_text THEN EXCLUDED.embedding ELSE COALESCE(EXCLUDED.embedding, content_chunks.embedding) END,
         model = COALESCE(EXCLUDED.model, content_chunks.model),
         token_count = EXCLUDED.token_count,
         embedded_at = CASE
           WHEN EXCLUDED.chunk_text != content_chunks.chunk_text AND EXCLUDED.embedding IS NULL THEN NULL
           ELSE COALESCE(EXCLUDED.embedded_at, content_chunks.embedded_at)
         END,
         language = EXCLUDED.language,
         symbol_name = EXCLUDED.symbol_name,
         symbol_type = EXCLUDED.symbol_type,
         start_line = EXCLUDED.start_line,
         end_line = EXCLUDED.end_line,
         parent_symbol_path = EXCLUDED.parent_symbol_path,
         doc_comment = EXCLUDED.doc_comment,
         symbol_name_qualified = EXCLUDED.symbol_name_qualified,
         modality = EXCLUDED.modality,
         embedding_image = COALESCE(EXCLUDED.embedding_image, content_chunks.embedding_image)`,
      params
    );
  }
,
  getChunks: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<Chunk[]> {
    const sourceId = opts?.sourceId ?? 'default';
    const { rows } = await this.db.query(
      `SELECT cc.* FROM content_chunks cc
       JOIN pages p ON p.id = cc.page_id
       WHERE p.slug = $1 AND p.source_id = $2
       ORDER BY cc.chunk_index`,
      [slug, sourceId]
    );
    return (rows as Record<string, unknown>[]).map(r => rowToChunk(r));
  }
,
  countStaleChunks: async function(this: PGLiteEngineLike, opts?: { sourceId?: string }): Promise<number> {
    // D7: source-scoped count for `gbrain embed --stale --source X`.
    if (opts?.sourceId === undefined) {
      const { rows } = await this.db.query(
        `SELECT count(*)::int AS count
           FROM content_chunks
          WHERE embedding IS NULL`,
      );
      const count = (rows[0] as { count: number } | undefined)?.count ?? 0;
      return Number(count);
    }
    const { rows } = await this.db.query(
      `SELECT count(*)::int AS count
         FROM content_chunks cc
         JOIN pages p ON p.id = cc.page_id
        WHERE cc.embedding IS NULL
          AND p.source_id = $1`,
      [opts.sourceId],
    );
    const count = (rows[0] as { count: number } | undefined)?.count ?? 0;
    return Number(count);
  }
,
  listStaleChunks: async function(this: PGLiteEngineLike, opts?: {
    batchSize?: number;
    afterPageId?: number;
    afterChunkIndex?: number;
    sourceId?: string;
  }): Promise<StaleChunkRow[]> {
    const limit = opts?.batchSize ?? 2000;
    const afterPid = opts?.afterPageId ?? 0;
    const afterIdx = opts?.afterChunkIndex ?? -1;
    // D7: optional source-scoped cursor scan. PGLite mirrors postgres-engine
    // so the engine-parity E2E catches drift.
    if (opts?.sourceId === undefined) {
      const { rows } = await this.db.query(
        `SELECT p.slug, cc.chunk_index, cc.chunk_text, cc.chunk_source,
                cc.model, cc.token_count, p.source_id, cc.page_id
           FROM content_chunks cc
           JOIN pages p ON p.id = cc.page_id
          WHERE cc.embedding IS NULL
            AND (cc.page_id, cc.chunk_index) > ($1, $2)
          ORDER BY cc.page_id, cc.chunk_index
          LIMIT $3`,
        [afterPid, afterIdx, limit],
      );
      return rows as unknown as StaleChunkRow[];
    }
    const { rows } = await this.db.query(
      `SELECT p.slug, cc.chunk_index, cc.chunk_text, cc.chunk_source,
              cc.model, cc.token_count, p.source_id, cc.page_id
         FROM content_chunks cc
         JOIN pages p ON p.id = cc.page_id
        WHERE cc.embedding IS NULL
          AND p.source_id = $1
          AND (cc.page_id, cc.chunk_index) > ($2, $3)
        ORDER BY cc.page_id, cc.chunk_index
        LIMIT $4`,
      [opts.sourceId, afterPid, afterIdx, limit],
    );
    return rows as unknown as StaleChunkRow[];
  }

};
