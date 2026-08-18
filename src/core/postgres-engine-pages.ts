/**
 * PostgresEngine pages methods — split out of postgres-engine.ts (BET-Y1Q3-T6-04).
 * Injected onto PostgresEngine.prototype via Object.assign.
 */
import { DEFAULT_EMBEDDING_MODEL } from './ai/defaults.ts';
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
import { contentHash, rowToChunk, rowToPage, rowToSearchResult, tryParseEmbedding, validateSlug } from './utils.ts';
import postgres from 'postgres';


export interface PostgresEngineLike {
  sql: ReturnType<typeof postgres>;
  connectionManager: import('./connection-manager.ts').ConnectionManager | null;
  [key: string]: any;
}

export const postgresPagesMethods: Record<string, any> = {
  getPage: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string; includeDeleted?: boolean }): Promise<Page | null> {
    const sql = this.sql;
    const includeDeleted = opts?.includeDeleted === true;
    const sourceId = opts?.sourceId;
    // v0.26.5: default hides soft-deleted rows. Compose with optional sourceId
    // filter via fragment chaining (postgres.js supports sql`` composition).
    const sourceCondition = sourceId ? sql`AND source_id = ${sourceId}` : sql``;
    const deletedCondition = includeDeleted ? sql`` : sql`AND deleted_at IS NULL`;
    const rows = await sql`
      SELECT id, source_id, slug, type, title, compiled_truth, timeline, frontmatter, content_hash, created_at, updated_at, deleted_at
      FROM pages
      WHERE slug = ${slug} ${sourceCondition} ${deletedCondition}
      LIMIT 1
    `;
    if (rows.length === 0) return null;
    return rowToPage(rows[0]);
  }
,
  putPage: async function(this: PostgresEngineLike, slug: string, page: PageInput, opts?: { sourceId?: string }): Promise<Page> {
    slug = validateSlug(slug);
    const sql = this.sql;
    const hash = page.content_hash || contentHash(page);
    const frontmatter = page.frontmatter || {};
    const sourceId = opts?.sourceId ?? 'default';

    // v0.18.0 Step 5+: source_id is now in the INSERT column list so multi-
    // source callers actually land on the (source_id, slug) row they intend.
    // Pre-fix: omitting source_id let the schema DEFAULT 'default' apply, so
    // a caller syncing under 'jarvis-memory' silently fabricated a duplicate
    // at (default, slug); subsequent bare-slug subqueries (getTags, deleteChunks,
    // etc.) then matched 2 rows and blew up with Postgres 21000.
    // ON CONFLICT target is (source_id, slug); global UNIQUE(slug) dropped in v17.
    const pageKind = page.page_kind || 'markdown';
    // v0.29.1 — effective_date / effective_date_source / import_filename are
    // additive opt-in inputs from the importer (computeEffectiveDate). When
    // omitted, the ON CONFLICT path preserves any existing value via
    // COALESCE(EXCLUDED.x, pages.x) so a putPage that doesn't know about
    // these columns (auto-link, code reindex, etc.) doesn't blank them out.
    const effectiveDate = page.effective_date ?? null;
    const effectiveDateSource = page.effective_date_source ?? null;
    const importFilename = page.import_filename ?? null;
    // v0.32.7 CJK wave: chunker_version + source_path columns.
    const chunkerVersion = page.chunker_version ?? null;
    const sourcePath = page.source_path ?? null;
    const rows = await sql`
      INSERT INTO pages (source_id, slug, type, page_kind, title, compiled_truth, timeline, frontmatter, content_hash, updated_at, effective_date, effective_date_source, import_filename, chunker_version, source_path)
      VALUES (${sourceId}, ${slug}, ${page.type}, ${pageKind}, ${page.title}, ${page.compiled_truth}, ${page.timeline || ''}, ${sql.json(frontmatter as Parameters<typeof sql.json>[0])}, ${hash}, now(), ${effectiveDate}, ${effectiveDateSource}, ${importFilename}, COALESCE(${chunkerVersion}::smallint, 1), ${sourcePath})
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
      RETURNING id, source_id, slug, type, title, compiled_truth, timeline, frontmatter, content_hash, created_at, updated_at, effective_date, effective_date_source, import_filename
    `;
    return rowToPage(rows[0]);
  }
,
  deletePage: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<void> {
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';
    await sql`DELETE FROM pages WHERE slug = ${slug} AND source_id = ${sourceId}`;
  }
,
  softDeletePage: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<{ slug: string } | null> {
    const sql = this.sql;
    const sourceId = opts?.sourceId;
    // Idempotent-as-null contract: only flip rows that are currently active.
    // RETURNING projects the slug so we can tell hit-vs-miss without a probe.
    const sourceCondition = sourceId ? sql`AND source_id = ${sourceId}` : sql``;
    const rows = await sql`
      UPDATE pages SET deleted_at = now()
      WHERE slug = ${slug} AND deleted_at IS NULL ${sourceCondition}
      RETURNING slug
    `;
    if (rows.length === 0) return null;
    return { slug: rows[0].slug as string };
  }
,
  restorePage: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<boolean> {
    const sql = this.sql;
    const sourceId = opts?.sourceId;
    const sourceCondition = sourceId ? sql`AND source_id = ${sourceId}` : sql``;
    const rows = await sql`
      UPDATE pages SET deleted_at = NULL
      WHERE slug = ${slug} AND deleted_at IS NOT NULL ${sourceCondition}
      RETURNING slug
    `;
    return rows.length > 0;
  }
,
  purgeDeletedPages: async function(this: PostgresEngineLike, olderThanHours: number): Promise<{ slugs: string[]; count: number }> {
    const sql = this.sql;
    // Clamp to non-negative integer; runaway purge protection. The DELETE
    // cascades through content_chunks, page_links, chunk_relations via FKs.
    const hours = Math.max(0, Math.floor(olderThanHours));
    const rows = await sql`
      DELETE FROM pages
      WHERE deleted_at IS NOT NULL
        AND deleted_at < now() - (${hours} || ' hours')::interval
      RETURNING slug
    `;
    const slugs = rows.map((r) => r.slug as string);
    return { slugs, count: slugs.length };
  }
,
  refreshPageBody: async function(this: PostgresEngineLike, 
    slug: string,
    sourceId: string,
    compiledTruth: string,
    timeline: string,
    contentHash: string,
  ): Promise<void> {
    const sql = this.sql;
    // Narrow UPDATE — leaves frontmatter, type, chunks, links, embeddings,
    // tags, takes untouched. Skips soft-deleted rows so a redirect retry
    // can't accidentally reanimate the body of a deleted canonical.
    await sql`
      UPDATE pages
      SET compiled_truth = ${compiledTruth},
          timeline = ${timeline},
          content_hash = ${contentHash},
          updated_at = now()
      WHERE source_id = ${sourceId}
        AND slug = ${slug}
        AND deleted_at IS NULL
    `;
  }
,
  migrateFactsToCanonical: async function(this: PostgresEngineLike, 
    phantomSlug: string,
    canonicalSlug: string,
    sourceId: string,
  ): Promise<{ migrated: number }> {
    const sql = this.sql;
    // UPDATE preserves every other column (embedding, valid_*, kind,
    // status, notability, confidence, source_session, ...). Idempotent
    // by virtue of the WHERE clause matching nothing on re-run.
    //
    // We scope to `expired_at IS NULL` so the migration touches only
    // active facts. Forgotten / superseded rows that already carry an
    // expiry stay where they are — soft-deleting the phantom page is
    // sufficient to make them invisible without rewriting their slug
    // (and rewriting would break the audit trail in listSupersessions).
    const result = await sql`
      UPDATE facts
      SET entity_slug = ${canonicalSlug},
          source_markdown_slug = ${canonicalSlug}
      WHERE source_id = ${sourceId}
        AND source_markdown_slug = ${phantomSlug}
        AND expired_at IS NULL
    `;
    return { migrated: result.count ?? 0 };
  }
,
  listPages: async function(this: PostgresEngineLike, filters?: PageFilters): Promise<Page[]> {
    const sql = this.sql;
    const limit = filters?.limit || 100;
    const offset = filters?.offset || 0;
    const updatedAfter = filters?.updated_after;

    // postgres.js sql.unsafe is awkward for conditional WHERE; use raw query branching.
    // The 4 dimensions (type, tag, updated_after, none) cross-product into 8 cases;
    // we use postgres.js's tagged-template chaining via sql`` fragments instead.

    // Build conditions with sql fragments. postgres.js supports fragment composition.
    const typeCondition = filters?.type ? sql`AND p.type = ${filters.type}` : sql``;
    const tagJoin = filters?.tag ? sql`JOIN tags t ON t.page_id = p.id` : sql``;
    const tagCondition = filters?.tag ? sql`AND t.tag = ${filters.tag}` : sql``;
    const updatedCondition = updatedAfter ? sql`AND p.updated_at > ${updatedAfter}::timestamptz` : sql``;
    // slugPrefix uses the (source_id, slug) UNIQUE btree index for range scans.
    // Escape LIKE metacharacters so the user prefix is treated as a literal.
    const slugPrefix = filters?.slugPrefix;
    const slugCondition = slugPrefix
      ? sql`AND p.slug LIKE ${slugPrefix.replace(/[\\%_]/g, (c) => '\\' + c) + '%'} ESCAPE '\\'`
      : sql``;
    // v0.31.12 + v0.34.1 (#876, D9): scope to a single source OR an array
    // of sources. When BOTH are set, the array wins (federated semantics
    // subsume the scalar case). When neither is set, no filter applies.
    const sourceCondition = filters?.sourceIds && filters.sourceIds.length > 0
      ? sql`AND p.source_id = ANY(${filters.sourceIds}::text[])`
      : filters?.sourceId
        ? sql`AND p.source_id = ${filters.sourceId}`
        : sql``;
    // v0.26.5: hide soft-deleted by default; opt in via filters.includeDeleted.
    const deletedCondition = filters?.includeDeleted === true
      ? sql``
      : sql`AND p.deleted_at IS NULL`;

    // v0.29: ORDER BY threading via PAGE_SORT_SQL whitelist (no SQL injection).
    // postgres.js sql.unsafe lets us splice the literal fragment safely.
    const sortKey = filters?.sort && PAGE_SORT_SQL[filters.sort] ? filters.sort : 'updated_desc';
    const orderBy = sql.unsafe(PAGE_SORT_SQL[sortKey]);

    const rows = await sql`
      SELECT p.* FROM pages p
      ${tagJoin}
      WHERE 1=1 ${typeCondition} ${tagCondition} ${updatedCondition} ${slugCondition} ${sourceCondition} ${deletedCondition}
      ORDER BY ${orderBy} LIMIT ${limit} OFFSET ${offset}
    `;

    return rows.map(rowToPage);
  }
,
  getAllSlugs: async function(this: PostgresEngineLike, opts?: { sourceId?: string }): Promise<Set<string>> {
    const sql = this.sql;
    // v0.31.8 (D12): two-branch. See pglite-engine.ts:getAllSlugs for context.
    if (opts?.sourceId) {
      const rows = await sql`SELECT slug FROM pages WHERE source_id = ${opts.sourceId}`;
      return new Set(rows.map((r) => r.slug as string));
    }
    const rows = await sql`SELECT slug FROM pages`;
    return new Set(rows.map((r) => r.slug as string));
  }
,
  listAllPageRefs: async function(this: PostgresEngineLike, ): Promise<Array<{ slug: string; source_id: string }>> {
    // v0.32.8: cross-source page enumeration. ORDER BY (source_id, slug) for
    // deterministic iteration (F11) — same-slug-different-source pages stay
    // grouped predictably. WHERE deleted_at IS NULL matches default getPage
    // visibility semantics (v0.26.5).
    const sql = this.sql;
    const rows = await sql`
      SELECT slug, source_id FROM pages
      WHERE deleted_at IS NULL
      ORDER BY source_id, slug
    `;
    return rows.map((r) => ({ slug: r.slug as string, source_id: r.source_id as string }));
  }
,
  listPrefixSampledPages: async function(this: PostgresEngineLike, opts: DomainBankSampleOpts): Promise<DomainBankRow[]> {
    const sql = this.sql;
    if (opts.prefixes.length === 0) return [];
    const exclude = opts.excludeSlugs ?? [];
    const staleBias = opts.staleBias === true;
    const staleThreshold = opts.staleThresholdDays ?? 90;
    // Source scoping (D5, codex r2 #2 — federated array wins over scalar).
    const sourceIds = opts.sourceIds ?? null;
    const sourceId = opts.sourceId ?? null;
    const rows = await sql`
      WITH prefix_pages AS (
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
          AND substring(p.slug from '^[^/]+/[^/]+') = ANY(${opts.prefixes}::text[])
          AND (cardinality(${exclude}::text[]) = 0 OR NOT (p.slug = ANY(${exclude}::text[])))
          AND (
            (${sourceIds}::text[] IS NOT NULL AND p.source_id = ANY(${sourceIds}::text[]))
            OR (${sourceIds}::text[] IS NULL AND ${sourceId}::text IS NOT NULL AND p.source_id = ${sourceId})
            OR (${sourceIds}::text[] IS NULL AND ${sourceId}::text IS NULL)
          )
        GROUP BY p.id, p.slug, p.source_id, p.title, p.compiled_truth, p.last_retrieved_at
      ),
      ranked AS (
        SELECT
          pp.*,
          (CASE WHEN ${staleBias}::boolean THEN
            CASE
              WHEN pp.last_retrieved_at IS NULL THEN 2
              WHEN pp.last_retrieved_at < NOW() - (${staleThreshold}::int * INTERVAL '1 day') THEN 1
              ELSE 0
            END
          ELSE 0
          END) AS stale_score,
          ROW_NUMBER() OVER (
            PARTITION BY pp.prefix
            ORDER BY
              (CASE WHEN ${staleBias}::boolean THEN
                CASE
                  WHEN pp.last_retrieved_at IS NULL THEN 2
                  WHEN pp.last_retrieved_at < NOW() - (${staleThreshold}::int * INTERVAL '1 day') THEN 1
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
      ORDER BY prefix
    `;
    return rows.map((r): DomainBankRow => ({
      slug: r.slug as string,
      source_id: r.source_id as string,
      prefix: r.prefix as string | null,
      page_id: Number(r.page_id),
      title: r.title as string | null,
      compiled_truth: (r.compiled_truth as string | null) ?? '',
      connection_count: Number(r.connection_count),
      last_retrieved_at: r.last_retrieved_at as Date | null,
      representative_chunk_id: r.representative_chunk_id == null ? null : Number(r.representative_chunk_id),
    }));
  }
,
  listCorpusSample: async function(this: PostgresEngineLike, opts: CorpusSampleOpts): Promise<DomainBankRow[]> {
    const sql = this.sql;
    if (opts.n <= 0) return [];
    const exclude = opts.excludeSlugs ?? [];
    const sourceIds = opts.sourceIds ?? null;
    const sourceId = opts.sourceId ?? null;
    // setseed deterministic path: use SELECT setseed($1) + RANDOM(). PGLite/Postgres
    // both honor setseed for the same session/transaction. For tests this gives
    // identical ordering across runs.
    if (typeof opts.seed === 'number') {
      // Clamp to [-1, 1] required by setseed.
      const clamped = Math.max(-1, Math.min(1, opts.seed));
      await sql`SELECT setseed(${clamped}::float8)`;
    }
    const rows = await sql`
      WITH sampled AS (
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
          AND (cardinality(${exclude}::text[]) = 0 OR NOT (p.slug = ANY(${exclude}::text[])))
          AND (
            (${sourceIds}::text[] IS NOT NULL AND p.source_id = ANY(${sourceIds}::text[]))
            OR (${sourceIds}::text[] IS NULL AND ${sourceId}::text IS NOT NULL AND p.source_id = ${sourceId})
            OR (${sourceIds}::text[] IS NULL AND ${sourceId}::text IS NULL)
          )
        ORDER BY RANDOM()
        LIMIT ${opts.n}
      )
      SELECT
        s.*,
        (
          SELECT cc.id FROM content_chunks cc
          WHERE cc.page_id = s.page_id AND cc.embedding IS NOT NULL
          ORDER BY cc.chunk_index ASC
          LIMIT 1
        ) AS representative_chunk_id
      FROM sampled s
    `;
    return rows.map((r): DomainBankRow => ({
      slug: r.slug as string,
      source_id: r.source_id as string,
      prefix: r.prefix as string | null,
      page_id: Number(r.page_id),
      title: r.title as string | null,
      compiled_truth: (r.compiled_truth as string | null) ?? '',
      connection_count: Number(r.connection_count),
      last_retrieved_at: r.last_retrieved_at as Date | null,
      representative_chunk_id: r.representative_chunk_id == null ? null : Number(r.representative_chunk_id),
    }));
  }
,
  resolveSlugs: async function(this: PostgresEngineLike, partial: string): Promise<string[]> {
    const sql = this.sql;

    // Try exact match first
    const exact = await sql`SELECT slug FROM pages WHERE slug = ${partial}`;
    if (exact.length > 0) return [exact[0].slug];

    // Fuzzy match via pg_trgm
    const fuzzy = await sql`
      SELECT slug, similarity(title, ${partial}) AS sim
      FROM pages
      WHERE title % ${partial} OR slug ILIKE ${'%' + partial + '%'}
      ORDER BY sim DESC
      LIMIT 5
    `;
    return fuzzy.map((r) => r.slug as string);
  }
,
  searchKeyword: async function(this: PostgresEngineLike, query: string, opts?: SearchOpts): Promise<SearchResult[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts?.limit);
    const offset = opts?.offset || 0;
    const type = opts?.type;
    const excludeSlugs = opts?.exclude_slugs;
    const language = opts?.language;
    const symbolKind = opts?.symbolKind;

    if (opts?.limit && opts.limit > MAX_SEARCH_LIMIT) {
      console.warn(`[gbrain] Warning: search limit clamped from ${opts.limit} to ${MAX_SEARCH_LIMIT}`);
    }

    const detailLow = opts?.detail === 'low';
    // Fetch headroom for dedup: if we only fetch `limit` chunks, a cluster of
    // co-occurring terms in one page can eat the entire result set and we'd
    // ship < limit pages. 3x gives dedup enough to pick top N distinct pages.
    const innerLimit = Math.min(limit * 3, MAX_SEARCH_LIMIT * 3);

    // Source-aware ranking (v0.22): boost curated content (originals/,
    // concepts/, writing/) and dampen bulk content (chat/, daily/, media/x/)
    // by multiplying the chunk-grain ts_rank with a source-factor CASE.
    // Detail-gated — disabled for `detail='high'` (temporal queries) so
    // chat surfaces normally for date-framed lookups. Hard-exclude prefixes
    // (test/, archive/, attachments/, .raw/ by default) filter at the
    // chunk-rank stage so they never enter the candidate set.
    const boostMap = resolveBoostMap();
    const sourceFactorCase = buildSourceFactorCase('p.slug', boostMap, opts?.detail);
    const hardExcludePrefixes = resolveHardExcludes(opts?.exclude_slug_prefixes, opts?.include_slug_prefixes);
    const hardExcludeClause = buildHardExcludeClause('p.slug', hardExcludePrefixes);

    const params: unknown[] = [query];
    let typeClause = '';
    if (type) {
      params.push(type);
      typeClause = `AND p.type = $${params.length}`;
    }
    // v0.33: multi-type filter for whoknows. AND-applied alongside the
    // single-value `type` filter (callers can use either or both).
    let typesClause = '';
    if (opts?.types && opts.types.length > 0) {
      params.push(opts.types);
      typesClause = `AND p.type = ANY($${params.length}::text[])`;
    }
    let excludeSlugsClause = '';
    if (excludeSlugs?.length) {
      params.push(excludeSlugs);
      excludeSlugsClause = `AND p.slug != ALL($${params.length}::text[])`;
    }
    let languageClause = '';
    if (language) {
      params.push(language);
      languageClause = `AND cc.language = $${params.length}`;
    }
    let symbolKindClause = '';
    if (symbolKind) {
      params.push(symbolKind);
      symbolKindClause = `AND cc.symbol_type = $${params.length}`;
    }
    // v0.27.0: date filtering support
    let afterDateClause = '';
    if (opts?.afterDate) {
      params.push(opts.afterDate);
      afterDateClause = `AND COALESCE(p.updated_at, p.created_at) > $${params.length}::timestamptz`;
    }
    let beforeDateClause = '';
    if (opts?.beforeDate) {
      params.push(opts.beforeDate);
      beforeDateClause = `AND COALESCE(p.updated_at, p.created_at) < $${params.length}::timestamptz`;
    }
    // v0.34.1 (#861 — P0 leak seal): source-isolation filter. When the
    // caller's auth scope is set, narrow the inner CTE candidate set so
    // an authenticated MCP client cannot see foreign-source pages via
    // keyword search. Array form wins over scalar (federated subsumes
    // single-source). Index-backed by idx_pages_source_id; the filter is
    // pushed to the INNER CTE specifically so HNSW-style downstream
    // ranking sees a narrowed candidate set rather than re-ranking a
    // cross-source pool.
    let sourceClause = '';
    if (opts?.sourceIds && opts.sourceIds.length > 0) {
      params.push(opts.sourceIds);
      sourceClause = `AND p.source_id = ANY($${params.length}::text[])`;
    } else if (opts?.sourceId) {
      params.push(opts.sourceId);
      sourceClause = `AND p.source_id = $${params.length}`;
    }
    params.push(innerLimit);
    const innerLimitParam = `$${params.length}`;
    params.push(limit);
    const limitParam = `$${params.length}`;
    params.push(offset);
    const offsetParam = `$${params.length}`;

    // v0.26.5: visibility filter hides soft-deleted pages and pages from
    // archived sources. Joined `sources s` lets the predicate compile to a
    // column lookup. NOT bypassed by detail=high — soft-delete is a contract,
    // not a temporal preference.
    const visibilityClause = buildVisibilityClause('p', 's');

    const rawQuery = `
      WITH ranked_chunks AS (
        SELECT
          p.slug, p.id as page_id, p.title, p.type, p.source_id, p.access_count, p.confidence_score,
          p.effective_date, p.effective_date_source,
          cc.id as chunk_id, cc.chunk_index, cc.chunk_text, cc.chunk_source,
          ts_rank(cc.search_vector, websearch_to_tsquery('english', $1)) * ${sourceFactorCase} AS score
        FROM content_chunks cc
        JOIN pages p ON p.id = cc.page_id
        JOIN sources s ON s.id = p.source_id
        WHERE cc.search_vector @@ websearch_to_tsquery('english', $1)
          ${typeClause}
          ${typesClause}
          ${excludeSlugsClause}
          ${detailLow ? `AND cc.chunk_source = 'compiled_truth'` : ''}
          ${languageClause}
          ${symbolKindClause}
          ${afterDateClause}
          ${beforeDateClause}
          ${sourceClause}
          ${hardExcludeClause}
          ${visibilityClause}
          -- v0.27.1: hide image rows from text-keyword search so OCR text
          -- doesn't drown text-page hits. Image search runs a separate
          -- vector path on embedding_image.
          AND cc.modality = 'text'
        ORDER BY score DESC
        LIMIT ${innerLimitParam}
      ),
      best_per_page AS (
        SELECT DISTINCT ON (slug) *
        FROM ranked_chunks
        ORDER BY slug, score DESC
      )
      SELECT slug, page_id, title, type, source_id,
        effective_date, effective_date_source,
        chunk_id, chunk_index, chunk_text, chunk_source, score,
        false AS stale
      FROM best_per_page
      ORDER BY score DESC
      LIMIT ${limitParam}
      OFFSET ${offsetParam}
    `;

    // Search-only timeout. SET LOCAL inside sql.begin() scopes the GUC
    // to the transaction so it can never leak onto a pooled connection.
    const rows = await sql.begin(async sql => {
      await sql`SET LOCAL statement_timeout = '8s'`;
      return await sql.unsafe(rawQuery, params as Parameters<typeof sql.unsafe>[1]);
    });
    return rows.map(rowToSearchResult);
  }
,
  searchKeywordChunks: async function(this: PostgresEngineLike, query: string, opts?: SearchOpts): Promise<SearchResult[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts?.limit);
    const offset = opts?.offset || 0;
    const type = opts?.type;
    const excludeSlugs = opts?.exclude_slugs;
    const detailLow = opts?.detail === 'low';
    const language = opts?.language;
    const symbolKind = opts?.symbolKind;

    if (opts?.limit && opts.limit > MAX_SEARCH_LIMIT) {
      console.warn(`[gbrain] Warning: search limit clamped from ${opts.limit} to ${MAX_SEARCH_LIMIT}`);
    }

    // Source-aware ranking applies here too — searchKeywordChunks is the
    // chunk-grain anchor primitive that two-pass retrieval (Layer 7) uses,
    // so curated-vs-bulk dampening should affect the anchor pool. Same
    // detail-gate, same hard-exclude behavior as searchKeyword.
    const boostMap = resolveBoostMap();
    const sourceFactorCase = buildSourceFactorCase('p.slug', boostMap, opts?.detail);
    const hardExcludePrefixes = resolveHardExcludes(opts?.exclude_slug_prefixes, opts?.include_slug_prefixes);
    const hardExcludeClause = buildHardExcludeClause('p.slug', hardExcludePrefixes);

    const params: unknown[] = [query];
    let typeClause = '';
    if (type) {
      params.push(type);
      typeClause = `AND p.type = $${params.length}`;
    }
    // v0.33: multi-type filter for whoknows. AND-applied alongside the
    // single-value `type` filter (callers can use either or both).
    let typesClause = '';
    if (opts?.types && opts.types.length > 0) {
      params.push(opts.types);
      typesClause = `AND p.type = ANY($${params.length}::text[])`;
    }
    let excludeSlugsClause = '';
    if (excludeSlugs?.length) {
      params.push(excludeSlugs);
      excludeSlugsClause = `AND p.slug != ALL($${params.length}::text[])`;
    }
    let languageClause = '';
    if (language) {
      params.push(language);
      languageClause = `AND cc.language = $${params.length}`;
    }
    let symbolKindClause = '';
    if (symbolKind) {
      params.push(symbolKind);
      symbolKindClause = `AND cc.symbol_type = $${params.length}`;
    }
    // v0.27.0: date filtering support
    let afterDateClause = '';
    if (opts?.afterDate) {
      params.push(opts.afterDate);
      afterDateClause = `AND COALESCE(p.updated_at, p.created_at) > $${params.length}::timestamptz`;
    }
    let beforeDateClause = '';
    if (opts?.beforeDate) {
      params.push(opts.beforeDate);
      beforeDateClause = `AND COALESCE(p.updated_at, p.created_at) < $${params.length}::timestamptz`;
    }
    // v0.34.1 (#861 — P0 leak seal): source-isolation. Anchor primitive
    // for two-pass retrieval, so cross-source anchors would let the walk
    // discover foreign-source neighbors. Filter at chunk-rank time.
    let sourceClause = '';
    if (opts?.sourceIds && opts.sourceIds.length > 0) {
      params.push(opts.sourceIds);
      sourceClause = `AND p.source_id = ANY($${params.length}::text[])`;
    } else if (opts?.sourceId) {
      params.push(opts.sourceId);
      sourceClause = `AND p.source_id = $${params.length}`;
    }
    params.push(limit);
    const limitParam = `$${params.length}`;
    params.push(offset);
    const offsetParam = `$${params.length}`;

    // v0.26.5: visibility filter for searchKeywordChunks (anchor primitive).
    const visibilityClause = buildVisibilityClause('p', 's');

    const rawQuery = `
      SELECT
        p.slug, p.id as page_id, p.title, p.type, p.source_id, p.access_count, p.confidence_score,
        p.effective_date, p.effective_date_source,
        cc.id as chunk_id, cc.chunk_index, cc.chunk_text, cc.chunk_source,
        ts_rank(cc.search_vector, websearch_to_tsquery('english', $1)) * ${sourceFactorCase} AS score,
        false AS stale
      FROM content_chunks cc
      JOIN pages p ON p.id = cc.page_id
      JOIN sources s ON s.id = p.source_id
      WHERE cc.search_vector @@ websearch_to_tsquery('english', $1)
        ${typeClause}
        ${typesClause}
        ${excludeSlugsClause}
        ${detailLow ? `AND cc.chunk_source = 'compiled_truth'` : ''}
        ${languageClause}
        ${symbolKindClause}
        ${afterDateClause}
        ${beforeDateClause}
        ${sourceClause}
        ${hardExcludeClause}
        ${visibilityClause}
      ORDER BY score DESC
      LIMIT ${limitParam}
      OFFSET ${offsetParam}
    `;

    const rows = await sql.begin(async sql => {
      await sql`SET LOCAL statement_timeout = '8s'`;
      return await sql.unsafe(rawQuery, params as Parameters<typeof sql.unsafe>[1]);
    });
    return rows.map(rowToSearchResult);
  }
,
  searchVector: async function(this: PostgresEngineLike, embedding: Float32Array, opts?: SearchOpts): Promise<SearchResult[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts?.limit);
    const offset = opts?.offset || 0;
    const type = opts?.type;
    const excludeSlugs = opts?.exclude_slugs;
    const detailLow = opts?.detail === 'low';
    const language = opts?.language;
    const symbolKind = opts?.symbolKind;

    if (opts?.limit && opts.limit > MAX_SEARCH_LIMIT) {
      console.warn(`[gbrain] Warning: search limit clamped from ${opts.limit} to ${MAX_SEARCH_LIMIT}`);
    }

    const vecStr = '[' + Array.from(embedding).join(',') + ']';

    // Two-stage CTE (v0.22): inner CTE keeps a pure-distance ORDER BY so
    // the HNSW index stays usable. Folding source-boost into the inner
    // ORDER BY would force a sequential scan over every chunk (seconds vs
    // ~10ms with HNSW). Outer SELECT re-ranks the candidate pool by
    // raw_score * source_factor.
    //
    // innerLimit scales with offset to preserve the pagination contract:
    // a fixed cap of 100 would silently empty offset > 100.
    const boostMap = resolveBoostMap();
    const sourceFactorCaseOnSlug = buildSourceFactorCase('slug', boostMap, opts?.detail);
    const hardExcludePrefixes = resolveHardExcludes(opts?.exclude_slug_prefixes, opts?.include_slug_prefixes);
    const hardExcludeClause = buildHardExcludeClause('p.slug', hardExcludePrefixes);
    const innerLimit = offset + Math.max(limit * 5, 100);

    const params: unknown[] = [vecStr];
    let typeClause = '';
    if (type) {
      params.push(type);
      typeClause = `AND p.type = $${params.length}`;
    }
    // v0.33: multi-type filter for whoknows. AND-applied alongside the
    // single-value `type` filter (callers can use either or both).
    let typesClause = '';
    if (opts?.types && opts.types.length > 0) {
      params.push(opts.types);
      typesClause = `AND p.type = ANY($${params.length}::text[])`;
    }
    let excludeSlugsClause = '';
    if (excludeSlugs?.length) {
      params.push(excludeSlugs);
      excludeSlugsClause = `AND p.slug != ALL($${params.length}::text[])`;
    }
    let languageClause = '';
    if (language) {
      params.push(language);
      languageClause = `AND cc.language = $${params.length}`;
    }
    let symbolKindClause = '';
    if (symbolKind) {
      params.push(symbolKind);
      symbolKindClause = `AND cc.symbol_type = $${params.length}`;
    }
    // v0.27.0: date filtering support
    let afterDateClause = '';
    if (opts?.afterDate) {
      params.push(opts.afterDate);
      afterDateClause = `AND COALESCE(p.updated_at, p.created_at) > $${params.length}::timestamptz`;
    }
    let beforeDateClause = '';
    if (opts?.beforeDate) {
      params.push(opts.beforeDate);
      beforeDateClause = `AND COALESCE(p.updated_at, p.created_at) < $${params.length}::timestamptz`;
    }
    // v0.34.1 (#861, F2 — P0 leak seal): source-isolation in the INNER CTE
    // specifically. Pushing the filter inside narrows the HNSW candidate set
    // before re-rank; pushing it to the outer SELECT would force HNSW to
    // over-fetch then post-filter, wasting candidate slots. Codex flagged
    // this placement during plan review. Array form wins over scalar.
    let sourceClause = '';
    if (opts?.sourceIds && opts.sourceIds.length > 0) {
      params.push(opts.sourceIds);
      sourceClause = `AND p.source_id = ANY($${params.length}::text[])`;
    } else if (opts?.sourceId) {
      params.push(opts.sourceId);
      sourceClause = `AND p.source_id = $${params.length}`;
    }
    params.push(innerLimit);
    const innerLimitParam = `$${params.length}`;
    params.push(limit);
    const limitParam = `$${params.length}`;
    params.push(offset);
    const offsetParam = `$${params.length}`;

    // v0.26.5: visibility filter applied in the inner CTE so the HNSW index
    // sees the same row count it always did. Pulling the predicate to the
    // outer SELECT would force the HNSW scan to over-fetch and post-filter,
    // wasting candidate slots on hidden rows.
    const visibilityClause = buildVisibilityClause('p', 's');

    // v0.36 (D11): column routing via resolved descriptor. Engine doesn't
    // read config — caller (hybrid/op) resolved it and passed it in.
    // normalizeEngineColumn accepts the legacy union (string literals,
    // ResolvedColumn, undefined) and produces a canonical descriptor.
    //
    // v0.36 Phase 3: 'embedding_multimodal' is the unified column populated
    // by `gbrain reindex --multimodal`. Carries BOTH text and image content
    // in Voyage multimodal-3 space — no modality filter; the column itself
    // is the discriminator (rows without embedding_multimodal aren't searched).
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

    const rawQuery = `
      WITH hnsw_candidates AS (
        SELECT
          p.slug, p.id as page_id, p.title, p.type, p.source_id, p.access_count, p.confidence_score,
          p.effective_date, p.effective_date_source,
          cc.id as chunk_id, cc.chunk_index, cc.chunk_text, cc.chunk_source,
          1 - (cc.${col} <=> ${castSql}) AS raw_score
        FROM content_chunks cc
        JOIN pages p ON p.id = cc.page_id
        JOIN sources s ON s.id = p.source_id
        WHERE cc.${col} IS NOT NULL ${modalityFilter}
          ${detailLow ? `AND cc.chunk_source = 'compiled_truth'` : ''}
          ${typeClause}
          ${typesClause}
          ${excludeSlugsClause}
          ${languageClause}
          ${symbolKindClause}
          ${afterDateClause}
          ${beforeDateClause}
          ${sourceClause}
          ${hardExcludeClause}
          ${visibilityClause}
        ORDER BY cc.${col} <=> ${castSql}
        LIMIT ${innerLimitParam}
      )
      SELECT
        slug, page_id, title, type, source_id,
        effective_date, effective_date_source,
        chunk_id, chunk_index, chunk_text, chunk_source,
        raw_score * ${sourceFactorCaseOnSlug} AS score,
        false AS stale
      FROM hnsw_candidates
      ORDER BY score DESC
      LIMIT ${limitParam}
      OFFSET ${offsetParam}
    `;

    const rows = await sql.begin(async sql => {
      await sql`SET LOCAL statement_timeout = '8s'`;
      return await sql.unsafe(rawQuery, params as Parameters<typeof sql.unsafe>[1]);
    });
    return rows.map(rowToSearchResult);
  }
,
  getEmbeddingsByChunkIds: async function(this: PostgresEngineLike, 
    ids: number[],
    column: string = 'embedding',
  ): Promise<Map<number, Float32Array>> {
    if (ids.length === 0) return new Map();
    // v0.36 (D9): column parameter used by hybrid.cosineReScore so
    // rescoring rehydrates from the active column's embedding space,
    // not always 'embedding'. Engine has no resolver access; the
    // caller must pass a known column name. Identifier-quoted (D12
    // defense layer 2) plus a strict regex check (D12 defense layer 1)
    // so even a misconfigured caller can't smuggle a SQL fragment.
    if (!COLUMN_NAME_REGEX.test(column)) {
      throw new EmbeddingColumnNotRegisteredError(column, []);
    }
    const quotedCol = quoteIdentifier(column);
    const sql = this.sql;
    const rawQuery = `
      SELECT id, ${quotedCol} AS embedding FROM content_chunks
      WHERE id = ANY($1::int[]) AND ${quotedCol} IS NOT NULL
    `;
    const rows = await sql.unsafe(rawQuery, [ids] as Parameters<typeof sql.unsafe>[1]);
    const result = new Map<number, Float32Array>();
    for (const row of rows) {
      const embedding = tryParseEmbedding(row.embedding);
      if (embedding) result.set(row.id as number, embedding);
    }
    return result;
  }
,
  upsertChunks: async function(this: PostgresEngineLike, slug: string, chunks: ChunkInput[], opts?: { sourceId?: string }): Promise<void> {
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';

    // Source-scope the page-id lookup. Without this filter, multi-source
    // brains where the slug exists in 2+ sources return >1 row and the
    // chunk replacement targets the wrong page (or fans out across pages).
    const pages = await sql`SELECT id FROM pages WHERE slug = ${slug} AND source_id = ${sourceId}`;
    if (pages.length === 0) throw new Error(`Page not found: ${slug} (source=${sourceId})`);
    const pageId = pages[0].id;

    // Remove chunks that no longer exist (chunk_index beyond new count)
    const newIndices = chunks.map(c => c.chunk_index);
    if (newIndices.length > 0) {
      await sql`DELETE FROM content_chunks WHERE page_id = ${pageId} AND chunk_index != ALL(${newIndices})`;
    } else {
      await sql`DELETE FROM content_chunks WHERE page_id = ${pageId}`;
      return;
    }

    // Batch upsert: build a single multi-row INSERT ON CONFLICT statement.
    // v0.19.0: includes language/symbol_name/symbol_type/start_line/end_line
    // so code chunks carry tree-sitter metadata into the DB. Markdown chunks
    // pass NULL for all five.
    // v0.20.0 Cathedral II Layer 6: adds parent_symbol_path / doc_comment /
    // symbol_name_qualified so nested-chunk emission (A3) can round-trip
    // scope metadata through upserts.
    // v0.27.1 (Phase 8): added `modality` + `embedding_image` to the column
    // list. Image chunks pass embedding=null + embedding_image=Float32Array.
    const cols = '(page_id, chunk_index, chunk_text, chunk_source, embedding, model, token_count, embedded_at, language, symbol_name, symbol_type, start_line, end_line, parent_symbol_path, doc_comment, symbol_name_qualified, modality, embedding_image)';
    const rows: string[] = [];
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

      const embeddingPh = embeddingStr ? `$${paramIdx++}::vector` : 'NULL';
      const embeddedAtPh = embeddingStr ? 'now()' : 'NULL';
      const embeddingImagePh = embeddingImageStr ? `$${paramIdx++}::vector` : 'NULL';

      rows.push(
        `($${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, ` +
        `${embeddingPh}, $${paramIdx++}, $${paramIdx++}, ${embeddedAtPh}, ` +
        `$${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, $${paramIdx++}, ` +
        `$${paramIdx++}::text[], $${paramIdx++}, $${paramIdx++}, ` +
        `$${paramIdx++}, ${embeddingImagePh})`,
      );

      // Param push order MUST match placeholder allocation order.
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

    // Single statement upsert: preserves existing embeddings via COALESCE when new value is NULL.
    // CONSISTENCY: when chunk_text changes and no new embedding is supplied, BOTH embedding AND
    // embedded_at must reset to NULL so `embed --stale` correctly picks up the row for re-embedding.
    // Without this, embedded_at lies (says "embedded" while embedding=NULL), and any staleness
    // predicate on embedded_at would silently skip the row. This is why the egress fix predicates
    // on `embedding IS NULL` rather than `embedded_at IS NULL` — and it's why we now keep both
    // columns honest at write time.
    await sql.unsafe(
      `INSERT INTO content_chunks ${cols} VALUES ${rows.join(', ')}
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
      params as Parameters<typeof sql.unsafe>[1],
    );
  }
,
  getChunks: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<Chunk[]> {
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';
    const rows = await sql`
      SELECT cc.* FROM content_chunks cc
      JOIN pages p ON p.id = cc.page_id
      WHERE p.slug = ${slug} AND p.source_id = ${sourceId}
      ORDER BY cc.chunk_index
    `;
    return rows.map((r) => rowToChunk(r as Record<string, unknown>));
  }
,
  countStaleChunks: async function(this: PostgresEngineLike, opts?: { sourceId?: string }): Promise<number> {
    const sql = this.sql;
    // Fast path: no source filter → bare count query, no join.
    // Slow path: source-scoped count → join pages.
    // D7: closes the bug where `gbrain embed --stale --source X` silently
    // dropped X and counted across every source.
    if (opts?.sourceId === undefined) {
      const [row] = await sql`
        SELECT count(*)::int AS count
        FROM content_chunks
        WHERE embedding IS NULL
      `;
      return Number((row as { count?: number } | undefined)?.count ?? 0);
    }
    const [row] = await sql`
      SELECT count(*)::int AS count
      FROM content_chunks cc
      JOIN pages p ON p.id = cc.page_id
      WHERE cc.embedding IS NULL
        AND p.source_id = ${opts.sourceId}
    `;
    return Number((row as { count?: number } | undefined)?.count ?? 0);
  }
,
  listStaleChunks: async function(this: PostgresEngineLike, opts?: {
    batchSize?: number;
    afterPageId?: number;
    afterChunkIndex?: number;
    sourceId?: string;
  }): Promise<StaleChunkRow[]> {
    const sql = this.sql;
    const limit = opts?.batchSize ?? 2000;
    const afterPid = opts?.afterPageId ?? 0;
    const afterIdx = opts?.afterChunkIndex ?? -1;
    // Cursor-paginated: keyset pagination on (page_id, chunk_index).
    // The partial index idx_chunks_embedding_null makes the WHERE fast;
    // LIMIT keeps each round-trip well within statement_timeout.
    //
    // D7: optional source_id filter. NULL/undefined = scan all sources
    // (pre-existing behavior); a value scopes to that source so
    // `gbrain embed --stale --source X` actually does what it says.
    if (opts?.sourceId === undefined) {
      const rows = await sql`
        SELECT p.slug, cc.chunk_index, cc.chunk_text, cc.chunk_source,
               cc.model, cc.token_count, p.source_id, cc.page_id
        FROM content_chunks cc
        JOIN pages p ON p.id = cc.page_id
        WHERE cc.embedding IS NULL
          AND (cc.page_id, cc.chunk_index) > (${afterPid}, ${afterIdx})
        ORDER BY cc.page_id, cc.chunk_index
        LIMIT ${limit}
      `;
      return rows as unknown as StaleChunkRow[];
    }
    const rows = await sql`
      SELECT p.slug, cc.chunk_index, cc.chunk_text, cc.chunk_source,
             cc.model, cc.token_count, p.source_id, cc.page_id
      FROM content_chunks cc
      JOIN pages p ON p.id = cc.page_id
      WHERE cc.embedding IS NULL
        AND p.source_id = ${opts.sourceId}
        AND (cc.page_id, cc.chunk_index) > (${afterPid}, ${afterIdx})
      ORDER BY cc.page_id, cc.chunk_index
      LIMIT ${limit}
    `;
    return rows as unknown as StaleChunkRow[];
  }
,
  deleteChunks: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<void> {
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';
    await sql`
      DELETE FROM content_chunks
      WHERE page_id = (SELECT id FROM pages WHERE slug = ${slug} AND source_id = ${sourceId})
    `;
  }

};
