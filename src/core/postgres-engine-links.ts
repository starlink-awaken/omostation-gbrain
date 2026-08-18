/**
 * PostgresEngine links methods — split out of postgres-engine.ts (BET-Y1Q3-T6-04).
 * Injected onto PostgresEngine.prototype via Object.assign.
 */
import {
  MAX_SEARCH_LIMIT,
  clampSearchLimit,
  type DreamVerdict,
  type DreamVerdictInput,
  type FactInsertStatus,
  type FactKind,
  type FactListOpts,
  type FactRow,
  type FactVisibility,
  type FactsHealth,
  type FileRow,
  type FileSpec,
  type LinkBatchInput,
  type NewFact,
  type TimelineBatchInput,
} from './engine.ts';
import { type GraphNode, GraphPath, Link, RawData, TimelineEntry, TimelineInput, TimelineOpts } from './types.ts';
import { contentHash, tryParseEmbedding } from './utils.ts';
import postgres from 'postgres';


export interface PostgresEngineLike {
  sql: ReturnType<typeof postgres>;
  connectionManager: import('./connection-manager.ts').ConnectionManager | null;
  [key: string]: any;
}

export const postgresLinksMethods: Record<string, any> = {
  addLink: async function(this: PostgresEngineLike, 
    from: string,
    to: string,
    context?: string,
    linkType?: string,
    linkSource?: string,
    originSlug?: string,
    originField?: string,
    opts?: { fromSourceId?: string; toSourceId?: string; originSourceId?: string },
  ): Promise<void> {
    const sql = this.sql;
    const fromSrc = opts?.fromSourceId ?? 'default';
    const toSrc = opts?.toSourceId ?? 'default';
    const originSrc = opts?.originSourceId ?? 'default';

    // Pre-check existence so we can throw a clear error (ON CONFLICT DO UPDATE
    // returns 0 rows when source SELECT is empty, indistinguishable from missing
    // page). Source-qualified — pre-v0.18 the bare slug check matched ANY source,
    // letting addLink succeed even when the intended source row was missing.
    const exists = await sql`
      SELECT 1 FROM pages WHERE slug = ${from} AND source_id = ${fromSrc}
      INTERSECT
      SELECT 1 FROM pages WHERE slug = ${to} AND source_id = ${toSrc}
    `;
    if (exists.length === 0) {
      throw new Error(`addLink failed: page "${from}" (source=${fromSrc}) or "${to}" (source=${toSrc}) not found`);
    }
    // Default link_source to 'markdown' for back-compat with pre-v0.13 callers.
    // Mirror addLinksBatch's VALUES + JOIN-on-(slug, source_id) shape. The old
    // `FROM pages f, pages t` cross-product fanned out across every source
    // containing either slug, so a multi-source brain silently created edges
    // pointing at the wrong pages.
    const src = linkSource ?? 'markdown';
    await sql`
      INSERT INTO links (from_page_id, to_page_id, link_type, context, link_source, origin_page_id, origin_field)
      SELECT f.id, t.id, v.link_type, v.context, v.link_source, o.id, v.origin_field
      FROM (VALUES (${from}, ${to}, ${linkType || ''}, ${context || ''}, ${src}, ${originSlug ?? null}, ${originField ?? null}, ${fromSrc}, ${toSrc}, ${originSrc}))
        AS v(from_slug, to_slug, link_type, context, link_source, origin_slug, origin_field, from_source_id, to_source_id, origin_source_id)
      JOIN pages f ON f.slug = v.from_slug AND f.source_id = v.from_source_id
      JOIN pages t ON t.slug = v.to_slug AND t.source_id = v.to_source_id
      LEFT JOIN pages o ON o.slug = v.origin_slug AND o.source_id = v.origin_source_id
      ON CONFLICT (from_page_id, to_page_id, link_type, link_source, origin_page_id) DO UPDATE SET
        context = EXCLUDED.context,
        origin_field = EXCLUDED.origin_field
    `;
  }
,
  addLinksBatch: async function(this: PostgresEngineLike, links: LinkBatchInput[]): Promise<number> {
    if (links.length === 0) return 0;
    const sql = this.sql;
    // unnest() pattern: 7 array-typed bound parameters regardless of batch size.
    // Avoids the 65535-parameter cap and the postgres-js sql(rows, ...) helper's
    // identifier-escape gotcha when used inside a (VALUES) subquery.
    //
    // v0.13: added link_source, origin_slug, origin_field. Defaults:
    //   link_source  → 'markdown' (back-compat with pre-v0.13 callers)
    //   origin_slug  → NULL (resolves to origin_page_id IS NULL via LEFT JOIN)
    //   origin_field → NULL
    const fromSlugs = links.map(l => l.from_slug);
    const toSlugs = links.map(l => l.to_slug);
    const linkTypes = links.map(l => l.link_type || '');
    const contexts = links.map(l => l.context || '');
    const linkSources = links.map(l => l.link_source || 'markdown');
    const originSlugs = links.map(l => l.origin_slug || null);
    const originFields = links.map(l => l.origin_field || null);
    const fromSourceIds = links.map(l => l.from_source_id || 'default');
    const toSourceIds = links.map(l => l.to_source_id || 'default');
    const originSourceIds = links.map(l => l.origin_source_id || 'default');
    const result = await sql`
      INSERT INTO links (from_page_id, to_page_id, link_type, context, link_source, origin_page_id, origin_field)
      SELECT f.id, t.id, v.link_type, v.context, v.link_source, o.id, v.origin_field
      FROM unnest(
        ${fromSlugs}::text[], ${toSlugs}::text[], ${linkTypes}::text[],
        ${contexts}::text[], ${linkSources}::text[], ${originSlugs}::text[],
        ${originFields}::text[], ${fromSourceIds}::text[], ${toSourceIds}::text[],
        ${originSourceIds}::text[]
      ) AS v(from_slug, to_slug, link_type, context, link_source, origin_slug, origin_field, from_source_id, to_source_id, origin_source_id)
      JOIN pages f ON f.slug = v.from_slug AND f.source_id = v.from_source_id
      JOIN pages t ON t.slug = v.to_slug AND t.source_id = v.to_source_id
      LEFT JOIN pages o ON o.slug = v.origin_slug AND o.source_id = v.origin_source_id
      ON CONFLICT (from_page_id, to_page_id, link_type, link_source, origin_page_id) DO NOTHING
      RETURNING 1
    `;
    return result.length;
  }
,
  removeLink: async function(this: PostgresEngineLike, 
    from: string,
    to: string,
    linkType?: string,
    linkSource?: string,
    opts?: { fromSourceId?: string; toSourceId?: string },
  ): Promise<void> {
    const sql = this.sql;
    const fromSrc = opts?.fromSourceId ?? 'default';
    const toSrc = opts?.toSourceId ?? 'default';
    // Build up filters dynamically. linkType + linkSource are independent
    // optional constraints; all four combinations are valid. Each branch's
    // page-id subquery is source-qualified so multi-source brains don't
    // delete the wrong (from, to) pair.
    if (linkType !== undefined && linkSource !== undefined) {
      await sql`
        DELETE FROM links
        WHERE from_page_id = (SELECT id FROM pages WHERE slug = ${from} AND source_id = ${fromSrc})
          AND to_page_id = (SELECT id FROM pages WHERE slug = ${to} AND source_id = ${toSrc})
          AND link_type = ${linkType}
          AND link_source IS NOT DISTINCT FROM ${linkSource}
      `;
    } else if (linkType !== undefined) {
      await sql`
        DELETE FROM links
        WHERE from_page_id = (SELECT id FROM pages WHERE slug = ${from} AND source_id = ${fromSrc})
          AND to_page_id = (SELECT id FROM pages WHERE slug = ${to} AND source_id = ${toSrc})
          AND link_type = ${linkType}
      `;
    } else if (linkSource !== undefined) {
      await sql`
        DELETE FROM links
        WHERE from_page_id = (SELECT id FROM pages WHERE slug = ${from} AND source_id = ${fromSrc})
          AND to_page_id = (SELECT id FROM pages WHERE slug = ${to} AND source_id = ${toSrc})
          AND link_source IS NOT DISTINCT FROM ${linkSource}
      `;
    } else {
      await sql`
        DELETE FROM links
        WHERE from_page_id = (SELECT id FROM pages WHERE slug = ${from} AND source_id = ${fromSrc})
          AND to_page_id = (SELECT id FROM pages WHERE slug = ${to} AND source_id = ${toSrc})
      `;
    }
  }
,
  getLinks: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<Link[]> {
    const sql = this.sql;
    // v0.31.8 (D16): two-branch query. Without opts.sourceId, no source filter
    // (preserves pre-v0.31.8 cross-source semantics). With opts.sourceId,
    // scope the from-page lookup. See pglite-engine.ts:getLinks for context.
    if (opts?.sourceId) {
      const rows = await sql`
        SELECT f.slug as from_slug, t.slug as to_slug,
               l.link_type, l.context, l.link_source,
               o.slug as origin_slug, l.origin_field
        FROM links l
        JOIN pages f ON f.id = l.from_page_id
        JOIN pages t ON t.id = l.to_page_id
        LEFT JOIN pages o ON o.id = l.origin_page_id
        WHERE f.slug = ${slug} AND f.source_id = ${opts.sourceId}
      `;
      return rows as unknown as Link[];
    }
    const rows = await sql`
      SELECT f.slug as from_slug, t.slug as to_slug,
             l.link_type, l.context, l.link_source,
             o.slug as origin_slug, l.origin_field
      FROM links l
      JOIN pages f ON f.id = l.from_page_id
      JOIN pages t ON t.id = l.to_page_id
      LEFT JOIN pages o ON o.id = l.origin_page_id
      WHERE f.slug = ${slug}
    `;
    return rows as unknown as Link[];
  }
,
  getBacklinks: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<Link[]> {
    const sql = this.sql;
    // v0.31.8 (D16): two-branch query, mirrors getLinks above.
    if (opts?.sourceId) {
      const rows = await sql`
        SELECT f.slug as from_slug, t.slug as to_slug,
               l.link_type, l.context, l.link_source,
               o.slug as origin_slug, l.origin_field
        FROM links l
        JOIN pages f ON f.id = l.from_page_id
        JOIN pages t ON t.id = l.to_page_id
        LEFT JOIN pages o ON o.id = l.origin_page_id
        WHERE t.slug = ${slug} AND t.source_id = ${opts.sourceId}
      `;
      return rows as unknown as Link[];
    }
    const rows = await sql`
      SELECT f.slug as from_slug, t.slug as to_slug,
             l.link_type, l.context, l.link_source,
             o.slug as origin_slug, l.origin_field
      FROM links l
      JOIN pages f ON f.id = l.from_page_id
      JOIN pages t ON t.id = l.to_page_id
      LEFT JOIN pages o ON o.id = l.origin_page_id
      WHERE t.slug = ${slug}
    `;
    return rows as unknown as Link[];
  }
,
  findByTitleFuzzy: async function(this: PostgresEngineLike, 
    name: string,
    dirPrefix?: string,
    minSimilarity: number = 0.55,
  ): Promise<{ slug: string; similarity: number } | null> {
    const sql = this.sql;
    // Use the `similarity()` function directly with an explicit threshold
    // comparison. DO NOT use `SET LOCAL pg_trgm.similarity_threshold` +
    // the `%` operator here — postgres.js auto-commits each sql`` call
    // so `SET LOCAL` is a no-op across statement boundaries. Inline
    // comparison is the only way to get predictable threshold behavior
    // without wrapping the caller in a transaction.
    //
    // Tie-breaker: sort by slug after similarity so re-runs return the
    // same winner when multiple pages score equally (prevents churn
    // in put_page auto-link reconciliation).
    const prefixPattern = dirPrefix ? `${dirPrefix}/%` : '%';
    const rows = await sql`
      SELECT slug, similarity(title, ${name}) AS sim
      FROM pages
      WHERE similarity(title, ${name}) >= ${minSimilarity}
        AND slug LIKE ${prefixPattern}
      ORDER BY sim DESC, slug ASC
      LIMIT 1
    `;
    if (rows.length === 0) return null;
    const row = rows[0] as { slug: string; sim: number };
    return { slug: row.slug, similarity: row.sim };
  }
,
  traverseGraph: async function(this: PostgresEngineLike, 
    slug: string,
    depth: number = 5,
    opts?: import('./engine.ts').TraverseGraphOpts,
  ): Promise<GraphNode[]> {
    const sql = this.sql;
    // v0.34.1 (#861 — P0 leak seal): scope visited nodes to the caller's
    // source(s). Without this, the walk follows edges into pages from
    // foreign sources, leaking topology + page metadata. The filter
    // applies at BOTH the seed (root must be in scope) AND the recursive
    // step (every visited neighbor must be in scope). The aggregation
    // subquery also filters so the per-node `links` array only includes
    // edges to in-scope pages.
    const useSourceIds = opts?.sourceIds && opts.sourceIds.length > 0;
    const seedScope = useSourceIds
      ? sql`AND p.source_id = ANY(${opts!.sourceIds!}::text[])`
      : opts?.sourceId
        ? sql`AND p.source_id = ${opts.sourceId}`
        : sql``;
    const stepScope = useSourceIds
      ? sql`AND p2.source_id = ANY(${opts!.sourceIds!}::text[])`
      : opts?.sourceId
        ? sql`AND p2.source_id = ${opts.sourceId}`
        : sql``;
    const aggScope = useSourceIds
      ? sql`AND p3.source_id = ANY(${opts!.sourceIds!}::text[])`
      : opts?.sourceId
        ? sql`AND p3.source_id = ${opts.sourceId}`
        : sql``;
    // T8 (v0.36+): frontier cap. When set, the recursive term applies a
    // parenthesized LIMIT N with ORDER BY (slug, id) for stable selection.
    // Postgres' parenthesized-LIMIT inside a recursive term caps per
    // ITERATION, which maps approximately to per-BFS-LAYER (the mapping is
    // exact when fanout is bounded; for hub-fanout graphs the cap fires
    // early). Post-query, count rows per depth — if any depth == cap, fire
    // the truncation callback.
    const cap = opts?.frontierCap;
    const recursiveStep = cap !== undefined && cap > 0
      ? sql`(SELECT p2.id, p2.slug, p2.title, p2.type, g.depth + 1, g.visited || p2.id
             FROM graph g
             JOIN links l ON l.from_page_id = g.id
             JOIN pages p2 ON p2.id = l.to_page_id
             WHERE g.depth < ${depth}
               AND NOT (p2.id = ANY(g.visited))
               ${stepScope}
             ORDER BY p2.slug ASC, p2.id ASC
             LIMIT ${cap})`
      : sql`SELECT p2.id, p2.slug, p2.title, p2.type, g.depth + 1, g.visited || p2.id
            FROM graph g
            JOIN links l ON l.from_page_id = g.id
            JOIN pages p2 ON p2.id = l.to_page_id
            WHERE g.depth < ${depth}
              AND NOT (p2.id = ANY(g.visited))
              ${stepScope}`;
    // Cycle prevention: visited array tracks page IDs already in the path.
    const rows = await sql`
      WITH RECURSIVE graph AS (
        SELECT p.id, p.slug, p.title, p.type, 0 as depth, ARRAY[p.id] as visited
        FROM pages p WHERE p.slug = ${slug} ${seedScope}

        UNION ALL

        ${recursiveStep}
      )
      SELECT DISTINCT g.slug, g.title, g.type, g.depth,
        coalesce(
          -- jsonb_agg(DISTINCT ...) collapses duplicate (to_slug, link_type)
          -- edges that originate from different provenance (markdown body
          -- vs frontmatter vs auto-extracted). The underlying links table
          -- preserves every row with its origin_page_id / link_source —
          -- the dedup is presentation-only for the legacy traverseGraph
          -- aggregation. traversePaths has its own in-memory dedup at a
          -- different layer. See plan Bug 6/10.
          (SELECT jsonb_agg(DISTINCT jsonb_build_object('to_slug', p3.slug, 'link_type', l2.link_type))
           FROM links l2
           JOIN pages p3 ON p3.id = l2.to_page_id
           WHERE l2.from_page_id = g.id ${aggScope}),
          '[]'::jsonb
        ) as links
      FROM graph g
      ORDER BY g.depth, g.slug
    `;

    // T8 truncation-detection callback was designed here but the v1 algorithm
    // had both false-positive (organic count == cap) and false-negative
    // (LIMIT-before-DISTINCT in diamond graphs) cases caught by adversarial
    // review. Stripped pending the dedupe-then-cap SQL rewrite + real Postgres
    // parity coverage. See (tracked TODO) → "T8 truncation signal".

    return rows.map((r: Record<string, unknown>) => ({
      slug: r.slug as string,
      title: r.title as string,
      type: r.type as string,
      depth: r.depth as number,
      links: (typeof r.links === 'string' ? JSON.parse(r.links) : r.links) as { to_slug: string; link_type: string }[],
    }));
  }
,
  traversePaths: async function(this: PostgresEngineLike, 
    slug: string,
    opts?: { depth?: number; linkType?: string; direction?: 'in' | 'out' | 'both'; sourceId?: string; sourceIds?: string[] },
  ): Promise<GraphPath[]> {
    const sql = this.sql;
    const depth = opts?.depth ?? 5;
    const direction = opts?.direction ?? 'out';
    const linkType = opts?.linkType ?? null;
    const linkTypeMatches = linkType !== null;
    // v0.34.1 (#861 — P0 leak seal): source-scope filter fragments. Applied
    // at seed (root must be in scope) AND at every recursive step (neighbor
    // must be in scope) AND in the SELECT join (final edges respect scope).
    // The 'both' branch needs filters on BOTH endpoint joins.
    const useSourceIds = opts?.sourceIds && opts.sourceIds.length > 0;
    const seedScope = useSourceIds
      ? sql`AND p.source_id = ANY(${opts!.sourceIds!}::text[])`
      : opts?.sourceId
        ? sql`AND p.source_id = ${opts.sourceId}`
        : sql``;
    const stepScope = useSourceIds
      ? sql`AND p2.source_id = ANY(${opts!.sourceIds!}::text[])`
      : opts?.sourceId
        ? sql`AND p2.source_id = ${opts.sourceId}`
        : sql``;
    // For the 'both' direction's final SELECT, both endpoint joins (pf, pt)
    // get scope filters so edges crossing into a foreign source are dropped.
    const pfScope = useSourceIds
      ? sql`AND pf.source_id = ANY(${opts!.sourceIds!}::text[])`
      : opts?.sourceId
        ? sql`AND pf.source_id = ${opts.sourceId}`
        : sql``;
    const ptScope = useSourceIds
      ? sql`AND pt.source_id = ANY(${opts!.sourceIds!}::text[])`
      : opts?.sourceId
        ? sql`AND pt.source_id = ${opts.sourceId}`
        : sql``;

    let rows;
    if (direction === 'out') {
      rows = await sql`
        WITH RECURSIVE walk AS (
          SELECT p.id, p.slug, 0::int as depth, ARRAY[p.id] as visited
          FROM pages p WHERE p.slug = ${slug} ${seedScope}
          UNION ALL
          SELECT p2.id, p2.slug, w.depth + 1, w.visited || p2.id
          FROM walk w
          JOIN links l ON l.from_page_id = w.id
          JOIN pages p2 ON p2.id = l.to_page_id
          WHERE w.depth < ${depth}
            AND NOT (p2.id = ANY(w.visited))
            AND (${!linkTypeMatches} OR l.link_type = ${linkType ?? ''})
            ${stepScope}
        )
        SELECT w.slug as from_slug, p2.slug as to_slug,
               l.link_type, l.context, w.depth + 1 as depth
        FROM walk w
        JOIN links l ON l.from_page_id = w.id
        JOIN pages p2 ON p2.id = l.to_page_id
        WHERE w.depth < ${depth}
          AND (${!linkTypeMatches} OR l.link_type = ${linkType ?? ''})
          ${stepScope}
        ORDER BY depth, from_slug, to_slug
      `;
    } else if (direction === 'in') {
      rows = await sql`
        WITH RECURSIVE walk AS (
          SELECT p.id, p.slug, 0::int as depth, ARRAY[p.id] as visited
          FROM pages p WHERE p.slug = ${slug} ${seedScope}
          UNION ALL
          SELECT p2.id, p2.slug, w.depth + 1, w.visited || p2.id
          FROM walk w
          JOIN links l ON l.to_page_id = w.id
          JOIN pages p2 ON p2.id = l.from_page_id
          WHERE w.depth < ${depth}
            AND NOT (p2.id = ANY(w.visited))
            AND (${!linkTypeMatches} OR l.link_type = ${linkType ?? ''})
            ${stepScope}
        )
        SELECT p2.slug as from_slug, w.slug as to_slug,
               l.link_type, l.context, w.depth + 1 as depth
        FROM walk w
        JOIN links l ON l.to_page_id = w.id
        JOIN pages p2 ON p2.id = l.from_page_id
        WHERE w.depth < ${depth}
          AND (${!linkTypeMatches} OR l.link_type = ${linkType ?? ''})
          ${stepScope}
        ORDER BY depth, from_slug, to_slug
      `;
    } else {
      rows = await sql`
        WITH RECURSIVE walk AS (
          SELECT p.id, 0::int as depth, ARRAY[p.id] as visited
          FROM pages p WHERE p.slug = ${slug} ${seedScope}
          UNION ALL
          SELECT p2.id, w.depth + 1, w.visited || p2.id
          FROM walk w
          JOIN links l ON (l.from_page_id = w.id OR l.to_page_id = w.id)
          JOIN pages p2 ON p2.id = CASE WHEN l.from_page_id = w.id THEN l.to_page_id ELSE l.from_page_id END
          WHERE w.depth < ${depth}
            AND NOT (p2.id = ANY(w.visited))
            AND (${!linkTypeMatches} OR l.link_type = ${linkType ?? ''})
            ${stepScope}
        )
        SELECT pf.slug as from_slug, pt.slug as to_slug,
               l.link_type, l.context, w.depth + 1 as depth
        FROM walk w
        JOIN links l ON (l.from_page_id = w.id OR l.to_page_id = w.id)
        JOIN pages pf ON pf.id = l.from_page_id
        JOIN pages pt ON pt.id = l.to_page_id
        WHERE w.depth < ${depth}
          AND (${!linkTypeMatches} OR l.link_type = ${linkType ?? ''})
          ${pfScope}
          ${ptScope}
        ORDER BY depth, from_slug, to_slug
      `;
    }

    // Dedup edges (same edge can appear via multiple visited paths).
    const seen = new Set<string>();
    const result: GraphPath[] = [];
    for (const r of rows as Record<string, unknown>[]) {
      const key = `${r.from_slug}|${r.to_slug}|${r.link_type}|${r.depth}`;
      if (seen.has(key)) continue;
      seen.add(key);
      result.push({
        from_slug: r.from_slug as string,
        to_slug: r.to_slug as string,
        link_type: r.link_type as string,
        context: (r.context as string) || '',
        depth: Number(r.depth),
      });
    }
    return result;
  }
,
  getBacklinkCounts: async function(this: PostgresEngineLike, slugs: string[]): Promise<Map<string, number>> {
    const result = new Map<string, number>();
    if (slugs.length === 0) return result;
    for (const s of slugs) result.set(s, 0);

    const sql = this.sql;
    const rows = await sql`
      SELECT p.slug as slug, COUNT(l.id)::int as cnt
      FROM pages p
      LEFT JOIN links l ON l.to_page_id = p.id
      WHERE p.slug = ANY(${slugs}::text[])
      GROUP BY p.slug
    `;
    for (const r of rows as unknown as { slug: string; cnt: number }[]) {
      result.set(r.slug, Number(r.cnt));
    }
    return result;
  }
,
  getPageTimestamps: async function(this: PostgresEngineLike, slugs: string[]): Promise<Map<string, Date>> {
    if (slugs.length === 0) return new Map();
    const sql = this.sql;
    const rows = await sql`
      SELECT slug, COALESCE(updated_at, created_at) as ts
      FROM pages WHERE slug = ANY(${slugs}::text[])
    `;
    return new Map(rows.map(r => [r.slug as string, new Date(r.ts as string)]));
  }
,
  getEffectiveDates: async function(this: PostgresEngineLike, refs: Array<{slug: string; source_id: string}>): Promise<Map<string, Date>> {
    if (refs.length === 0) return new Map();
    const sql = this.sql;
    const slugs = refs.map(r => r.slug);
    const sourceIds = refs.map(r => r.source_id);
    // Composite-keyed: a page is unique by (source_id, slug). unnest the
    // two arrays in lockstep so multi-source brains don't fan out across
    // sources (codex pass-1 finding #3).
    const rows = await sql`
      SELECT p.slug, p.source_id, COALESCE(p.effective_date, p.updated_at, p.created_at) AS ts
        FROM pages p
        JOIN unnest(${slugs}::text[], ${sourceIds}::text[]) AS u(slug, source_id)
          ON p.slug = u.slug AND p.source_id = u.source_id
    `;
    const out = new Map<string, Date>();
    for (const raw of rows as unknown as Array<Record<string, unknown>>) {
      const r = raw as { slug: string; source_id: string; ts: string | Date };
      const key = `${r.source_id}::${r.slug}`;
      out.set(key, r.ts instanceof Date ? r.ts : new Date(r.ts));
    }
    return out;
  }
,
  getSalienceScores: async function(this: PostgresEngineLike, refs: Array<{slug: string; source_id: string}>): Promise<Map<string, number>> {
    if (refs.length === 0) return new Map();
    const sql = this.sql;
    const slugs = refs.map(r => r.slug);
    const sourceIds = refs.map(r => r.source_id);
    // Salience = emotional_weight × 5 + ln(1 + take_count). Pure mattering
    // signal — NO time component (per D9: salience and recency are
    // orthogonal axes). Composite-keyed for multi-source isolation.
    const rows = await sql`
      SELECT p.slug, p.source_id,
             (COALESCE(p.emotional_weight, 0) * 5
              + ln(1 + COUNT(DISTINCT t.id))) AS score
        FROM pages p
        JOIN unnest(${slugs}::text[], ${sourceIds}::text[]) AS u(slug, source_id)
          ON p.slug = u.slug AND p.source_id = u.source_id
        LEFT JOIN takes t ON t.page_id = p.id AND t.active = TRUE
       GROUP BY p.id
    `;
    const out = new Map<string, number>();
    for (const raw of rows as unknown as Array<Record<string, unknown>>) {
      const r = raw as { slug: string; source_id: string; score: number | string };
      const key = `${r.source_id}::${r.slug}`;
      out.set(key, Number(r.score));
    }
    return out;
  }
,
  findOrphanPages: async function(this: PostgresEngineLike, ): Promise<Array<{ slug: string; title: string; domain: string | null }>> {
    const sql = this.sql;
    // Soft-delete filter on BOTH sides:
    //   - candidate: p.deleted_at IS NULL — soft-deleted pages aren't orphan candidates
    //   - link source: src.deleted_at IS NULL — links FROM soft-deleted pages don't count as inbound
    // Without the link-source filter, a live page can hide from orphan results purely
    // because a soft-deleted page links to it. v0.26.5 invariant; codex C11.
    const rows = await sql`
      SELECT
        p.slug,
        COALESCE(p.title, p.slug) AS title,
        p.frontmatter->>'domain' AS domain
      FROM pages p
      WHERE p.deleted_at IS NULL
        AND NOT EXISTS (
          SELECT 1
          FROM links l
          JOIN pages src ON src.id = l.from_page_id
          WHERE l.to_page_id = p.id
            AND src.deleted_at IS NULL
        )
      ORDER BY p.slug
    `;
    return rows as unknown as Array<{ slug: string; title: string; domain: string | null }>;
  }
,
  addTag: async function(this: PostgresEngineLike, slug: string, tag: string, opts?: { sourceId?: string }): Promise<void> {
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';
    // Verify page exists before attempting insert (ON CONFLICT DO NOTHING
    // swallows the "already tagged" case, but we still need to detect missing
    // pages). Source-scoped lookup — pre-v0.18 the bare-slug subquery returned
    // multiple rows in multi-source brains and crashed with Postgres 21000.
    const page = await sql`SELECT id FROM pages WHERE slug = ${slug} AND source_id = ${sourceId}`;
    if (page.length === 0) throw new Error(`addTag failed: page "${slug}" (source=${sourceId}) not found`);
    await sql`
      INSERT INTO tags (page_id, tag)
      VALUES (${page[0].id}, ${tag})
      ON CONFLICT (page_id, tag) DO NOTHING
    `;
  }
,
  removeTag: async function(this: PostgresEngineLike, slug: string, tag: string, opts?: { sourceId?: string }): Promise<void> {
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';
    await sql`
      DELETE FROM tags
      WHERE page_id = (SELECT id FROM pages WHERE slug = ${slug} AND source_id = ${sourceId})
        AND tag = ${tag}
    `;
  }
,
  getTags: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<string[]> {
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';
    const rows = await sql`
      SELECT tag FROM tags
      WHERE page_id = (SELECT id FROM pages WHERE slug = ${slug} AND source_id = ${sourceId})
      ORDER BY tag
    `;
    return rows.map((r) => r.tag as string);
  }
,
  addTimelineEntry: async function(this: PostgresEngineLike, 
    slug: string,
    entry: TimelineInput,
    opts?: { skipExistenceCheck?: boolean; sourceId?: string },
  ): Promise<void> {
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';
    if (!opts?.skipExistenceCheck) {
      const exists = await sql`SELECT 1 FROM pages WHERE slug = ${slug} AND source_id = ${sourceId}`;
      if (exists.length === 0) {
        throw new Error(`addTimelineEntry failed: page "${slug}" (source=${sourceId}) not found`);
      }
    }
    // ON CONFLICT DO NOTHING via the (page_id, date, summary) unique index.
    // Returning 0 rows means either page missing OR duplicate; skipExistenceCheck
    // makes that ambiguity safe (caller asserts page exists). Source-qualify
    // the page-id lookup so multi-source brains don't fan timeline rows out
    // across every source containing the slug.
    await sql`
      INSERT INTO timeline_entries (page_id, date, source, summary, detail)
      SELECT id, ${entry.date}::date, ${entry.source || ''}, ${entry.summary}, ${entry.detail || ''}
      FROM pages WHERE slug = ${slug} AND source_id = ${sourceId}
      ON CONFLICT (page_id, date, summary) DO NOTHING
    `;
  }
,
  addTimelineEntriesBatch: async function(this: PostgresEngineLike, entries: TimelineBatchInput[]): Promise<number> {
    if (entries.length === 0) return 0;
    const sql = this.sql;
    const slugs = entries.map(e => e.slug);
    const dates = entries.map(e => e.date);
    const sources = entries.map(e => e.source || '');
    const summaries = entries.map(e => e.summary);
    const details = entries.map(e => e.detail || '');
    const sourceIds = entries.map(e => e.source_id || 'default');
    const result = await sql`
      INSERT INTO timeline_entries (page_id, date, source, summary, detail)
      SELECT p.id, v.date::date, v.source, v.summary, v.detail
      FROM unnest(${slugs}::text[], ${dates}::text[], ${sources}::text[], ${summaries}::text[], ${details}::text[], ${sourceIds}::text[])
        AS v(slug, date, source, summary, detail, source_id)
      JOIN pages p ON p.slug = v.slug AND p.source_id = v.source_id
      ON CONFLICT (page_id, date, summary) DO NOTHING
      RETURNING 1
    `;
    return result.length;
  }
,
  getTimeline: async function(this: PostgresEngineLike, slug: string, opts?: TimelineOpts): Promise<TimelineEntry[]> {
    const sql = this.sql;
    const limit = opts?.limit || 100;
    // v0.31.8 (D16): branch on every combination of (after, before, sourceId).
    // 8 cases is too many — use an explicit branch on sourceId, then nested
    // branches on after/before. Mirrors pglite-engine but stays in postgres.js
    // template-literal idiom (which doesn't compose fragment WHERE chains
    // cleanly).
    const sourceId = opts?.sourceId;
    let rows;
    if (sourceId) {
      if (opts?.after && opts?.before) {
        rows = await sql`SELECT te.* FROM timeline_entries te JOIN pages p ON p.id = te.page_id
          WHERE p.slug = ${slug} AND p.source_id = ${sourceId}
            AND te.date >= ${opts.after}::date AND te.date <= ${opts.before}::date
          ORDER BY te.date DESC LIMIT ${limit}`;
      } else if (opts?.after) {
        rows = await sql`SELECT te.* FROM timeline_entries te JOIN pages p ON p.id = te.page_id
          WHERE p.slug = ${slug} AND p.source_id = ${sourceId}
            AND te.date >= ${opts.after}::date
          ORDER BY te.date DESC LIMIT ${limit}`;
      } else if (opts?.before) {
        rows = await sql`SELECT te.* FROM timeline_entries te JOIN pages p ON p.id = te.page_id
          WHERE p.slug = ${slug} AND p.source_id = ${sourceId}
            AND te.date <= ${opts.before}::date
          ORDER BY te.date DESC LIMIT ${limit}`;
      } else {
        rows = await sql`SELECT te.* FROM timeline_entries te JOIN pages p ON p.id = te.page_id
          WHERE p.slug = ${slug} AND p.source_id = ${sourceId}
          ORDER BY te.date DESC LIMIT ${limit}`;
      }
    } else if (opts?.after && opts?.before) {
      rows = await sql`SELECT te.* FROM timeline_entries te JOIN pages p ON p.id = te.page_id
        WHERE p.slug = ${slug} AND te.date >= ${opts.after}::date AND te.date <= ${opts.before}::date
        ORDER BY te.date DESC LIMIT ${limit}`;
    } else if (opts?.after) {
      rows = await sql`SELECT te.* FROM timeline_entries te JOIN pages p ON p.id = te.page_id
        WHERE p.slug = ${slug} AND te.date >= ${opts.after}::date
        ORDER BY te.date DESC LIMIT ${limit}`;
    } else if (opts?.before) {
      rows = await sql`SELECT te.* FROM timeline_entries te JOIN pages p ON p.id = te.page_id
        WHERE p.slug = ${slug} AND te.date <= ${opts.before}::date
        ORDER BY te.date DESC LIMIT ${limit}`;
    } else {
      rows = await sql`SELECT te.* FROM timeline_entries te JOIN pages p ON p.id = te.page_id
        WHERE p.slug = ${slug}
        ORDER BY te.date DESC LIMIT ${limit}`;
    }
    return rows as unknown as TimelineEntry[];
  }
,
  putRawData: async function(this: PostgresEngineLike, 
    slug: string,
    source: string,
    data: object,
    opts?: { sourceId?: string },
  ): Promise<void> {
    const sql = this.sql;
    // v0.31.8 (D21): two-branch INSERT-SELECT. Without opts.sourceId, the
    // page-id lookup matches every same-slug page (pre-v0.31.8 behavior).
    // With opts.sourceId, the lookup is source-scoped.
    if (opts?.sourceId) {
      const result = await sql`
        INSERT INTO raw_data (page_id, source, data)
        SELECT id, ${source}, ${sql.json(data as Parameters<typeof sql.json>[0])}
        FROM pages WHERE slug = ${slug} AND source_id = ${opts.sourceId}
        ON CONFLICT (page_id, source) DO UPDATE SET
          data = EXCLUDED.data,
          fetched_at = now()
        RETURNING id
      `;
      if (result.length === 0) {
        throw new Error(`putRawData failed: page "${slug}" (source=${opts.sourceId}) not found`);
      }
      return;
    }
    const result = await sql`
      INSERT INTO raw_data (page_id, source, data)
      SELECT id, ${source}, ${sql.json(data as Parameters<typeof sql.json>[0])}
      FROM pages WHERE slug = ${slug}
      ON CONFLICT (page_id, source) DO UPDATE SET
        data = EXCLUDED.data,
        fetched_at = now()
      RETURNING id
    `;
    if (result.length === 0) throw new Error(`putRawData failed: page "${slug}" not found`);
  }
,
  getRawData: async function(this: PostgresEngineLike, 
    slug: string,
    source?: string,
    opts?: { sourceId?: string },
  ): Promise<RawData[]> {
    const sql = this.sql;
    // v0.31.8 (D21): four-branch shape on (source provided, sourceId provided).
    // Postgres.js template-literal style doesn't compose fragments cleanly so
    // we enumerate.
    const sourceId = opts?.sourceId;
    let rows;
    if (source && sourceId) {
      rows = await sql`SELECT rd.source, rd.data, rd.fetched_at FROM raw_data rd
        JOIN pages p ON p.id = rd.page_id
        WHERE p.slug = ${slug} AND rd.source = ${source} AND p.source_id = ${sourceId}`;
    } else if (source) {
      rows = await sql`SELECT rd.source, rd.data, rd.fetched_at FROM raw_data rd
        JOIN pages p ON p.id = rd.page_id
        WHERE p.slug = ${slug} AND rd.source = ${source}`;
    } else if (sourceId) {
      rows = await sql`SELECT rd.source, rd.data, rd.fetched_at FROM raw_data rd
        JOIN pages p ON p.id = rd.page_id
        WHERE p.slug = ${slug} AND p.source_id = ${sourceId}`;
    } else {
      rows = await sql`SELECT rd.source, rd.data, rd.fetched_at FROM raw_data rd
        JOIN pages p ON p.id = rd.page_id
        WHERE p.slug = ${slug}`;
    }
    return rows as unknown as RawData[];
  }
,
  upsertFile: async function(this: PostgresEngineLike, spec: FileSpec): Promise<{ id: number; created: boolean }> {
    const sql = this.sql;
    const sourceId = spec.source_id ?? 'default';
    const metadata = (spec.metadata ?? {}) as Parameters<typeof sql.json>[0];
    const rows = await sql<Array<{ id: number; created: boolean }>>`
      INSERT INTO files (source_id, page_slug, page_id, filename, storage_path, mime_type, size_bytes, content_hash, metadata)
      VALUES (${sourceId}, ${spec.page_slug ?? null}, ${spec.page_id ?? null}, ${spec.filename}, ${spec.storage_path}, ${spec.mime_type ?? null}, ${spec.size_bytes ?? null}, ${spec.content_hash}, ${sql.json(metadata)})
      ON CONFLICT (storage_path) DO UPDATE SET
        page_slug = EXCLUDED.page_slug,
        page_id = EXCLUDED.page_id,
        filename = EXCLUDED.filename,
        mime_type = EXCLUDED.mime_type,
        size_bytes = EXCLUDED.size_bytes,
        content_hash = EXCLUDED.content_hash,
        metadata = EXCLUDED.metadata
      RETURNING id, (xmax = 0) AS created
    `;
    if (rows.length === 0) throw new Error(`upsertFile returned no rows for ${spec.storage_path}`);
    return { id: rows[0].id, created: !!rows[0].created };
  }
,
  getFile: async function(this: PostgresEngineLike, sourceId: string, storagePath: string): Promise<FileRow | null> {
    const sql = this.sql;
    const rows = await sql<Array<FileRow>>`
      SELECT id, source_id, page_slug, page_id, filename, storage_path, mime_type, size_bytes, content_hash, metadata, created_at
      FROM files
      WHERE source_id = ${sourceId} AND storage_path = ${storagePath}
      LIMIT 1
    `;
    return rows.length > 0 ? rows[0] : null;
  }
,
  listFilesForPage: async function(this: PostgresEngineLike, pageId: number): Promise<FileRow[]> {
    const sql = this.sql;
    const rows = await sql<Array<FileRow>>`
      SELECT id, source_id, page_slug, page_id, filename, storage_path, mime_type, size_bytes, content_hash, metadata, created_at
      FROM files
      WHERE page_id = ${pageId}
      ORDER BY created_at ASC
    `;
    return rows as FileRow[];
  }
,
  getDreamVerdict: async function(this: PostgresEngineLike, filePath: string, contentHash: string): Promise<DreamVerdict | null> {
    const sql = this.sql;
    const rows = await sql<Array<{
      worth_processing: boolean;
      reasons: string[] | null;
      judged_at: Date;
    }>>`
      SELECT worth_processing, reasons, judged_at
      FROM dream_verdicts
      WHERE file_path = ${filePath} AND content_hash = ${contentHash}
    `;
    if (rows.length === 0) return null;
    const r = rows[0];
    return {
      worth_processing: r.worth_processing,
      reasons: r.reasons ?? [],
      judged_at: r.judged_at instanceof Date ? r.judged_at.toISOString() : String(r.judged_at),
    };
  }
,
  putDreamVerdict: async function(this: PostgresEngineLike, filePath: string, contentHash: string, verdict: DreamVerdictInput): Promise<void> {
    const sql = this.sql;
    await sql`
      INSERT INTO dream_verdicts (file_path, content_hash, worth_processing, reasons)
      VALUES (${filePath}, ${contentHash}, ${verdict.worth_processing}, ${sql.json(verdict.reasons as Parameters<typeof sql.json>[0])})
      ON CONFLICT (file_path, content_hash) DO UPDATE SET
        worth_processing = EXCLUDED.worth_processing,
        reasons = EXCLUDED.reasons,
        judged_at = now()
    `;
  }
,
  insertFact: async function(this: PostgresEngineLike, 
    input: NewFact,
    ctx: { source_id: string; supersedeId?: number },
  ): Promise<{ id: number; status: FactInsertStatus }> {
    const sql = this.sql;
    const validFrom = input.valid_from ?? new Date();
    const validUntil = input.valid_until ?? null;
    const kind = input.kind ?? 'fact';
    const visibility = input.visibility ?? 'private';
    const notability = input.notability ?? 'medium';
    const confidence = input.confidence ?? 1.0;
    const entitySlug = input.entity_slug ?? null;
    const context = input.context ?? null;
    const sourceSession = input.source_session ?? null;
    const embedding = input.embedding ?? null;
    const embeddedAt = embedding ? new Date() : null;
    const embedLit = embedding ? toPgVectorLiteral(embedding) : null;
    // v0.35.4 (D-CDX-5) — typed-claim columns. All four nullable.
    const claimMetric = input.claim_metric ?? null;
    const claimValue  = input.claim_value  ?? null;
    const claimUnit   = input.claim_unit   ?? null;
    const claimPeriod = input.claim_period ?? null;

    if (ctx.supersedeId !== undefined) {
      // Per-entity advisory lock + atomic insert + supersede in one txn.
      const supersedeId = ctx.supersedeId;
      const newId = await sql.begin(async (tx) => {
        if (entitySlug) {
          await tx`SELECT pg_advisory_xact_lock(hashtextextended(${ctx.source_id} || ':' || ${entitySlug}, 0))`;
        }
        const ins = await tx<Array<{ id: number }>>`
          INSERT INTO facts (
            source_id, entity_slug, fact, kind, visibility, notability, context,
            valid_from, valid_until, source, source_session, confidence,
            embedding, embedded_at,
            claim_metric, claim_value, claim_unit, claim_period
          ) VALUES (
            ${ctx.source_id}, ${entitySlug}, ${input.fact}, ${kind}, ${visibility}, ${notability}, ${context},
            ${validFrom}, ${validUntil}, ${input.source}, ${sourceSession}, ${confidence},
            ${embedLit === null ? null : tx.unsafe(`'${embedLit}'::vector`)}, ${embeddedAt},
            ${claimMetric}, ${claimValue}, ${claimUnit}, ${claimPeriod}
          ) RETURNING id
        `;
        const id = Number(ins[0].id);
        await tx`UPDATE facts SET expired_at = now(), superseded_by = ${id}
                 WHERE id = ${supersedeId} AND expired_at IS NULL`;
        return id;
      });
      return { id: newId, status: 'superseded' };
    }

    // Plain insert path with optional advisory lock for the dedup window.
    const id = await sql.begin(async (tx) => {
      if (entitySlug) {
        await tx`SELECT pg_advisory_xact_lock(hashtextextended(${ctx.source_id} || ':' || ${entitySlug}, 0))`;
      }
      const ins = await tx<Array<{ id: number }>>`
        INSERT INTO facts (
          source_id, entity_slug, fact, kind, visibility, notability, context,
          valid_from, valid_until, source, source_session, confidence,
          embedding, embedded_at,
          claim_metric, claim_value, claim_unit, claim_period
        ) VALUES (
          ${ctx.source_id}, ${entitySlug}, ${input.fact}, ${kind}, ${visibility}, ${notability}, ${context},
          ${validFrom}, ${validUntil}, ${input.source}, ${sourceSession}, ${confidence},
          ${embedLit === null ? null : tx.unsafe(`'${embedLit}'::vector`)}, ${embeddedAt},
          ${claimMetric}, ${claimValue}, ${claimUnit}, ${claimPeriod}
        ) RETURNING id
      `;
      return Number(ins[0].id);
    });
    return { id, status: 'inserted' };
  }
,
  expireFact: async function(this: PostgresEngineLike, id: number, opts?: { supersededBy?: number; at?: Date }): Promise<boolean> {
    const sql = this.sql;
    const at = opts?.at ?? new Date();
    const supersededBy = opts?.supersededBy ?? null;
    const result = await sql`
      UPDATE facts SET expired_at = ${at}, superseded_by = COALESCE(${supersededBy}, superseded_by)
      WHERE id = ${id} AND expired_at IS NULL
    `;
    return (result.count ?? 0) > 0;
  }
,
  insertFacts: async function(this: PostgresEngineLike, 
    rows: Array<NewFact & { row_num: number; source_markdown_slug: string }>,
    ctx: { source_id: string },
  ): Promise<{ inserted: number; ids: number[] }> {
    if (rows.length === 0) return { inserted: 0, ids: [] };

    const sql = this.sql;
    // Single transaction so the v51 partial UNIQUE index can roll back
    // the whole batch on constraint violation. Per-row INSERTs (not
    // multi-row VALUES) keep the embedding-vs-no-embedding branching
    // readable; batch sizes are small (5-30 rows per page in practice).
    // No supersede flow in this path — fence reconciliation is the
    // canonical source-of-truth direction, not the consolidator path.
    const ids = await sql.begin(async (tx) => {
      const out: number[] = [];
      for (const input of rows) {
        const validFrom = input.valid_from ?? new Date();
        const validUntil = input.valid_until ?? null;
        const kind = input.kind ?? 'fact';
        const visibility = input.visibility ?? 'private';
        const notability = input.notability ?? 'medium';
        const confidence = input.confidence ?? 1.0;
        const entitySlug = input.entity_slug ?? null;
        const context = input.context ?? null;
        const sourceSession = input.source_session ?? null;
        const embedding = input.embedding ?? null;
        const embeddedAt = embedding ? new Date() : null;
        const embedLit = embedding ? toPgVectorLiteral(embedding) : null;
        // v0.35.4 (D-CDX-5) — typed-claim columns. All four nullable.
        const claimMetric = input.claim_metric ?? null;
        const claimValue  = input.claim_value  ?? null;
        const claimUnit   = input.claim_unit   ?? null;
        const claimPeriod = input.claim_period ?? null;

        const ins = await tx<Array<{ id: number }>>`
          INSERT INTO facts (
            source_id, entity_slug, fact, kind, visibility, notability, context,
            valid_from, valid_until, source, source_session, confidence,
            embedding, embedded_at,
            row_num, source_markdown_slug,
            claim_metric, claim_value, claim_unit, claim_period
          ) VALUES (
            ${ctx.source_id}, ${entitySlug}, ${input.fact}, ${kind}, ${visibility}, ${notability}, ${context},
            ${validFrom}, ${validUntil}, ${input.source}, ${sourceSession}, ${confidence},
            ${embedLit === null ? null : tx.unsafe(`'${embedLit}'::vector`)}, ${embeddedAt},
            ${input.row_num}, ${input.source_markdown_slug},
            ${claimMetric}, ${claimValue}, ${claimUnit}, ${claimPeriod}
          ) RETURNING id
        `;
        out.push(Number(ins[0].id));
      }
      return out;
    });
    return { inserted: ids.length, ids };
  }
,
  deleteFactsForPage: async function(this: PostgresEngineLike, slug: string, source_id: string): Promise<{ deleted: number }> {
    const sql = this.sql;
    const result = await sql`
      DELETE FROM facts WHERE source_id = ${source_id} AND source_markdown_slug = ${slug}
    `;
    return { deleted: result.count ?? 0 };
  }
,
  listFactsByEntity: async function(this: PostgresEngineLike, 
    source_id: string,
    entitySlug: string,
    opts?: FactListOpts,
  ): Promise<FactRow[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts?.limit, 50, MAX_SEARCH_LIMIT);
    const offset = Math.max(0, opts?.offset ?? 0);
    const activeOnly = opts?.activeOnly !== false;
    const kinds = (opts?.kinds && opts.kinds.length > 0) ? opts.kinds : null;
    const visibility = (opts?.visibility && opts.visibility.length > 0) ? opts.visibility : null;
    const rows = await sql<FactRowSqlShape[]>`
      SELECT * FROM facts
      WHERE source_id = ${source_id}
        AND entity_slug = ${entitySlug}
        ${activeOnly ? sql`AND expired_at IS NULL` : sql``}
        ${kinds ? sql`AND kind = ANY(${kinds}::text[])` : sql``}
        ${visibility ? sql`AND visibility = ANY(${visibility}::text[])` : sql``}
      ORDER BY valid_from DESC, id DESC
      LIMIT ${limit} OFFSET ${offset}
    `;
    return rows.map(rowToFactPg);
  }
,
  listFactsSince: async function(this: PostgresEngineLike, 
    source_id: string,
    since: Date,
    opts?: FactListOpts & { entitySlug?: string },
  ): Promise<FactRow[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts?.limit, 50, MAX_SEARCH_LIMIT);
    const offset = Math.max(0, opts?.offset ?? 0);
    const activeOnly = opts?.activeOnly !== false;
    const kinds = (opts?.kinds && opts.kinds.length > 0) ? opts.kinds : null;
    const visibility = (opts?.visibility && opts.visibility.length > 0) ? opts.visibility : null;
    const entitySlug = opts?.entitySlug ?? null;
    const rows = await sql<FactRowSqlShape[]>`
      SELECT * FROM facts
      WHERE source_id = ${source_id}
        AND created_at >= ${since}
        ${entitySlug ? sql`AND entity_slug = ${entitySlug}` : sql``}
        ${activeOnly ? sql`AND expired_at IS NULL` : sql``}
        ${kinds ? sql`AND kind = ANY(${kinds}::text[])` : sql``}
        ${visibility ? sql`AND visibility = ANY(${visibility}::text[])` : sql``}
      ORDER BY created_at DESC, id DESC
      LIMIT ${limit} OFFSET ${offset}
    `;
    return rows.map(rowToFactPg);
  }
,
  listFactsBySession: async function(this: PostgresEngineLike, 
    source_id: string,
    sessionId: string,
    opts?: FactListOpts,
  ): Promise<FactRow[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts?.limit, 50, MAX_SEARCH_LIMIT);
    const offset = Math.max(0, opts?.offset ?? 0);
    const activeOnly = opts?.activeOnly !== false;
    const kinds = (opts?.kinds && opts.kinds.length > 0) ? opts.kinds : null;
    const visibility = (opts?.visibility && opts.visibility.length > 0) ? opts.visibility : null;
    const rows = await sql<FactRowSqlShape[]>`
      SELECT * FROM facts
      WHERE source_id = ${source_id}
        AND source_session = ${sessionId}
        ${activeOnly ? sql`AND expired_at IS NULL` : sql``}
        ${kinds ? sql`AND kind = ANY(${kinds}::text[])` : sql``}
        ${visibility ? sql`AND visibility = ANY(${visibility}::text[])` : sql``}
      ORDER BY created_at DESC, id DESC
      LIMIT ${limit} OFFSET ${offset}
    `;
    return rows.map(rowToFactPg);
  }
,
  listSupersessions: async function(this: PostgresEngineLike, 
    source_id: string,
    opts?: { since?: Date; limit?: number },
  ): Promise<FactRow[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts?.limit, 50, MAX_SEARCH_LIMIT);
    const since = opts?.since ?? null;
    const rows = await sql<FactRowSqlShape[]>`
      SELECT * FROM facts
      WHERE source_id = ${source_id}
        AND expired_at IS NOT NULL
        AND superseded_by IS NOT NULL
        ${since ? sql`AND expired_at >= ${since}` : sql``}
      ORDER BY expired_at DESC, id DESC
      LIMIT ${limit}
    `;
    return rows.map(rowToFactPg);
  }
,
  countUnconsolidatedFacts: async function(this: PostgresEngineLike, source_id: string): Promise<number> {
    const sql = this.sql;
    const rows = await sql<{ count: number }[]>`
      SELECT COUNT(*)::int AS count FROM facts
      WHERE source_id = ${source_id}
        AND consolidated_at IS NULL
        AND expired_at IS NULL
    `;
    return Number(rows[0]?.count ?? 0);
  }
,
  findCandidateDuplicates: async function(this: PostgresEngineLike, 
    source_id: string,
    entitySlug: string,
    factText: string,
    opts?: { k?: number; embedding?: Float32Array },
  ): Promise<FactRow[]> {
    const sql = this.sql;
    const k = Math.min(Math.max(opts?.k ?? 5, 1), 20);
    if (opts?.embedding) {
      const lit = toPgVectorLiteral(opts.embedding);
      const rows = await sql<FactRowSqlShape[]>`
        SELECT * FROM facts
        WHERE source_id = ${source_id}
          AND entity_slug = ${entitySlug}
          AND expired_at IS NULL
          AND embedding IS NOT NULL
        ORDER BY embedding <=> ${sql.unsafe(`'${lit}'::vector`)}
        LIMIT ${k}
      `;
      return rows.map(rowToFactPg);
    }
    const rows = await sql<FactRowSqlShape[]>`
      SELECT * FROM facts
      WHERE source_id = ${source_id}
        AND entity_slug = ${entitySlug}
        AND expired_at IS NULL
      ORDER BY created_at DESC, id DESC
      LIMIT ${k}
    `;
    return rows.map(rowToFactPg);
  }
,
  consolidateFact: async function(this: PostgresEngineLike, id: number, takeId: number): Promise<void> {
    const sql = this.sql;
    await sql`UPDATE facts SET consolidated_at = now(), consolidated_into = ${takeId} WHERE id = ${id}`;
  }
,
  findTrajectory: async function(this: PostgresEngineLike, opts: import('./engine.ts').TrajectoryOpts): Promise<import('./engine.ts').TrajectoryPoint[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts.limit, 100, 500);
    const sinceDate = opts.since ? new Date(opts.since) : null;
    const untilDate = opts.until ? new Date(opts.until) : null;
    const metric = opts.metric ?? null;
    const useArray = Array.isArray(opts.sourceIds) && opts.sourceIds.length > 0;
    const sourceIds = useArray ? opts.sourceIds! : null;
    const sourceId = opts.sourceId ?? 'default';
    const remoteFilter = opts.remote === true;

    // Source-scope predicate: array path (federated) wins over scalar.
    // Engine.ts contract: returns chronological points; regressions +
    // drift_score are computed by the caller (src/core/trajectory.ts).
    const rows = await sql<Array<{
      id: number;
      valid_from: Date;
      claim_metric: string | null;
      claim_value: number | null;
      claim_unit: string | null;
      claim_period: string | null;
      fact: string;
      source_session: string | null;
      source_markdown_slug: string | null;
      embedding: string | null;
    }>>`
      SELECT id, valid_from,
             claim_metric, claim_value, claim_unit, claim_period,
             fact, source_session, source_markdown_slug,
             embedding::text AS embedding
      FROM facts
      WHERE ${useArray ? sql`source_id = ANY(${sourceIds}::text[])` : sql`source_id = ${sourceId}`}
        AND entity_slug = ${opts.entitySlug}
        AND expired_at IS NULL
        ${remoteFilter ? sql`AND visibility = 'world'` : sql``}
        ${metric !== null ? sql`AND claim_metric = ${metric}` : sql``}
        ${sinceDate ? sql`AND valid_from >= ${sinceDate}` : sql``}
        ${untilDate ? sql`AND valid_from <= ${untilDate}` : sql``}
      ORDER BY valid_from ASC, id ASC
      LIMIT ${limit}
    `;

    return rows.map(r => ({
      fact_id: Number(r.id),
      valid_from: r.valid_from,
      metric: r.claim_metric,
      value: r.claim_value === null ? null : Number(r.claim_value),
      unit: r.claim_unit,
      period: r.claim_period,
      text: r.fact,
      source_session: r.source_session,
      source_markdown_slug: r.source_markdown_slug,
      embedding: tryParseEmbedding(r.embedding),
    }));
  }
,
  getFactsHealth: async function(this: PostgresEngineLike, source_id: string): Promise<FactsHealth> {
    const sql = this.sql;
    const totals = await sql<Array<{
      total_active: bigint; total_today: bigint; total_week: bigint;
      total_expired: bigint; total_consolidated: bigint;
    }>>`
      SELECT
        COUNT(*) FILTER (WHERE expired_at IS NULL)                                     AS total_active,
        COUNT(*) FILTER (WHERE expired_at IS NULL AND created_at > now() - interval '24 hours') AS total_today,
        COUNT(*) FILTER (WHERE expired_at IS NULL AND created_at > now() - interval '7 days')   AS total_week,
        COUNT(*) FILTER (WHERE expired_at IS NOT NULL)                                 AS total_expired,
        COUNT(*) FILTER (WHERE consolidated_at IS NOT NULL)                            AS total_consolidated
      FROM facts WHERE source_id = ${source_id}
    `;
    const top = await sql<Array<{ entity_slug: string; count: bigint }>>`
      SELECT entity_slug, COUNT(*) AS count
      FROM facts
      WHERE source_id = ${source_id} AND expired_at IS NULL AND entity_slug IS NOT NULL
      GROUP BY entity_slug
      ORDER BY count DESC, entity_slug ASC
      LIMIT 5
    `;
    const r = totals[0] ?? {
      total_active: 0n, total_today: 0n, total_week: 0n, total_expired: 0n, total_consolidated: 0n,
    };
    return {
      source_id,
      total_active: Number(r.total_active),
      total_today: Number(r.total_today),
      total_week: Number(r.total_week),
      total_expired: Number(r.total_expired),
      total_consolidated: Number(r.total_consolidated),
      top_entities: top.map(t => ({ entity_slug: t.entity_slug, count: Number(t.count) })),
    };
  }

};

// Fact row mapping helpers (moved from postgres-engine.ts)
interface FactRowSqlShape {
  id: number | bigint;
  source_id: string;
  entity_slug: string | null;
  fact: string;
  kind: FactKind;
  visibility: FactVisibility;
  notability: 'high' | 'medium' | 'low';
  context: string | null;
  valid_from: Date;
  valid_until: Date | null;
  expired_at: Date | null;
  superseded_by: number | bigint | null;
  consolidated_at: Date | null;
  consolidated_into: number | bigint | null;
  source: string;
  source_session: string | null;
  confidence: number | string;
  embedding: string | number[] | Float32Array | null;
  embedded_at: Date | null;
  created_at: Date;
}

function rowToFactPg(row: FactRowSqlShape): FactRow {
  let embedding: Float32Array | null = null;
  if (row.embedding != null) {
    if (row.embedding instanceof Float32Array) embedding = row.embedding;
    else if (Array.isArray(row.embedding)) embedding = new Float32Array(row.embedding);
    else if (typeof row.embedding === 'string') {
      const trimmed = row.embedding.trim();
      const inner = trimmed.startsWith('[') ? trimmed.slice(1, -1) : trimmed;
      const parts = inner.split(',').map(p => parseFloat(p.trim())).filter(Number.isFinite);
      embedding = parts.length > 0 ? new Float32Array(parts) : null;
    }
  }
  return {
    id: Number(row.id),
    source_id: row.source_id,
    entity_slug: row.entity_slug,
    fact: row.fact,
    kind: row.kind,
    visibility: row.visibility,
    // v0.31.2: notability column added by migration v46. Pre-v46 rows that
    // somehow survive a SELECT (shouldn't on a fully-migrated brain) fall
    // back to 'medium' to keep the contract total. Belt-and-suspenders with
    // the migration's NOT NULL DEFAULT.
    notability: row.notability ?? 'medium',
    context: row.context,
    valid_from: row.valid_from,
    valid_until: row.valid_until,
    expired_at: row.expired_at,
    superseded_by: row.superseded_by == null ? null : Number(row.superseded_by),
    consolidated_at: row.consolidated_at,
    consolidated_into: row.consolidated_into == null ? null : Number(row.consolidated_into),
    source: row.source,
    source_session: row.source_session,
    confidence: typeof row.confidence === 'string' ? parseFloat(row.confidence) : row.confidence,
    embedding,
    embedded_at: row.embedded_at,
    created_at: row.created_at,
  };
}


function toPgVectorLiteral(v: Float32Array | number[]): string {
  if (v instanceof Float32Array) return '[' + Array.from(v).join(',') + ']';
  return '[' + v.join(',') + ']';
}


