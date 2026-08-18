/**
 * PGLiteEngine links methods — split out of pglite-engine.ts (BET-Y1Q3-T6-04).
 * Injected onto PGLiteEngine.prototype via Object.assign.
 */
import {
  type DreamVerdict,
  type DreamVerdictInput,
  type FileRow,
  type FileSpec,
  type LinkBatchInput,
  type TimelineBatchInput,
} from './engine.ts';
import { type GraphNode, GraphPath, Link, RawData, TimelineEntry, TimelineInput, TimelineOpts } from './types.ts';
import { contentHash } from './utils.ts';
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm';

export interface PGLiteEngineLike {
  db: import('@electric-sql/pglite').PGlite;
  [key: string]: any;
}

export const pgliteLinksMethods: Record<string, any> = {
  deleteChunks: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<void> {
    const sourceId = opts?.sourceId ?? 'default';
    // Source-qualify the page-id subquery; slugs are only unique per source.
    await this.db.query(
      `DELETE FROM content_chunks
       WHERE page_id = (SELECT id FROM pages WHERE slug = $1 AND source_id = $2)`,
      [slug, sourceId]
    );
  }
,
  addLink: async function(this: PGLiteEngineLike, 
    from: string,
    to: string,
    context?: string,
    linkType?: string,
    linkSource?: string,
    originSlug?: string,
    originField?: string,
    opts?: { fromSourceId?: string; toSourceId?: string; originSourceId?: string },
  ): Promise<void> {
    const fromSrc = opts?.fromSourceId ?? 'default';
    const toSrc = opts?.toSourceId ?? 'default';
    const originSrc = opts?.originSourceId ?? 'default';

    // Source-qualified pre-check gives a clean missing-page error before the
    // INSERT SELECT path can silently return zero rows.
    const exists = await this.db.query(
      `SELECT 1 FROM pages WHERE slug = $1 AND source_id = $2
       INTERSECT
       SELECT 1 FROM pages WHERE slug = $3 AND source_id = $4`,
      [from, fromSrc, to, toSrc]
    );
    if (exists.rows.length === 0) {
      throw new Error(`addLink failed: page "${from}" (source=${fromSrc}) or "${to}" (source=${toSrc}) not found`);
    }
    const src = linkSource ?? 'markdown';
    // Mirror addLinksBatch's VALUES + composite JOIN shape. The old cross-
    // product over pages f/t fanned out across sources containing the slugs.
    await this.db.query(
      `INSERT INTO links (from_page_id, to_page_id, link_type, context, link_source, origin_page_id, origin_field)
       SELECT f.id, t.id, v.link_type, v.context, v.link_source, o.id, v.origin_field
       FROM (VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10))
         AS v(from_slug, to_slug, link_type, context, link_source, origin_slug, origin_field, from_source_id, to_source_id, origin_source_id)
       JOIN pages f ON f.slug = v.from_slug AND f.source_id = v.from_source_id
       JOIN pages t ON t.slug = v.to_slug AND t.source_id = v.to_source_id
       LEFT JOIN pages o ON o.slug = v.origin_slug AND o.source_id = v.origin_source_id
       ON CONFLICT (from_page_id, to_page_id, link_type, link_source, origin_page_id) DO UPDATE SET
         context = EXCLUDED.context,
         origin_field = EXCLUDED.origin_field`,
      [from, to, linkType || '', context || '', src, originSlug ?? null, originField ?? null, fromSrc, toSrc, originSrc]
    );
  }
,
  addLinksBatch: async function(this: PGLiteEngineLike, links: LinkBatchInput[]): Promise<number> {
    if (links.length === 0) return 0;
    // unnest() pattern: 10 array-typed bound parameters regardless of batch
    // size. Same shape as PostgresEngine (v0.18). Avoids the 65535-parameter
    // cap.
    //
    // v0.18.0: every JOIN composite-keys on (slug, source_id) so the batch
    // can't fan out across sources when the same slug exists in multiple
    // sources. Origin JOIN uses LEFT JOIN on a composite key — NULL
    // origin_slug leaves origin_page_id NULL, same as pre-v0.18.
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
    const result = await this.db.query(
      `INSERT INTO links (from_page_id, to_page_id, link_type, context, link_source, origin_page_id, origin_field)
       SELECT f.id, t.id, v.link_type, v.context, v.link_source, o.id, v.origin_field
       FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::text[])
         AS v(from_slug, to_slug, link_type, context, link_source, origin_slug, origin_field, from_source_id, to_source_id, origin_source_id)
       JOIN pages f ON f.slug = v.from_slug AND f.source_id = v.from_source_id
       JOIN pages t ON t.slug = v.to_slug AND t.source_id = v.to_source_id
       LEFT JOIN pages o ON o.slug = v.origin_slug AND o.source_id = v.origin_source_id
       ON CONFLICT (from_page_id, to_page_id, link_type, link_source, origin_page_id) DO NOTHING
       RETURNING 1`,
      [fromSlugs, toSlugs, linkTypes, contexts, linkSources, originSlugs, originFields, fromSourceIds, toSourceIds, originSourceIds]
    );
    return result.rows.length;
  }
,
  removeLink: async function(this: PGLiteEngineLike, 
    from: string,
    to: string,
    linkType?: string,
    linkSource?: string,
    opts?: { fromSourceId?: string; toSourceId?: string },
  ): Promise<void> {
    const fromSrc = opts?.fromSourceId ?? 'default';
    const toSrc = opts?.toSourceId ?? 'default';
    // Each branch source-qualifies page-id subqueries so a delete only targets
    // the intended edge between per-source slug rows.
    if (linkType !== undefined && linkSource !== undefined) {
      await this.db.query(
        `DELETE FROM links
         WHERE from_page_id = (SELECT id FROM pages WHERE slug = $1 AND source_id = $2)
           AND to_page_id = (SELECT id FROM pages WHERE slug = $3 AND source_id = $4)
           AND link_type = $5
           AND link_source IS NOT DISTINCT FROM $6`,
        [from, fromSrc, to, toSrc, linkType, linkSource]
      );
    } else if (linkType !== undefined) {
      await this.db.query(
        `DELETE FROM links
         WHERE from_page_id = (SELECT id FROM pages WHERE slug = $1 AND source_id = $2)
           AND to_page_id = (SELECT id FROM pages WHERE slug = $3 AND source_id = $4)
           AND link_type = $5`,
        [from, fromSrc, to, toSrc, linkType]
      );
    } else if (linkSource !== undefined) {
      await this.db.query(
        `DELETE FROM links
         WHERE from_page_id = (SELECT id FROM pages WHERE slug = $1 AND source_id = $2)
           AND to_page_id = (SELECT id FROM pages WHERE slug = $3 AND source_id = $4)
           AND link_source IS NOT DISTINCT FROM $5`,
        [from, fromSrc, to, toSrc, linkSource]
      );
    } else {
      await this.db.query(
        `DELETE FROM links
         WHERE from_page_id = (SELECT id FROM pages WHERE slug = $1 AND source_id = $2)
           AND to_page_id = (SELECT id FROM pages WHERE slug = $3 AND source_id = $4)`,
        [from, fromSrc, to, toSrc]
      );
    }
  }
,
  getLinks: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<Link[]> {
    // v0.31.8 (D16): two-branch query. Without opts.sourceId, no source filter
    // (preserves pre-v0.31.8 cross-source semantics for back-link validators
    // and read-side op handlers that haven't threaded sourceId yet). With
    // opts.sourceId, scope to that source — used by reconcileLinks and any
    // ctx.sourceId-aware read op (D20).
    if (opts?.sourceId) {
      const { rows } = await this.db.query(
        `SELECT f.slug as from_slug, t.slug as to_slug,
                l.link_type, l.context, l.link_source,
                o.slug as origin_slug, l.origin_field
         FROM links l
         JOIN pages f ON f.id = l.from_page_id
         JOIN pages t ON t.id = l.to_page_id
         LEFT JOIN pages o ON o.id = l.origin_page_id
         WHERE f.slug = $1 AND f.source_id = $2`,
        [slug, opts.sourceId]
      );
      return rows as unknown as Link[];
    }
    const { rows } = await this.db.query(
      `SELECT f.slug as from_slug, t.slug as to_slug,
              l.link_type, l.context, l.link_source,
              o.slug as origin_slug, l.origin_field
       FROM links l
       JOIN pages f ON f.id = l.from_page_id
       JOIN pages t ON t.id = l.to_page_id
       LEFT JOIN pages o ON o.id = l.origin_page_id
       WHERE f.slug = $1`,
      [slug]
    );
    return rows as unknown as Link[];
  }
,
  getBacklinks: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<Link[]> {
    // v0.31.8 (D16): two-branch query. See getLinks() comment.
    if (opts?.sourceId) {
      const { rows } = await this.db.query(
        `SELECT f.slug as from_slug, t.slug as to_slug,
                l.link_type, l.context, l.link_source,
                o.slug as origin_slug, l.origin_field
         FROM links l
         JOIN pages f ON f.id = l.from_page_id
         JOIN pages t ON t.id = l.to_page_id
         LEFT JOIN pages o ON o.id = l.origin_page_id
         WHERE t.slug = $1 AND t.source_id = $2`,
        [slug, opts.sourceId]
      );
      return rows as unknown as Link[];
    }
    const { rows } = await this.db.query(
      `SELECT f.slug as from_slug, t.slug as to_slug,
              l.link_type, l.context, l.link_source,
              o.slug as origin_slug, l.origin_field
       FROM links l
       JOIN pages f ON f.id = l.from_page_id
       JOIN pages t ON t.id = l.to_page_id
       LEFT JOIN pages o ON o.id = l.origin_page_id
       WHERE t.slug = $1`,
      [slug]
    );
    return rows as unknown as Link[];
  }
,
  findByTitleFuzzy: async function(this: PGLiteEngineLike, 
    name: string,
    dirPrefix?: string,
    minSimilarity: number = 0.55,
  ): Promise<{ slug: string; similarity: number } | null> {
    // Inline threshold comparison instead of `SET LOCAL pg_trgm.similarity_threshold`.
    // The GUC only scopes to the current transaction and pglite auto-commits each
    // .query() call, so the SET LOCAL would be a no-op. Using similarity() >= $N
    // directly gives predictable behavior. Tie-breaker: sort by slug so re-runs
    // pick the same winner.
    const prefixPattern = dirPrefix ? `${dirPrefix}/%` : '%';
    const { rows } = await this.db.query(
      `SELECT slug, similarity(title, $1) AS sim
       FROM pages
       WHERE similarity(title, $1) >= $3
         AND slug LIKE $2
       ORDER BY sim DESC, slug ASC
       LIMIT 1`,
      [name, prefixPattern, minSimilarity]
    );
    if (rows.length === 0) return null;
    const row = rows[0] as { slug: string; sim: number };
    return { slug: row.slug, similarity: row.sim };
  }
,
  traverseGraph: async function(this: PGLiteEngineLike, 
    slug: string,
    depth: number = 5,
    opts?: import('./engine.ts').TraverseGraphOpts,
  ): Promise<GraphNode[]> {
    // v0.34.1 (#861 — P0 leak seal): source-scope filters at seed, step, and
    // aggregation subquery. Mirrors postgres-engine.traverseGraph placement.
    const params: unknown[] = [slug, depth];
    const useSourceIds = opts?.sourceIds && opts.sourceIds.length > 0;
    let seedScope = '';
    let stepScope = '';
    let aggScope = '';
    if (useSourceIds) {
      params.push(opts!.sourceIds);
      const idx = params.length;
      seedScope = `AND p.source_id = ANY($${idx}::text[])`;
      stepScope = `AND p2.source_id = ANY($${idx}::text[])`;
      aggScope = `AND p3.source_id = ANY($${idx}::text[])`;
    } else if (opts?.sourceId) {
      params.push(opts.sourceId);
      const idx = params.length;
      seedScope = `AND p.source_id = $${idx}`;
      stepScope = `AND p2.source_id = $${idx}`;
      aggScope = `AND p3.source_id = $${idx}`;
    }

    // T8 (v0.36+): frontier cap. When set, the recursive term applies a
    // parenthesized LIMIT N ORDER BY (slug, id) for stable selection. Per-
    // ITERATION cap, which maps approximately to per-BFS-LAYER (exact when
    // fanout is bounded; for hub-fanout the cap fires early). Truncation
    // signal computed post-query by counting rows per depth.
    const cap = opts?.frontierCap;
    let recursiveTerm: string;
    if (cap !== undefined && cap > 0) {
      params.push(cap);
      const capIdx = params.length;
      recursiveTerm = `(SELECT p2.id, p2.slug, p2.title, p2.type, g.depth + 1, g.visited || p2.id
        FROM graph g
        JOIN links l ON l.from_page_id = g.id
        JOIN pages p2 ON p2.id = l.to_page_id
        WHERE g.depth < $2
          AND NOT (p2.id = ANY(g.visited))
          ${stepScope}
        ORDER BY p2.slug ASC, p2.id ASC
        LIMIT $${capIdx})`;
    } else {
      recursiveTerm = `SELECT p2.id, p2.slug, p2.title, p2.type, g.depth + 1, g.visited || p2.id
        FROM graph g
        JOIN links l ON l.from_page_id = g.id
        JOIN pages p2 ON p2.id = l.to_page_id
        WHERE g.depth < $2
          AND NOT (p2.id = ANY(g.visited))
          ${stepScope}`;
    }

    // Cycle prevention: visited array tracks page IDs already in the path.
    // Prevents exponential blowup on cyclic subgraphs (e.g., A->B->A).
    const { rows } = await this.db.query(
      `WITH RECURSIVE graph AS (
        SELECT p.id, p.slug, p.title, p.type, 0 as depth, ARRAY[p.id] as visited
        FROM pages p WHERE p.slug = $1 ${seedScope}

        UNION ALL

        ${recursiveTerm}
      )
      SELECT DISTINCT g.slug, g.title, g.type, g.depth,
        coalesce(
          -- jsonb_agg(DISTINCT ...) collapses duplicate (to_slug, link_type)
          -- edges that originate from different provenance (markdown body
          -- vs frontmatter vs auto-extracted). Presentation-only dedup;
          -- the links table still preserves every provenance row. See
          -- plan Bug 6/10.
          (SELECT jsonb_agg(DISTINCT jsonb_build_object('to_slug', p3.slug, 'link_type', l2.link_type))
           FROM links l2
           JOIN pages p3 ON p3.id = l2.to_page_id
           WHERE l2.from_page_id = g.id ${aggScope}),
          '[]'::jsonb
        ) as links
      FROM graph g
      ORDER BY g.depth, g.slug`,
      params
    );

    // T8 truncation-detection callback stripped in /review — see
    // postgres-engine.traverseGraph for the parallel comment + (tracked TODO).

    return (rows as Record<string, unknown>[]).map(r => ({
      slug: r.slug as string,
      title: r.title as string,
      type: r.type as string,
      depth: r.depth as number,
      links: (typeof r.links === 'string' ? JSON.parse(r.links) : r.links) as { to_slug: string; link_type: string }[],
    }));
  }
,
  traversePaths: async function(this: PGLiteEngineLike, 
    slug: string,
    opts?: { depth?: number; linkType?: string; direction?: 'in' | 'out' | 'both'; sourceId?: string; sourceIds?: string[] },
  ): Promise<GraphPath[]> {
    const depth = opts?.depth ?? 5;
    const direction = opts?.direction ?? 'out';
    const linkType = opts?.linkType ?? null;
    const linkTypeWhere = linkType !== null ? 'AND l.link_type = $3' : '';
    const params: unknown[] = [slug, depth];
    if (linkType !== null) params.push(linkType);

    // v0.34.1 (#861 — P0 leak seal): source-scope filters at seed + step +
    // final SELECT joins (for the 'both' branch's pf + pt). Mirrors
    // postgres-engine.traversePaths placement.
    const useSourceIds = opts?.sourceIds && opts.sourceIds.length > 0;
    let seedScope = '';
    let stepScope = '';
    let pfScope = '';
    let ptScope = '';
    if (useSourceIds) {
      params.push(opts!.sourceIds);
      const idx = params.length;
      seedScope = `AND p.source_id = ANY($${idx}::text[])`;
      stepScope = `AND p2.source_id = ANY($${idx}::text[])`;
      pfScope = `AND pf.source_id = ANY($${idx}::text[])`;
      ptScope = `AND pt.source_id = ANY($${idx}::text[])`;
    } else if (opts?.sourceId) {
      params.push(opts.sourceId);
      const idx = params.length;
      seedScope = `AND p.source_id = $${idx}`;
      stepScope = `AND p2.source_id = $${idx}`;
      pfScope = `AND pf.source_id = $${idx}`;
      ptScope = `AND pt.source_id = $${idx}`;
    }

    let sql: string;
    if (direction === 'out') {
      sql = `
        WITH RECURSIVE walk AS (
          SELECT p.id, p.slug, 0::int AS depth, ARRAY[p.id] AS visited
          FROM pages p WHERE p.slug = $1 ${seedScope}
          UNION ALL
          SELECT p2.id, p2.slug, w.depth + 1, w.visited || p2.id
          FROM walk w
          JOIN links l ON l.from_page_id = w.id
          JOIN pages p2 ON p2.id = l.to_page_id
          WHERE w.depth < $2
            AND NOT (p2.id = ANY(w.visited))
            ${linkTypeWhere}
            ${stepScope}
        )
        SELECT w.slug AS from_slug, p2.slug AS to_slug,
               l.link_type, l.context, w.depth + 1 AS depth
        FROM walk w
        JOIN links l ON l.from_page_id = w.id
        JOIN pages p2 ON p2.id = l.to_page_id
        WHERE w.depth < $2
          ${linkTypeWhere}
          ${stepScope}
        ORDER BY depth, from_slug, to_slug
      `;
    } else if (direction === 'in') {
      sql = `
        WITH RECURSIVE walk AS (
          SELECT p.id, p.slug, 0::int AS depth, ARRAY[p.id] AS visited
          FROM pages p WHERE p.slug = $1 ${seedScope}
          UNION ALL
          SELECT p2.id, p2.slug, w.depth + 1, w.visited || p2.id
          FROM walk w
          JOIN links l ON l.to_page_id = w.id
          JOIN pages p2 ON p2.id = l.from_page_id
          WHERE w.depth < $2
            AND NOT (p2.id = ANY(w.visited))
            ${linkTypeWhere}
            ${stepScope}
        )
        SELECT p2.slug AS from_slug, w.slug AS to_slug,
               l.link_type, l.context, w.depth + 1 AS depth
        FROM walk w
        JOIN links l ON l.to_page_id = w.id
        JOIN pages p2 ON p2.id = l.from_page_id
        WHERE w.depth < $2
          ${linkTypeWhere}
          ${stepScope}
        ORDER BY depth, from_slug, to_slug
      `;
    } else {
      // both: walk in both directions, emit every traversed edge (preserving its
      // natural from->to direction from the links table).
      sql = `
        WITH RECURSIVE walk AS (
          SELECT p.id, 0::int AS depth, ARRAY[p.id] AS visited
          FROM pages p WHERE p.slug = $1 ${seedScope}
          UNION ALL
          SELECT p2.id, w.depth + 1, w.visited || p2.id
          FROM walk w
          JOIN links l ON (l.from_page_id = w.id OR l.to_page_id = w.id)
          JOIN pages p2 ON p2.id = CASE WHEN l.from_page_id = w.id THEN l.to_page_id ELSE l.from_page_id END
          WHERE w.depth < $2
            AND NOT (p2.id = ANY(w.visited))
            ${linkTypeWhere}
            ${stepScope}
        )
        SELECT pf.slug AS from_slug, pt.slug AS to_slug,
               l.link_type, l.context, w.depth + 1 AS depth
        FROM walk w
        JOIN links l ON (l.from_page_id = w.id OR l.to_page_id = w.id)
        JOIN pages pf ON pf.id = l.from_page_id
        JOIN pages pt ON pt.id = l.to_page_id
        WHERE w.depth < $2
          ${linkTypeWhere}
          ${pfScope}
          ${ptScope}
        ORDER BY depth, from_slug, to_slug
      `;
    }

    const { rows } = await this.db.query(sql, params);
    // Dedup edges (same from/to/type/depth can appear via multiple visited paths).
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
        depth: r.depth as number,
      });
    }
    return result;
  }
,
  getBacklinkCounts: async function(this: PGLiteEngineLike, slugs: string[]): Promise<Map<string, number>> {
    const result = new Map<string, number>();
    if (slugs.length === 0) return result;
    // Initialize all slugs to 0 so callers get a consistent map.
    for (const s of slugs) result.set(s, 0);

    // PGLite needs explicit cast for array binding (does not auto-serialize JS arrays).
    const { rows } = await this.db.query(
      `SELECT p.slug AS slug, COUNT(l.id)::int AS cnt
       FROM pages p
       LEFT JOIN links l ON l.to_page_id = p.id
       WHERE p.slug = ANY($1::text[])
       GROUP BY p.slug`,
      [slugs]
    );
    for (const r of rows as { slug: string; cnt: number }[]) {
      result.set(r.slug, Number(r.cnt));
    }
    return result;
  }
,
  getPageTimestamps: async function(this: PGLiteEngineLike, slugs: string[]): Promise<Map<string, Date>> {
    if (slugs.length === 0) return new Map();
    const { rows } = await this.db.query(
      `SELECT slug, COALESCE(updated_at, created_at) as ts
       FROM pages WHERE slug = ANY($1::text[])`,
      [slugs]
    );
    return new Map(rows.map((r: any) => [r.slug as string, new Date(r.ts as string)]));
  }
,
  getEffectiveDates: async function(this: PGLiteEngineLike, refs: Array<{slug: string; source_id: string}>): Promise<Map<string, Date>> {
    if (refs.length === 0) return new Map();
    const slugs = refs.map(r => r.slug);
    const sourceIds = refs.map(r => r.source_id);
    const { rows } = await this.db.query(
      `SELECT p.slug, p.source_id, COALESCE(p.effective_date, p.updated_at, p.created_at) AS ts
         FROM pages p
         JOIN unnest($1::text[], $2::text[]) AS u(slug, source_id)
           ON p.slug = u.slug AND p.source_id = u.source_id`,
      [slugs, sourceIds],
    );
    const out = new Map<string, Date>();
    for (const r of rows as Array<{slug: string; source_id: string; ts: string | Date}>) {
      const key = `${r.source_id}::${r.slug}`;
      out.set(key, r.ts instanceof Date ? r.ts : new Date(r.ts));
    }
    return out;
  }
,
  getSalienceScores: async function(this: PGLiteEngineLike, refs: Array<{slug: string; source_id: string}>): Promise<Map<string, number>> {
    if (refs.length === 0) return new Map();
    const slugs = refs.map(r => r.slug);
    const sourceIds = refs.map(r => r.source_id);
    const { rows } = await this.db.query(
      `SELECT p.slug, p.source_id,
              (COALESCE(p.emotional_weight, 0) * 5
               + ln(1 + COUNT(DISTINCT t.id))) AS score
         FROM pages p
         JOIN unnest($1::text[], $2::text[]) AS u(slug, source_id)
           ON p.slug = u.slug AND p.source_id = u.source_id
         LEFT JOIN takes t ON t.page_id = p.id AND t.active = TRUE
        GROUP BY p.id`,
      [slugs, sourceIds],
    );
    const out = new Map<string, number>();
    for (const r of rows as Array<{slug: string; source_id: string; score: number | string}>) {
      const key = `${r.source_id}::${r.slug}`;
      out.set(key, Number(r.score));
    }
    return out;
  }
,
  findOrphanPages: async function(this: PGLiteEngineLike, ): Promise<Array<{ slug: string; title: string; domain: string | null }>> {
    // Soft-delete filter on BOTH sides:
    //   - candidate: p.deleted_at IS NULL — soft-deleted pages aren't orphan candidates
    //   - link source: src.deleted_at IS NULL — links FROM soft-deleted pages don't count as inbound
    // Without the link-source filter, a live page can hide from orphan results purely
    // because a soft-deleted page links to it. v0.26.5 invariant; codex C11.
    const { rows } = await this.db.query(
      `SELECT
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
       ORDER BY p.slug`
    );
    return rows as Array<{ slug: string; title: string; domain: string | null }>;
  }
,
  addTag: async function(this: PGLiteEngineLike, slug: string, tag: string, opts?: { sourceId?: string }): Promise<void> {
    const sourceId = opts?.sourceId ?? 'default';
    // Pre-check source-scoped page existence; ON CONFLICT only handles the
    // already-tagged case, not missing pages.
    const page = await this.db.query(
      'SELECT id FROM pages WHERE slug = $1 AND source_id = $2',
      [slug, sourceId]
    );
    if (page.rows.length === 0) throw new Error(`addTag failed: page "${slug}" (source=${sourceId}) not found`);
    await this.db.query(
      `INSERT INTO tags (page_id, tag)
       VALUES ($1, $2)
       ON CONFLICT (page_id, tag) DO NOTHING`,
      [(page.rows[0] as { id: number }).id, tag]
    );
  }
,
  removeTag: async function(this: PGLiteEngineLike, slug: string, tag: string, opts?: { sourceId?: string }): Promise<void> {
    const sourceId = opts?.sourceId ?? 'default';
    // Source-qualify the page-id subquery; slugs are only unique per source.
    await this.db.query(
      `DELETE FROM tags
       WHERE page_id = (SELECT id FROM pages WHERE slug = $1 AND source_id = $2)
         AND tag = $3`,
      [slug, sourceId, tag]
    );
  }
,
  getTags: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<string[]> {
    const sourceId = opts?.sourceId ?? 'default';
    // Source-qualify the page-id subquery; slugs are only unique per source.
    const { rows } = await this.db.query(
      `SELECT tag FROM tags
       WHERE page_id = (SELECT id FROM pages WHERE slug = $1 AND source_id = $2)
       ORDER BY tag`,
      [slug, sourceId]
    );
    return (rows as { tag: string }[]).map(r => r.tag);
  }
,
  addTimelineEntry: async function(this: PGLiteEngineLike, 
    slug: string,
    entry: TimelineInput,
    opts?: { skipExistenceCheck?: boolean; sourceId?: string },
  ): Promise<void> {
    const sourceId = opts?.sourceId ?? 'default';
    if (!opts?.skipExistenceCheck) {
      const { rows } = await this.db.query(
        'SELECT 1 FROM pages WHERE slug = $1 AND source_id = $2',
        [slug, sourceId]
      );
      if (rows.length === 0) {
        throw new Error(`addTimelineEntry failed: page "${slug}" (source=${sourceId}) not found`);
      }
    }
    // ON CONFLICT DO NOTHING via the (page_id, date, summary) unique index.
    // Source-qualify the page-id lookup so multi-source brains don't fan
    // timeline rows out across every source containing the slug.
    await this.db.query(
      `INSERT INTO timeline_entries (page_id, date, source, summary, detail)
       SELECT id, $2::date, $3, $4, $5
       FROM pages WHERE slug = $1 AND source_id = $6
       ON CONFLICT (page_id, date, summary) DO NOTHING`,
      [slug, entry.date, entry.source || '', entry.summary, entry.detail || '', sourceId]
    );
  }
,
  addTimelineEntriesBatch: async function(this: PGLiteEngineLike, entries: TimelineBatchInput[]): Promise<number> {
    if (entries.length === 0) return 0;
    const slugs = entries.map(e => e.slug);
    const dates = entries.map(e => e.date);
    const sources = entries.map(e => e.source || '');
    const summaries = entries.map(e => e.summary);
    const details = entries.map(e => e.detail || '');
    const sourceIds = entries.map(e => e.source_id || 'default');
    const result = await this.db.query(
      `INSERT INTO timeline_entries (page_id, date, source, summary, detail)
       SELECT p.id, v.date::date, v.source, v.summary, v.detail
       FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])
         AS v(slug, date, source, summary, detail, source_id)
       JOIN pages p ON p.slug = v.slug AND p.source_id = v.source_id
       ON CONFLICT (page_id, date, summary) DO NOTHING
       RETURNING 1`,
      [slugs, dates, sources, summaries, details, sourceIds]
    );
    return result.rows.length;
  }
,
  getTimeline: async function(this: PGLiteEngineLike, slug: string, opts?: TimelineOpts): Promise<TimelineEntry[]> {
    // v0.31.8 (D16): build WHERE clause dynamically so opts.sourceId composes
    // cleanly with the existing after/before filters. Without sourceId, no
    // source filter applies (preserves pre-v0.31.8 cross-source semantics).
    const limit = opts?.limit || 100;
    const where: string[] = ['p.slug = $1'];
    const params: unknown[] = [slug];
    if (opts?.after) {
      params.push(opts.after);
      where.push(`te.date >= $${params.length}::date`);
    }
    if (opts?.before) {
      params.push(opts.before);
      where.push(`te.date <= $${params.length}::date`);
    }
    if (opts?.sourceId) {
      params.push(opts.sourceId);
      where.push(`p.source_id = $${params.length}`);
    }
    params.push(limit);
    const result = await this.db.query(
      `SELECT te.* FROM timeline_entries te
       JOIN pages p ON p.id = te.page_id
       WHERE ${where.join(' AND ')}
       ORDER BY te.date DESC LIMIT $${params.length}`,
      params
    );
    return result.rows as unknown as TimelineEntry[];
  }
,
  putRawData: async function(this: PGLiteEngineLike, 
    slug: string,
    source: string,
    data: object,
    opts?: { sourceId?: string },
  ): Promise<void> {
    // v0.31.8 (D21): two-branch INSERT-SELECT. Without opts.sourceId, the
    // page-id lookup matches every same-slug page (pre-v0.31.8 behavior; can
    // still trip Postgres 21000 on multi-source brains — caller's choice).
    // With opts.sourceId, the lookup is source-scoped so the right row
    // gets the raw_data attached.
    if (opts?.sourceId) {
      await this.db.query(
        `INSERT INTO raw_data (page_id, source, data)
         SELECT id, $2, $3::jsonb
         FROM pages WHERE slug = $1 AND source_id = $4
         ON CONFLICT (page_id, source) DO UPDATE SET
           data = EXCLUDED.data,
           fetched_at = now()`,
        [slug, source, JSON.stringify(data), opts.sourceId]
      );
      return;
    }
    await this.db.query(
      `INSERT INTO raw_data (page_id, source, data)
       SELECT id, $2, $3::jsonb
       FROM pages WHERE slug = $1
       ON CONFLICT (page_id, source) DO UPDATE SET
         data = EXCLUDED.data,
         fetched_at = now()`,
      [slug, source, JSON.stringify(data)]
    );
  }
,
  getRawData: async function(this: PGLiteEngineLike, 
    slug: string,
    source?: string,
    opts?: { sourceId?: string },
  ): Promise<RawData[]> {
    // v0.31.8 (D21): build WHERE clause dynamically. Without opts.sourceId,
    // no source filter (preserves pre-v0.31.8 cross-source read).
    const where: string[] = ['p.slug = $1'];
    const params: unknown[] = [slug];
    if (source) {
      params.push(source);
      where.push(`rd.source = $${params.length}`);
    }
    if (opts?.sourceId) {
      params.push(opts.sourceId);
      where.push(`p.source_id = $${params.length}`);
    }
    const result = await this.db.query(
      `SELECT rd.source, rd.data, rd.fetched_at FROM raw_data rd
       JOIN pages p ON p.id = rd.page_id
       WHERE ${where.join(' AND ')}`,
      params
    );
    return result.rows as unknown as RawData[];
  }
,
  upsertFile: async function(this: PGLiteEngineLike, spec: FileSpec): Promise<{ id: number; created: boolean }> {
    const sourceId = spec.source_id ?? 'default';
    const result = await this.db.query<{ id: number; created: boolean }>(
      `INSERT INTO files (source_id, page_slug, page_id, filename, storage_path, mime_type, size_bytes, content_hash, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
       ON CONFLICT (storage_path) DO UPDATE SET
         page_slug = EXCLUDED.page_slug,
         page_id = EXCLUDED.page_id,
         filename = EXCLUDED.filename,
         mime_type = EXCLUDED.mime_type,
         size_bytes = EXCLUDED.size_bytes,
         content_hash = EXCLUDED.content_hash,
         metadata = EXCLUDED.metadata
       RETURNING id, (xmax = 0) AS created`,
      [
        sourceId,
        spec.page_slug ?? null,
        spec.page_id ?? null,
        spec.filename,
        spec.storage_path,
        spec.mime_type ?? null,
        spec.size_bytes ?? null,
        spec.content_hash,
        JSON.stringify(spec.metadata ?? {}),
      ]
    );
    if (result.rows.length === 0) {
      throw new Error(`upsertFile returned no rows for ${spec.storage_path}`);
    }
    return { id: result.rows[0].id, created: !!result.rows[0].created };
  }
,
  getFile: async function(this: PGLiteEngineLike, sourceId: string, storagePath: string): Promise<FileRow | null> {
    const result = await this.db.query<FileRow>(
      `SELECT id, source_id, page_slug, page_id, filename, storage_path, mime_type, size_bytes, content_hash, metadata, created_at
       FROM files
       WHERE source_id = $1 AND storage_path = $2
       LIMIT 1`,
      [sourceId, storagePath]
    );
    return result.rows.length > 0 ? (result.rows[0] as FileRow) : null;
  }
,
  listFilesForPage: async function(this: PGLiteEngineLike, pageId: number): Promise<FileRow[]> {
    const result = await this.db.query<FileRow>(
      `SELECT id, source_id, page_slug, page_id, filename, storage_path, mime_type, size_bytes, content_hash, metadata, created_at
       FROM files
       WHERE page_id = $1
       ORDER BY created_at ASC`,
      [pageId]
    );
    return result.rows as FileRow[];
  }
,
  getDreamVerdict: async function(this: PGLiteEngineLike, filePath: string, contentHash: string): Promise<DreamVerdict | null> {
    const result = await this.db.query<{
      worth_processing: boolean;
      reasons: string[] | null;
      judged_at: Date | string;
    }>(
      `SELECT worth_processing, reasons, judged_at
       FROM dream_verdicts
       WHERE file_path = $1 AND content_hash = $2`,
      [filePath, contentHash]
    );
    if (result.rows.length === 0) return null;
    const r = result.rows[0];
    return {
      worth_processing: r.worth_processing,
      reasons: r.reasons ?? [],
      judged_at: r.judged_at instanceof Date ? r.judged_at.toISOString() : String(r.judged_at),
    };
  }
,
  putDreamVerdict: async function(this: PGLiteEngineLike, filePath: string, contentHash: string, verdict: DreamVerdictInput): Promise<void> {
    await this.db.query(
      `INSERT INTO dream_verdicts (file_path, content_hash, worth_processing, reasons)
       VALUES ($1, $2, $3, $4::jsonb)
       ON CONFLICT (file_path, content_hash) DO UPDATE SET
         worth_processing = EXCLUDED.worth_processing,
         reasons = EXCLUDED.reasons,
         judged_at = now()`,
      [filePath, contentHash, verdict.worth_processing, JSON.stringify(verdict.reasons)]
    );
  }

};
