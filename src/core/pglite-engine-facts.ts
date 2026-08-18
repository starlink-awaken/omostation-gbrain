/**
 * PGLiteEngine facts methods — split out of pglite-engine.ts (BET-Y1Q3-T6-04).
 * Injected onto PGLiteEngine.prototype via Object.assign.
 */
import { MAX_SEARCH_LIMIT, clampSearchLimit, type FactInsertStatus, type FactKind, type FactListOpts, type FactRow, type FactVisibility, type FactsHealth, type NewFact } from './engine.ts';
import { vector } from '@electric-sql/pglite/vector';

export interface PGLiteEngineLike {
  db: import('@electric-sql/pglite').PGlite;
  [key: string]: any;
}

export const pgliteFactsMethods: Record<string, any> = {
  insertFact: async function(this: PGLiteEngineLike, 
    input: NewFact,
    ctx: { source_id: string; supersedeId?: number },
  ): Promise<{ id: number; status: FactInsertStatus }> {
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
    const embedStr = embedding ? toPgVectorLiteral(embedding) : null;
    // v0.35.4 (D-CDX-5) — typed-claim columns. All four nullable.
    const claimMetric = input.claim_metric ?? null;
    const claimValue  = input.claim_value  ?? null;
    const claimUnit   = input.claim_unit   ?? null;
    const claimPeriod = input.claim_period ?? null;

    if (ctx.supersedeId !== undefined) {
      // Supersede flow: insert new + expire old in one txn so observers never
      // see both rows active simultaneously.
      const result = await this.db.transaction(async (tx) => {
        const ins = await tx.query<{ id: number }>(
          embedStr === null
            ? `INSERT INTO facts (
                 source_id, entity_slug, fact, kind, visibility, notability, context,
                 valid_from, valid_until, source, source_session, confidence,
                 embedding, embedded_at,
                 claim_metric, claim_value, claim_unit, claim_period
               ) VALUES (
                 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                 NULL, NULL,
                 $13, $14, $15, $16
               ) RETURNING id`
            : `INSERT INTO facts (
                 source_id, entity_slug, fact, kind, visibility, notability, context,
                 valid_from, valid_until, source, source_session, confidence,
                 embedding, embedded_at,
                 claim_metric, claim_value, claim_unit, claim_period
               ) VALUES (
                 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                 $13::vector, $14,
                 $15, $16, $17, $18
               ) RETURNING id`,
          embedStr === null
            ? [ctx.source_id, entitySlug, input.fact, kind, visibility, notability, context, validFrom, validUntil, input.source, sourceSession, confidence, claimMetric, claimValue, claimUnit, claimPeriod]
            : [ctx.source_id, entitySlug, input.fact, kind, visibility, notability, context, validFrom, validUntil, input.source, sourceSession, confidence, embedStr, embeddedAt, claimMetric, claimValue, claimUnit, claimPeriod],
        );
        const newId = ins.rows[0].id;
        await tx.query(
          `UPDATE facts SET expired_at = now(), superseded_by = $1
           WHERE id = $2 AND expired_at IS NULL`,
          [newId, ctx.supersedeId],
        );
        return newId;
      });
      return { id: result, status: 'superseded' };
    }

    const ins = await this.db.query<{ id: number }>(
      embedStr === null
        ? `INSERT INTO facts (
             source_id, entity_slug, fact, kind, visibility, notability, context,
             valid_from, valid_until, source, source_session, confidence,
             embedding, embedded_at,
             claim_metric, claim_value, claim_unit, claim_period
           ) VALUES (
             $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
             NULL, NULL,
             $13, $14, $15, $16
           ) RETURNING id`
        : `INSERT INTO facts (
             source_id, entity_slug, fact, kind, visibility, notability, context,
             valid_from, valid_until, source, source_session, confidence,
             embedding, embedded_at,
             claim_metric, claim_value, claim_unit, claim_period
           ) VALUES (
             $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
             $13::vector, $14,
             $15, $16, $17, $18
           ) RETURNING id`,
      embedStr === null
        ? [ctx.source_id, entitySlug, input.fact, kind, visibility, notability, context, validFrom, validUntil, input.source, sourceSession, confidence, claimMetric, claimValue, claimUnit, claimPeriod]
        : [ctx.source_id, entitySlug, input.fact, kind, visibility, notability, context, validFrom, validUntil, input.source, sourceSession, confidence, embedStr, embeddedAt, claimMetric, claimValue, claimUnit, claimPeriod],
    );
    return { id: ins.rows[0].id, status: 'inserted' };
  }
,
  expireFact: async function(this: PGLiteEngineLike, id: number, opts?: { supersededBy?: number; at?: Date }): Promise<boolean> {
    const at = opts?.at ?? new Date();
    const result = await this.db.query(
      `UPDATE facts SET expired_at = $1, superseded_by = COALESCE($2, superseded_by)
       WHERE id = $3 AND expired_at IS NULL`,
      [at, opts?.supersededBy ?? null, id],
    );
    return (result.affectedRows ?? 0) > 0;
  }
,
  insertFacts: async function(this: PGLiteEngineLike, 
    rows: Array<NewFact & { row_num: number; source_markdown_slug: string }>,
    ctx: { source_id: string },
  ): Promise<{ inserted: number; ids: number[] }> {
    if (rows.length === 0) return { inserted: 0, ids: [] };

    // Single transaction so the v51 partial UNIQUE index can roll back the
    // whole batch on constraint violation. Per-row INSERTs (not multi-row
    // VALUES) keep the embedding-vs-no-embedding branching readable; batch
    // sizes are small (5-30 rows per page in practice) so the loop overhead
    // is negligible vs the embedding compute cost.
    const ids = await this.db.transaction(async (tx) => {
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
        const embedStr = embedding ? toPgVectorLiteral(embedding) : null;
        // v0.35.4 (D-CDX-5) — typed-claim columns. All four nullable.
        const claimMetric = input.claim_metric ?? null;
        const claimValue  = input.claim_value  ?? null;
        const claimUnit   = input.claim_unit   ?? null;
        const claimPeriod = input.claim_period ?? null;

        // Param-positional dispatch: embedStr presence shifts the trailing
        // slots by one. Order of named slots stays stable across both
        // branches: embedded_at, row_num, source_markdown_slug,
        // claim_metric, claim_value, claim_unit, claim_period.
        const ins = await tx.query<{ id: number }>(
          embedStr === null
            ? `INSERT INTO facts (
                 source_id, entity_slug, fact, kind, visibility, notability, context,
                 valid_from, valid_until, source, source_session, confidence,
                 embedding, embedded_at,
                 row_num, source_markdown_slug,
                 claim_metric, claim_value, claim_unit, claim_period
               ) VALUES (
                 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                 NULL, $13,
                 $14, $15,
                 $16, $17, $18, $19
               ) RETURNING id`
            : `INSERT INTO facts (
                 source_id, entity_slug, fact, kind, visibility, notability, context,
                 valid_from, valid_until, source, source_session, confidence,
                 embedding, embedded_at,
                 row_num, source_markdown_slug,
                 claim_metric, claim_value, claim_unit, claim_period
               ) VALUES (
                 $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                 $13::vector, $14,
                 $15, $16,
                 $17, $18, $19, $20
               ) RETURNING id`,
          embedStr === null
            ? [ctx.source_id, entitySlug, input.fact, kind, visibility, notability, context, validFrom, validUntil, input.source, sourceSession, confidence, embeddedAt, input.row_num, input.source_markdown_slug, claimMetric, claimValue, claimUnit, claimPeriod]
            : [ctx.source_id, entitySlug, input.fact, kind, visibility, notability, context, validFrom, validUntil, input.source, sourceSession, confidence, embedStr, embeddedAt, input.row_num, input.source_markdown_slug, claimMetric, claimValue, claimUnit, claimPeriod],
        );
        out.push(ins.rows[0].id);
      }
      return out;
    });
    return { inserted: ids.length, ids };
  }
,
  deleteFactsForPage: async function(this: PGLiteEngineLike, slug: string, source_id: string): Promise<{ deleted: number }> {
    const result = await this.db.query(
      `DELETE FROM facts WHERE source_id = $1 AND source_markdown_slug = $2`,
      [source_id, slug],
    );
    return { deleted: result.affectedRows ?? 0 };
  }
,
  listFactsByEntity: async function(this: PGLiteEngineLike, 
    source_id: string,
    entitySlug: string,
    opts?: FactListOpts,
  ): Promise<FactRow[]> {
    return this._listFacts(source_id, {
      ...opts,
      whereClauses: [`entity_slug = $entitySlug`],
      whereParams: { entitySlug },
      order: 'valid_from DESC, id DESC',
    });
  }
,
  listFactsSince: async function(this: PGLiteEngineLike, 
    source_id: string,
    since: Date,
    opts?: FactListOpts & { entitySlug?: string },
  ): Promise<FactRow[]> {
    const where: string[] = [`created_at >= $since`];
    const params: Record<string, unknown> = { since };
    if (opts?.entitySlug) {
      where.push(`entity_slug = $entitySlug`);
      params.entitySlug = opts.entitySlug;
    }
    return this._listFacts(source_id, {
      ...opts,
      whereClauses: where,
      whereParams: params,
      order: 'created_at DESC, id DESC',
    });
  }
,
  listFactsBySession: async function(this: PGLiteEngineLike, 
    source_id: string,
    sessionId: string,
    opts?: FactListOpts,
  ): Promise<FactRow[]> {
    return this._listFacts(source_id, {
      ...opts,
      whereClauses: [`source_session = $sessionId`],
      whereParams: { sessionId },
      order: 'created_at DESC, id DESC',
    });
  }
,
  listSupersessions: async function(this: PGLiteEngineLike, 
    source_id: string,
    opts?: { since?: Date; limit?: number },
  ): Promise<FactRow[]> {
    const where: string[] = [`expired_at IS NOT NULL`, `superseded_by IS NOT NULL`];
    const params: Record<string, unknown> = {};
    if (opts?.since) {
      where.push(`expired_at >= $since`);
      params.since = opts.since;
    }
    return this._listFacts(source_id, {
      activeOnly: false,
      limit: opts?.limit,
      whereClauses: where,
      whereParams: params,
      order: 'expired_at DESC, id DESC',
    });
  }
,
  countUnconsolidatedFacts: async function(this: PGLiteEngineLike, source_id: string): Promise<number> {
    const r = await this.db.query<{ count: number }>(
      `SELECT COUNT(*)::int AS count FROM facts
       WHERE source_id = $1 AND consolidated_at IS NULL AND expired_at IS NULL`,
      [source_id],
    );
    return Number(r.rows[0]?.count ?? 0);
  }
,
  findCandidateDuplicates: async function(this: PGLiteEngineLike, 
    source_id: string,
    entitySlug: string,
    factText: string,
    opts?: { k?: number; embedding?: Float32Array },
  ): Promise<FactRow[]> {
    const k = Math.min(Math.max(opts?.k ?? 5, 1), 20);
    if (opts?.embedding) {
      // Embedding-cosine ordered candidates within the entity bucket.
      const vec = toPgVectorLiteral(opts.embedding);
      const result = await this.db.query<FactRowSqlShape>(
        `SELECT * FROM facts
         WHERE source_id = $1
           AND entity_slug = $2
           AND expired_at IS NULL
           AND embedding IS NOT NULL
         ORDER BY embedding <=> $3::vector
         LIMIT $4`,
        [source_id, entitySlug, vec, k],
      );
      return result.rows.map(rowToFact);
    }
    // Recency fallback when no embedding.
    const result = await this.db.query<FactRowSqlShape>(
      `SELECT * FROM facts
       WHERE source_id = $1
         AND entity_slug = $2
         AND expired_at IS NULL
       ORDER BY created_at DESC, id DESC
       LIMIT $3`,
      [source_id, entitySlug, k],
    );
    return result.rows.map(rowToFact);
  }
,
  findTrajectory: async function(this: PGLiteEngineLike, opts: import('./engine.ts').TrajectoryOpts): Promise<import('./engine.ts').TrajectoryPoint[]> {
    const limit = clampSearchLimit(opts.limit, 100, 500);
    const sinceDate = opts.since ? new Date(opts.since) : null;
    const untilDate = opts.until ? new Date(opts.until) : null;
    const metric = opts.metric ?? null;
    const useArray = Array.isArray(opts.sourceIds) && opts.sourceIds.length > 0;
    const sourceIds = useArray ? opts.sourceIds! : null;
    const sourceId = opts.sourceId ?? 'default';
    const remoteFilter = opts.remote === true;

    // Build SQL dynamically. PGLite uses $N positional params; we
    // assemble the WHERE clauses + params array in tandem to keep them
    // aligned. Final shape is single SELECT, ORDER BY (valid_from, id) ASC.
    const where: string[] = [
      useArray ? `source_id = ANY($1::text[])` : `source_id = $1`,
      `entity_slug = $2`,
      `expired_at IS NULL`,
    ];
    const params: unknown[] = [useArray ? sourceIds : sourceId, opts.entitySlug];
    let p = 3;
    if (remoteFilter) {
      where.push(`visibility = 'world'`);
    }
    if (metric !== null) {
      where.push(`claim_metric = $${p}`);
      params.push(metric);
      p += 1;
    }
    if (sinceDate) {
      where.push(`valid_from >= $${p}`);
      params.push(sinceDate);
      p += 1;
    }
    if (untilDate) {
      where.push(`valid_from <= $${p}`);
      params.push(untilDate);
      p += 1;
    }
    params.push(limit);
    const limitPlaceholder = p;

    const sqlText = `
      SELECT id, valid_from,
             claim_metric, claim_value, claim_unit, claim_period,
             fact, source_session, source_markdown_slug,
             embedding
      FROM facts
      WHERE ${where.join(' AND ')}
      ORDER BY valid_from ASC, id ASC
      LIMIT $${limitPlaceholder}
    `;
    const result = await this.db.query<{
      id: number;
      valid_from: Date | string;
      claim_metric: string | null;
      claim_value: number | null;
      claim_unit: string | null;
      claim_period: string | null;
      fact: string;
      source_session: string | null;
      source_markdown_slug: string | null;
      embedding: string | number[] | Float32Array | null;
    }>(sqlText, params);

    return result.rows.map(r => {
      // Inline embedding parser — mirrors rowToFact() at line 3911.
      let embedding: Float32Array | null = null;
      if (r.embedding != null) {
        if (r.embedding instanceof Float32Array) embedding = r.embedding;
        else if (Array.isArray(r.embedding)) embedding = new Float32Array(r.embedding);
        else if (typeof r.embedding === 'string') {
          const trimmed = r.embedding.trim();
          const inner = trimmed.startsWith('[') ? trimmed.slice(1, -1) : trimmed;
          const parts = inner.split(',').map(s => parseFloat(s.trim())).filter(Number.isFinite);
          embedding = parts.length > 0 ? new Float32Array(parts) : null;
        }
      }
      return {
        fact_id: Number(r.id),
        valid_from: r.valid_from instanceof Date ? r.valid_from : new Date(r.valid_from),
        metric: r.claim_metric,
        value: r.claim_value === null ? null : Number(r.claim_value),
        unit: r.claim_unit,
        period: r.claim_period,
        text: r.fact,
        source_session: r.source_session,
        source_markdown_slug: r.source_markdown_slug,
        embedding,
      };
    });
  }
,
  consolidateFact: async function(this: PGLiteEngineLike, id: number, takeId: number): Promise<void> {
    await this.db.query(
      `UPDATE facts SET consolidated_at = now(), consolidated_into = $1 WHERE id = $2`,
      [takeId, id],
    );
  }
,
  getFactsHealth: async function(this: PGLiteEngineLike, source_id: string): Promise<FactsHealth> {
    const total = await this.db.query<{
      total_active: number; total_today: number; total_week: number;
      total_expired: number; total_consolidated: number;
    }>(
      `SELECT
         COUNT(*) FILTER (WHERE expired_at IS NULL)                                    AS total_active,
         COUNT(*) FILTER (WHERE expired_at IS NULL AND created_at > now() - interval '24 hours') AS total_today,
         COUNT(*) FILTER (WHERE expired_at IS NULL AND created_at > now() - interval '7 days')   AS total_week,
         COUNT(*) FILTER (WHERE expired_at IS NOT NULL)                                AS total_expired,
         COUNT(*) FILTER (WHERE consolidated_at IS NOT NULL)                           AS total_consolidated
       FROM facts WHERE source_id = $1`,
      [source_id],
    );
    const top = await this.db.query<{ entity_slug: string; count: number }>(
      `SELECT entity_slug, COUNT(*)::int AS count
       FROM facts
       WHERE source_id = $1 AND expired_at IS NULL AND entity_slug IS NOT NULL
       GROUP BY entity_slug
       ORDER BY count DESC, entity_slug ASC
       LIMIT 5`,
      [source_id],
    );
    const r = total.rows[0] ?? {
      total_active: 0, total_today: 0, total_week: 0, total_expired: 0, total_consolidated: 0,
    };
    return {
      source_id,
      total_active: Number(r.total_active),
      total_today: Number(r.total_today),
      total_week: Number(r.total_week),
      total_expired: Number(r.total_expired),
      total_consolidated: Number(r.total_consolidated),
      top_entities: top.rows.map(t => ({ entity_slug: t.entity_slug, count: Number(t.count) })),
    };
  }
,

  _listFacts: async function(this: PGLiteEngineLike,
    source_id: string,
    opts: FactListOpts & {
      whereClauses?: string[];
      whereParams?: Record<string, unknown>;
      order: string;
    },
  ): Promise<FactRow[]> {
    const limit = clampSearchLimit(opts.limit, 50, MAX_SEARCH_LIMIT);
    const offset = Math.max(0, opts.offset ?? 0);
    const whereParts: string[] = [`source_id = $source_id`];
    const params: Record<string, unknown> = { source_id };
    if (opts.activeOnly !== false) {
      whereParts.push(`expired_at IS NULL`);
    }
    if (opts.kinds && opts.kinds.length > 0) {
      whereParts.push(`kind = ANY($kinds)`);
      params.kinds = opts.kinds;
    }
    if (opts.visibility && opts.visibility.length > 0) {
      whereParts.push(`visibility = ANY($visibility)`);
      params.visibility = opts.visibility;
    }
    for (const c of opts.whereClauses ?? []) whereParts.push(c);
    Object.assign(params, opts.whereParams ?? {});

    // Convert $name placeholders to numbered $1, $2, ... for PGLite.
    const orderedKeys = Object.keys(params);
    const indexFor = (name: string): number => orderedKeys.indexOf(name) + 1;
    const sql = `SELECT * FROM facts
       WHERE ${whereParts.join(' AND ').replace(/\$(\w+)/g, (_m, k) => `$${indexFor(k)}`)}
       ORDER BY ${opts.order}
       LIMIT ${limit} OFFSET ${offset}`;
    const result = await this.db.query<FactRowSqlShape>(sql, orderedKeys.map(k => params[k]));
    return result.rows.map(rowToFact);
  }


};

// Row mapping helpers (moved from pglite-engine.ts)
interface FactRowSqlShape {
  id: number;
  source_id: string;
  entity_slug: string | null;
  fact: string;
  kind: FactKind;
  visibility: FactVisibility;
  notability: 'high' | 'medium' | 'low';
  context: string | null;
  valid_from: Date | string;
  valid_until: Date | string | null;
  expired_at: Date | string | null;
  superseded_by: number | null;
  consolidated_at: Date | string | null;
  consolidated_into: number | null;
  source: string;
  source_session: string | null;
  confidence: number;
  embedding: string | number[] | Float32Array | null;
  embedded_at: Date | string | null;
  created_at: Date | string;
}

function toDate(v: Date | string | null): Date | null {
  if (v == null) return null;
  if (v instanceof Date) return v;
  return new Date(v);
}

function rowToFact(row: FactRowSqlShape): FactRow {
  let embedding: Float32Array | null = null;
  if (row.embedding != null) {
    if (row.embedding instanceof Float32Array) embedding = row.embedding;
    else if (Array.isArray(row.embedding)) embedding = new Float32Array(row.embedding);
    else if (typeof row.embedding === 'string') {
      // pgvector text format: "[0.1,0.2,...]"
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
    // v0.31.2: notability column added by migration v46. Same fallback
    // as Postgres (belt-and-suspenders with the NOT NULL DEFAULT).
    notability: row.notability ?? 'medium',
    context: row.context,
    valid_from: toDate(row.valid_from)!,
    valid_until: toDate(row.valid_until),
    expired_at: toDate(row.expired_at),
    superseded_by: row.superseded_by == null ? null : Number(row.superseded_by),
    consolidated_at: toDate(row.consolidated_at),
    consolidated_into: row.consolidated_into == null ? null : Number(row.consolidated_into),
    source: row.source,
    source_session: row.source_session,
    confidence: Number(row.confidence),
    embedding,
    embedded_at: toDate(row.embedded_at),
    created_at: toDate(row.created_at)!,
  };
}

function toPgVectorLiteral(v: Float32Array | number[]): string {
  if (v instanceof Float32Array) return '[' + Array.from(v).join(',') + ']';
  return '[' + v.join(',') + ']';
}

