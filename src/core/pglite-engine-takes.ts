/**
 * PGLiteEngine takes methods — split out of pglite-engine.ts (BET-Y1Q3-T6-04).
 * Injected onto PGLiteEngine.prototype via Object.assign.
 */
import { computeAnomaliesFromBuckets } from './cycle/anomaly.ts';
import {
  clampSearchLimit,
  type CalibrationBucket,
  type CalibrationCurveOpts,
  type StaleTakeRow,
  type SynthesisEvidenceInput,
  type Take,
  type TakeBatchInput,
  type TakeHit,
  type TakeResolution,
  type TakesListOpts,
  type TakesScorecard,
  type TakesScorecardOpts,
} from './engine.ts';
import { buildRecencyComponentSql } from './search/sql-ranking.ts';
import { normalizeWeightForStorage } from './takes-fence.ts';
import { deriveResolutionTuple, finalizeScorecard } from './takes-resolution.ts';
import {
  GBrainError,
  type AnomaliesOpts,
  type AnomalyResult,
  type BrainHealth,
  type BrainStats,
  type Chunk,
  type EmotionalWeightInputRow,
  type EmotionalWeightWriteRow,
  type EvalCandidate,
  type EvalCandidateInput,
  type EvalCaptureFailure,
  type EvalCaptureFailureReason,
  type IngestLogEntry,
  type IngestLogInput,
  type PageVersion,
  type SalienceOpts,
  type SalienceResult,
} from './types.ts';
import { rowToChunk, takeRowToTake, validateSlug } from './utils.ts';
import { vector } from '@electric-sql/pglite/vector';

export interface PGLiteEngineLike {
  db: import('@electric-sql/pglite').PGlite;
  [key: string]: any;
}

export const pgliteTakesMethods: Record<string, any> = {
  addTakesBatch: async function(this: PGLiteEngineLike, rowsIn: TakeBatchInput[]): Promise<number> {
    if (rowsIn.length === 0) return 0;
    let weightClamped = 0;
    const pageIds   = rowsIn.map(r => r.page_id);
    const rowNums   = rowsIn.map(r => r.row_num);
    const claims    = rowsIn.map(r => r.claim);
    const kinds     = rowsIn.map(r => r.kind);
    const holders   = rowsIn.map(r => r.holder);
    const weights   = rowsIn.map(r => {
      const { weight, clamped } = normalizeWeightForStorage(r.weight);
      if (clamped) weightClamped++;
      return weight;
    });
    const sinces    = rowsIn.map(r => r.since_date ?? null);
    const untils    = rowsIn.map(r => r.until_date ?? null);
    const sources   = rowsIn.map(r => r.source ?? null);
    const supersededBys = rowsIn.map(r => r.superseded_by ?? null);
    const actives   = rowsIn.map(r => r.active ?? true);
    if (weightClamped > 0) {
      process.stderr.write(`[takes] TAKES_WEIGHT_CLAMPED: ${weightClamped} row(s) had weight outside [0,1]; clamped\n`);
    }
    const result = await this.db.query(
      `INSERT INTO takes (page_id, row_num, claim, kind, holder, weight, since_date, until_date, source, superseded_by, active)
       SELECT v.page_id::int, v.row_num::int, v.claim, v.kind, v.holder, v.weight::real,
              v.since_date::text, v.until_date::text, v.source, v.superseded_by::int, v.active::boolean
       FROM unnest($1::int[], $2::int[], $3::text[], $4::text[], $5::text[], $6::real[],
                   $7::text[], $8::text[], $9::text[], $10::int[], $11::boolean[])
         AS v(page_id, row_num, claim, kind, holder, weight, since_date, until_date, source, superseded_by, active)
       ON CONFLICT (page_id, row_num) DO UPDATE SET
         claim         = EXCLUDED.claim,
         kind          = EXCLUDED.kind,
         holder        = EXCLUDED.holder,
         weight        = EXCLUDED.weight,
         since_date    = EXCLUDED.since_date,
         until_date    = EXCLUDED.until_date,
         source        = EXCLUDED.source,
         superseded_by = EXCLUDED.superseded_by,
         active        = EXCLUDED.active,
         updated_at    = now()
       RETURNING 1`,
      [pageIds, rowNums, claims, kinds, holders, weights, sinces, untils, sources, supersededBys, actives]
    );
    return result.rows.length;
  }
,
  listActiveTakesForPages: async function(this: PGLiteEngineLike, 
    pageIds: number[],
    opts: { takesHoldersAllowList?: string[] } = {},
  ): Promise<Map<number, Take[]>> {
    const out = new Map<number, Take[]>();
    for (const pid of pageIds) out.set(pid, []);
    if (pageIds.length === 0) return out;
    const { rows } = await this.db.query(
      `SELECT t.*, p.slug AS page_slug
       FROM takes t
       JOIN pages p ON p.id = t.page_id
       WHERE t.page_id = ANY($1::int[])
         AND t.active = true
         AND ($2::text[] IS NULL OR t.holder = ANY($2::text[]))
       ORDER BY t.page_id, t.row_num`,
      [pageIds, opts.takesHoldersAllowList ?? null]
    );
    for (const r of rows) {
      const take = takeRowToTake(r as Record<string, unknown>);
      const bucket = out.get(take.page_id);
      if (bucket) bucket.push(take);
    }
    return out;
  }
,
  writeContradictionsRun: async function(this: PGLiteEngineLike, row: {
    run_id: string;
    judge_model: string;
    prompt_version: string;
    queries_evaluated: number;
    queries_with_contradiction: number;
    total_contradictions_flagged: number;
    wilson_ci_lower: number;
    wilson_ci_upper: number;
    judge_errors_total: number;
    cost_usd_total: number;
    duration_ms: number;
    source_tier_breakdown: Record<string, unknown>;
    report_json: Record<string, unknown>;
  }): Promise<boolean> {
    const result = await this.db.query(
      `INSERT INTO eval_contradictions_runs (
         run_id, judge_model, prompt_version,
         queries_evaluated, queries_with_contradiction, total_contradictions_flagged,
         wilson_ci_lower, wilson_ci_upper, judge_errors_total,
         cost_usd_total, duration_ms,
         source_tier_breakdown, report_json
       ) VALUES (
         $1, $2, $3,
         $4, $5, $6,
         $7, $8, $9,
         $10, $11,
         $12::jsonb, $13::jsonb
       )
       ON CONFLICT (run_id) DO NOTHING`,
      [
        row.run_id, row.judge_model, row.prompt_version,
        row.queries_evaluated, row.queries_with_contradiction, row.total_contradictions_flagged,
        row.wilson_ci_lower, row.wilson_ci_upper, row.judge_errors_total,
        row.cost_usd_total, row.duration_ms,
        row.source_tier_breakdown, row.report_json,
      ]
    );
    return (result.affectedRows ?? 0) > 0;
  }
,
  loadContradictionsTrend: async function(this: PGLiteEngineLike, days: number): Promise<Array<{
    run_id: string;
    ran_at: string;
    judge_model: string;
    queries_evaluated: number;
    queries_with_contradiction: number;
    total_contradictions_flagged: number;
    wilson_ci_lower: number;
    wilson_ci_upper: number;
    judge_errors_total: number;
    cost_usd_total: number;
    duration_ms: number;
    source_tier_breakdown: Record<string, unknown>;
    report_json: Record<string, unknown>;
  }>> {
    const cutoff = new Date(Date.now() - Math.max(0, days) * 86400000);
    const { rows } = await this.db.query(
      `SELECT run_id, ran_at, judge_model,
              queries_evaluated, queries_with_contradiction, total_contradictions_flagged,
              wilson_ci_lower, wilson_ci_upper, judge_errors_total,
              cost_usd_total, duration_ms,
              source_tier_breakdown, report_json
       FROM eval_contradictions_runs
       WHERE ran_at >= $1
       ORDER BY ran_at DESC`,
      [cutoff]
    );
    return (rows as Record<string, unknown>[]).map((r) => ({
      run_id: r.run_id as string,
      ran_at: r.ran_at instanceof Date ? (r.ran_at as Date).toISOString() : String(r.ran_at),
      judge_model: r.judge_model as string,
      queries_evaluated: Number(r.queries_evaluated),
      queries_with_contradiction: Number(r.queries_with_contradiction),
      total_contradictions_flagged: Number(r.total_contradictions_flagged),
      wilson_ci_lower: Number(r.wilson_ci_lower),
      wilson_ci_upper: Number(r.wilson_ci_upper),
      judge_errors_total: Number(r.judge_errors_total),
      cost_usd_total: Number(r.cost_usd_total),
      duration_ms: Number(r.duration_ms),
      source_tier_breakdown: r.source_tier_breakdown as Record<string, unknown>,
      report_json: r.report_json as Record<string, unknown>,
    }));
  }
,
  getContradictionCacheEntry: async function(this: PGLiteEngineLike, key: {
    chunk_a_hash: string;
    chunk_b_hash: string;
    model_id: string;
    prompt_version: string;
    truncation_policy: string;
  }): Promise<Record<string, unknown> | null> {
    const { rows } = await this.db.query(
      `SELECT verdict FROM eval_contradictions_cache
       WHERE chunk_a_hash = $1
         AND chunk_b_hash = $2
         AND model_id = $3
         AND prompt_version = $4
         AND truncation_policy = $5
         AND expires_at > now()
       LIMIT 1`,
      [key.chunk_a_hash, key.chunk_b_hash, key.model_id, key.prompt_version, key.truncation_policy]
    );
    if (rows.length === 0) return null;
    return (rows[0] as Record<string, unknown>).verdict as Record<string, unknown>;
  }
,
  putContradictionCacheEntry: async function(this: PGLiteEngineLike, opts: {
    chunk_a_hash: string;
    chunk_b_hash: string;
    model_id: string;
    prompt_version: string;
    truncation_policy: string;
    verdict: Record<string, unknown>;
    ttl_seconds?: number;
  }): Promise<void> {
    const ttl = Math.max(60, opts.ttl_seconds ?? 30 * 86400);
    const expiresAt = new Date(Date.now() + ttl * 1000);
    await this.db.query(
      `INSERT INTO eval_contradictions_cache (
         chunk_a_hash, chunk_b_hash, model_id, prompt_version, truncation_policy,
         verdict, expires_at
       ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
       ON CONFLICT (chunk_a_hash, chunk_b_hash, model_id, prompt_version, truncation_policy)
       DO UPDATE SET
         verdict = EXCLUDED.verdict,
         expires_at = EXCLUDED.expires_at,
         created_at = now()`,
      [
        opts.chunk_a_hash, opts.chunk_b_hash, opts.model_id,
        opts.prompt_version, opts.truncation_policy,
        opts.verdict, expiresAt,
      ]
    );
  }
,
  sweepContradictionCache: async function(this: PGLiteEngineLike, ): Promise<number> {
    const result = await this.db.query(
      `DELETE FROM eval_contradictions_cache WHERE expires_at <= now()`
    );
    return result.affectedRows ?? 0;
  }
,
  listTakes: async function(this: PGLiteEngineLike, opts: TakesListOpts = {}): Promise<Take[]> {
    const limit = clampSearchLimit(opts.limit, 100, 500);
    const offset = Math.max(0, Math.floor(opts.offset ?? 0));
    const active = opts.active ?? true;
    const sortBy = opts.sortBy ?? 'created_at';
    const { rows } = await this.db.query(
      `SELECT t.*, p.slug AS page_slug
       FROM takes t
       JOIN pages p ON p.id = t.page_id
       WHERE 1=1
         AND ($1::int   IS NULL OR t.page_id = $1::int)
         AND ($2::text  IS NULL OR p.slug    = $2::text)
         AND ($3::text  IS NULL OR t.holder  = $3::text)
         AND ($4::text  IS NULL OR t.kind    = $4::text)
         AND ($5::boolean IS NULL OR t.active = $5::boolean)
         AND (
           $6::boolean IS NULL
           OR ($6::boolean = true  AND t.resolved_at IS NOT NULL)
           OR ($6::boolean = false AND t.resolved_at IS NULL)
         )
         AND ($7::text[] IS NULL OR t.holder = ANY($7::text[]))
       ORDER BY
         CASE WHEN $8 = 'weight'      THEN t.weight     END DESC NULLS LAST,
         CASE WHEN $8 = 'since_date'  THEN t.since_date END DESC NULLS LAST,
         CASE WHEN $8 = 'created_at'  THEN t.created_at END DESC NULLS LAST
       LIMIT $9 OFFSET $10`,
      [
        opts.page_id ?? null,
        opts.page_slug ?? null,
        opts.holder ?? null,
        opts.kind ?? null,
        active,
        opts.resolved === undefined ? null : opts.resolved,
        opts.takesHoldersAllowList ?? null,
        sortBy,
        limit,
        offset,
      ]
    );
    return rows.map((r) => takeRowToTake(r as Record<string, unknown>));
  }
,
  searchTakes: async function(this: PGLiteEngineLike, 
    query: string,
    opts: { limit?: number; takesHoldersAllowList?: string[] } = {},
  ): Promise<TakeHit[]> {
    const limit = clampSearchLimit(opts.limit, 30, 100);
    const { rows } = await this.db.query(
      `SELECT t.id AS take_id, t.page_id, p.slug AS page_slug, t.row_num,
              t.claim, t.kind, t.holder, t.weight,
              similarity(t.claim, $1)::real AS score
       FROM takes t
       JOIN pages p ON p.id = t.page_id
       WHERE t.active
         AND t.claim % $1
         AND ($2::text[] IS NULL OR t.holder = ANY($2::text[]))
       ORDER BY score DESC, t.weight DESC
       LIMIT $3`,
      [query, opts.takesHoldersAllowList ?? null, limit]
    );
    return rows as unknown as TakeHit[];
  }
,
  searchTakesVector: async function(this: PGLiteEngineLike, 
    embedding: Float32Array,
    opts: { limit?: number; takesHoldersAllowList?: string[] } = {},
  ): Promise<TakeHit[]> {
    const limit = clampSearchLimit(opts.limit, 30, 100);
    const vec = `[${Array.from(embedding).join(',')}]`;
    const { rows } = await this.db.query(
      `SELECT t.id AS take_id, t.page_id, p.slug AS page_slug, t.row_num,
              t.claim, t.kind, t.holder, t.weight,
              (1 - (t.embedding <=> $1::vector))::real AS score
       FROM takes t
       JOIN pages p ON p.id = t.page_id
       WHERE t.active
         AND t.embedding IS NOT NULL
         AND ($2::text[] IS NULL OR t.holder = ANY($2::text[]))
       ORDER BY t.embedding <=> $1::vector
       LIMIT $3`,
      [vec, opts.takesHoldersAllowList ?? null, limit]
    );
    return rows as unknown as TakeHit[];
  }
,
  getTakeEmbeddings: async function(this: PGLiteEngineLike, ids: number[]): Promise<Map<number, Float32Array>> {
    if (ids.length === 0) return new Map();
    const { rows } = await this.db.query(
      `SELECT id, embedding FROM takes WHERE id = ANY($1::bigint[]) AND embedding IS NOT NULL`,
      [ids]
    );
    const out = new Map<number, Float32Array>();
    for (const r of rows as Array<{ id: number; embedding: unknown }>) {
      const v = r.embedding;
      if (typeof v === 'string') {
        const trimmed = v.replace(/^\[|\]$/g, '');
        const arr = trimmed.split(',').map(parseFloat).filter(n => !Number.isNaN(n));
        out.set(Number(r.id), new Float32Array(arr));
      } else if (Array.isArray(v)) {
        out.set(Number(r.id), new Float32Array(v as number[]));
      }
    }
    return out;
  }
,
  countStaleTakes: async function(this: PGLiteEngineLike, ): Promise<number> {
    const { rows } = await this.db.query(
      `SELECT count(*)::int AS count FROM takes WHERE active AND embedding IS NULL`
    );
    return Number((rows[0] as { count?: number } | undefined)?.count ?? 0);
  }
,
  listStaleTakes: async function(this: PGLiteEngineLike, ): Promise<StaleTakeRow[]> {
    const { rows } = await this.db.query(
      `SELECT t.id AS take_id, p.slug AS page_slug, t.row_num, t.claim
       FROM takes t
       JOIN pages p ON p.id = t.page_id
       WHERE t.active AND t.embedding IS NULL
       ORDER BY t.id
       LIMIT 100000`
    );
    return rows as unknown as StaleTakeRow[];
  }
,
  updateTake: async function(this: PGLiteEngineLike, 
    pageId: number,
    rowNum: number,
    fields: { weight?: number; since_date?: string; source?: string },
  ): Promise<void> {
    let weight = fields.weight;
    if (weight !== undefined) {
      const norm = normalizeWeightForStorage(weight);
      if (norm.clamped) {
        process.stderr.write(`[takes] TAKES_WEIGHT_CLAMPED: updateTake clamped weight ${weight} → ${norm.weight}\n`);
      }
      weight = norm.weight;
    }
    const result = await this.db.query(
      `UPDATE takes SET
         weight     = COALESCE($3::real, weight),
         since_date = COALESCE($4::text, since_date),
         source     = COALESCE($5::text, source),
         updated_at = now()
       WHERE page_id = $1 AND row_num = $2
       RETURNING 1`,
      [pageId, rowNum, weight ?? null, fields.since_date ?? null, fields.source ?? null]
    );
    if (result.rows.length === 0) {
      throw new GBrainError(
        'TAKE_ROW_NOT_FOUND',
        `take not found at page_id=${pageId} row=${rowNum}`,
        'list takes for this page with `gbrain takes <slug>` to see valid row numbers',
      );
    }
  }
,
  supersedeTake: async function(this: PGLiteEngineLike, 
    pageId: number,
    oldRow: number,
    newRow: Omit<TakeBatchInput, 'page_id' | 'row_num' | 'superseded_by'>,
  ): Promise<{ oldRow: number; newRow: number }> {
    return await this.db.transaction(async (tx) => {
      const existingRes = await tx.query(
        `SELECT resolved_at FROM takes WHERE page_id = $1 AND row_num = $2`,
        [pageId, oldRow]
      );
      const existing = existingRes.rows[0] as { resolved_at?: unknown } | undefined;
      if (!existing) {
        throw new GBrainError('TAKE_ROW_NOT_FOUND', `take not found at page_id=${pageId} row=${oldRow}`, 'list takes with `gbrain takes <slug>`');
      }
      if (existing.resolved_at) {
        throw new GBrainError('TAKE_RESOLVED_IMMUTABLE', `take ${pageId}#${oldRow} is resolved`, 'resolved bets are immutable; add a new take instead');
      }
      const maxRowRes = await tx.query(
        `SELECT COALESCE(MAX(row_num), 0) + 1 AS next FROM takes WHERE page_id = $1`,
        [pageId]
      );
      const newRowNum = Number((maxRowRes.rows[0] as { next?: number })?.next ?? 1);
      const w = Math.max(0, Math.min(1, newRow.weight ?? 0.5));
      await tx.query(
        `INSERT INTO takes (page_id, row_num, claim, kind, holder, weight, since_date, until_date, source, active)
         VALUES ($1, $2, $3, $4, $5, $6, $7::text, $8::text, $9, $10)`,
        [
          pageId, newRowNum, newRow.claim, newRow.kind, newRow.holder, w,
          newRow.since_date ?? null, newRow.until_date ?? null, newRow.source ?? null,
          newRow.active ?? true,
        ]
      );
      await tx.query(
        `UPDATE takes SET active = false, superseded_by = $3, updated_at = now()
         WHERE page_id = $1 AND row_num = $2`,
        [pageId, oldRow, newRowNum]
      );
      return { oldRow, newRow: newRowNum };
    });
  }
,
  resolveTake: async function(this: PGLiteEngineLike, pageId: number, rowNum: number, resolution: TakeResolution): Promise<void> {
    const existingRes = await this.db.query(
      `SELECT resolved_at FROM takes WHERE page_id = $1 AND row_num = $2`,
      [pageId, rowNum]
    );
    const existing = existingRes.rows[0] as { resolved_at?: unknown } | undefined;
    if (!existing) {
      throw new GBrainError('TAKE_ROW_NOT_FOUND', `take not found at page_id=${pageId} row=${rowNum}`, 'list takes with `gbrain takes <slug>`');
    }
    if (existing.resolved_at) {
      throw new GBrainError('TAKE_ALREADY_RESOLVED', `take ${pageId}#${rowNum} already resolved`, 'resolution is immutable; add a new take to record a new outcome');
    }
    // v0.30.0: derive (quality, outcome) tuple. quality wins when both set.
    const { quality, outcome } = deriveResolutionTuple(resolution);
    await this.db.query(
      `UPDATE takes SET
         resolved_at      = now(),
         resolved_quality = $3::text,
         resolved_outcome = $4,
         resolved_value   = $5::real,
         resolved_unit    = $6::text,
         resolved_source  = $7::text,
         resolved_by      = $8,
         updated_at       = now()
       WHERE page_id = $1 AND row_num = $2`,
      [
        pageId, rowNum,
        quality,
        outcome,
        resolution.value ?? null,
        resolution.unit ?? null,
        resolution.source ?? null,
        resolution.resolvedBy,
      ]
    );
  }
,
  getScorecard: async function(this: PGLiteEngineLike, opts: TakesScorecardOpts, allowList: string[] | undefined): Promise<TakesScorecard> {
    // Build the WHERE clause with positional params. PGLite (postgres-via-WASM)
    // shares the SQL dialect with real Postgres so the math expressions match.
    const params: unknown[] = [];
    const clauses: string[] = [];
    if (opts.holder !== undefined) { params.push(opts.holder); clauses.push(`AND holder = $${params.length}`); }
    if (opts.domainPrefix !== undefined) {
      params.push(opts.domainPrefix + '%');
      clauses.push(`AND EXISTS (SELECT 1 FROM pages p WHERE p.id = takes.page_id AND p.slug LIKE $${params.length})`);
    }
    if (opts.since !== undefined) { params.push(opts.since); clauses.push(`AND since_date >= $${params.length}`); }
    if (opts.until !== undefined) { params.push(opts.until); clauses.push(`AND since_date <= $${params.length}`); }
    if (allowList !== undefined) { params.push(allowList); clauses.push(`AND holder = ANY($${params.length}::text[])`); }
    const where = clauses.join(' ');
    // v0.36.1.1 T1c: `resolved` deliberately filters to the 3-state subset
    // (correct|incorrect|partial) — NOT `resolved_quality IS NOT NULL` — so
    // historical comparisons against pre-v74 scorecards stay valid.
    // `unresolvable_count` is a sibling field counting the new 4th state.
    const res = await this.db.query(
      `SELECT
         COUNT(*) FILTER (WHERE kind = 'bet')::int                                              AS total_bets,
         COUNT(*) FILTER (WHERE resolved_quality IN ('correct','incorrect','partial'))::int     AS resolved,
         COUNT(*) FILTER (WHERE resolved_quality = 'correct')::int                              AS correct,
         COUNT(*) FILTER (WHERE resolved_quality = 'incorrect')::int                            AS incorrect,
         COUNT(*) FILTER (WHERE resolved_quality = 'partial')::int                              AS partial,
         COUNT(*) FILTER (WHERE resolved_quality = 'unresolvable')::int                         AS unresolvable_count,
         AVG(
           CASE WHEN resolved_quality IN ('correct','incorrect')
                THEN POWER(weight - (CASE resolved_quality WHEN 'correct' THEN 1 ELSE 0 END), 2)
           END
         )::float                                                                               AS brier
       FROM takes
       WHERE 1=1 ${where}`,
      params,
    );
    const r = res.rows[0] as { total_bets: number; resolved: number; correct: number; incorrect: number; partial: number; unresolvable_count: number; brier: number | null };
    return finalizeScorecard(r);
  }
,
  getCalibrationCurve: async function(this: PGLiteEngineLike, opts: CalibrationCurveOpts, allowList: string[] | undefined): Promise<CalibrationBucket[]> {
    const bucketSize = opts.bucketSize && opts.bucketSize > 0 && opts.bucketSize <= 1 ? opts.bucketSize : 0.1;
    const maxIdx = Math.floor(1 / bucketSize) - 1;
    const params: unknown[] = [bucketSize, maxIdx];
    const clauses: string[] = [];
    if (opts.holder !== undefined) { params.push(opts.holder); clauses.push(`AND holder = $${params.length}`); }
    if (allowList !== undefined) { params.push(allowList); clauses.push(`AND holder = ANY($${params.length}::text[])`); }
    const where = clauses.join(' ');
    // NUMERIC casts for exact decimal arithmetic — keeps PGLite + Postgres
    // bucket boundaries identical at FP-edge weights (e.g. 0.7/0.1).
    // See parity test in test/e2e/takes-scorecard-parity.test.ts.
    const res = await this.db.query(
      `WITH binned AS (
         SELECT
           LEAST(FLOOR(weight::numeric / $1::numeric)::int, $2::int)::int AS bucket_idx,
           weight,
           (resolved_quality = 'correct')::int            AS hit
         FROM takes
         WHERE resolved_quality IN ('correct','incorrect')
           ${where}
       )
       SELECT
         (bucket_idx::numeric * $1::numeric)::float        AS bucket_lo,
         ((bucket_idx + 1)::numeric * $1::numeric)::float  AS bucket_hi,
         COUNT(*)::int                                     AS n,
         AVG(hit)::float                                   AS observed,
         AVG(weight)::float                                AS predicted
       FROM binned
       GROUP BY bucket_idx
       ORDER BY bucket_idx`,
      params,
    );
    return (res.rows as { bucket_lo: number; bucket_hi: number; n: number; observed: number | null; predicted: number | null }[]).map(r => ({
      bucket_lo: r.bucket_lo,
      bucket_hi: r.bucket_hi,
      n: r.n,
      observed: r.n > 0 ? r.observed : null,
      predicted: r.n > 0 ? r.predicted : null,
    }));
  }
,
  addSynthesisEvidence: async function(this: PGLiteEngineLike, rowsIn: SynthesisEvidenceInput[]): Promise<number> {
    if (rowsIn.length === 0) return 0;
    const synthesisIds = rowsIn.map(r => r.synthesis_page_id);
    const takePageIds  = rowsIn.map(r => r.take_page_id);
    const takeRowNums  = rowsIn.map(r => r.take_row_num);
    const citationIxs  = rowsIn.map(r => r.citation_index);
    const result = await this.db.query(
      `INSERT INTO synthesis_evidence (synthesis_page_id, take_page_id, take_row_num, citation_index)
       SELECT v.synthesis_page_id::int, v.take_page_id::int, v.take_row_num::int, v.citation_index::int
       FROM unnest($1::int[], $2::int[], $3::int[], $4::int[])
         AS v(synthesis_page_id, take_page_id, take_row_num, citation_index)
       ON CONFLICT (synthesis_page_id, take_page_id, take_row_num) DO NOTHING
       RETURNING 1`,
      [synthesisIds, takePageIds, takeRowNums, citationIxs]
    );
    return result.rows.length;
  }
,
  createVersion: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<PageVersion> {
    const sourceId = opts?.sourceId ?? 'default';
    const { rows } = await this.db.query(
      `INSERT INTO page_versions (page_id, compiled_truth, frontmatter)
       SELECT id, compiled_truth, frontmatter
       FROM pages WHERE slug = $1 AND source_id = $2
       RETURNING *`,
      [slug, sourceId]
    );
    if (rows.length === 0) throw new Error(`createVersion failed: page "${slug}" (source=${sourceId}) not found`);
    return rows[0] as unknown as PageVersion;
  }
,
  getVersions: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<PageVersion[]> {
    // v0.31.8 (D16): two-branch. Without opts.sourceId, joins return versions
    // for every same-slug page (preserves pre-v0.31.8 cross-source view).
    if (opts?.sourceId) {
      const { rows } = await this.db.query(
        `SELECT pv.* FROM page_versions pv
         JOIN pages p ON p.id = pv.page_id
         WHERE p.slug = $1 AND p.source_id = $2
         ORDER BY pv.snapshot_at DESC`,
        [slug, opts.sourceId]
      );
      return rows as unknown as PageVersion[];
    }
    const { rows } = await this.db.query(
      `SELECT pv.* FROM page_versions pv
       JOIN pages p ON p.id = pv.page_id
       WHERE p.slug = $1
       ORDER BY pv.snapshot_at DESC`,
      [slug]
    );
    return rows as unknown as PageVersion[];
  }
,
  revertToVersion: async function(this: PGLiteEngineLike, 
    slug: string,
    versionId: number,
    opts?: { sourceId?: string },
  ): Promise<void> {
    // v0.31.8 (D12): when opts.sourceId is set, scope BOTH the page lookup
    // and the version row reference. Without it, multi-source brains can
    // revert the wrong same-slug page (the one Postgres returns first).
    if (opts?.sourceId) {
      await this.db.query(
        `UPDATE pages SET
          compiled_truth = pv.compiled_truth,
          frontmatter = pv.frontmatter,
          updated_at = now()
        FROM page_versions pv
        WHERE pages.slug = $1 AND pages.source_id = $3
              AND pv.id = $2 AND pv.page_id = pages.id`,
        [slug, versionId, opts.sourceId]
      );
      return;
    }
    await this.db.query(
      `UPDATE pages SET
        compiled_truth = pv.compiled_truth,
        frontmatter = pv.frontmatter,
        updated_at = now()
      FROM page_versions pv
      WHERE pages.slug = $1 AND pv.id = $2 AND pv.page_id = pages.id`,
      [slug, versionId]
    );
  }
,
  getStats: async function(this: PGLiteEngineLike, ): Promise<BrainStats> {
    const { rows: [stats] } = await this.db.query(`
      SELECT
        -- v0.26.5: exclude soft-deleted from page_count (mirrors postgres-engine).
        (SELECT count(*) FROM pages WHERE deleted_at IS NULL) as page_count,
        (SELECT count(*) FROM content_chunks) as chunk_count,
        (SELECT count(*) FROM content_chunks WHERE embedded_at IS NOT NULL) as embedded_count,
        (SELECT count(*) FROM links) as link_count,
        (SELECT count(DISTINCT tag) FROM tags) as tag_count,
        (SELECT count(*) FROM timeline_entries) as timeline_entry_count
    `);

    const { rows: types } = await this.db.query(
      `SELECT type, count(*)::int as count FROM pages GROUP BY type ORDER BY count DESC`
    );
    const pages_by_type: Record<string, number> = {};
    for (const t of types as { type: string; count: number }[]) {
      pages_by_type[t.type] = t.count;
    }

    const s = stats as Record<string, unknown>;
    return {
      page_count: Number(s.page_count),
      chunk_count: Number(s.chunk_count),
      embedded_count: Number(s.embedded_count),
      link_count: Number(s.link_count),
      tag_count: Number(s.tag_count),
      timeline_entry_count: Number(s.timeline_entry_count),
      pages_by_type,
    };
  }
,
  getHealth: async function(this: PGLiteEngineLike, ): Promise<BrainHealth> {
    // Combined metrics from master (brain_score components: dead_links, link_count,
    // pages_with_timeline) and v0.10.3 graph layer (link_coverage, timeline_coverage,
    // most_connected). Both coexist: master's brain_score is the composite
    // dashboard, v0.10.3 metrics give entity-page-level granularity.
    const { rows: [h] } = await this.db.query(`
      WITH entity_pages AS (
        SELECT id, slug FROM pages WHERE type IN ('person', 'company')
      )
      SELECT
        (SELECT count(*) FROM pages) as page_count,
        (SELECT count(*) FROM content_chunks WHERE embedded_at IS NOT NULL)::float /
          GREATEST((SELECT count(*) FROM content_chunks), 1)::float as embed_coverage,
        (SELECT count(*) FROM pages p
         WHERE p.updated_at < (SELECT MAX(te.created_at) FROM timeline_entries te WHERE te.page_id = p.id)
        ) as stale_pages,
        -- Bug 11 — orphan = islanded (no inbound AND no outbound).
        -- See BrainHealth.orphan_pages docstring; docs updated to match this.
        (SELECT count(*) FROM pages p
         WHERE NOT EXISTS (SELECT 1 FROM links l WHERE l.to_page_id = p.id)
           AND NOT EXISTS (SELECT 1 FROM links l WHERE l.from_page_id = p.id)
        ) as orphan_pages,
        (SELECT count(*) FROM links l
         WHERE NOT EXISTS (SELECT 1 FROM pages p WHERE p.id = l.to_page_id)
        ) as dead_links,
        (SELECT count(*) FROM content_chunks WHERE embedded_at IS NULL) as missing_embeddings,
        (SELECT count(*) FROM links) as link_count,
        (SELECT count(DISTINCT page_id) FROM timeline_entries) as pages_with_timeline,
        (SELECT count(*) FROM entity_pages e
         WHERE EXISTS (SELECT 1 FROM links l WHERE l.to_page_id = e.id))::float /
          GREATEST((SELECT count(*) FROM entity_pages), 1)::float as link_coverage,
        (SELECT count(*) FROM entity_pages e
         WHERE EXISTS (SELECT 1 FROM timeline_entries te WHERE te.page_id = e.id))::float /
          GREATEST((SELECT count(*) FROM entity_pages), 1)::float as timeline_coverage
    `);

    // Top 5 most connected entities by total link count (in + out).
    const { rows: connected } = await this.db.query(`
      SELECT p.slug,
             (SELECT count(*) FROM links l WHERE l.from_page_id = p.id OR l.to_page_id = p.id)::int as link_count
      FROM pages p
      WHERE p.type IN ('person', 'company')
      ORDER BY link_count DESC
      LIMIT 5
    `);

    const r = h as Record<string, unknown>;
    const pageCount = Number(r.page_count);
    const embedCoverage = Number(r.embed_coverage);
    const orphanPages = Number(r.orphan_pages);
    const deadLinks = Number(r.dead_links);
    const linkCount = Number(r.link_count);
    const pagesWithTimeline = Number(r.pages_with_timeline);

    const linkDensity = pageCount > 0 ? Math.min(linkCount / pageCount, 1) : 0;
    const timelineCoverageDensity = pageCount > 0 ? Math.min(pagesWithTimeline / pageCount, 1) : 0;
    const noOrphans = pageCount > 0 ? 1 - (orphanPages / pageCount) : 1;
    const noDeadLinks = pageCount > 0 ? 1 - Math.min(deadLinks / pageCount, 1) : 1;
    // Bug 11 — per-component points. Sum equals brainScore by construction
    // so `doctor` can render a breakdown that adds up to the total.
    //
    // v0.37.10.0: empty brains (pageCount === 0) get FULL marks (100/100),
    // not 0. Semantically an empty brain has no coverage problem to penalize
    // — there's nothing to embed, nothing to link, nothing to orphan. The
    // pre-fix "empty = 0" caused fresh-init brains to score as critically
    // unhealthy on `gbrain doctor`, which was a structural surprise to users
    // who'd just successfully run init.
    const embedCoverageScore = pageCount === 0 ? 35 : Math.round(embedCoverage * 35);
    const linkDensityScore = pageCount === 0 ? 25 : Math.round(linkDensity * 25);
    const timelineCoverageScore = pageCount === 0 ? 15 : Math.round(timelineCoverageDensity * 15);
    const noOrphansScore = pageCount === 0 ? 15 : Math.round(noOrphans * 15);
    const noDeadLinksScore = pageCount === 0 ? 10 : Math.round(noDeadLinks * 10);
    const brainScore = embedCoverageScore + linkDensityScore + timelineCoverageScore + noOrphansScore + noDeadLinksScore;

    return {
      page_count: pageCount,
      embed_coverage: embedCoverage,
      stale_pages: Number(r.stale_pages),
      orphan_pages: orphanPages,
      missing_embeddings: Number(r.missing_embeddings),
      brain_score: brainScore,
      dead_links: deadLinks,
      link_coverage: Number(r.link_coverage),
      timeline_coverage: Number(r.timeline_coverage),
      most_connected: (connected as { slug: string; link_count: number }[]).map(c => ({
        slug: c.slug,
        link_count: Number(c.link_count),
      })),
      embed_coverage_score: embedCoverageScore,
      link_density_score: linkDensityScore,
      timeline_coverage_score: timelineCoverageScore,
      no_orphans_score: noOrphansScore,
      no_dead_links_score: noDeadLinksScore,
    };
  }
,
  logIngest: async function(this: PGLiteEngineLike, entry: IngestLogInput): Promise<void> {
    // v0.31.2 (codex P1 #3): source_id threaded so multi-source brains can
    // scope ingest_log queries. Default 'default' matches the column DEFAULT.
    const sourceId = entry.source_id ?? 'default';
    await this.db.query(
      `INSERT INTO ingest_log (source_id, source_type, source_ref, pages_updated, summary)
       VALUES ($1, $2, $3, $4::jsonb, $5)`,
      [sourceId, entry.source_type, entry.source_ref, JSON.stringify(entry.pages_updated), entry.summary]
    );
  }
,
  getIngestLog: async function(this: PGLiteEngineLike, opts?: { limit?: number }): Promise<IngestLogEntry[]> {
    const limit = opts?.limit || 50;
    const { rows } = await this.db.query(
      `SELECT * FROM ingest_log ORDER BY created_at DESC LIMIT $1`,
      [limit]
    );
    // Belt-and-suspenders source_id fallback for any pre-v50 row that
    // somehow survived without the backfill.
    return (rows as unknown as IngestLogEntry[]).map(r => ({
      ...r,
      source_id: r.source_id ?? 'default',
    }));
  }
,
  updateSlug: async function(this: PGLiteEngineLike, oldSlug: string, newSlug: string, opts?: { sourceId?: string }): Promise<void> {
    newSlug = validateSlug(newSlug);
    const sourceId = opts?.sourceId ?? 'default';
    // Source-qualify so a rename in source A doesn't sweep up same-slug rows
    // in sources B/C/D (mirrors postgres-engine.ts).
    await this.db.query(
      `UPDATE pages SET slug = $1, updated_at = now() WHERE slug = $2 AND source_id = $3`,
      [newSlug, oldSlug, sourceId]
    );
  }
,
  rewriteLinks: async function(this: PGLiteEngineLike, _oldSlug: string, _newSlug: string): Promise<void> {
    // Stub: links use integer page_id FKs, already correct after updateSlug.
  }
,
  getConfig: async function(this: PGLiteEngineLike, key: string): Promise<string | null> {
    const { rows } = await this.db.query('SELECT value FROM config WHERE key = $1', [key]);
    return rows.length > 0 ? (rows[0] as { value: string }).value : null;
  }
,
  setConfig: async function(this: PGLiteEngineLike, key: string, value: string): Promise<void> {
    await this.db.query(
      `INSERT INTO config (key, value) VALUES ($1, $2)
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
      [key, value]
    );
  }
,
  unsetConfig: async function(this: PGLiteEngineLike, key: string): Promise<number> {
    const { affectedRows } = await this.db.query(
      'DELETE FROM config WHERE key = $1',
      [key],
    ) as { affectedRows?: number };
    return affectedRows ?? 0;
  }
,
  listConfigKeys: async function(this: PGLiteEngineLike, prefix: string): Promise<string[]> {
    // LIKE-escape the prefix so a user-supplied % or _ doesn't act as a wildcard.
    const escaped = prefix.replace(/\\/g, '\\\\').replace(/%/g, '\\%').replace(/_/g, '\\_');
    const { rows } = await this.db.query(
      `SELECT key FROM config WHERE key LIKE $1 || '%' ESCAPE '\\' ORDER BY key`,
      [escaped],
    );
    return (rows as { key: string }[]).map(r => r.key);
  }
,
  runMigration: async function(this: PGLiteEngineLike, _version: number, sql: string): Promise<void> {
    await this.db.exec(sql);
  }
,
  getChunksWithEmbeddings: async function(this: PGLiteEngineLike, slug: string, opts?: { sourceId?: string }): Promise<Chunk[]> {
    const sourceId = opts?.sourceId;
    const { rows } = sourceId
      ? await this.db.query(
          `SELECT cc.* FROM content_chunks cc
           JOIN pages p ON p.id = cc.page_id
           WHERE p.slug = $1 AND p.source_id = $2
           ORDER BY cc.chunk_index`,
          [slug, sourceId]
        )
      : await this.db.query(
          `SELECT cc.* FROM content_chunks cc
           JOIN pages p ON p.id = cc.page_id
           WHERE p.slug = $1
           ORDER BY cc.chunk_index`,
          [slug]
        );
    return (rows as Record<string, unknown>[]).map(r => rowToChunk(r, true));
  }
,
  addCodeEdges: async function(this: PGLiteEngineLike, edges: import('./types.ts').CodeEdgeInput[]): Promise<number> {
    if (edges.length === 0) return 0;
    let inserted = 0;
    // Split into resolved vs unresolved. Resolved rows carry to_chunk_id
    // (known target chunk); unresolved rows only know the qualified name.
    const resolved = edges.filter(e => e.to_chunk_id != null);
    const unresolved = edges.filter(e => e.to_chunk_id == null);

    if (resolved.length > 0) {
      const rowParts: string[] = [];
      const params: unknown[] = [];
      let p = 1;
      for (const e of resolved) {
        rowParts.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++}, $${p++}::jsonb, $${p++})`);
        params.push(
          e.from_chunk_id, e.to_chunk_id, e.from_symbol_qualified,
          e.to_symbol_qualified, e.edge_type,
          JSON.stringify(e.edge_metadata ?? {}),
          e.source_id ?? null,
        );
      }
      const res = await this.db.query(
        `INSERT INTO code_edges_chunk
           (from_chunk_id, to_chunk_id, from_symbol_qualified, to_symbol_qualified, edge_type, edge_metadata, source_id)
         VALUES ${rowParts.join(', ')}
         ON CONFLICT (from_chunk_id, to_chunk_id, edge_type) DO NOTHING`,
        params,
      );
      inserted += res.affectedRows ?? 0;
    }
    if (unresolved.length > 0) {
      const rowParts: string[] = [];
      const params: unknown[] = [];
      let p = 1;
      for (const e of unresolved) {
        rowParts.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++}::jsonb, $${p++})`);
        params.push(
          e.from_chunk_id, e.from_symbol_qualified, e.to_symbol_qualified, e.edge_type,
          JSON.stringify(e.edge_metadata ?? {}),
          e.source_id ?? null,
        );
      }
      const res = await this.db.query(
        `INSERT INTO code_edges_symbol
           (from_chunk_id, from_symbol_qualified, to_symbol_qualified, edge_type, edge_metadata, source_id)
         VALUES ${rowParts.join(', ')}
         ON CONFLICT (from_chunk_id, to_symbol_qualified, edge_type) DO NOTHING`,
        params,
      );
      inserted += res.affectedRows ?? 0;
    }
    return inserted;
  }
,
  deleteCodeEdgesForChunks: async function(this: PGLiteEngineLike, chunkIds: number[]): Promise<void> {
    if (chunkIds.length === 0) return;
    // Both directions on code_edges_chunk; from-only on code_edges_symbol
    // (unresolved edges don't have a to_chunk_id to match against).
    await this.db.query(
      `DELETE FROM code_edges_chunk WHERE from_chunk_id = ANY($1::int[]) OR to_chunk_id = ANY($1::int[])`,
      [chunkIds],
    );
    await this.db.query(
      `DELETE FROM code_edges_symbol WHERE from_chunk_id = ANY($1::int[])`,
      [chunkIds],
    );
  }
,
  getCallersOf: async function(this: PGLiteEngineLike, 
    qualifiedName: string,
    opts?: { sourceId?: string; allSources?: boolean; limit?: number },
  ): Promise<import('./types.ts').CodeEdgeResult[]> {
    const limit = Math.min(opts?.limit ?? 100, 500);
    const sourceClause = opts?.allSources || !opts?.sourceId
      ? ''
      : `AND source_id = '${opts.sourceId.replace(/'/g, "''")}'`;
    const { rows } = await this.db.query(
      `SELECT id, from_chunk_id, to_chunk_id, from_symbol_qualified, to_symbol_qualified,
              edge_type, edge_metadata, source_id, true as resolved
         FROM code_edges_chunk
         WHERE to_symbol_qualified = $1 ${sourceClause}
       UNION ALL
       SELECT id, from_chunk_id, NULL as to_chunk_id, from_symbol_qualified, to_symbol_qualified,
              edge_type, edge_metadata, source_id, false as resolved
         FROM code_edges_symbol
         WHERE to_symbol_qualified = $1 ${sourceClause}
       LIMIT $2`,
      [qualifiedName, limit],
    );
    return (rows as Record<string, unknown>[]).map(rowToCodeEdge);
  }
,
  getCalleesOf: async function(this: PGLiteEngineLike, 
    qualifiedName: string,
    opts?: { sourceId?: string; allSources?: boolean; limit?: number },
  ): Promise<import('./types.ts').CodeEdgeResult[]> {
    const limit = Math.min(opts?.limit ?? 100, 500);
    const sourceClause = opts?.allSources || !opts?.sourceId
      ? ''
      : `AND source_id = '${opts.sourceId.replace(/'/g, "''")}'`;
    const { rows } = await this.db.query(
      `SELECT id, from_chunk_id, to_chunk_id, from_symbol_qualified, to_symbol_qualified,
              edge_type, edge_metadata, source_id, true as resolved
         FROM code_edges_chunk
         WHERE from_symbol_qualified = $1 ${sourceClause}
       UNION ALL
       SELECT id, from_chunk_id, NULL as to_chunk_id, from_symbol_qualified, to_symbol_qualified,
              edge_type, edge_metadata, source_id, false as resolved
         FROM code_edges_symbol
         WHERE from_symbol_qualified = $1 ${sourceClause}
       LIMIT $2`,
      [qualifiedName, limit],
    );
    return (rows as Record<string, unknown>[]).map(rowToCodeEdge);
  }
,
  getEdgesByChunk: async function(this: PGLiteEngineLike, 
    chunkId: number,
    opts?: { direction?: 'in' | 'out' | 'both'; edgeType?: string; limit?: number },
  ): Promise<import('./types.ts').CodeEdgeResult[]> {
    const direction = opts?.direction ?? 'both';
    const limit = Math.min(opts?.limit ?? 50, 200);
    const edgeTypeClause = opts?.edgeType ? `AND edge_type = '${opts.edgeType.replace(/'/g, "''")}'` : '';
    // Build the chunk-table filter based on direction. Unresolved edges
    // (code_edges_symbol) only carry from_chunk_id — there's no inbound
    // direction into them from a chunk ID, so we include them only when
    // direction is 'out' or 'both'.
    let chunkFilter = '';
    if (direction === 'in') chunkFilter = `WHERE to_chunk_id = $1`;
    else if (direction === 'out') chunkFilter = `WHERE from_chunk_id = $1`;
    else chunkFilter = `WHERE from_chunk_id = $1 OR to_chunk_id = $1`;

    let symbolFilter = '';
    if (direction === 'out' || direction === 'both') {
      symbolFilter = `WHERE from_chunk_id = $1`;
    }

    const unionClause = symbolFilter ? `
      UNION ALL
      SELECT id, from_chunk_id, NULL as to_chunk_id, from_symbol_qualified, to_symbol_qualified,
             edge_type, edge_metadata, source_id, false as resolved
        FROM code_edges_symbol
        ${symbolFilter} ${edgeTypeClause}
    ` : '';

    const { rows } = await this.db.query(
      `SELECT id, from_chunk_id, to_chunk_id, from_symbol_qualified, to_symbol_qualified,
              edge_type, edge_metadata, source_id, true as resolved
         FROM code_edges_chunk
         ${chunkFilter} ${edgeTypeClause}
       ${unionClause}
       LIMIT $2`,
      [chunkId, limit],
    );
    return (rows as Record<string, unknown>[]).map(rowToCodeEdge);
  }
,
  logEvalCandidate: async function(this: PGLiteEngineLike, input: EvalCandidateInput): Promise<number> {
    const { rows } = await this.db.query<{ id: number }>(
      `INSERT INTO eval_candidates (
         tool_name, query, retrieved_slugs, retrieved_chunk_ids, source_ids,
         expand_enabled, detail, detail_resolved, vector_enabled, expansion_applied,
         latency_ms, remote, job_id, subagent_id, embedding_column
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
       RETURNING id`,
      [
        input.tool_name,
        input.query,
        input.retrieved_slugs,
        input.retrieved_chunk_ids,
        input.source_ids,
        input.expand_enabled,
        input.detail,
        input.detail_resolved,
        input.vector_enabled,
        input.expansion_applied,
        input.latency_ms,
        input.remote,
        input.job_id,
        input.subagent_id,
        input.embedding_column ?? null,
      ]
    );
    return rows[0]!.id;
  }
,
  listEvalCandidates: async function(this: PGLiteEngineLike, filter?: { since?: Date; limit?: number; tool?: 'query' | 'search' }): Promise<EvalCandidate[]> {
    const raw = filter?.limit;
    const limit = (raw === undefined || raw === null || !Number.isFinite(raw) || raw <= 0)
      ? 1000
      : Math.min(Math.floor(raw), 100000);
    const since = filter?.since ?? new Date(0);
    const tool = filter?.tool ?? null;
    // id DESC tiebreaker — see postgres-engine for rationale.
    const { rows } = tool
      ? await this.db.query(
          `SELECT * FROM eval_candidates
           WHERE created_at >= $1 AND tool_name = $2
           ORDER BY created_at DESC, id DESC LIMIT $3`,
          [since, tool, limit]
        )
      : await this.db.query(
          `SELECT * FROM eval_candidates
           WHERE created_at >= $1
           ORDER BY created_at DESC, id DESC LIMIT $2`,
          [since, limit]
        );
    return rows as unknown as EvalCandidate[];
  }
,
  deleteEvalCandidatesBefore: async function(this: PGLiteEngineLike, date: Date): Promise<number> {
    const { rows } = await this.db.query(
      `DELETE FROM eval_candidates WHERE created_at < $1 RETURNING id`,
      [date]
    );
    return rows.length;
  }
,
  logEvalCaptureFailure: async function(this: PGLiteEngineLike, reason: EvalCaptureFailureReason): Promise<void> {
    await this.db.query(
      `INSERT INTO eval_capture_failures (reason) VALUES ($1)`,
      [reason]
    );
  }
,
  listEvalCaptureFailures: async function(this: PGLiteEngineLike, filter?: { since?: Date }): Promise<EvalCaptureFailure[]> {
    const since = filter?.since ?? new Date(0);
    const { rows } = await this.db.query(
      `SELECT * FROM eval_capture_failures WHERE ts >= $1 ORDER BY ts DESC`,
      [since]
    );
    return rows as unknown as EvalCaptureFailure[];
  }
,
  batchLoadEmotionalInputs: async function(this: PGLiteEngineLike, slugs?: string[]): Promise<EmotionalWeightInputRow[]> {
    // Two CTEs avoid the N×M cartesian product (codex C4#4).
    const baseSql = `
      WITH page_tags AS (
        SELECT page_id, array_agg(DISTINCT tag) AS tags
          FROM tags GROUP BY page_id
      ),
      page_takes AS (
        SELECT page_id, json_agg(json_build_object(
                 'holder', holder, 'weight', weight, 'kind', kind, 'active', active
               )) AS takes
          FROM takes WHERE active = TRUE GROUP BY page_id
      )
      SELECT p.slug, p.source_id,
             COALESCE(pt.tags, ARRAY[]::text[]) AS tags,
             COALESCE(pk.takes, '[]'::json) AS takes
        FROM pages p
        LEFT JOIN page_tags pt  ON pt.page_id = p.id
        LEFT JOIN page_takes pk ON pk.page_id = p.id
    `;
    const { rows } = slugs
      ? await this.db.query(`${baseSql} WHERE p.slug = ANY($1::text[])`, [slugs])
      : await this.db.query(baseSql);
    return (rows as Record<string, unknown>[]).map(r => ({
      slug: String(r.slug),
      source_id: String(r.source_id),
      tags: (r.tags as string[]) ?? [],
      takes: (r.takes as EmotionalWeightInputRow['takes']) ?? [],
    }));
  }
,
  setEmotionalWeightBatch: async function(this: PGLiteEngineLike, rows: EmotionalWeightWriteRow[]): Promise<number> {
    if (rows.length === 0) return 0;
    const slugs = rows.map(r => r.slug);
    const sourceIds = rows.map(r => r.source_id);
    const weights = rows.map(r => r.weight);
    // Composite-keyed UPDATE FROM unnest (codex C4#3).
    // v0.29.1: bump salience_touched_at when emotional_weight actually changes
    // so the salience query window picks up newly-salient old pages. Mirror
    // of postgres-engine.ts.
    const result = await this.db.query(
      `UPDATE pages
          SET emotional_weight = u.weight,
              salience_touched_at = CASE
                WHEN pages.emotional_weight IS DISTINCT FROM u.weight THEN now()
                ELSE pages.salience_touched_at
              END
         FROM unnest($1::text[], $2::text[], $3::real[])
           AS u(slug, source_id, weight)
        WHERE pages.slug = u.slug AND pages.source_id = u.source_id
        RETURNING 1`,
      [slugs, sourceIds, weights]
    );
    return result.rows.length;
  }
,
  getRecentSalience: async function(this: PGLiteEngineLike, opts: SalienceOpts): Promise<SalienceResult[]> {
    const days = Math.max(0, opts.days ?? 14);
    const limit = clampSearchLimit(opts.limit, 20, 100);
    const slugPrefix = opts.slugPrefix;
    const boundaryIso = new Date(Date.now() - days * 86400000).toISOString();

    const params: unknown[] = [boundaryIso];
    let prefixCondition = '';
    if (slugPrefix) {
      const escaped = slugPrefix.replace(/[\\%_]/g, (c) => '\\' + c) + '%';
      params.push(escaped);
      prefixCondition = `AND p.slug LIKE $${params.length} ESCAPE '\\'`;
    }
    params.push(limit);
    const limitParam = `$${params.length}`;

    // v0.29.1: third score term via buildRecencyComponentSql. Default
    // 'flat' = v0.29.0 behavior. 'on' opts into per-prefix decay.
    const recencyBias = opts.recency_bias ?? 'flat';
    let recencySql: string;
    if (recencyBias === 'on') {
      const { resolveRecencyDecayMap, DEFAULT_FALLBACK } = await import('./search/recency-decay.ts');
      recencySql = buildRecencyComponentSql({
        slugColumn: 'p.slug',
        dateExpr: 'COALESCE(p.effective_date, p.updated_at)',
        decayMap: resolveRecencyDecayMap(),
        fallback: DEFAULT_FALLBACK,
      });
    } else {
      recencySql = buildRecencyComponentSql({
        slugColumn: 'p.slug',
        dateExpr: 'p.updated_at',
        decayMap: {},
        fallback: { halflifeDays: 1, coefficient: 1.0 },
      });
    }
    const { rows } = await this.db.query(
      `SELECT p.slug, p.source_id, p.title, p.type, p.updated_at, p.emotional_weight,
              COUNT(DISTINCT t.id) AS take_count,
              COALESCE(AVG(t.weight), 0) AS take_avg_weight,
              (p.emotional_weight * 5)
                + ln(1 + COUNT(DISTINCT t.id))
                + ${recencySql}
                AS score
         FROM pages p
         LEFT JOIN takes t ON t.page_id = p.id AND t.active = TRUE
        WHERE GREATEST(p.updated_at, COALESCE(p.salience_touched_at, p.updated_at)) >= $1::timestamptz
          ${prefixCondition}
        GROUP BY p.id
        ORDER BY score DESC
        LIMIT ${limitParam}`,
      params
    );
    return (rows as Record<string, unknown>[]).map(r => ({
      slug: String(r.slug),
      source_id: String(r.source_id),
      title: String(r.title ?? ''),
      type: r.type as SalienceResult['type'],
      updated_at: r.updated_at as Date,
      emotional_weight: Number(r.emotional_weight ?? 0),
      take_count: Number(r.take_count ?? 0),
      take_avg_weight: Number(r.take_avg_weight ?? 0),
      score: Number(r.score ?? 0),
    }));
  }
,
  findAnomalies: async function(this: PGLiteEngineLike, opts: AnomaliesOpts): Promise<AnomalyResult[]> {
    const sigma = opts.sigma ?? 3.0;
    const lookbackDays = Math.max(1, opts.lookback_days ?? 30);
    const sinceIso = (opts.since ?? new Date().toISOString().slice(0, 10));
    const sinceDate = new Date(sinceIso + 'T00:00:00Z');
    const sinceEnd = new Date(sinceDate.getTime() + 86400000);
    const baselineStart = new Date(sinceDate.getTime() - lookbackDays * 86400000);

    const tagBaselineRes = await this.db.query(
      `WITH days AS (
         SELECT day::date FROM generate_series(
           $1::date, $2::date - 1, '1 day'::interval
         ) AS day
       ),
       cohort_keys AS (
         SELECT DISTINCT t.tag FROM tags t JOIN pages p ON p.id = t.page_id
          WHERE p.updated_at >= $1::timestamptz AND p.updated_at < $2::timestamptz
       ),
       touched AS (
         SELECT t.tag,
                date_trunc('day', p.updated_at)::date AS day,
                COUNT(DISTINCT p.id) AS cnt
           FROM tags t JOIN pages p ON p.id = t.page_id
          WHERE p.updated_at >= $1::timestamptz AND p.updated_at < $2::timestamptz
          GROUP BY 1, 2
       )
       SELECT cd.tag AS cohort_value, d.day::text AS day, COALESCE(t.cnt, 0)::int AS count
         FROM cohort_keys cd CROSS JOIN days d
         LEFT JOIN touched t ON t.tag = cd.tag AND t.day = d.day`,
      [baselineStart.toISOString(), sinceDate.toISOString()]
    );

    const typeBaselineRes = await this.db.query(
      `WITH days AS (
         SELECT day::date FROM generate_series(
           $1::date, $2::date - 1, '1 day'::interval
         ) AS day
       ),
       cohort_keys AS (
         SELECT DISTINCT p.type FROM pages p
          WHERE p.updated_at >= $1::timestamptz AND p.updated_at < $2::timestamptz
       ),
       touched AS (
         SELECT p.type,
                date_trunc('day', p.updated_at)::date AS day,
                COUNT(DISTINCT p.id) AS cnt
           FROM pages p
          WHERE p.updated_at >= $1::timestamptz AND p.updated_at < $2::timestamptz
          GROUP BY 1, 2
       )
       SELECT cd.type AS cohort_value, d.day::text AS day, COALESCE(t.cnt, 0)::int AS count
         FROM cohort_keys cd CROSS JOIN days d
         LEFT JOIN touched t ON t.type = cd.type AND t.day = d.day`,
      [baselineStart.toISOString(), sinceDate.toISOString()]
    );

    const tagTodayRes = await this.db.query(
      `SELECT t.tag AS cohort_value,
              COUNT(DISTINCT p.id)::int AS count,
              array_agg(DISTINCT p.slug) AS slugs
         FROM tags t JOIN pages p ON p.id = t.page_id
        WHERE p.updated_at >= $1::timestamptz AND p.updated_at < $2::timestamptz
        GROUP BY 1`,
      [sinceIso, sinceEnd.toISOString()]
    );

    const typeTodayRes = await this.db.query(
      `SELECT p.type AS cohort_value,
              COUNT(DISTINCT p.id)::int AS count,
              array_agg(DISTINCT p.slug) AS slugs
         FROM pages p
        WHERE p.updated_at >= $1::timestamptz AND p.updated_at < $2::timestamptz
        GROUP BY 1`,
      [sinceIso, sinceEnd.toISOString()]
    );

    const baseline = [
      ...(tagBaselineRes.rows as Record<string, unknown>[]).map(r => ({
        cohort_kind: 'tag' as const,
        cohort_value: String(r.cohort_value),
        day: String(r.day),
        count: Number(r.count),
      })),
      ...(typeBaselineRes.rows as Record<string, unknown>[]).map(r => ({
        cohort_kind: 'type' as const,
        cohort_value: String(r.cohort_value),
        day: String(r.day),
        count: Number(r.count),
      })),
    ];
    const today = [
      ...(tagTodayRes.rows as Record<string, unknown>[]).map(r => ({
        cohort_kind: 'tag' as const,
        cohort_value: String(r.cohort_value),
        count: Number(r.count),
        page_slugs: (r.slugs as string[]) ?? [],
      })),
      ...(typeTodayRes.rows as Record<string, unknown>[]).map(r => ({
        cohort_kind: 'type' as const,
        cohort_value: String(r.cohort_value),
        count: Number(r.count),
        page_slugs: (r.slugs as string[]) ?? [],
      })),
    ];

    return computeAnomaliesFromBuckets(baseline, today, sigma);
  }

};


// Code-edge row mapping helper (moved from pglite-engine.ts)
function rowToCodeEdge(row: Record<string, unknown>): import('./types.ts').CodeEdgeResult {
  return {
    id: row.id as number,
    from_chunk_id: row.from_chunk_id as number,
    to_chunk_id: row.to_chunk_id == null ? null : (row.to_chunk_id as number),
    from_symbol_qualified: (row.from_symbol_qualified as string) ?? '',
    to_symbol_qualified: (row.to_symbol_qualified as string) ?? '',
    edge_type: (row.edge_type as string) ?? '',
    edge_metadata: (row.edge_metadata as Record<string, unknown>) ?? {},
    source_id: row.source_id == null ? null : (row.source_id as string),
    resolved: Boolean(row.resolved),
  };
}

