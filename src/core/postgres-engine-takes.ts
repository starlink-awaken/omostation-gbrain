/**
 * PostgresEngine takes methods — split out of postgres-engine.ts (BET-Y1Q3-T6-04).
 * Injected onto PostgresEngine.prototype via Object.assign.
 */
import { computeAnomaliesFromBuckets } from './cycle/anomaly.ts';
import { getConnection } from './db.ts';
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
  type SearchOpts,
} from './types.ts';
import { rowToChunk, takeRowToTake, tryParseEmbedding, validateSlug } from './utils.ts';
import postgres from 'postgres';


export interface PostgresEngineLike {
  sql: ReturnType<typeof postgres>;
  connectionManager: import('./connection-manager.ts').ConnectionManager | null;
  [key: string]: any;
}

export const postgresTakesMethods: Record<string, any> = {
  addTakesBatch: async function(this: PostgresEngineLike, rowsIn: TakeBatchInput[]): Promise<number> {
    if (rowsIn.length === 0) return 0;
    const sql = this.sql;
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
    // postgres-js needs boolean arrays passed as text[] then SQL-cast to boolean[],
    // otherwise the driver mis-detects element type. Same pattern as how the
    // existing batch methods handle bools.
    const actives   = rowsIn.map(r => (r.active ?? true) ? 'true' : 'false');
    if (weightClamped > 0) {
      process.stderr.write(`[takes] TAKES_WEIGHT_CLAMPED: ${weightClamped} row(s) had weight outside [0,1]; clamped\n`);
    }
    const result = await sql`
      INSERT INTO takes (page_id, row_num, claim, kind, holder, weight, since_date, until_date, source, superseded_by, active)
      SELECT v.page_id::int, v.row_num::int, v.claim, v.kind, v.holder, v.weight::real,
             v.since_date::text, v.until_date::text, v.source, v.superseded_by::int, v.active::boolean
      FROM unnest(
        ${pageIds}::int[], ${rowNums}::int[], ${claims}::text[], ${kinds}::text[],
        ${holders}::text[], ${weights}::real[], ${sinces}::text[], ${untils}::text[],
        ${sources}::text[], ${supersededBys}::int[], ${actives}::text[]::boolean[]
      ) AS v(page_id, row_num, claim, kind, holder, weight, since_date, until_date, source, superseded_by, active)
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
      RETURNING 1
    `;
    return result.length;
  }
,
  listActiveTakesForPages: async function(this: PostgresEngineLike, 
    pageIds: number[],
    opts: { takesHoldersAllowList?: string[] } = {},
  ): Promise<Map<number, Take[]>> {
    const out = new Map<number, Take[]>();
    for (const pid of pageIds) out.set(pid, []);
    if (pageIds.length === 0) return out;
    const sql = this.sql;
    const rows = await sql`
      SELECT t.*, p.slug AS page_slug
      FROM takes t
      JOIN pages p ON p.id = t.page_id
      WHERE t.page_id = ANY(${pageIds}::int[])
        AND t.active = true
        AND (
          ${opts.takesHoldersAllowList ?? null}::text[] IS NULL
          OR t.holder = ANY(${opts.takesHoldersAllowList ?? null}::text[])
        )
      ORDER BY t.page_id, t.row_num
    `;
    for (const r of rows) {
      const take = takeRowToTake(r as Record<string, unknown>);
      const bucket = out.get(take.page_id);
      if (bucket) bucket.push(take);
    }
    return out;
  }
,
  writeContradictionsRun: async function(this: PostgresEngineLike, row: {
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
    const sql = this.sql;
    const result = await sql`
      INSERT INTO eval_contradictions_runs (
        run_id, judge_model, prompt_version,
        queries_evaluated, queries_with_contradiction, total_contradictions_flagged,
        wilson_ci_lower, wilson_ci_upper, judge_errors_total,
        cost_usd_total, duration_ms,
        source_tier_breakdown, report_json
      ) VALUES (
        ${row.run_id}, ${row.judge_model}, ${row.prompt_version},
        ${row.queries_evaluated}, ${row.queries_with_contradiction}, ${row.total_contradictions_flagged},
        ${row.wilson_ci_lower}, ${row.wilson_ci_upper}, ${row.judge_errors_total},
        ${row.cost_usd_total}, ${row.duration_ms},
        ${sql.json(row.source_tier_breakdown as Parameters<typeof sql.json>[0])},
        ${sql.json(row.report_json as Parameters<typeof sql.json>[0])}
      )
      ON CONFLICT (run_id) DO NOTHING
    `;
    return result.count > 0;
  }
,
  loadContradictionsTrend: async function(this: PostgresEngineLike, days: number): Promise<Array<{
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
    const sql = this.sql;
    const cutoff = new Date(Date.now() - Math.max(0, days) * 86400000);
    const rows = await sql`
      SELECT run_id, ran_at, judge_model,
             queries_evaluated, queries_with_contradiction, total_contradictions_flagged,
             wilson_ci_lower, wilson_ci_upper, judge_errors_total,
             cost_usd_total, duration_ms,
             source_tier_breakdown, report_json
      FROM eval_contradictions_runs
      WHERE ran_at >= ${cutoff}
      ORDER BY ran_at DESC
    `;
    return rows.map((r) => ({
      run_id: r.run_id as string,
      ran_at: (r.ran_at instanceof Date ? r.ran_at.toISOString() : String(r.ran_at)),
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
  getContradictionCacheEntry: async function(this: PostgresEngineLike, key: {
    chunk_a_hash: string;
    chunk_b_hash: string;
    model_id: string;
    prompt_version: string;
    truncation_policy: string;
  }): Promise<Record<string, unknown> | null> {
    const sql = this.sql;
    const rows = await sql`
      SELECT verdict
      FROM eval_contradictions_cache
      WHERE chunk_a_hash = ${key.chunk_a_hash}
        AND chunk_b_hash = ${key.chunk_b_hash}
        AND model_id = ${key.model_id}
        AND prompt_version = ${key.prompt_version}
        AND truncation_policy = ${key.truncation_policy}
        AND expires_at > now()
      LIMIT 1
    `;
    if (rows.length === 0) return null;
    return rows[0].verdict as Record<string, unknown>;
  }
,
  putContradictionCacheEntry: async function(this: PostgresEngineLike, opts: {
    chunk_a_hash: string;
    chunk_b_hash: string;
    model_id: string;
    prompt_version: string;
    truncation_policy: string;
    verdict: Record<string, unknown>;
    ttl_seconds?: number;
  }): Promise<void> {
    const sql = this.sql;
    const ttl = Math.max(60, opts.ttl_seconds ?? 30 * 86400);
    const expiresAt = new Date(Date.now() + ttl * 1000);
    await sql`
      INSERT INTO eval_contradictions_cache (
        chunk_a_hash, chunk_b_hash, model_id, prompt_version, truncation_policy,
        verdict, expires_at
      ) VALUES (
        ${opts.chunk_a_hash}, ${opts.chunk_b_hash}, ${opts.model_id},
        ${opts.prompt_version}, ${opts.truncation_policy},
        ${sql.json(opts.verdict as Parameters<typeof sql.json>[0])}, ${expiresAt}
      )
      ON CONFLICT (chunk_a_hash, chunk_b_hash, model_id, prompt_version, truncation_policy)
      DO UPDATE SET
        verdict = EXCLUDED.verdict,
        expires_at = EXCLUDED.expires_at,
        created_at = now()
    `;
  }
,
  sweepContradictionCache: async function(this: PostgresEngineLike, ): Promise<number> {
    const sql = this.sql;
    const result = await sql`
      DELETE FROM eval_contradictions_cache WHERE expires_at <= now()
    `;
    return result.count ?? 0;
  }
,
  listTakes: async function(this: PostgresEngineLike, opts: TakesListOpts = {}): Promise<Take[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts.limit, 100, 500);
    const offset = Math.max(0, Math.floor(opts.offset ?? 0));
    const active = opts.active ?? true;
    const rows = await sql`
      SELECT t.*, p.slug AS page_slug
      FROM takes t
      JOIN pages p ON p.id = t.page_id
      WHERE 1=1
        AND (${opts.page_id ?? null}::int   IS NULL OR t.page_id = ${opts.page_id ?? null}::int)
        AND (${opts.page_slug ?? null}::text IS NULL OR p.slug   = ${opts.page_slug ?? null}::text)
        AND (${opts.holder ?? null}::text   IS NULL OR t.holder  = ${opts.holder ?? null}::text)
        AND (${opts.kind ?? null}::text     IS NULL OR t.kind    = ${opts.kind ?? null}::text)
        AND (${active}::boolean IS NULL OR t.active = ${active}::boolean)
        AND (
          ${opts.resolved === undefined ? null : opts.resolved}::boolean IS NULL
          OR (${opts.resolved === undefined ? null : opts.resolved}::boolean = true  AND t.resolved_at IS NOT NULL)
          OR (${opts.resolved === undefined ? null : opts.resolved}::boolean = false AND t.resolved_at IS NULL)
        )
        AND (
          ${opts.takesHoldersAllowList ?? null}::text[] IS NULL
          OR t.holder = ANY(${opts.takesHoldersAllowList ?? null}::text[])
        )
      ORDER BY
        CASE WHEN ${opts.sortBy ?? 'created_at'} = 'weight'      THEN t.weight     END DESC NULLS LAST,
        CASE WHEN ${opts.sortBy ?? 'created_at'} = 'since_date'  THEN t.since_date END DESC NULLS LAST,
        CASE WHEN ${opts.sortBy ?? 'created_at'} = 'created_at'  THEN t.created_at END DESC NULLS LAST
      LIMIT ${limit} OFFSET ${offset}
    `;
    return rows.map((r) => takeRowToTake(r as Record<string, unknown>));
  }
,
  searchTakes: async function(this: PostgresEngineLike, query: string, opts: SearchOpts & { takesHoldersAllowList?: string[] } = {}): Promise<TakeHit[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts.limit, 30, 100);
    const rows = await sql`
      SELECT t.id AS take_id, t.page_id, p.slug AS page_slug, t.row_num,
             t.claim, t.kind, t.holder, t.weight,
             similarity(t.claim, ${query})::real AS score
      FROM takes t
      JOIN pages p ON p.id = t.page_id
      WHERE t.active
        AND t.claim % ${query}
        AND (
          ${opts.takesHoldersAllowList ?? null}::text[] IS NULL
          OR t.holder = ANY(${opts.takesHoldersAllowList ?? null}::text[])
        )
      ORDER BY score DESC, t.weight DESC
      LIMIT ${limit}
    `;
    return rows as unknown as TakeHit[];
  }
,
  searchTakesVector: async function(this: PostgresEngineLike, 
    embedding: Float32Array,
    opts: SearchOpts & { takesHoldersAllowList?: string[] } = {},
  ): Promise<TakeHit[]> {
    const sql = this.sql;
    const limit = clampSearchLimit(opts.limit, 30, 100);
    const vec = `[${Array.from(embedding).join(',')}]`;
    const rows = await sql`
      SELECT t.id AS take_id, t.page_id, p.slug AS page_slug, t.row_num,
             t.claim, t.kind, t.holder, t.weight,
             (1 - (t.embedding <=> ${vec}::vector))::real AS score
      FROM takes t
      JOIN pages p ON p.id = t.page_id
      WHERE t.active
        AND t.embedding IS NOT NULL
        AND (
          ${opts.takesHoldersAllowList ?? null}::text[] IS NULL
          OR t.holder = ANY(${opts.takesHoldersAllowList ?? null}::text[])
        )
      ORDER BY t.embedding <=> ${vec}::vector
      LIMIT ${limit}
    `;
    return rows as unknown as TakeHit[];
  }
,
  getTakeEmbeddings: async function(this: PostgresEngineLike, ids: number[]): Promise<Map<number, Float32Array>> {
    if (ids.length === 0) return new Map();
    const sql = this.sql;
    const rows = await sql`
      SELECT id, embedding FROM takes WHERE id = ANY(${ids}::bigint[]) AND embedding IS NOT NULL
    `;
    const out = new Map<number, Float32Array>();
    for (const r of rows as unknown as Array<{ id: number; embedding: unknown }>) {
      const parsed = tryParseEmbedding(r.embedding);
      if (parsed) out.set(Number(r.id), parsed);
    }
    return out;
  }
,
  countStaleTakes: async function(this: PostgresEngineLike, ): Promise<number> {
    const sql = this.sql;
    const [row] = await sql`
      SELECT count(*)::int AS count FROM takes WHERE active AND embedding IS NULL
    `;
    return Number((row as { count?: number } | undefined)?.count ?? 0);
  }
,
  listStaleTakes: async function(this: PostgresEngineLike, ): Promise<StaleTakeRow[]> {
    const sql = this.sql;
    const rows = await sql`
      SELECT t.id AS take_id, p.slug AS page_slug, t.row_num, t.claim
      FROM takes t
      JOIN pages p ON p.id = t.page_id
      WHERE t.active AND t.embedding IS NULL
      ORDER BY t.id
      LIMIT 100000
    `;
    return rows as unknown as StaleTakeRow[];
  }
,
  updateTake: async function(this: PostgresEngineLike, 
    pageId: number,
    rowNum: number,
    fields: { weight?: number; since_date?: string; source?: string },
  ): Promise<void> {
    const sql = this.sql;
    let weight = fields.weight;
    if (weight !== undefined) {
      const norm = normalizeWeightForStorage(weight);
      if (norm.clamped) {
        process.stderr.write(`[takes] TAKES_WEIGHT_CLAMPED: updateTake clamped weight ${weight} → ${norm.weight}\n`);
      }
      weight = norm.weight;
    }
    const result = await sql`
      UPDATE takes SET
        weight     = COALESCE(${weight ?? null}::real, weight),
        since_date = COALESCE(${fields.since_date ?? null}::text, since_date),
        source     = COALESCE(${fields.source ?? null}::text, source),
        updated_at = now()
      WHERE page_id = ${pageId} AND row_num = ${rowNum}
      RETURNING 1
    `;
    if (result.length === 0) {
      throw new GBrainError('TAKE_ROW_NOT_FOUND', `take not found at page_id=${pageId} row=${rowNum}`, 'list takes for this page with `gbrain takes <slug>` to see valid row numbers');
    }
  }
,
  supersedeTake: async function(this: PostgresEngineLike, 
    pageId: number,
    oldRow: number,
    newRow: Omit<TakeBatchInput, 'page_id' | 'row_num' | 'superseded_by'>,
  ): Promise<{ oldRow: number; newRow: number }> {
    const conn = this._sql || getConnection();
    return await conn.begin(async (tx: ReturnType<typeof postgres>) => {
      const [existing] = await tx`
        SELECT resolved_at FROM takes WHERE page_id = ${pageId} AND row_num = ${oldRow}
      `;
      if (!existing) throw new GBrainError('TAKE_ROW_NOT_FOUND', `take not found at page_id=${pageId} row=${oldRow}`, 'list takes with `gbrain takes <slug>`');
      if ((existing as { resolved_at?: unknown }).resolved_at) {
        throw new GBrainError('TAKE_RESOLVED_IMMUTABLE', `take ${pageId}#${oldRow} is resolved`, 'resolved bets are immutable; add a new take instead');
      }
      const [maxRow] = await tx`SELECT COALESCE(MAX(row_num), 0) + 1 AS next FROM takes WHERE page_id = ${pageId}`;
      const newRowNum = Number((maxRow as { next?: number })?.next ?? 1);
      const wClamped = Math.max(0, Math.min(1, newRow.weight ?? 0.5));
      await tx`
        INSERT INTO takes (page_id, row_num, claim, kind, holder, weight, since_date, until_date, source, active)
        VALUES (${pageId}, ${newRowNum}, ${newRow.claim}, ${newRow.kind}, ${newRow.holder}, ${wClamped},
                ${newRow.since_date ?? null}::text, ${newRow.until_date ?? null}::text,
                ${newRow.source ?? null}, ${newRow.active ?? true})
      `;
      await tx`
        UPDATE takes SET active = false, superseded_by = ${newRowNum}, updated_at = now()
        WHERE page_id = ${pageId} AND row_num = ${oldRow}
      `;
      return { oldRow, newRow: newRowNum };
    }) as { oldRow: number; newRow: number };
  }
,
  resolveTake: async function(this: PostgresEngineLike, pageId: number, rowNum: number, resolution: TakeResolution): Promise<void> {
    const sql = this.sql;
    const [existing] = await sql`SELECT resolved_at FROM takes WHERE page_id = ${pageId} AND row_num = ${rowNum}`;
    if (!existing) throw new GBrainError('TAKE_ROW_NOT_FOUND', `take not found at page_id=${pageId} row=${rowNum}`, 'list takes for this page with `gbrain takes <slug>` to see valid row numbers');
    if ((existing as { resolved_at?: unknown }).resolved_at) {
      throw new GBrainError('TAKE_ALREADY_RESOLVED', `take ${pageId}#${rowNum} already resolved`, 'resolution is immutable; add a new take to record a new outcome');
    }
    // v0.30.0: derive (quality, outcome) tuple. quality wins when both set.
    // Schema CHECK enforces consistency as a defense-in-depth backstop.
    const { quality, outcome } = deriveResolutionTuple(resolution);
    await sql`
      UPDATE takes SET
        resolved_at      = now(),
        resolved_quality = ${quality}::text,
        resolved_outcome = ${outcome},
        resolved_value   = ${resolution.value ?? null}::real,
        resolved_unit    = ${resolution.unit ?? null}::text,
        resolved_source  = ${resolution.source ?? null}::text,
        resolved_by      = ${resolution.resolvedBy},
        updated_at       = now()
      WHERE page_id = ${pageId} AND row_num = ${rowNum}
    `;
  }
,
  getScorecard: async function(this: PostgresEngineLike, opts: TakesScorecardOpts, allowList: string[] | undefined): Promise<TakesScorecard> {
    const sql = this.sql;
    const allowed = allowList ? sql`AND holder = ANY(${allowList}::text[])` : sql``;
    const holderClause = opts.holder ? sql`AND holder = ${opts.holder}` : sql``;
    const domainClause = opts.domainPrefix
      ? sql`AND EXISTS (SELECT 1 FROM pages p WHERE p.id = takes.page_id AND p.slug LIKE ${opts.domainPrefix + '%'})`
      : sql``;
    const sinceClause = opts.since ? sql`AND since_date >= ${opts.since}` : sql``;
    const untilClause = opts.until ? sql`AND since_date <= ${opts.until}` : sql``;
    // v0.36.1.1 T1c: `resolved` deliberately filters to the 3-state subset
    // (correct|incorrect|partial) — NOT `resolved_quality IS NOT NULL` — so
    // historical comparisons against pre-v74 scorecards stay valid.
    // `unresolvable_count` is a sibling field counting the new 4th state.
    const rows = await sql`
      SELECT
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
      WHERE 1=1 ${holderClause} ${domainClause} ${sinceClause} ${untilClause} ${allowed}
    `;
    const r = rows[0] as { total_bets: number; resolved: number; correct: number; incorrect: number; partial: number; unresolvable_count: number; brier: number | null };
    return finalizeScorecard(r);
  }
,
  getCalibrationCurve: async function(this: PostgresEngineLike, opts: CalibrationCurveOpts, allowList: string[] | undefined): Promise<CalibrationBucket[]> {
    const sql = this.sql;
    const bucketSize = opts.bucketSize && opts.bucketSize > 0 && opts.bucketSize <= 1 ? opts.bucketSize : 0.1;
    const maxIdx = Math.floor(1 / bucketSize) - 1;
    const allowed = allowList ? sql`AND holder = ANY(${allowList}::text[])` : sql``;
    const holderClause = opts.holder ? sql`AND holder = ${opts.holder}` : sql``;
    // Bucketing uses NUMERIC for exact decimal arithmetic. Going through
    // FLOAT introduces IEEE 754 rounding (e.g. 0.7/0.1 = 6.9999..., FLOOR=6
    // instead of the expected 7), which makes Postgres and PGLite diverge
    // at bucket boundaries. NUMERIC is exact, so the bucket index is
    // engine-agnostic and the parity test holds.
    const rows = await sql`
      WITH binned AS (
        SELECT
          LEAST(FLOOR(weight::numeric / ${bucketSize}::numeric)::int, ${maxIdx}::int)::int AS bucket_idx,
          weight,
          (resolved_quality = 'correct')::int AS hit
        FROM takes
        WHERE resolved_quality IN ('correct','incorrect')
          ${holderClause} ${allowed}
      )
      SELECT
        (bucket_idx::numeric * ${bucketSize}::numeric)::float       AS bucket_lo,
        ((bucket_idx + 1)::numeric * ${bucketSize}::numeric)::float AS bucket_hi,
        COUNT(*)::int                                                AS n,
        AVG(hit)::float                                              AS observed,
        AVG(weight)::float                                           AS predicted
      FROM binned
      GROUP BY bucket_idx
      ORDER BY bucket_idx
    `;
    return (rows as unknown as { bucket_lo: number; bucket_hi: number; n: number; observed: number | null; predicted: number | null }[]).map(r => ({
      bucket_lo: r.bucket_lo,
      bucket_hi: r.bucket_hi,
      n: r.n,
      observed: r.n > 0 ? r.observed : null,
      predicted: r.n > 0 ? r.predicted : null,
    }));
  }
,
  addSynthesisEvidence: async function(this: PostgresEngineLike, rowsIn: SynthesisEvidenceInput[]): Promise<number> {
    if (rowsIn.length === 0) return 0;
    const sql = this.sql;
    const synthesisIds = rowsIn.map(r => r.synthesis_page_id);
    const takePageIds  = rowsIn.map(r => r.take_page_id);
    const takeRowNums  = rowsIn.map(r => r.take_row_num);
    const citationIxs  = rowsIn.map(r => r.citation_index);
    const result = await sql`
      INSERT INTO synthesis_evidence (synthesis_page_id, take_page_id, take_row_num, citation_index)
      SELECT v.synthesis_page_id::int, v.take_page_id::int, v.take_row_num::int, v.citation_index::int
      FROM unnest(
        ${synthesisIds}::int[], ${takePageIds}::int[], ${takeRowNums}::int[], ${citationIxs}::int[]
      ) AS v(synthesis_page_id, take_page_id, take_row_num, citation_index)
      ON CONFLICT (synthesis_page_id, take_page_id, take_row_num) DO NOTHING
      RETURNING 1
    `;
    return result.length;
  }
,
  createVersion: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<PageVersion> {
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';
    const rows = await sql`
      INSERT INTO page_versions (page_id, compiled_truth, frontmatter)
      SELECT id, compiled_truth, frontmatter
      FROM pages WHERE slug = ${slug} AND source_id = ${sourceId}
      RETURNING *
    `;
    if (rows.length === 0) throw new Error(`createVersion failed: page "${slug}" (source=${sourceId}) not found`);
    return rows[0] as unknown as PageVersion;
  }
,
  getVersions: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<PageVersion[]> {
    const sql = this.sql;
    // v0.31.8 (D16): two-branch.
    if (opts?.sourceId) {
      const rows = await sql`
        SELECT pv.* FROM page_versions pv
        JOIN pages p ON p.id = pv.page_id
        WHERE p.slug = ${slug} AND p.source_id = ${opts.sourceId}
        ORDER BY pv.snapshot_at DESC
      `;
      return rows as unknown as PageVersion[];
    }
    const rows = await sql`
      SELECT pv.* FROM page_versions pv
      JOIN pages p ON p.id = pv.page_id
      WHERE p.slug = ${slug}
      ORDER BY pv.snapshot_at DESC
    `;
    return rows as unknown as PageVersion[];
  }
,
  revertToVersion: async function(this: PostgresEngineLike, 
    slug: string,
    versionId: number,
    opts?: { sourceId?: string },
  ): Promise<void> {
    const sql = this.sql;
    // v0.31.8 (D12): two-branch. With opts.sourceId, scope BOTH the page lookup
    // AND the version reference. Without it, multi-source brains can revert
    // the wrong same-slug page.
    if (opts?.sourceId) {
      await sql`
        UPDATE pages SET
          compiled_truth = pv.compiled_truth,
          frontmatter = pv.frontmatter,
          updated_at = now()
        FROM page_versions pv
        WHERE pages.slug = ${slug} AND pages.source_id = ${opts.sourceId}
              AND pv.id = ${versionId} AND pv.page_id = pages.id
      `;
      return;
    }
    await sql`
      UPDATE pages SET
        compiled_truth = pv.compiled_truth,
        frontmatter = pv.frontmatter,
        updated_at = now()
      FROM page_versions pv
      WHERE pages.slug = ${slug} AND pv.id = ${versionId} AND pv.page_id = pages.id
    `;
  }
,
  getStats: async function(this: PostgresEngineLike, ): Promise<BrainStats> {
    const sql = this.sql;
    const [stats] = await sql`
      SELECT
        -- v0.26.5: exclude soft-deleted from page_count. Same posture as the
        -- search filter and getPage default — soft-deleted is hidden everywhere
        -- the user looks. Chunks/links stay raw because they still occupy
        -- storage until the autopilot purge phase runs.
        (SELECT count(*) FROM pages WHERE deleted_at IS NULL) as page_count,
        (SELECT count(*) FROM content_chunks) as chunk_count,
        (SELECT count(*) FROM content_chunks WHERE embedded_at IS NOT NULL) as embedded_count,
        (SELECT count(*) FROM links) as link_count,
        (SELECT count(DISTINCT tag) FROM tags) as tag_count,
        (SELECT count(*) FROM timeline_entries) as timeline_entry_count
    `;

    const types = await sql`
      SELECT type, count(*)::int as count FROM pages GROUP BY type ORDER BY count DESC
    `;
    const pages_by_type: Record<string, number> = {};
    for (const t of types) {
      pages_by_type[t.type as string] = t.count as number;
    }

    return {
      page_count: Number(stats.page_count),
      chunk_count: Number(stats.chunk_count),
      embedded_count: Number(stats.embedded_count),
      link_count: Number(stats.link_count),
      tag_count: Number(stats.tag_count),
      timeline_entry_count: Number(stats.timeline_entry_count),
      pages_by_type,
    };
  }
,
  getHealth: async function(this: PostgresEngineLike, ): Promise<BrainHealth> {
    const sql = this.sql;
    // Bug 11 doc-drift fix — orphan_pages means "islanded" (no inbound AND
    // no outbound links), aligning both engines with the user-facing
    // definition. The type comment previously said "no inbound" but the
    // SQL required both — docs now match code so users can trust the
    // number. A hub page that links out to many but has no back-references
    // is working as intended, not an orphan.
    const [h] = await sql`
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
    `;

    const connected = await sql`
      SELECT p.slug,
             (SELECT count(*) FROM links l WHERE l.from_page_id = p.id OR l.to_page_id = p.id)::int as link_count
      FROM pages p
      WHERE p.type IN ('person', 'company')
      ORDER BY link_count DESC
      LIMIT 5
    `;

    const pageCount = Number(h.page_count);
    const embedCoverage = Number(h.embed_coverage);
    const orphanPages = Number(h.orphan_pages);
    const deadLinks = Number(h.dead_links);
    const linkCount = Number(h.link_count);
    const pagesWithTimeline = Number(h.pages_with_timeline);

    // brain_score: 0-100 weighted average
    const linkDensity = pageCount > 0 ? Math.min(linkCount / pageCount, 1) : 0;
    const timelineCoverageWhole = pageCount > 0 ? Math.min(pagesWithTimeline / pageCount, 1) : 0;
    const noOrphans = pageCount > 0 ? 1 - (orphanPages / pageCount) : 1;
    const noDeadLinks = pageCount > 0 ? 1 - Math.min(deadLinks / pageCount, 1) : 1;
    // Per-component points. Sum equals brainScore by construction.
    //
    // v0.37.10.0: empty brains (pageCount === 0) get FULL marks (100/100),
    // not 0. Semantically an empty brain has no coverage problem to penalize
    // — there's nothing to embed, nothing to link, nothing to orphan. The
    // pre-fix "empty = 0" caused fresh-init brains to score as critically
    // unhealthy on `gbrain doctor`, which was a structural surprise to users
    // who'd just successfully run init. PGLite path has the same fix.
    const embedCoverageScore = pageCount === 0 ? 35 : Math.round(embedCoverage * 35);
    const linkDensityScore = pageCount === 0 ? 25 : Math.round(linkDensity * 25);
    const timelineCoverageScore = pageCount === 0 ? 15 : Math.round(timelineCoverageWhole * 15);
    const noOrphansScore = pageCount === 0 ? 15 : Math.round(noOrphans * 15);
    const noDeadLinksScore = pageCount === 0 ? 10 : Math.round(noDeadLinks * 10);
    const brainScore = embedCoverageScore + linkDensityScore + timelineCoverageScore + noOrphansScore + noDeadLinksScore;

    return {
      page_count: pageCount,
      embed_coverage: embedCoverage,
      stale_pages: Number(h.stale_pages),
      orphan_pages: orphanPages,
      missing_embeddings: Number(h.missing_embeddings),
      brain_score: brainScore,
      dead_links: deadLinks,
      link_coverage: Number(h.link_coverage),
      timeline_coverage: Number(h.timeline_coverage),
      most_connected: (connected as unknown as { slug: string; link_count: number }[]).map(c => ({
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
  logIngest: async function(this: PostgresEngineLike, entry: IngestLogInput): Promise<void> {
    const sql = this.sql;
    // v0.31.2 (codex P1 #3): source_id threaded so multi-source brains can
    // scope ingest_log queries. Default 'default' matches the column DEFAULT.
    const sourceId = entry.source_id ?? 'default';
    await sql`
      INSERT INTO ingest_log (source_id, source_type, source_ref, pages_updated, summary)
      VALUES (${sourceId}, ${entry.source_type}, ${entry.source_ref}, ${sql.json(entry.pages_updated)}, ${entry.summary})
    `;
  }
,
  getIngestLog: async function(this: PostgresEngineLike, opts?: { limit?: number }): Promise<IngestLogEntry[]> {
    const sql = this.sql;
    const limit = opts?.limit || 50;
    const rows = await sql`
      SELECT * FROM ingest_log ORDER BY created_at DESC LIMIT ${limit}
    `;
    // Belt-and-suspenders source_id fallback for any pre-v50 row.
    return (rows as unknown as IngestLogEntry[]).map(r => ({
      ...r,
      source_id: r.source_id ?? 'default',
    }));
  }
,
  updateSlug: async function(this: PostgresEngineLike, oldSlug: string, newSlug: string, opts?: { sourceId?: string }): Promise<void> {
    newSlug = validateSlug(newSlug);
    const sql = this.sql;
    const sourceId = opts?.sourceId ?? 'default';
    // Source-qualify so a rename in source A doesn't sweep up same-slug rows
    // in sources B/C/D (which would either rename them all OR fail the
    // (source_id, slug) UNIQUE if the new slug already exists in another source).
    await sql`UPDATE pages SET slug = ${newSlug}, updated_at = now() WHERE slug = ${oldSlug} AND source_id = ${sourceId}`;
  }
,
  rewriteLinks: async function(this: PostgresEngineLike, _oldSlug: string, _newSlug: string): Promise<void> {
    // Stub in v0.2. Links table uses integer page_id FKs, which are already
    // correct after updateSlug (page_id doesn't change, only slug does).
    // Textual [[wiki-links]] in compiled_truth are NOT rewritten here.
    // The maintain skill's dead link detector surfaces stale references.
  }
,
  getConfig: async function(this: PostgresEngineLike, key: string): Promise<string | null> {
    const sql = this.sql;
    const rows = await sql`SELECT value FROM config WHERE key = ${key}`;
    return rows.length > 0 ? (rows[0].value as string) : null;
  }
,
  setConfig: async function(this: PostgresEngineLike, key: string, value: string): Promise<void> {
    const sql = this.sql;
    await sql`
      INSERT INTO config (key, value) VALUES (${key}, ${value})
      ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    `;
  }
,
  unsetConfig: async function(this: PostgresEngineLike, key: string): Promise<number> {
    const sql = this.sql;
    const result = await sql`DELETE FROM config WHERE key = ${key}` as unknown as { count: number };
    return result.count ?? 0;
  }
,
  listConfigKeys: async function(this: PostgresEngineLike, prefix: string): Promise<string[]> {
    const sql = this.sql;
    // LIKE-escape literal % and _ so a config key with those chars resolves correctly.
    const escaped = prefix.replace(/\\/g, '\\\\').replace(/%/g, '\\%').replace(/_/g, '\\_');
    const pattern = `${escaped}%`;
    const rows = await sql<{ key: string }[]>`
      SELECT key FROM config WHERE key LIKE ${pattern} ESCAPE '\\' ORDER BY key
    `;
    return rows.map(r => r.key);
  }
,
  runMigration: async function(this: PostgresEngineLike, _version: number, sqlStr: string): Promise<void> {
    const conn = this.sql;
    await conn.unsafe(sqlStr);
  }
,
  getChunksWithEmbeddings: async function(this: PostgresEngineLike, slug: string, opts?: { sourceId?: string }): Promise<Chunk[]> {
    const conn = this.sql;
    const sourceId = opts?.sourceId;
    const rows = sourceId
      ? await conn`
          SELECT cc.* FROM content_chunks cc
          JOIN pages p ON p.id = cc.page_id
          WHERE p.slug = ${slug} AND p.source_id = ${sourceId}
          ORDER BY cc.chunk_index
        `
      : await conn`
          SELECT cc.* FROM content_chunks cc
          JOIN pages p ON p.id = cc.page_id
          WHERE p.slug = ${slug}
          ORDER BY cc.chunk_index
        `;
    return rows.map((r) => rowToChunk(r as Record<string, unknown>, true));
  }
,
  reconnect: async function(this: PostgresEngineLike, ): Promise<void> {
    if (!this._savedConfig || this._reconnecting) return;
    this._reconnecting = true;
    try {
      // Tear down old pool (best-effort — it may already be dead)
      try { await this.disconnect(); } catch { /* swallow */ }
      // Create fresh pool
      await this.connect(this._savedConfig);
    } finally {
      this._reconnecting = false;
    }
  }
,
  addCodeEdges: async function(this: PostgresEngineLike, edges: import('./types.ts').CodeEdgeInput[]): Promise<number> {
    if (edges.length === 0) return 0;
    const sql = this.sql;
    let inserted = 0;
    const resolved = edges.filter(e => e.to_chunk_id != null);
    const unresolved = edges.filter(e => e.to_chunk_id == null);

    if (resolved.length > 0) {
      const fromIds = resolved.map(e => e.from_chunk_id);
      const toIds = resolved.map(e => e.to_chunk_id as number);
      const fromQual = resolved.map(e => e.from_symbol_qualified);
      const toQual = resolved.map(e => e.to_symbol_qualified);
      const edgeTypes = resolved.map(e => e.edge_type);
      const metas = resolved.map(e => JSON.stringify(e.edge_metadata ?? {}));
      const sources = resolved.map(e => e.source_id ?? null);
      const res = await sql`
        INSERT INTO code_edges_chunk (from_chunk_id, to_chunk_id, from_symbol_qualified, to_symbol_qualified, edge_type, edge_metadata, source_id)
        SELECT * FROM unnest(
          ${fromIds}::int[], ${toIds}::int[],
          ${fromQual}::text[], ${toQual}::text[],
          ${edgeTypes}::text[], ${metas}::jsonb[],
          ${sources}::text[]
        )
        ON CONFLICT (from_chunk_id, to_chunk_id, edge_type) DO NOTHING
      `;
      inserted += (res as unknown as { count: number }).count ?? 0;
    }

    if (unresolved.length > 0) {
      const fromIds = unresolved.map(e => e.from_chunk_id);
      const fromQual = unresolved.map(e => e.from_symbol_qualified);
      const toQual = unresolved.map(e => e.to_symbol_qualified);
      const edgeTypes = unresolved.map(e => e.edge_type);
      const metas = unresolved.map(e => JSON.stringify(e.edge_metadata ?? {}));
      const sources = unresolved.map(e => e.source_id ?? null);
      const res = await sql`
        INSERT INTO code_edges_symbol (from_chunk_id, from_symbol_qualified, to_symbol_qualified, edge_type, edge_metadata, source_id)
        SELECT * FROM unnest(
          ${fromIds}::int[],
          ${fromQual}::text[], ${toQual}::text[],
          ${edgeTypes}::text[], ${metas}::jsonb[],
          ${sources}::text[]
        )
        ON CONFLICT (from_chunk_id, to_symbol_qualified, edge_type) DO NOTHING
      `;
      inserted += (res as unknown as { count: number }).count ?? 0;
    }

    return inserted;
  }
,
  deleteCodeEdgesForChunks: async function(this: PostgresEngineLike, chunkIds: number[]): Promise<void> {
    if (chunkIds.length === 0) return;
    const sql = this.sql;
    await sql`DELETE FROM code_edges_chunk WHERE from_chunk_id = ANY(${chunkIds}::int[]) OR to_chunk_id = ANY(${chunkIds}::int[])`;
    await sql`DELETE FROM code_edges_symbol WHERE from_chunk_id = ANY(${chunkIds}::int[])`;
  }
,
  getCallersOf: async function(this: PostgresEngineLike, 
    qualifiedName: string,
    opts?: { sourceId?: string; allSources?: boolean; limit?: number },
  ): Promise<import('./types.ts').CodeEdgeResult[]> {
    const sql = this.sql;
    const limit = Math.min(opts?.limit ?? 100, 500);
    const scopedSource: string | null =
      !opts?.allSources && opts?.sourceId ? opts.sourceId : null;
    const rows = await sql`
      SELECT id, from_chunk_id, to_chunk_id, from_symbol_qualified, to_symbol_qualified,
             edge_type, edge_metadata, source_id, true as resolved
        FROM code_edges_chunk
        WHERE to_symbol_qualified = ${qualifiedName}
        ${scopedSource ? sql`AND source_id = ${scopedSource}` : sql``}
      UNION ALL
      SELECT id, from_chunk_id, NULL::int as to_chunk_id, from_symbol_qualified, to_symbol_qualified,
             edge_type, edge_metadata, source_id, false as resolved
        FROM code_edges_symbol
        WHERE to_symbol_qualified = ${qualifiedName}
        ${scopedSource ? sql`AND source_id = ${scopedSource}` : sql``}
      LIMIT ${limit}
    `;
    return rows.map(r => pgRowToCodeEdge(r as Record<string, unknown>));
  }
,
  getCalleesOf: async function(this: PostgresEngineLike, 
    qualifiedName: string,
    opts?: { sourceId?: string; allSources?: boolean; limit?: number },
  ): Promise<import('./types.ts').CodeEdgeResult[]> {
    const sql = this.sql;
    const limit = Math.min(opts?.limit ?? 100, 500);
    const scopedSource: string | null =
      !opts?.allSources && opts?.sourceId ? opts.sourceId : null;
    const rows = await sql`
      SELECT id, from_chunk_id, to_chunk_id, from_symbol_qualified, to_symbol_qualified,
             edge_type, edge_metadata, source_id, true as resolved
        FROM code_edges_chunk
        WHERE from_symbol_qualified = ${qualifiedName}
        ${scopedSource ? sql`AND source_id = ${scopedSource}` : sql``}
      UNION ALL
      SELECT id, from_chunk_id, NULL::int as to_chunk_id, from_symbol_qualified, to_symbol_qualified,
             edge_type, edge_metadata, source_id, false as resolved
        FROM code_edges_symbol
        WHERE from_symbol_qualified = ${qualifiedName}
        ${scopedSource ? sql`AND source_id = ${scopedSource}` : sql``}
      LIMIT ${limit}
    `;
    return rows.map(r => pgRowToCodeEdge(r as Record<string, unknown>));
  }
,
  getEdgesByChunk: async function(this: PostgresEngineLike, 
    chunkId: number,
    opts?: { direction?: 'in' | 'out' | 'both'; edgeType?: string; limit?: number },
  ): Promise<import('./types.ts').CodeEdgeResult[]> {
    const sql = this.sql;
    const direction = opts?.direction ?? 'both';
    const limit = Math.min(opts?.limit ?? 50, 200);
    const typeFilter = opts?.edgeType;

    const chunkRows = await sql`
      SELECT id, from_chunk_id, to_chunk_id, from_symbol_qualified, to_symbol_qualified,
             edge_type, edge_metadata, source_id, true as resolved
        FROM code_edges_chunk
        WHERE
          ${direction === 'in' ? sql`to_chunk_id = ${chunkId}`
            : direction === 'out' ? sql`from_chunk_id = ${chunkId}`
            : sql`(from_chunk_id = ${chunkId} OR to_chunk_id = ${chunkId})`}
          ${typeFilter ? sql`AND edge_type = ${typeFilter}` : sql``}
        LIMIT ${limit}
    `;
    let symbolRows: unknown[] = [];
    if (direction !== 'in') {
      const sRows = await sql`
        SELECT id, from_chunk_id, NULL::int as to_chunk_id, from_symbol_qualified, to_symbol_qualified,
               edge_type, edge_metadata, source_id, false as resolved
          FROM code_edges_symbol
          WHERE from_chunk_id = ${chunkId}
            ${typeFilter ? sql`AND edge_type = ${typeFilter}` : sql``}
          LIMIT ${limit}
      `;
      symbolRows = [...sRows];
    }
    return [...chunkRows, ...symbolRows].map(r => pgRowToCodeEdge(r as Record<string, unknown>));
  }
,
  logEvalCandidate: async function(this: PostgresEngineLike, input: EvalCandidateInput): Promise<number> {
    const sql = this.sql;
    const rows = await sql`
      INSERT INTO eval_candidates (
        tool_name, query, retrieved_slugs, retrieved_chunk_ids, source_ids,
        expand_enabled, detail, detail_resolved, vector_enabled, expansion_applied,
        latency_ms, remote, job_id, subagent_id, embedding_column
      ) VALUES (
        ${input.tool_name}, ${input.query}, ${input.retrieved_slugs}, ${input.retrieved_chunk_ids}, ${input.source_ids},
        ${input.expand_enabled}, ${input.detail}, ${input.detail_resolved}, ${input.vector_enabled}, ${input.expansion_applied},
        ${input.latency_ms}, ${input.remote}, ${input.job_id}, ${input.subagent_id}, ${input.embedding_column ?? null}
      )
      RETURNING id
    `;
    return rows[0]!.id as number;
  }
,
  listEvalCandidates: async function(this: PostgresEngineLike, filter?: { since?: Date; limit?: number; tool?: 'query' | 'search' }): Promise<EvalCandidate[]> {
    const sql = this.sql;
    const raw = filter?.limit;
    const limit = (raw === undefined || raw === null || !Number.isFinite(raw) || raw <= 0)
      ? 1000
      : Math.min(Math.floor(raw), 100000);
    const since = filter?.since ?? new Date(0);
    const tool = filter?.tool ?? null;
    // id DESC tiebreaker so same-millisecond inserts return deterministically
    // — without this, `gbrain eval export --since` could dupe or miss rows
    // across non-overlapping windows.
    const rows = tool
      ? await sql`
          SELECT * FROM eval_candidates
          WHERE created_at >= ${since} AND tool_name = ${tool}
          ORDER BY created_at DESC, id DESC
          LIMIT ${limit}
        `
      : await sql`
          SELECT * FROM eval_candidates
          WHERE created_at >= ${since}
          ORDER BY created_at DESC, id DESC
          LIMIT ${limit}
        `;
    return rows as unknown as EvalCandidate[];
  }
,
  deleteEvalCandidatesBefore: async function(this: PostgresEngineLike, date: Date): Promise<number> {
    const sql = this.sql;
    const rows = await sql`
      DELETE FROM eval_candidates WHERE created_at < ${date} RETURNING id
    `;
    return rows.length;
  }
,
  logEvalCaptureFailure: async function(this: PostgresEngineLike, reason: EvalCaptureFailureReason): Promise<void> {
    const sql = this.sql;
    await sql`INSERT INTO eval_capture_failures (reason) VALUES (${reason})`;
  }
,
  listEvalCaptureFailures: async function(this: PostgresEngineLike, filter?: { since?: Date }): Promise<EvalCaptureFailure[]> {
    const sql = this.sql;
    const since = filter?.since ?? new Date(0);
    const rows = await sql`
      SELECT * FROM eval_capture_failures
      WHERE ts >= ${since}
      ORDER BY ts DESC
    `;
    return rows as unknown as EvalCaptureFailure[];
  }
,
  batchLoadEmotionalInputs: async function(this: PostgresEngineLike, slugs?: string[]): Promise<EmotionalWeightInputRow[]> {
    const sql = this.sql;
    // Two CTEs avoid the N×M cartesian product (codex C4#4): a page with N tags
    // and M takes joined directly would emit N×M rows and corrupt aggregates.
    // Per-table aggregation keeps each table's grouping correct.
    const rows = slugs
      ? await sql`
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
           WHERE p.slug = ANY(${slugs}::text[])
        `
      : await sql`
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
    return rows.map((r: Record<string, unknown>) => ({
      slug: String(r.slug),
      source_id: String(r.source_id),
      tags: (r.tags as string[]) ?? [],
      takes: (r.takes as EmotionalWeightInputRow['takes']) ?? [],
    }));
  }
,
  setEmotionalWeightBatch: async function(this: PostgresEngineLike, rows: EmotionalWeightWriteRow[]): Promise<number> {
    if (rows.length === 0) return 0;
    const sql = this.sql;
    const slugs = rows.map(r => r.slug);
    const sourceIds = rows.map(r => r.source_id);
    const weights = rows.map(r => r.weight);
    // Composite-keyed UPDATE FROM unnest (codex C4#3): pages.slug is unique
    // only within a source, so a slug-only join would fan out across sources.
    //
    // v0.29.1: bump salience_touched_at to NOW() ONLY when emotional_weight
    // actually changes. The salience query window then includes the page in
    // GREATEST(updated_at, salience_touched_at) >= boundary, so a previously
    // calm page that just became salient surfaces in the recent salience
    // results without a content edit. No-op writes (same weight) leave
    // salience_touched_at alone — preserves "actual change" semantics.
    const result = await sql`
      UPDATE pages
         SET emotional_weight = u.weight,
             salience_touched_at = CASE
               WHEN pages.emotional_weight IS DISTINCT FROM u.weight THEN now()
               ELSE pages.salience_touched_at
             END
        FROM unnest(${slugs}::text[], ${sourceIds}::text[], ${weights}::real[])
          AS u(slug, source_id, weight)
       WHERE pages.slug = u.slug AND pages.source_id = u.source_id
      RETURNING 1
    `;
    return result.length;
  }
,
  getRecentSalience: async function(this: PostgresEngineLike, opts: SalienceOpts): Promise<SalienceResult[]> {
    const sql = this.sql;
    const days = Math.max(0, opts.days ?? 14);
    const limit = clampSearchLimit(opts.limit, 20, 100);
    const slugPrefix = opts.slugPrefix;
    // Compute the boundary in JS so the SQL is identical across engines (eng review D5).
    const boundaryIso = new Date(Date.now() - days * 86400000).toISOString();
    // Escape LIKE meta for the optional prefix match.
    const prefixCondition = slugPrefix
      ? sql`AND p.slug LIKE ${slugPrefix.replace(/[\\%_]/g, (c) => '\\' + c) + '%'} ESCAPE '\\'`
      : sql``;
    // v0.29.1: third score term via buildRecencyComponentSql. Default
    // 'flat' = v0.29.0 behavior (1 / (1 + days_old)). 'on' opts into the
    // per-prefix decay map (concepts/ evergreen, daily/ aggressive, etc.).
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
    const rows = await sql`
      SELECT p.slug, p.source_id, p.title, p.type, p.updated_at, p.emotional_weight,
             COUNT(DISTINCT t.id) AS take_count,
             COALESCE(AVG(t.weight), 0) AS take_avg_weight,
             (p.emotional_weight * 5)
               + ln(1 + COUNT(DISTINCT t.id))
               + ${sql.unsafe(recencySql)}
               AS score
        FROM pages p
        LEFT JOIN takes t ON t.page_id = p.id AND t.active = TRUE
       WHERE GREATEST(p.updated_at, COALESCE(p.salience_touched_at, p.updated_at)) >= ${boundaryIso}::timestamptz
         ${prefixCondition}
       GROUP BY p.id
       ORDER BY score DESC
       LIMIT ${limit}
    `;
    return rows.map((r: Record<string, unknown>) => ({
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
  findAnomalies: async function(this: PostgresEngineLike, opts: AnomaliesOpts): Promise<AnomalyResult[]> {
    const sql = this.sql;
    const sigma = opts.sigma ?? 3.0;
    const lookbackDays = Math.max(1, opts.lookback_days ?? 30);
    // Boundaries: today's window is [since, since+1day); baseline is [since-lookback, since).
    const sinceIso = (opts.since ?? new Date().toISOString().slice(0, 10)); // YYYY-MM-DD
    const sinceDate = new Date(sinceIso + 'T00:00:00Z');
    const sinceEnd = new Date(sinceDate.getTime() + 86400000);
    const baselineStart = new Date(sinceDate.getTime() - lookbackDays * 86400000);

    // Tag cohort baseline with day densification + zero-fill (codex C4#6).
    const tagBaseline = await sql`
      WITH days AS (
        SELECT day::date FROM generate_series(
          ${baselineStart.toISOString()}::date,
          ${sinceDate.toISOString()}::date - 1,
          '1 day'::interval
        ) AS day
      ),
      cohort_keys AS (
        SELECT DISTINCT t.tag FROM tags t JOIN pages p ON p.id = t.page_id
         WHERE p.updated_at >= ${baselineStart.toISOString()}::timestamptz
           AND p.updated_at <  ${sinceDate.toISOString()}::timestamptz
      ),
      touched AS (
        SELECT t.tag,
               date_trunc('day', p.updated_at)::date AS day,
               COUNT(DISTINCT p.id) AS cnt
          FROM tags t JOIN pages p ON p.id = t.page_id
         WHERE p.updated_at >= ${baselineStart.toISOString()}::timestamptz
           AND p.updated_at <  ${sinceDate.toISOString()}::timestamptz
         GROUP BY 1, 2
      )
      SELECT cd.tag AS cohort_value, d.day::text AS day, COALESCE(t.cnt, 0)::int AS count
        FROM cohort_keys cd CROSS JOIN days d
        LEFT JOIN touched t ON t.tag = cd.tag AND t.day = d.day
    `;

    const typeBaseline = await sql`
      WITH days AS (
        SELECT day::date FROM generate_series(
          ${baselineStart.toISOString()}::date,
          ${sinceDate.toISOString()}::date - 1,
          '1 day'::interval
        ) AS day
      ),
      cohort_keys AS (
        SELECT DISTINCT p.type FROM pages p
         WHERE p.updated_at >= ${baselineStart.toISOString()}::timestamptz
           AND p.updated_at <  ${sinceDate.toISOString()}::timestamptz
      ),
      touched AS (
        SELECT p.type,
               date_trunc('day', p.updated_at)::date AS day,
               COUNT(DISTINCT p.id) AS cnt
          FROM pages p
         WHERE p.updated_at >= ${baselineStart.toISOString()}::timestamptz
           AND p.updated_at <  ${sinceDate.toISOString()}::timestamptz
         GROUP BY 1, 2
      )
      SELECT cd.type AS cohort_value, d.day::text AS day, COALESCE(t.cnt, 0)::int AS count
        FROM cohort_keys cd CROSS JOIN days d
        LEFT JOIN touched t ON t.type = cd.type AND t.day = d.day
    `;

    // Today's window — current counts + slugs per cohort.
    const tagToday = await sql`
      SELECT t.tag AS cohort_value,
             COUNT(DISTINCT p.id)::int AS count,
             array_agg(DISTINCT p.slug) AS slugs
        FROM tags t JOIN pages p ON p.id = t.page_id
       WHERE p.updated_at >= ${sinceIso}::timestamptz
         AND p.updated_at <  ${sinceEnd.toISOString()}::timestamptz
       GROUP BY 1
    `;
    const typeToday = await sql`
      SELECT p.type AS cohort_value,
             COUNT(DISTINCT p.id)::int AS count,
             array_agg(DISTINCT p.slug) AS slugs
        FROM pages p
       WHERE p.updated_at >= ${sinceIso}::timestamptz
         AND p.updated_at <  ${sinceEnd.toISOString()}::timestamptz
       GROUP BY 1
    `;

    const baseline = [
      ...tagBaseline.map((r: Record<string, unknown>) => ({
        cohort_kind: 'tag' as const,
        cohort_value: String(r.cohort_value),
        day: String(r.day),
        count: Number(r.count),
      })),
      ...typeBaseline.map((r: Record<string, unknown>) => ({
        cohort_kind: 'type' as const,
        cohort_value: String(r.cohort_value),
        day: String(r.day),
        count: Number(r.count),
      })),
    ];
    const today = [
      ...tagToday.map((r: Record<string, unknown>) => ({
        cohort_kind: 'tag' as const,
        cohort_value: String(r.cohort_value),
        count: Number(r.count),
        page_slugs: (r.slugs as string[]) ?? [],
      })),
      ...typeToday.map((r: Record<string, unknown>) => ({
        cohort_kind: 'type' as const,
        cohort_value: String(r.cohort_value),
        count: Number(r.count),
        page_slugs: (r.slugs as string[]) ?? [],
      })),
    ];

    return computeAnomaliesFromBuckets(baseline, today, sigma);
  }

};

// Code-edge row mapping helper (moved from postgres-engine.ts)
function pgRowToCodeEdge(row: Record<string, unknown>): import('./types.ts').CodeEdgeResult {
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

