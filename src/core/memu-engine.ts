/**
 * memU Engine — lightweight embedded storage engine for gbrain
 *
 * Uses Bun's built-in SQLite as the storage backend.
 * Implements the BrainEngine interface for all 54+ compatible MCP tools.
 *
 * Storage layout (SQLite tables): see initSchema() below.
 */
import { Database } from 'bun:sqlite';
import type {
  BrainEngine, EngineConfig, ReservedConnection,
  Page, PageInput, PageFilters, GetPageOpts, Chunk, ChunkInput,
  SearchResult, SearchOpts, Link, LinkBatchInput, LinkResult,
  GraphNode, GraphPath, TagRow,
  TimelineEntry, TimelineInput, TimelineOpts,
  RawData, FileRow, FileSpec,
  PageVersion, BrainStats, BrainHealth,
  IngestLogEntry, IngestLogInput,
  CodeEdgeInput, CodeEdgeResult,
  TrajectoryOpts, TrajectoryPoint,
  TraverseGraphOpts,
  FileUploadOpts,
  TakesListOpts, Take, TakeBatchInput, TakeHit, TakesScorecard, TakesScorecardOpts,
  CalibrationCurveOpts, CalibrationBucket,
  SynthesisEvidenceInput,
  DreamVerdict, DreamVerdictInput,
  FactRow, NewFact, FactListOpts, FactsHealth,
  SalienceOpts, SalienceResult, AnomaliesOpts, AnomalyResult,
  EmotionalWeightInputRow, EmotionalWeightWriteRow,
  EvalCandidate, EvalCandidateInput,
  EvalCaptureFailure, EvalCaptureFailureReason,
  DomainBankSampleOpts, CorpusSampleOpts, DomainBankRow,
} from './types.ts';

const DB_FILENAME = 'memu-brain.db';

export class MemUEngine implements BrainEngine {
  readonly kind = 'memu' as const;
  private _db: Database | null = null;
  private _dbPath: string = '';

  get db(): Database {
    if (!this._db) throw new Error('memU Engine not connected');
    return this._db;
  }

  // ── Lifecycle ─────────────────────────────────────────────

  async connect(config: EngineConfig): Promise<void> {
    this._dbPath = config.dbPath || DB_FILENAME;
    this._db = new Database(this._dbPath);
    this._db.exec('PRAGMA journal_mode=WAL');
    this._db.exec('PRAGMA busy_timeout=5000');
    this._db.exec('PRAGMA synchronous=NORMAL');
  }

  async disconnect(): Promise<void> {
    if (this._db) {
      this._db.close();
      this._db = null;
    }
  }

  async initSchema(): Promise<void> {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS pages (
        slug TEXT NOT NULL,
        source_id TEXT NOT NULL DEFAULT 'default',
        title TEXT NOT NULL DEFAULT '',
        body TEXT NOT NULL DEFAULT '',
        frontmatter TEXT NOT NULL DEFAULT '{}',
        compiled_truth TEXT NOT NULL DEFAULT '{}',
        page_type TEXT NOT NULL DEFAULT 'doc',
        word_count INTEGER NOT NULL DEFAULT 0,
        embedding INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        deleted_at TEXT,
        PRIMARY KEY (source_id, slug)
      );
      CREATE TABLE IF NOT EXISTS content_chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page_slug TEXT NOT NULL,
        source_id TEXT NOT NULL DEFAULT 'default',
        content TEXT NOT NULL,
        heading TEXT NOT NULL DEFAULT '',
        chunk_index INTEGER NOT NULL DEFAULT 0,
        char_start INTEGER NOT NULL DEFAULT 0,
        char_end INTEGER NOT NULL DEFAULT 0,
        token_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (source_id, page_slug) REFERENCES pages(source_id, slug)
      );
      CREATE TABLE IF NOT EXISTS page_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_slug TEXT NOT NULL,
        source_id TEXT NOT NULL DEFAULT 'default',
        target_slug TEXT NOT NULL,
        target_id TEXT NOT NULL DEFAULT 'default',
        link_type TEXT NOT NULL DEFAULT 'wiki',
        context TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS page_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page_slug TEXT NOT NULL,
        source_id TEXT NOT NULL DEFAULT 'default',
        tag TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(source_id, page_slug, tag)
      );
      CREATE TABLE IF NOT EXISTS timeline_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page_slug TEXT NOT NULL,
        source_id TEXT NOT NULL DEFAULT 'default',
        entry_date TEXT NOT NULL,
        content TEXT NOT NULL,
        source TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS raw_data (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        page_slug TEXT,
        source_id TEXT NOT NULL DEFAULT 'default',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_id TEXT NOT NULL DEFAULT 'default',
        page_slug TEXT,
        page_id INTEGER,
        filename TEXT NOT NULL,
        storage_path TEXT NOT NULL,
        mime_type TEXT,
        size_bytes INTEGER,
        content_hash TEXT NOT NULL,
        metadata TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS takes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page_slug TEXT NOT NULL,
        source_id TEXT NOT NULL DEFAULT 'default',
        claim TEXT NOT NULL,
        weight REAL NOT NULL DEFAULT 1.0,
        kind TEXT NOT NULL DEFAULT 'statement',
        tags TEXT NOT NULL DEFAULT '[]',
        superseded_by INTEGER,
        resolved_at TEXT,
        resolution TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS facts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity TEXT NOT NULL,
        attribute TEXT NOT NULL,
        value TEXT NOT NULL,
        source TEXT,
        confidence REAL NOT NULL DEFAULT 1.0,
        session_id TEXT,
        expired_at TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS ingest_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        slugs TEXT NOT NULL DEFAULT '[]',
        status TEXT NOT NULL DEFAULT 'success',
        message TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS code_edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_slug TEXT NOT NULL,
        target_slug TEXT NOT NULL,
        edge_type TEXT NOT NULL DEFAULT 'calls',
        metadata TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS config_store (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS page_versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page_slug TEXT NOT NULL,
        source_id TEXT NOT NULL DEFAULT 'default',
        title TEXT,
        body TEXT,
        version INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS dream_verdicts (
        page_slug TEXT NOT NULL,
        source_id TEXT NOT NULL DEFAULT 'default',
        verdict TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (source_id, page_slug)
      );
      CREATE TABLE IF NOT EXISTS contradictions_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT UNIQUE NOT NULL,
        value TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS eval_candidates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_name TEXT NOT NULL,
        input TEXT NOT NULL,
        output TEXT NOT NULL,
        key TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE TABLE IF NOT EXISTS eval_failures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tool_name TEXT NOT NULL,
        input TEXT NOT NULL,
        reason TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_chunks_page ON content_chunks(source_id, page_slug);
      CREATE INDEX IF NOT EXISTS idx_links_source ON page_links(source_id, source_slug);
      CREATE INDEX IF NOT EXISTS idx_links_target ON page_links(source_id, target_slug);
      CREATE INDEX IF NOT EXISTS idx_tags_page ON page_tags(source_id, page_slug);
      CREATE INDEX IF NOT EXISTS idx_timeline_page ON timeline_entries(source_id, page_slug);
      CREATE INDEX IF NOT EXISTS idx_takes_page ON takes(source_id, page_slug);
      CREATE INDEX IF NOT EXISTS idx_facts_entity ON facts(entity);
      CREATE INDEX IF NOT EXISTS idx_code_source ON code_edges(source_slug);
      CREATE INDEX IF NOT EXISTS idx_code_target ON code_edges(target_slug);
    `);
  }

  async transaction<T>(fn: (engine: BrainEngine) => Promise<T>): Promise<T> {
    const tx = this.db.transaction(() => fn(this));
    return tx();
  }

  async withReservedConnection<T>(fn: (conn: ReservedConnection) => Promise<T>): Promise<T> {
    // memU is single-connection; pass-through
    return fn({ engine: this, release: () => Promise.resolve() });
  }

  // ── Pages CRUD ────────────────────────────────────────────

  async getPage(slug: string, opts?: GetPageOpts): Promise<Page | null> {
    const q = opts?.includeDeleted
      ? `SELECT * FROM pages WHERE slug = ? AND source_id = ?`
      : `SELECT * FROM pages WHERE slug = ? AND source_id = ? AND deleted_at IS NULL`;
    const row = this.db.query(q).get(slug, opts?.sourceId || 'default') as any;
    return row ? this._rowToPage(row) : null;
  }

  async putPage(slug: string, page: PageInput, opts?: { sourceId?: string }): Promise<Page> {
    const sid = opts?.sourceId || 'default';
    const existing = this.db.query(`SELECT * FROM pages WHERE slug = ? AND source_id = ?`).get(slug, sid) as any;
    if (existing) {
      this.db.query(`
        UPDATE pages SET title = ?, body = ?, frontmatter = ?, updated_at = datetime('now')
        WHERE slug = ? AND source_id = ?
      `).run(page.title || existing.title, page.body ?? existing.body,
            JSON.stringify(page.frontmatter ?? {}), slug, sid);
    } else {
      this.db.query(`
        INSERT INTO pages (slug, source_id, title, body, frontmatter)
        VALUES (?, ?, ?, ?, ?)
      `).run(slug, sid, page.title || '', page.body || '', JSON.stringify(page.frontmatter || {}));
    }
    return (await this.getPage(slug, { sourceId: sid }))!;
  }

  async deletePage(slug: string, opts?: { sourceId?: string }): Promise<void> {
    const sid = opts?.sourceId || 'default';
    this.db.query(`DELETE FROM content_chunks WHERE page_slug = ? AND source_id = ?`).run(slug, sid);
    this.db.query(`DELETE FROM page_links WHERE source_slug = ? AND source_id = ?`).run(slug, sid);
    this.db.query(`DELETE FROM page_tags WHERE page_slug = ? AND source_id = ?`).run(slug, sid);
    this.db.query(`DELETE FROM pages WHERE slug = ? AND source_id = ?`).run(slug, sid);
  }

  async softDeletePage(slug: string, opts?: { sourceId?: string }): Promise<{ slug: string } | null> {
    const sid = opts?.sourceId || 'default';
    const r = this.db.query(`UPDATE pages SET deleted_at = datetime('now') WHERE slug = ? AND source_id = ? AND deleted_at IS NULL`)
      .run(slug, sid);
    return r.changes > 0 ? { slug } : null;
  }

  async restorePage(slug: string, opts?: { sourceId?: string }): Promise<boolean> {
    const sid = opts?.sourceId || 'default';
    const r = this.db.query(`UPDATE pages SET deleted_at = NULL WHERE slug = ? AND source_id = ? AND deleted_at IS NOT NULL`)
      .run(slug, sid);
    return r.changes > 0;
  }

  async purgeDeletedPages(olderThanHours: number): Promise<{ slugs: string[]; count: number }> {
    const rows = this.db.query(`
      SELECT slug, source_id FROM pages WHERE deleted_at IS NOT NULL AND
      datetime(deleted_at, '+' || ? || ' hours') <= datetime('now')
    `).all(olderThanHours) as any[];
    for (const r of rows) {
      await this.deletePage(r.slug, { sourceId: r.source_id });
    }
    return { slugs: rows.map(r => r.slug), count: rows.length };
  }

  async listPages(filters?: PageFilters): Promise<Page[]> {
    let sql = `SELECT * FROM pages WHERE 1=1`;
    const params: any[] = [];
    if (!filters?.includeDeleted) { sql += ` AND deleted_at IS NULL`; }
    if (filters?.sourceId) { sql += ` AND source_id = ?`; params.push(filters.sourceId); }
    if (filters?.prefix) { sql += ` AND slug LIKE ?`; params.push(filters.prefix + '%'); }
    if (filters?.limit) { sql += ` LIMIT ?`; params.push(filters.limit); }
    const rows = this.db.query(sql).all(...params) as any[];
    return rows.map(r => this._rowToPage(r));
  }

  async resolveSlugs(partial: string): Promise<string[]> {
    const rows = this.db.query(`SELECT slug FROM pages WHERE slug LIKE ? LIMIT 20`).all(`%${partial}%`) as any[];
    return rows.map(r => r.slug);
  }

  async getAllSlugs(opts?: { sourceId?: string }): Promise<Set<string>> {
    let sql = `SELECT slug FROM pages WHERE deleted_at IS NULL`;
    const params: any[] = [];
    if (opts?.sourceId) { sql += ` AND source_id = ?`; params.push(opts.sourceId); }
    const rows = this.db.query(sql).all(...params) as any[];
    return new Set(rows.map(r => r.slug));
  }

  async listAllPageRefs(): Promise<Array<{ slug: string; source_id: string }>> {
    return this.db.query(`SELECT slug, source_id FROM pages WHERE deleted_at IS NULL`).all() as any[];
  }

  async listPrefixSampledPages(_opts: DomainBankSampleOpts): Promise<DomainBankRow[]> {
    return [];
  }

  async listCorpusSample(_opts: CorpusSampleOpts): Promise<DomainBankRow[]> {
    return [];
  }

  // ── Search ────────────────────────────────────────────────

  async searchKeyword(query: string, opts?: SearchOpts): Promise<SearchResult[]> {
    const limit = opts?.limit || 20;
    const rows = this.db.query(`
      SELECT slug, source_id, title, body FROM pages
      WHERE deleted_at IS NULL AND (title LIKE ? OR body LIKE ?)
      LIMIT ?
    `).all(`%${query}%`, `%${query}%`, limit) as any[];
    return rows.map(r => ({
      slug: r.slug,
      source_id: r.source_id,
      title: r.title,
      snippet: r.body?.substring(0, 200) || '',
      score: 0.5,
    }));
  }

  async searchVector(_embedding: Float32Array, opts?: SearchOpts): Promise<SearchResult[]> {
    // memU v1: no vector search — returns empty results gracefully
    return [];
  }

  async getEmbeddingsByChunkIds(_ids: number[], _column?: string): Promise<Map<number, Float32Array>> {
    return new Map();
  }

  // ── Chunks ────────────────────────────────────────────────

  async upsertChunks(slug: string, chunks: ChunkInput[], opts?: { sourceId?: string }): Promise<Chunk[]> {
    const sid = opts?.sourceId || 'default';
    this.db.query(`DELETE FROM content_chunks WHERE page_slug = ? AND source_id = ?`).run(slug, sid);
    for (let i = 0; i < chunks.length; i++) {
      const c = chunks[i];
      this.db.query(`
        INSERT INTO content_chunks (page_slug, source_id, content, heading, chunk_index, char_start, char_end, token_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(slug, sid, c.content, c.heading || '', i, c.char_start || 0, c.char_end || 0, c.token_count || 0);
    }
    return this.getChunks(slug, { sourceId: sid }) as Promise<Chunk[]>;
  }

  async getChunks(slug: string, opts?: { sourceId?: string }): Promise<Chunk[]> {
    const sid = opts?.sourceId || 'default';
    return this.db.query(`
      SELECT * FROM content_chunks WHERE page_slug = ? AND source_id = ? ORDER BY chunk_index
    `).all(slug, sid) as any[];
  }

  async countStaleChunks(): Promise<number> {
    return 0;
  }

  async listStaleChunks(_limit?: number): Promise<StaleChunkRow[]> {
    return [];
  }

  async deleteChunks(_ids: number[]): Promise<void> {}

  // ── Links ─────────────────────────────────────────────────

  async addLink(src: string, target: string, opts?: { sourceId?: string; linkType?: string; context?: string }): Promise<void> {
    const sid = opts?.sourceId || 'default';
    this.db.query(`
      INSERT OR IGNORE INTO page_links (source_slug, source_id, target_slug, target_id, link_type, context)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(src, sid, target, sid, opts?.linkType || 'wiki', opts?.context || null);
  }

  async addLinksBatch(inputs: LinkBatchInput[]): Promise<void> {
    for (const inp of inputs) {
      await this.addLink(inp.sourceSlug, inp.targetSlug, { sourceId: inp.sourceId, linkType: inp.linkType, context: inp.context });
    }
  }

  async removeLink(src: string, target: string, opts?: { sourceId?: string }): Promise<void> {
    const sid = opts?.sourceId || 'default';
    this.db.query(`DELETE FROM page_links WHERE source_slug = ? AND source_id = ? AND target_slug = ?`).run(src, sid, target);
  }

  async getLinks(slug: string, opts?: { sourceId?: string }): Promise<Link[]> {
    const sid = opts?.sourceId || 'default';
    return this.db.query(`SELECT * FROM page_links WHERE source_slug = ? AND source_id = ?`).all(slug, sid) as any[];
  }

  async getBacklinks(slug: string, opts?: { sourceId?: string }): Promise<Link[]> {
    const sid = opts?.sourceId || 'default';
    return this.db.query(`SELECT * FROM page_links WHERE target_slug = ? AND target_id = ?`).all(slug, sid) as any[];
  }

  async findByTitleFuzzy(_title: string, _opts?: { limit?: number; sourceId?: string }): Promise<any[]> {
    return [];
  }

  async traverseGraph(startSlug: string, opts?: TraverseGraphOpts): Promise<GraphNode[]> {
    const visited = new Set<string>();
    const result: GraphNode[] = [];
    const queue = [startSlug];
    const maxDepth = opts?.frontierCap || 3;
    let depth = 0;

    while (queue.length > 0 && depth < maxDepth) {
      const levelSize = queue.length;
      for (let i = 0; i < levelSize; i++) {
        const slug = queue.shift()!;
        if (visited.has(slug)) continue;
        visited.add(slug);
        const page = await this.getPage(slug);
        if (page) result.push({ slug, title: page.title });
        const links = await this.getLinks(slug);
        for (const link of links) {
          if (!visited.has(link.target_slug)) queue.push(link.target_slug);
        }
      }
      depth++;
    }
    return result;
  }

  async traversePaths(_start: string, _end: string, _opts?: { maxDepth?: number }): Promise<GraphPath[]> {
    return [];
  }

  async getBacklinkCounts(_slugs: string[]): Promise<Map<string, number>> {
    return new Map();
  }

  async getPageTimestamps(_slugs: string[]): Promise<Map<string, { created_at: Date; updated_at: Date }>> {
    return new Map();
  }

  async getEffectiveDates(_slugs: string[]): Promise<Map<string, string>> {
    return new Map();
  }

  async getSalienceScores(_slugs: string[]): Promise<Map<string, number>> {
    return new Map();
  }

  async findOrphanPages(_opts?: { thresholdDays?: number; includeDeleted?: boolean }): Promise<string[]> {
    return [];
  }

  // ── Tags ──────────────────────────────────────────────────

  async addTag(slug: string, tag: string, opts?: { sourceId?: string }): Promise<void> {
    const sid = opts?.sourceId || 'default';
    this.db.query(`INSERT OR IGNORE INTO page_tags (page_slug, source_id, tag) VALUES (?, ?, ?)`).run(slug, sid, tag);
  }

  async removeTag(slug: string, tag: string, opts?: { sourceId?: string }): Promise<void> {
    const sid = opts?.sourceId || 'default';
    this.db.query(`DELETE FROM page_tags WHERE page_slug = ? AND source_id = ? AND tag = ?`).run(slug, sid, tag);
  }

  async getTags(slug: string, opts?: { sourceId?: string }): Promise<TagRow[]> {
    const sid = opts?.sourceId || 'default';
    return this.db.query(`SELECT tag as name, created_at FROM page_tags WHERE page_slug = ? AND source_id = ?`).all(slug, sid) as any[];
  }

  // ── Timeline ──────────────────────────────────────────────

  async addTimelineEntry(slug: string, entry: TimelineInput, opts?: { sourceId?: string }): Promise<void> {
    const sid = opts?.sourceId || 'default';
    this.db.query(`
      INSERT INTO timeline_entries (page_slug, source_id, entry_date, content, source)
      VALUES (?, ?, ?, ?, ?)
    `).run(slug, sid, entry.date, entry.content, entry.source || null);
  }

  async addTimelineEntriesBatch(slug: string, entries: TimelineInput[], opts?: { sourceId?: string }): Promise<void> {
    for (const e of entries) await this.addTimelineEntry(slug, e, opts);
  }

  async getTimeline(slug: string, opts?: TimelineOpts): Promise<TimelineEntry[]> {
    const sid = opts?.sourceId || 'default';
    let sql = `SELECT * FROM timeline_entries WHERE page_slug = ? AND source_id = ? ORDER BY entry_date`;
    const params: any[] = [slug, sid];
    if (opts?.limit) { sql += ` LIMIT ?`; params.push(opts.limit); }
    return this.db.query(sql).all(...params) as any[];
  }

  // ── Raw Data ──────────────────────────────────────────────

  async putRawData(key: string, value: any, opts?: { pageSlug?: string; sourceId?: string }): Promise<void> {
    const sid = opts?.sourceId || 'default';
    this.db.query(`
      INSERT INTO raw_data (key, value, page_slug, source_id, updated_at)
      VALUES (?, ?, ?, ?, datetime('now'))
      ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')
    `).run(key, typeof value === 'string' ? value : JSON.stringify(value), opts?.pageSlug || null, sid);
  }

  async getRawData(key: string): Promise<RawData | null> {
    return this.db.query(`SELECT * FROM raw_data WHERE key = ?`).get(key) as any || null;
  }

  // ── Files ─────────────────────────────────────────────────

  async upsertFile(spec: FileSpec): Promise<FileRow> {
    this.db.query(`
      INSERT INTO files (source_id, page_slug, filename, storage_path, mime_type, size_bytes, content_hash, metadata)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT DO NOTHING
    `).run(spec.source_id || 'default', spec.page_slug || null, spec.filename, spec.storage_path,
           spec.mime_type || null, spec.size_bytes || null, spec.content_hash,
           JSON.stringify(spec.metadata || {}));
    return this.db.query(`SELECT * FROM files WHERE content_hash = ?`).get(spec.content_hash) as any;
  }

  async getFile(_id: number): Promise<FileRow | null> {
    return null;
  }

  async listFilesForPage(slug: string, opts?: { sourceId?: string }): Promise<FileRow[]> {
    const sid = opts?.sourceId || 'default';
    return this.db.query(`SELECT * FROM files WHERE page_slug = ? AND source_id = ?`).all(slug, sid) as any[];
  }

  // ── Takes ─────────────────────────────────────────────────

  async addTakesBatch(slug: string, takes: TakeBatchInput[], opts?: { sourceId?: string }): Promise<void> {
    const sid = opts?.sourceId || 'default';
    for (const t of takes) {
      this.db.query(`
        INSERT INTO takes (page_slug, source_id, claim, weight, kind, tags)
        VALUES (?, ?, ?, ?, ?, ?)
      `).run(slug, sid, t.claim, t.weight || 1.0, t.kind || 'statement', JSON.stringify(t.tags || []));
    }
  }

  async listTakes(slug: string, opts?: TakesListOpts): Promise<Take[]> {
    const sid = opts?.sourceId || 'default';
    let sql = `SELECT * FROM takes WHERE page_slug = ? AND source_id = ? AND superseded_by IS NULL`;
    const params: any[] = [slug, sid];
    if (opts?.kind) { sql += ` AND kind = ?`; params.push(opts.kind); }
    if (opts?.limit) { sql += ` LIMIT ?`; params.push(opts.limit); }
    return this.db.query(sql).all(...params) as any[];
  }

  async searchTakes(query: string, _opts?: { limit?: number }): Promise<TakeHit[]> {
    const limit = _opts?.limit || 20;
    return this.db.query(`
      SELECT t.*, p.title as page_title FROM takes t
      JOIN pages p ON p.slug = t.page_slug AND p.source_id = t.source_id
      WHERE t.claim LIKE ? AND t.superseded_by IS NULL LIMIT ?
    `).all(`%${query}%`, limit) as any[];
  }

  async searchTakesVector(_embedding: Float32Array, _opts?: { limit?: number }): Promise<TakeHit[]> {
    return [];
  }

  async getTakeEmbeddings(_ids: number[]): Promise<Map<number, Float32Array>> {
    return new Map();
  }

  async countStaleTakes(): Promise<number> { return 0; }
  async listStaleTakes(_limit?: number): Promise<any[]> { return []; }
  async updateTake(_id: number, _update: Partial<Take>): Promise<void> {}
  async supersedeTake(_id: number, _replacementId: number): Promise<void> {}
  async resolveTake(_id: number, _resolution: string): Promise<void> {}
  async getScorecard(_opts?: TakesScorecardOpts): Promise<TakesScorecard> {
    return { buckets: [], totalTakes: 0, resolvedTakes: 0, accuracy: 0 };
  }
  async getCalibrationCurve(_opts?: CalibrationCurveOpts): Promise<CalibrationBucket[]> { return []; }
  async addSynthesisEvidence(_input: SynthesisEvidenceInput): Promise<void> {}
  async listActiveTakesForPages(_slugs: string[]): Promise<Map<string, Take[]>> { return new Map(); }

  // ── Dream Cycle ───────────────────────────────────────────

  async getDreamVerdict(slug: string, _opts?: { sourceId?: string }): Promise<DreamVerdict | null> {
    return this.db.query(`SELECT * FROM dream_verdicts WHERE page_slug = ?`).get(slug) as any || null;
  }

  async putDreamVerdict(slug: string, verdict: DreamVerdictInput, _opts?: { sourceId?: string }): Promise<void> {
    this.db.query(`
      INSERT OR REPLACE INTO dream_verdicts (page_slug, source_id, verdict) VALUES (?, ?, ?)
    `).run(slug, _opts?.sourceId || 'default', JSON.stringify(verdict));
  }

  // ── Contradictions ────────────────────────────────────────

  async writeContradictionsRun(_key: string, _data: any): Promise<void> {}
  async loadContradictionsTrend(_key: string): Promise<any> { return null; }
  async getContradictionCacheEntry(key: string): Promise<any> {
    const row = this.db.query(`SELECT * FROM contradictions_cache WHERE key = ?`).get(key) as any;
    return row ? JSON.parse(row.value) : null;
  }
  async putContradictionCacheEntry(key: string, value: any): Promise<void> {
    this.db.query(`
      INSERT OR REPLACE INTO contradictions_cache (key, value) VALUES (?, ?)
    `).run(key, JSON.stringify(value));
  }
  async sweepContradictionCache(): Promise<void> {
    this.db.query(`DELETE FROM contradictions_cache`).run();
  }

  // ── Facts ─────────────────────────────────────────────────

  async insertFact(entity: string, fact: NewFact, _opts?: { sessionId?: string }): Promise<FactRow> {
    this.db.query(`
      INSERT INTO facts (entity, attribute, value, source, confidence, session_id)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(entity, fact.attribute, fact.value, fact.source || null, fact.confidence ?? 1.0, _opts?.sessionId || null);
    return this.db.query(`SELECT * FROM facts WHERE id = last_insert_rowid()`).get() as any;
  }

  async insertFacts(facts: Array<{ entity: string; fact: NewFact; sessionId?: string }>): Promise<void> {
    for (const f of facts) await this.insertFact(f.entity, f.fact, { sessionId: f.sessionId });
  }

  async deleteFactsForPage(_slug: string): Promise<void> {}
  async expireFact(_id: number): Promise<void> {}
  async listFactsByEntity(entity: string, opts?: FactListOpts): Promise<FactRow[]> {
    let sql = `SELECT * FROM facts WHERE entity = ? AND expired_at IS NULL`;
    const params: any[] = [entity];
    if (opts?.limit) { sql += ` LIMIT ?`; params.push(opts.limit); }
    return this.db.query(sql).all(...params) as any[];
  }

  async listFactsSince(_since: Date): Promise<FactRow[]> { return []; }
  async listFactsBySession(_sessionId: string): Promise<FactRow[]> { return []; }
  async listSupersessions(_factId: number): Promise<FactRow[]> { return []; }
  async countUnconsolidatedFacts(): Promise<number> { return 0; }
  async findCandidateDuplicates(): Promise<Array<{ winner_id: number; loser_id: number }>> { return []; }
  async consolidateFact(_winnerId: number, _loserId: number): Promise<void> {}
  async findTrajectory(entity: string, opts?: TrajectoryOpts): Promise<TrajectoryPoint[]> {
    const limit = opts?.limit || 20;
    return this.db.query(`SELECT * FROM facts WHERE entity = ? AND expired_at IS NULL ORDER BY id LIMIT ?`)
      .all(entity, limit) as any[];
  }
  async getFactsHealth(): Promise<FactsHealth> {
    const count = (this.db.query(`SELECT COUNT(*) as c FROM facts`).get() as any).c;
    return { totalFacts: count, unconsolidated: 0, duplicatePairs: 0, health: count > 0 ? 'healthy' : 'empty' };
  }

  // ── Versions ──────────────────────────────────────────────

  async createVersion(slug: string, opts?: { sourceId?: string }): Promise<PageVersion> {
    const sid = opts?.sourceId || 'default';
    const page = await this.getPage(slug, { sourceId: sid });
    if (!page) throw new Error(`Page not found: ${slug}`);
    const maxVer = (this.db.query(`SELECT COALESCE(MAX(version), 0) as v FROM page_versions WHERE page_slug = ? AND source_id = ?`).get(slug, sid) as any).v;
    this.db.query(`
      INSERT INTO page_versions (page_slug, source_id, title, body, version) VALUES (?, ?, ?, ?, ?)
    `).run(slug, sid, page.title, page.body, maxVer + 1);
    return this.db.query(`SELECT * FROM page_versions WHERE id = last_insert_rowid()`).get() as any;
  }

  async getVersions(slug: string, _opts?: { sourceId?: string }): Promise<PageVersion[]> {
    return [];
  }

  async revertToVersion(_slug: string, _version: number, _opts?: { sourceId?: string }): Promise<Page> {
    throw new Error('Version revert not supported in memU');
  }

  // ── Stats + Health ────────────────────────────────────────

  async getStats(): Promise<BrainStats> {
    const pageCount = (this.db.query(`SELECT COUNT(*) as c FROM pages WHERE deleted_at IS NULL`).get() as any).c;
    const linkCount = (this.db.query(`SELECT COUNT(*) as c FROM page_links`).get() as any).c;
    const chunkCount = (this.db.query(`SELECT COUNT(*) as c FROM content_chunks`).get() as any).c;
    return {
      pageCount, linkCount, chunkCount,
      tagCount: 0, takeCount: 0, factCount: 0,
      dbSize: 0, uptime: 0,
    } as BrainStats;
  }

  async getHealth(): Promise<BrainHealth> {
    return {
      status: 'ok',
      engine: 'memu',
      pageCount: (this.db.query(`SELECT COUNT(*) as c FROM pages WHERE deleted_at IS NULL`).get() as any).c,
    } as BrainHealth;
  }

  // ── Ingest Log ────────────────────────────────────────────

  async logIngest(source: string, slugs: string[] | { source?: string; pages?: number; status?: string }, status?: string, message?: string): Promise<void> {
    // Support both (source, slugs) and ({ source, pages, status }) signatures
    const src = typeof source === 'object' ? (source as any).source || 'unknown' : source;
    const slugList = typeof source === 'object' ? JSON.stringify([]) : JSON.stringify(slugs);
    const sts = typeof source === 'object' ? (source as any).status || 'success' : (status || 'success');
    this.db.query(`
      INSERT INTO ingest_log (source, slugs, status, message) VALUES (?, ?, ?, ?)
    `).run(src, slugList, sts, message || null);
  }

  async getIngestLog(_opts?: { limit?: number }): Promise<IngestLogEntry[]> {
    const limit = _opts?.limit || 50;
    return this.db.query(`SELECT * FROM ingest_log ORDER BY id DESC LIMIT ?`).all(limit) as any[];
  }

  // ── Sync ──────────────────────────────────────────────────

  async updateSlug(_oldSlug: string, _newSlug: string, _opts?: { sourceId?: string }): Promise<void> {
    throw new Error('Slug rename not supported in memU');
  }

  async rewriteLinks(_oldSlug: string, _newSlug: string, _opts?: { sourceId?: string }): Promise<void> {}
  async refreshPageBody(_slug: string, _opts?: { sourceId?: string }): Promise<void> {}
  async migrateFactsToCanonical(): Promise<{ migrated: number; errors: string[] }> { return { migrated: 0, errors: [] }; }

  // ── Config ────────────────────────────────────────────────

  async getConfig(key: string): Promise<string | null> {
    const row = this.db.query(`SELECT value FROM config_store WHERE key = ?`).get(key) as any;
    return row?.value || null;
  }

  async setConfig(key: string, value: string): Promise<void> {
    this.db.query(`INSERT OR REPLACE INTO config_store (key, value) VALUES (?, ?)`).run(key, value);
  }

  async unsetConfig(key: string): Promise<void> {
    this.db.query(`DELETE FROM config_store WHERE key = ?`).run(key);
  }

  async listConfigKeys(): Promise<string[]> {
    const rows = this.db.query(`SELECT key FROM config_store`).all() as any[];
    return rows.map(r => r.key);
  }

  // ── Migration ─────────────────────────────────────────────

  async runMigration(_version: number): Promise<void> {}
  async getChunksWithEmbeddings(): Promise<any[]> { return []; }

  // ── Raw SQL ───────────────────────────────────────────────

  async executeRaw<T>(sql: string, ...params: any[]): Promise<T[]> {
    return this.db.query(sql).all(...params) as T[];
  }

  // ── Code Edges ────────────────────────────────────────────

  async addCodeEdges(edges: CodeEdgeInput[]): Promise<CodeEdgeResult> {
    let added = 0;
    for (const e of edges) {
      this.db.query(`
        INSERT OR IGNORE INTO code_edges (source_slug, target_slug, edge_type, metadata)
        VALUES (?, ?, ?, ?)
      `).run(e.sourceSlug, e.targetSlug, e.edgeType || 'calls', JSON.stringify(e.metadata || {}));
      added++;
    }
    return { added, total: added };
  }

  async deleteCodeEdgesForChunks(chunkIds: number[]): Promise<void> {
    // Not supported via chunk IDs in memU
  }

  async getCallersOf(slug: string): Promise<string[]> {
    const rows = this.db.query(`SELECT source_slug FROM code_edges WHERE target_slug = ?`).all(slug) as any[];
    return rows.map(r => r.source_slug);
  }

  async getCalleesOf(slug: string): Promise<string[]> {
    const rows = this.db.query(`SELECT target_slug FROM code_edges WHERE source_slug = ?`).all(slug) as any[];
    return rows.map(r => r.target_slug);
  }

  async getEdgesByChunk(_chunkId: number): Promise<any[]> { return []; }
  async searchKeywordChunks(_query: string): Promise<any[]> { return []; }

  // ── Eval Capture ──────────────────────────────────────────

  async logEvalCandidate(input: EvalCandidateInput): Promise<void> {
    this.db.query(`
      INSERT INTO eval_candidates (tool_name, input, output, key)
      VALUES (?, ?, ?, ?)
    `).run(input.toolName, JSON.stringify(input.input), JSON.stringify(input.output), input.key || null);
  }

  async listEvalCandidates(_opts?: { limit?: number; toolName?: string }): Promise<EvalCandidate[]> {
    return [];
  }

  async deleteEvalCandidatesBefore(_date: Date): Promise<void> {
    this.db.query(`DELETE FROM eval_candidates WHERE created_at < ?`).run(_date.toISOString());
  }

  async logEvalCaptureFailure(input: { toolName: string; input: any; reason: EvalCaptureFailureReason }): Promise<void> {
    this.db.query(`
      INSERT INTO eval_failures (tool_name, input, reason) VALUES (?, ?, ?)
    `).run(input.toolName, JSON.stringify(input.input), input.reason);
  }

  async listEvalCaptureFailures(_opts?: { limit?: number }): Promise<EvalCaptureFailure[]> {
    return [];
  }

  // ── Salience / Anomaly ────────────────────────────────────

  async batchLoadEmotionalInputs(_opts: SalienceOpts): Promise<EmotionalWeightInputRow[]> { return []; }
  async setEmotionalWeightBatch(_rows: EmotionalWeightWriteRow[]): Promise<void> {}
  async getRecentSalience(_opts: SalienceOpts): Promise<SalienceResult> {
    return { scores: [], window: _opts.windowDays || 7 } as SalienceResult;
  }
  async findAnomalies(_opts: AnomaliesOpts): Promise<AnomalyResult> {
    return { anomalies: [], method: 'zscore' } as AnomalyResult;
  }

  // ── Helpers ───────────────────────────────────────────────

  private _rowToPage(row: any): Page {
    return {
      slug: row.slug,
      source_id: row.source_id,
      title: row.title || '',
      body: row.body || '',
      frontmatter: safeJson(row.frontmatter, {}),
      compiled_truth: safeJson(row.compiled_truth, {}),
      page_type: row.page_type || 'doc',
      word_count: row.word_count || 0,
      embedding: row.embedding || 0,
      created_at: new Date(row.created_at),
      updated_at: new Date(row.updated_at),
      deleted_at: row.deleted_at ? new Date(row.deleted_at) : undefined,
    };
  }
}

function safeJson(raw: string, fallback: any): any {
  try { return JSON.parse(raw); } catch { return fallback; }
}
