/**
 * KEMS-v2 Hybrid Retrieval Adapter for gbrain.
 *
 * Reads a JSONL snapshot exported by kairon's KnowledgeGraph.save() and
 * provides in-memory BM25 + graph-expansion search compatible with
 * gbrain's SearchResult contract.
 *
 * Usage:
 *   import { KemsV2Adapter } from '../retrieval/hybrid.ts';
 *   const adapter = KemsV2Adapter.fromFile('/path/to/graph.jsonl');
 *   const results = adapter.search('卫生健康', { topK: 10 });
 */

import { readFileSync } from 'node:fs';
import type { SearchResult } from '../core/types.ts';

// ── Types ──

interface KemsEntity {
  id: string;
  entity_type: string;
  title: string;
  content: string;
  source: string;
  tags: string[];
  metadata: Record<string, unknown>;
}

interface KemsEdge {
  source_id: string;
  target_id: string;
  relation: string;
  weight: number;
  metadata: Record<string, unknown>;
}

interface SearchResultOpts {
  topK?: number;
  graphHops?: number;
  graphWeight?: number;
}

// ── Tokenizer ──

/** CJK-aware tokenizer matching kairon's _tokenize behavior. */
function tokenize(text: string): string[] {
  const lower = text.normalize('NFKC').toLowerCase();
  const tokens: string[] = [];
  let current: string[] = [];
  for (const ch of lower) {
    const code = ch.codePointAt(0)!;
    if (code >= 0x4e00 && code <= 0x9fff) {
      // CJK character — flush Latin buffer, emit as individual token
      if (current.length > 0) {
        tokens.push(current.join(''));
        current = [];
      }
      tokens.push(ch);
    } else if (/[a-z0-9]/.test(ch)) {
      current.push(ch);
    } else {
      if (current.length > 0) {
        tokens.push(current.join(''));
        current = [];
      }
    }
  }
  if (current.length > 0) tokens.push(current.join(''));
  return tokens;
}

// ── Adapter ──

export class KemsV2Adapter {
  private entities: Map<string, KemsEntity> = new Map();
  private outgoing: Map<string, KemsEdge[]> = new Map();
  private incoming: Map<string, KemsEdge[]> = new Map();
  private tokenIndex: Map<string, Set<string>> = new Map();

  /** Build adapter from JSONL snapshot exported by KnowledgeGraph.save(). */
  static fromFile(path: string): KemsV2Adapter {
    const adapter = new KemsV2Adapter();
    const content = readFileSync(path, 'utf-8');
    for (const line of content.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const record = JSON.parse(trimmed);
      const t = record._t;
      delete record._t;
      if (t === 'entity') {
        adapter.addEntity(record as KemsEntity);
      } else if (t === 'edge') {
        adapter.addEdge(record as KemsEdge);
      }
    }
    return adapter;
  }

  private addEntity(entity: KemsEntity): void {
    this.entities.set(entity.id, entity);
    const text = [entity.title, entity.content, (entity.tags || []).join(' ')].join(' ');
    for (const token of tokenize(text)) {
      if (!this.tokenIndex.has(token)) this.tokenIndex.set(token, new Set());
      this.tokenIndex.get(token)!.add(entity.id);
    }
  }

  private addEdge(edge: KemsEdge): void {
    const out = this.outgoing.get(edge.source_id) || [];
    out.push(edge);
    this.outgoing.set(edge.source_id, out);
    const inc = this.incoming.get(edge.target_id) || [];
    inc.push(edge);
    this.incoming.set(edge.target_id, inc);
  }

  /**
   * Hybrid search: BM25 text scoring + graph 1-hop expansion with decay.
   * Returns results compatible with gbrain's SearchResult contract.
   */
  search(query: string, opts: SearchResultOpts = {}): SearchResult[] {
    const { topK = 10, graphHops = 1, graphWeight = 0.3 } = opts;
    const queryTokens = tokenize(query);
    if (queryTokens.length === 0) return [];

    // Phase 1: BM25 scoring
    const bm25Scores = new Map<string, number>();
    for (const token of queryTokens) {
      const ids = this.tokenIndex.get(token);
      if (!ids) continue;
      for (const eid of ids) {
        bm25Scores.set(eid, (bm25Scores.get(eid) || 0) + 1);
      }
    }
    if (bm25Scores.size === 0) return [];

    // Phase 2: Graph expansion
    const scores = new Map<string, number>();
    for (const [eid, score] of bm25Scores) {
      scores.set(eid, (scores.get(eid) || 0) + score);
      // BFS expansion
      const visited = new Set([eid]);
      let frontier = [eid];
      for (let hop = 1; hop <= graphHops; hop++) {
        const nextFrontier: string[] = [];
        const decay = graphWeight / hop;
        for (const feid of frontier) {
          const outEdges = this.outgoing.get(feid) || [];
          const inEdges = this.incoming.get(feid) || [];
          for (const edge of [...outEdges, ...inEdges]) {
            const neighborId = edge.source_id === feid ? edge.target_id : edge.source_id;
            if (!visited.has(neighborId) && this.entities.has(neighborId)) {
              visited.add(neighborId);
              scores.set(neighborId, (scores.get(neighborId) || 0) + score * decay);
              nextFrontier.push(neighborId);
            }
          }
        }
        frontier = nextFrontier;
      }
    }

    // Phase 3: Re-rank and convert to SearchResult
    const ranked = [...scores.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, topK);

    return ranked.map(([eid, score], idx) => {
      const entity = this.entities.get(eid)!;
      return {
        slug: eid,
        page_id: idx,
        title: entity.title,
        type: 'note' as const,
        chunk_text: entity.content,
        chunk_source: 'compiled_truth' as const,
        chunk_id: 0,
        chunk_index: 0,
        score,
        stale: false,
        source_id: entity.source || 'kems-v2',
      };
    });
  }

  /** Graph statistics. */
  stats(): { entities: number; edges: number; tokens: number } {
    let edgeCount = 0;
    for (const edges of this.outgoing.values()) edgeCount += edges.length;
    return {
      entities: this.entities.size,
      edges: edgeCount,
      tokens: this.tokenIndex.size,
    };
  }
}
