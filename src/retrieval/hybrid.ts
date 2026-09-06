/**
 * hybrid.ts — KEMS-v2 知识图谱混合检索适配层 (BET-Y1Q3-T10-117).
 *
 * 消费 kairon KEMS-v2 导出的图谱 JSONL 快照（entities.jsonl + edges.jsonl，
 * 由 KnowledgeGraph.save() 产物转换），在 gbrain 侧提供 BM25 词频检索 ×
 * 二跳图关系扩散的混合查询，结果带溯源路径（node → source file:line）。
 *
 * 纯 Bun/TS 标准库，零外部依赖。数据面只读。
 */

export interface KemsEntity {
  id: string;
  entity_type: string; // policy | regulation | adr | approval
  title: string;
  content: string;
  source: string; // e.g. "docs/adr/0042.md:L17"
  tags: string[];
}

export interface KemsEdge {
  source_id: string;
  target_id: string;
  relation: string; // cites | supersedes | implements | references
}

export interface HybridHit {
  entity: KemsEntity;
  score: number; // BM25 词频分 + 图扩散加分
  provenance: string[]; // 溯源链: entity.source + 邻接节点 source
}

export interface KemsSnapshot {
  entities: Map<string, KemsEntity>;
  adjacency: Map<string, string[]>; // source_id -> [target_id]
}

/** 从 kairon 导出的 JSONL 行加载快照（每行一个 JSON 对象）。 */
export function loadSnapshot(entitiesJsonl: string, edgesJsonl: string): KemsSnapshot {
  const entities = new Map<string, KemsEntity>();
  const adjacency = new Map<string, string[]>();

  for (const line of entitiesJsonl.split('\n')) {
    const s = line.trim();
    if (!s) continue;
    const e = JSON.parse(s) as KemsEntity;
    entities.set(e.id, e);
  }
  for (const line of edgesJsonl.split('\n')) {
    const s = line.trim();
    if (!s) continue;
    const e = JSON.parse(s) as KemsEdge;
    if (!entities.has(e.source_id) || !entities.has(e.target_id)) continue;
    const list = adjacency.get(e.source_id) ?? [];
    list.push(e.target_id);
    adjacency.set(e.source_id, list);
  }
  return { entities, adjacency };
}

function tokenize(text: string): string[] {
  // 与 kairon _tokenize 对齐: 连续字母数字段 + 中文单字
  const ascii = text.toLowerCase().match(/[a-z0-9]+/g) ?? [];
  const cjk = Array.from(text).filter((ch) => /[\u4e00-\u9fff]/.test(ch));
  return [...ascii, ...cjk];
}

/** BM25 近似（词频计数）检索 + 二跳图扩散加分。 */
export function hybridSearch(
  snapshot: KemsSnapshot,
  query: string,
  topK = 5,
): HybridHit[] {
  const tokens = tokenize(query);
  if (tokens.length === 0) return [];

  const scores = new Map<string, number>();
  for (const token of tokens) {
    for (const [id, e] of snapshot.entities) {
      const hay = `${e.title}\n${e.content}`;
      if (hay.toLowerCase().includes(token)) {
        scores.set(id, (scores.get(id) ?? 0) + 1);
      }
    }
  }

  // 二跳扩散: 命中节点的邻接节点得 0.3/0.1 加分
  const seedIds = [...scores.keys()];
  for (const seed of seedIds) {
    const base = scores.get(seed) ?? 0;
    for (const neighbor of snapshot.adjacency.get(seed) ?? []) {
      scores.set(neighbor, (scores.get(neighbor) ?? 0) + base * 0.3);
    }
  }

  const ranked = [...scores.entries()].sort((a, b) => b[1] - a[1]).slice(0, topK);
  const hits: HybridHit[] = [];
  for (const [id, score] of ranked) {
    const entity = snapshot.entities.get(id);
    if (!entity) continue;
    const provenance = [entity.source];
    for (const neighbor of (snapshot.adjacency.get(id) ?? []).slice(0, 3)) {
      const n = snapshot.entities.get(neighbor);
      if (n?.source) provenance.push(n.source);
    }
    hits.push({ entity, score, provenance });
  }
  return hits;
}
