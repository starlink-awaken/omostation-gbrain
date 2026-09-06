/**
 * Tests for KEMS-v2 hybrid retrieval adapter (BET-Y1Q3-T10-117).
 * Pure-Bun, no DB: snapshot load + hybrid search + provenance + latency.
 */
import { describe, it, expect } from 'bun:test';
import { loadSnapshot, hybridSearch } from '../src/retrieval/hybrid.ts';

function makeSnapshot(entities = 600) {
  const eLines: string[] = [];
  const eLine: string[] = [];
  for (let i = 0; i < entities; i++) {
    const kw = ['互联互通', '电子病历', '数据治理'][i % 3];
    eLine.push(JSON.stringify({
      id: `pol-${String(i).padStart(5, '0')}`,
      entity_type: 'policy',
      title: `卫生政策-${i}-${kw}`,
      content: `第${i}号政策内容，预算${i % 90 + 10}万元。`,
      source: `docs/adr/${i}.md:L${i % 200}`,
      tags: [],
    }));
  }
  for (let i = 1; i < entities; i++) {
    eLines.push(JSON.stringify({
      source_id: `pol-${String(i).padStart(5, '0')}`,
      target_id: `pol-${String(i - 1).padStart(5, '0')}`,
      relation: 'cites',
    }));
  }
  return loadSnapshot(eLine.join('\n'), eLines.join('\n'));
}

describe('KEMS hybrid retrieval', () => {
  const snap = makeSnapshot();

  it('loads full snapshot', () => {
    expect(snap.entities.size).toBe(600);
    expect(snap.adjacency.size).toBeGreaterThan(500);
  });

  it('hybridSearch returns ranked hits with provenance', () => {
    const hits = hybridSearch(snap, '电子病历', 5);
    expect(hits.length).toBeGreaterThan(0);
    for (const h of hits) {
      expect(h.entity.source.length).toBeGreaterThan(0);
      expect(h.score).toBeGreaterThan(0);
    }
  });

  it('top-3 recall on planted targets >= 95%', () => {
    let hits = 0, total = 0;
    for (let idx = 200; idx < 220; idx++) {
      const target = `pol-${String(idx).padStart(5, '0')}`;
      const results = hybridSearch(snap, String(idx), 3);
      total++;
      if (results.some((h) => h.entity.id === target)) hits++;
    }
    const recall = hits / total;
    expect(recall).toBeGreaterThanOrEqual(0.95);
  });

  it('search latency under 50ms per query', () => {
    hybridSearch(snap, '数据治理', 5); // warm
    const t0 = performance.now();
    hybridSearch(snap, '互联互通 600', 5);
    const ms = performance.now() - t0;
    expect(ms).toBeLessThan(50);
  });

  it('empty query returns no hits', () => {
    expect(hybridSearch(snap, '', 5).length).toBe(0);
  });
});
