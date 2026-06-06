import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { operationsByName } from '../src/core/operations.ts';

let engine: PGLiteEngine;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
});

afterAll(async () => {
  await engine.disconnect();
});

beforeEach(async () => {
  for (const table of ['facts', 'pages', 'sources', 'config']) {
    try {
      await engine.executeRaw(`DELETE FROM ${table}`);
    } catch {
      // ignore tables that are guarded by FK order; tests seed what they need next
    }
  }
  await engine.executeRaw(`INSERT INTO sources (id, name) VALUES ('default', 'default') ON CONFLICT (id) DO NOTHING`);
  await engine.executeRaw(`
    INSERT INTO pages (slug, source_id, type, title, compiled_truth, timeline)
    VALUES
      ('memory/agentmesh-routing', 'default', 'note', 'Agentmesh Routing', 'Claude traffic routes through LiteLLM.', ''),
      ('memory/gbrain-tree', 'default', 'note', 'Memory Tree', 'Tree keeps compressed memory branches.', '')
  `);
  await engine.insertFact(
    { fact: 'LiteLLM handles Claude routing', entity_slug: 'agentmesh', source: 'test' },
    { source_id: 'default' },
  );
  await engine.insertFact(
    { fact: 'Memory tree keeps pinned summaries', entity_slug: 'gbrain', source: 'test' },
    { source_id: 'default' },
  );
});

describe('memory_tree operation', () => {
  test('search builds a rooted tree with page and fact nodes', async () => {
    const op = operationsByName.memory_tree;
    const result = await op.handler({
      args: { action: 'search', query: 'memory', limit: 5 },
      engine,
    } as any);

    expect(result.action).toBe('search');
    expect(result.tree.root.label).toBe('memory');
    expect(result.tree.nodes.some((node: any) => node.type === 'page')).toBe(true);
    expect(result.tree.nodes.some((node: any) => node.type === 'fact')).toBe(true);
  });

  test('pin persists node ids into config', async () => {
    const op = operationsByName.memory_tree;
    const pinResult = await op.handler({
      args: { action: 'pin', node_ids: ['entity:gbrain', 'page:memory/gbrain-tree'] },
      engine,
    } as any);

    expect(pinResult.pinned).toEqual(['entity:gbrain', 'page:memory/gbrain-tree']);
    const stored = await engine.getConfig('memory_tree.pins');
    expect(stored).toContain('entity:gbrain');
  });

  test('stats reports pinned count and totals', async () => {
    await engine.setConfig('memory_tree.pins', JSON.stringify(['entity:gbrain']));
    const op = operationsByName.memory_tree;
    const stats = await op.handler({
      args: { action: 'stats' },
      engine,
    } as any);

    expect(stats.action).toBe('stats');
    expect(stats.totals.facts).toBeGreaterThanOrEqual(2);
    expect(stats.pinned_count).toBe(1);
  });
});
