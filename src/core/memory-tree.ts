import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';

import type { BrainEngine, FactRow } from './engine.ts';
import { gbrainPath } from './config.ts';
import type { SearchResult } from './types.ts';

const PINS_CONFIG_KEY = 'memory_tree.pins';
const PINS_PATH = gbrainPath('memory-tree-pins.json');

type MemoryTreeNodeType = 'root' | 'entity' | 'page' | 'fact';

export interface MemoryTreeNode {
  id: string;
  type: MemoryTreeNodeType;
  label: string;
  parent_id: string | null;
  summary?: string;
}

function unique<T>(values: T[]): T[] {
  return Array.from(new Set(values));
}

function readPinFile(): string[] {
  if (!existsSync(PINS_PATH)) return [];
  try {
    const raw = JSON.parse(readFileSync(PINS_PATH, 'utf8'));
    return Array.isArray(raw) ? raw.filter((value): value is string => typeof value === 'string') : [];
  } catch {
    return [];
  }
}

function writePinFile(nodeIds: string[]) {
  mkdirSync(gbrainPath(), { recursive: true });
  writeFileSync(PINS_PATH, JSON.stringify(unique(nodeIds), null, 2), 'utf8');
}

function entityId(slug: string) {
  return `entity:${slug}`;
}

function pageId(slug: string) {
  return `page:${slug}`;
}

function factId(fact: FactRow, index: number) {
  return `fact:${fact.entity_slug || 'unknown'}:${index}`;
}

function pageNode(result: SearchResult): MemoryTreeNode {
  const parent = result.slug.includes('/') ? entityId(result.slug.split('/')[1] ?? result.slug) : null;
  return {
    id: pageId(result.slug),
    type: 'page',
    label: result.title || result.slug,
    parent_id: parent,
    summary: result.snippet || '',
  };
}

function factNode(fact: FactRow, index: number): MemoryTreeNode {
  return {
    id: factId(fact, index),
    type: 'fact',
    label: fact.entity_slug || `fact-${index + 1}`,
    parent_id: fact.entity_slug ? entityId(fact.entity_slug) : null,
    summary: fact.fact,
  };
}

async function fallbackPages(engine: BrainEngine, query: string, limit: number): Promise<SearchResult[]> {
  const escaped = query.toLowerCase().replace(/'/g, "''");
  const rows = await engine.executeRaw(
    `SELECT slug, title, compiled_truth
     FROM pages
     WHERE lower(coalesce(title, '')) LIKE '%${escaped}%'
        OR lower(coalesce(compiled_truth, '')) LIKE '%${escaped}%'
     LIMIT ${Math.max(limit, 1)}`,
  ) as Array<{ slug: string; title: string; compiled_truth: string }>;
  return rows.map(row => ({
    slug: row.slug,
    title: row.title,
    snippet: row.compiled_truth,
    score: 1,
  })) as SearchResult[];
}

export async function buildMemoryTree(engine: BrainEngine, query: string, limit = 10) {
  const primaryResults = await engine.searchKeyword(query, { limit });
  const searchResults = primaryResults.length > 0 ? primaryResults : await fallbackPages(engine, query, limit);
  const facts = await engine.listFactsSince('default', new Date(0), { limit, activeOnly: true });
  const nodes: MemoryTreeNode[] = [{ id: 'root', type: 'root', label: query, parent_id: null }];
  const entitySlugs = unique([
    ...searchResults.map(result => result.slug.split('/')[1] ?? result.slug),
    ...facts.map(fact => fact.entity_slug).filter((slug): slug is string => Boolean(slug)),
  ]);

  for (const slug of entitySlugs) {
    nodes.push({ id: entityId(slug), type: 'entity', label: slug, parent_id: 'root' });
  }
  for (const result of searchResults) {
    nodes.push(pageNode(result));
  }
  for (const [index, fact] of facts.entries()) {
    nodes.push(factNode(fact, index));
  }

  return {
    root: nodes[0],
    nodes,
    edges: nodes
      .filter(node => node.parent_id)
      .map(node => ({ from: node.parent_id as string, to: node.id })),
    pinned: await readPinnedNodeIds(engine),
  };
}

export async function readPinnedNodeIds(engine: BrainEngine): Promise<string[]> {
  const stored = await engine.getConfig(PINS_CONFIG_KEY);
  if (!stored) return readPinFile();
  try {
    const parsed = JSON.parse(stored);
    return Array.isArray(parsed) ? parsed.filter((value): value is string => typeof value === 'string') : [];
  } catch {
    return readPinFile();
  }
}

export async function pinMemoryTreeNodes(engine: BrainEngine, nodeIds: string[]) {
  const merged = unique([...(await readPinnedNodeIds(engine)), ...nodeIds]);
  await engine.setConfig(PINS_CONFIG_KEY, JSON.stringify(merged));
  writePinFile(merged);
  return merged;
}

export async function getMemoryTreeStats(engine: BrainEngine) {
  const [pageRows, factRows] = await Promise.all([
    engine.executeRaw('SELECT COUNT(*) AS count FROM pages'),
    engine.executeRaw('SELECT COUNT(*) AS count FROM facts'),
  ]);
  const pinned = await readPinnedNodeIds(engine);
  return {
    pinned_count: pinned.length,
    totals: {
      pages: Number(pageRows[0]?.count ?? 0),
      facts: Number(factRows[0]?.count ?? 0),
    },
  };
}
