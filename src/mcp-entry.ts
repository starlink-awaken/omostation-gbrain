#!/usr/bin/env ts-node
/**
 * gbrain-mcp — MCP stdio server entry point.
 * Initializes BrainEngine and starts stdio-based MCP server
 * for integration with Agora / agentmesh WorkspaceMCPClient.
 *
 * Usage:
 *   npx tsx src/mcp-entry.ts
 *   # or after build:
 *   node dist/mcp-entry.js
 */
import { loadConfig } from './core/config.js';
import { createEngine } from './core/engine-factory.js';
import { startMcpServer } from './mcp/server.js';

async function main() {
  const baseConfig = loadConfig();
  const engine = await createEngine((baseConfig || {}) as any);
  // Do NOT hold the PGLite write lock for the MCP session's whole lifetime:
  // connect/disconnect wraps each tools/call instead (lazyConnect), so
  // concurrent CLI consumers (gbrain CLI, MOS live recall) never time out
  // waiting for the lock while an agent keeps this server open.
  await startMcpServer(engine, { lazyConnect: (baseConfig || {}) as any });
}

main().catch((err) => {
  console.error('gbrain-mcp failed:', err);
  process.exit(1);
});
