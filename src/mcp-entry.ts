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
import { loadConfigWithEngine } from './core/config.js';
import { startMcpServer } from './mcp/server.js';

async function main() {
  const config = await loadConfigWithEngine();
  const { engine } = config;
  await startMcpServer(engine);
}

main().catch((err) => {
  console.error('gbrain-mcp failed:', err);
  process.exit(1);
});
