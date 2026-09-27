/**
 * Preload: give bun's test hooks (beforeAll/beforeEach/afterAll/afterEach)
 * a 60s default timeout when the caller didn't pass one explicitly.
 *
 * Why this exists:
 *   bun's per-hook default timeout is 5s, and the `timeout` key in
 *   bunfig.toml applies to *test bodies only* — it does NOT reach hooks
 *   (verified on bun 1.3.14: an 8s beforeAll fails at ~5005ms with
 *   `timeout = 60_000` in bunfig, and `hookTimeout` / BUN_TEST_TIMEOUT /
 *   BUN_TIMEOUT are all ignored). The CLI flag `bun test --timeout=60000`
 *   DOES cover hooks, which is what scripts/run-unit-shard.sh already
 *   passes — so CI was protected while bare `bun test <file>` was not.
 *
 * Disk/in-memory PGLite cold start + initSchema() (85 migrations) runs
 * 5-26s on loaded machines, so those 5s hooks fail spuriously on every
 * bare local run. This preload makes the bunfig-declared 60s ceiling
 * actually apply to hooks without touching the ~186 test files that
 * register slow hooks without an explicit timeout.
 *
 * Explicit per-hook timeouts keep their own value (second arg is passed
 * through untouched), so existing 30_000 / 60_000 annotations still win.
 *
 * Imported by `bunfig.toml` via `preload`.
 */
import { mock } from 'bun:test';

const DEFAULT_HOOK_TIMEOUT = 60_000;

function wrapHook(hook: (...args: any[]) => any) {
  return (...args: any[]) => {
    // beforeAll(fn, timeout?) — only inject when no explicit timeout.
    if (args.length >= 2 && args[1] !== undefined) return hook(...args);
    return hook(args[0], DEFAULT_HOOK_TIMEOUT);
  };
}

const real = await import('bun:test');

mock.module('bun:test', () => ({
  ...real,
  beforeAll: wrapHook(real.beforeAll),
  beforeEach: wrapHook(real.beforeEach),
  afterAll: wrapHook(real.afterAll),
  afterEach: wrapHook(real.afterEach),
}));

if (process.env.GBRAIN_DEBUG_PRELOAD === '1') {
  console.error(`[hook-timeout-preload] default hook timeout = ${DEFAULT_HOOK_TIMEOUT}ms`);
}
