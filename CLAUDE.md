---
type: ssot
owner: governance-team
last_updated: 2026-09-03
last-reviewed: 2026-09-18
---

# CLAUDE.md — GBrain AI Context

    > Session loader for AI work inside `gbrain`.
    > Keep durable engineering rules in [`AGENTS.md`](AGENTS.md) and volatile facts in SSOT files.

    ## Load First

    1. [`AGENTS.md`](AGENTS.md)
    2. [`README.md`](README.md) when present
    3. The source files and tests directly related to the task
    4. Workspace context in [`../../../CLAUDE.md`](../../../CLAUDE.md) when the task crosses project boundaries

    ## Project Role

    - Layer: L2
    - Responsibility: TypeScript 知识数据库与 brain runtime
    - Stack: TypeScript / Bun

    ## Commands

    ```bash
    bun install
bun run test
bun run verify
bun run ci:local
    ```

    ## Safe Editing Rules

    - 信任边界是核心：远程 MCP 调用不能获得本地 CLI 权限。
- 跑测试用 `bun run test`（`scripts/run-unit-parallel.sh`，N 个分片各一个进程），**不要**裸跑 `bun test`。本仓用例不隔离跨文件全局态（`globalThis.fetch`、gateway/transport 单例），单进程跑全量会造出上百个假失败；`*.serial.test.ts` 也必须走分片后的串行段。
- `测试输出不要通过 tail/head 截断，失败信息必须完整保留。`
- `版本与发布规则以项目自身 VERSION/package/CHANGELOG 机制为准。`

    - Do not commit, push, reset, or bump submodule pointers unless the user explicitly asks.
    - Preserve unrelated dirty changes in this repository.
    - Keep Markdown pointed at SSOT files instead of copying generated facts.

    ## Closeout

    ```bash
    git status --short
    uv run --with "pyyaml" python "../../../bin/ssot/doc-ssot-lint.py" --json
    ```

    Report the checks you actually ran and any pre-existing dirty state that remains.
