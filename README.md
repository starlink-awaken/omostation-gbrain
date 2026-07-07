# GBrain

    > L2 · TypeScript 知识数据库与 brain runtime
    > Metadata SSOT: [`../../docs/project-registry.yaml`](../../docs/project-registry.yaml)

    ## What It Owns

    TypeScript 知识数据库与 brain runtime.

    ## Quick Start

    ```bash
    bun install
bun test
bun run verify
bun run ci:local
    ```

    ## Key Surfaces

    - `src/core/`
- `src/commands/`
- `src/mcp/`
- `test/`
- `docs/`

    ## Documentation

    - Developer guide: [`AGENTS.md`](AGENTS.md)
    - AI context loader: [`CLAUDE.md`](CLAUDE.md) when present
    - Workspace architecture: [`../../ARCHITECTURE.md`](../../ARCHITECTURE.md)
    - Layer placement: [`../../LAYER-INDEX.md`](../../LAYER-INDEX.md)


## Notes

- See upstream docs under docs/ for integrations, sync, OAuth, and skillpacks.
- Respect privacy and trust-boundary checks before publishing examples.

    ## SSOT Rules

    Runtime facts, counts, ports, health, and generated inventories are intentionally not maintained here. Use the workspace registries and project source as the truth.
## Project Governance

- [Contributing](CONTRIBUTING.md)
- [Security Policy](SECURITY.md)
- [Changelog](CHANGELOG.md)
- [License](LICENSE)
- [Code of Conduct](CODE_OF_CONDUCT.md)
