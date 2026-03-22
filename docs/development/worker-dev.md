# Worker Development

This document is retained for path stability, but the repository no longer documents a worker-first development flow.

## Current Direction

The supported development surface in this repository is the runtime host:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml
```

Optional runtime-owned features are enabled on the same host:

- `--features ui`
- `--features mcp`
- `--features ui,mcp`

## If You Are Looking For Execution Internals

The main runtime execution code lives in:

- `langbridge/runtime/hosting/`
- `langbridge/runtime/services/`
- `langbridge/runtime/execution/`
- `langbridge/federation/`
- `langbridge/orchestrator/`

For local development, use `docs/development/local-dev.md`.
