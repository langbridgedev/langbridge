# Worker Preview Notes

This document is retained for path stability. The repository does not currently document a worker-first development flow as the primary release path.

## Current Direction

The supported development surface in this repository is the runtime host:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml
```

Optional runtime-owned features are enabled on the same host:

- `--features ui`
- `--features mcp`
- `--features ui,mcp`

Coordinator/worker scale-out is still preview groundwork for a later release line. Treat it as implementation direction inside the federation/runtime code, not as the center of the v1 self-hosted product.

## If You Are Looking For Execution Internals

The main runtime execution code lives in:

- `langbridge/runtime/hosting/`
- `langbridge/runtime/services/`
- `langbridge/runtime/execution/`
- `langbridge/federation/`
- `langbridge/orchestrator/`

For local development, use `docs/development/local-dev.md`.
