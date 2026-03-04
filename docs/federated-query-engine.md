# Federated Query Engine

This page is retained for backward-compatible linking.

The current engine docs are now split into:
- `docs/architecture/federated-query-engine.md`
- `docs/features/federation.md`
- `docs/architecture/overview.md`

## Current Positioning

- The built-in federated planner + executor is the primary structured data engine.
- Worker runtime is the execution boundary.
- Trino and SQL gateway are deprecated legacy paths and not required for target architecture.
- SQL analyst tools bound to unified semantic models route execution through federation in the same worker job.
