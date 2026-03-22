# Architecture

Langbridge is a runtime monolith with clear internal boundaries and one primary product surface: the runtime host.

At a high level, the runtime is made of:

- connectors and plugin registration
- workspace-scoped datasets and dataset policies
- semantic models and semantic query services
- federated planning and execution
- runtime hosting, persistence, auth, MCP, and UI serving
- runtime-safe agent and tool orchestration

## Read Next

- `docs/architecture/overview.md`
- `docs/architecture/runtime-boundary.md`
- `docs/architecture/execution-plane.md`
- `docs/architecture/federated-query-engine.md`
- `docs/architecture/hybrid-deployment.md`
