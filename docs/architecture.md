# Architecture

Langbridge is a runtime monolith with explicit internal module boundaries.

At a high level, the runtime is made of:

- connectors and plugin registration
- workspace-scoped datasets and dataset policies
- semantic models and semantic query services
- federated planning and execution
- runtime hosting, bootstrap, persistence, and auth
- runtime-safe agent and tool orchestration

The main self-hosted product surface is the runtime host. The worker assembly
still exists, but it is one execution shape inside the runtime, not the runtime
architecture itself.

## Read Next

- `docs/architecture/overview.md`
- `docs/architecture/runtime-boundary.md`
- `docs/architecture/execution-plane.md`
- `docs/architecture/federated-query-engine.md`
- `docs/architecture/hybrid-deployment.md`
