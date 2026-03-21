# Langbridge

Langbridge is the runtime product in the Langbridge platform.

This repository owns the portable execution layer: local and self-hosted runtime
hosting, workspace-scoped runtime identity, connectors, datasets, semantic query,
federated query, and agent-oriented execution primitives.

`langbridge-cloud` is the separate cloud and control-plane product. It owns the
hosted product surfaces, control-plane APIs, web UI, and cloud orchestration
experience. This repository should only describe that boundary, not duplicate the
cloud implementation.

## What Lives Here

Langbridge is a Python monolith package with internal modules under the
`langbridge.*` namespace. The main runtime surfaces are:

- `langbridge.runtime`: runtime context, hosting, bootstrap, services, providers, persistence
- `langbridge.client`: Python SDK for local runtime, runtime host, or remote API access
- `langbridge.connectors`: built-in connector implementations
- `langbridge.plugins`: connector and plugin registration surface
- `langbridge.semantic`: semantic model contracts, loaders, and semantic query support
- `langbridge.federation`: federated planning and execution engine
- `langbridge.orchestrator`: runtime-safe agent and tool orchestration
- `langbridge.contracts`: runtime and API-facing contracts
- `langbridge.hosting`: public hosting namespace for the runtime host

Supporting runtime assembly and packaging code lives in:

- `apps/runtime_worker`: thin queued/edge worker assembly
- `packages/sdk`: packaging for the separate `langbridge-sdk` distribution
- `docs/`: architecture, deployment, and development docs
- `examples/`: runnable self-hosted and SDK examples

The old `langbridge.packages.*` architecture is no longer the repo story and
should not be used as the primary mental model.

## Runtime Model

Langbridge runtime execution is workspace-scoped.

The core execution identity is:

- `workspace_id`
- `actor_id`
- `roles`
- `request_id`

Self-hosted runtime auth is intentionally thin. The runtime host supports:

- `none`
- `static_token`
- `jwt`

Runtime-core execution does not center on org, project, or tenant claims. Those
may exist in product or control-plane systems, but the runtime executes against a
workspace-scoped context.

## Connectors And Plugins

Langbridge uses a plugin-style connector model:

- built-in connectors live under `langbridge.connectors.*`
- the registry and connector interfaces live under `langbridge.plugins`
- external packages can register connectors through entry points

Today the runtime includes connector families for SQL, SaaS/API, NoSQL, and
vector workloads. SaaS/API connectors sync external resources into
runtime-managed datasets instead of treating third-party APIs as the primary
query-time execution substrate.

## Self-Hosted Runtime

The main self-hosted surface today is the runtime host:

```bash
langbridge serve --config /path/to/langbridge_config.yml --host 0.0.0.0 --port 8000
```

That host serves runtime-owned endpoints under `/api/runtime/v1/` for:

- runtime info and health
- datasets list and preview
- semantic query
- SQL query
- agent ask
- connector discovery and connector sync

The host currently serves configured local runtimes. It is a core product
surface, not a temporary demo wrapper.

The queued worker still exists under `apps/runtime_worker`, but it should be
understood as a thin runtime-owned assembly for queued, hosted, or edge-style
execution, not the definition of the runtime product itself.

## Quick Start

Create an environment and install the runtime:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Seed the local demo data:

```bash
python examples/sdk/semantic_query/setup.py
```

Run the self-hosted runtime host:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --host 127.0.0.1 --port 8000
```

Or use Docker:

```bash
docker compose --profile host up --build runtime-host
```

If you need the queued worker stack instead:

```bash
docker compose up --build db redis worker
```

## Examples

- `examples/runtime_host/`: self-hosted runtime host over a local config
- `examples/runtime_host_sync/`: self-hosted SaaS connector sync example
- `examples/sdk/semantic_query/`: local SDK + semantic query walkthrough
- `examples/sdk/federated_query/`: local SDK + federated query walkthrough

## Documentation

Start with:

- `docs/README.md`
- `docs/architecture/runtime-boundary.md`
- `docs/deployment/self-hosted.md`
- `docs/development/local-dev.md`

## Runtime / Cloud Boundary

Use this repo for:

- runtime execution, hosting, and runtime contracts
- runtime-owned connectors, federation, semantic, datasets, and orchestration
- self-hosted and embedded runtime product surfaces

Use `../langbridge-cloud` for:

- hosted control-plane API work
- hosted worker orchestration
- web product surfaces
- cloud migrations and cloud-only operational tooling

## License

See `LICENSE`.
