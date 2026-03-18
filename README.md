# Langbridge

Langbridge is an open runtime for connecting data to LLM and analytical workloads.

It provides a single execution layer for working with operational databases, warehouses,
files, APIs, and virtual datasets without forcing all of your data into one place first.
You can run it locally, self-host it, or embed it into Python applications.

## What Langbridge Does

Langbridge is built around one idea:

**your data should be usable where it already lives.**

Instead of treating every source as an isolated integration, Langbridge provides a runtime
that can:

- connect to different data systems through connectors
- build virtual datasets over those sources
- execute semantic and federated queries
- power retrieval, analysis, and agent-style workflows
- expose runtime APIs for local, self-hosted, and hybrid execution

That makes it useful for products and teams that want to build:

- LLM applications grounded in real business data
- semantic query and analytics experiences
- federated data access across multiple systems
- runtime services for self-hosted or hybrid enterprise deployments
- developer tools that need a portable execution layer

## How To Think About Langbridge

Langbridge is a portable execution layer for data-aware applications.

This repository focuses on:

- connectors
- semantic execution
- federated execution
- virtual datasets
- retrieval and document execution
- analytical and ML-oriented runtime operations
- runtime APIs needed by the engine itself

## Core Concepts

### Connectors

Langbridge connects to different types of systems through runtime connectors.

Examples include:

- SQL databases
- cloud warehouses
- files and object storage
- API-backed data sources

The goal is to make those sources usable through one consistent runtime contract rather than
a collection of one-off integrations.

### Virtual Datasets

Langbridge can represent source data as runtime-managed datasets, whether the source is:

- a physical table
- a SQL definition
- a file
- a federated combination of other datasets

This lets applications work with a stable data model even when the underlying sources differ.

### Semantic And Federated Execution

Langbridge is designed to execute more than raw connector calls.

It supports:

- semantic query workflows over modeled data
- federated execution across multiple sources
- runtime-side policy enforcement such as limits and redaction
- execution planning that stays portable across local, hosted, and hybrid environments

### Runtime Modes

Langbridge is intended to work in more than one deployment shape:

- **Embedded**: use the runtime from Python inside your own application
- **Local**: run the runtime for development on your own machine
- **Self-hosted**: deploy the runtime inside your own infrastructure
- **Hybrid**: run the runtime in customer infrastructure while integrating with external systems

## Repository Scope

This repository is the home of the Langbridge runtime.

It is focused on portable execution concerns and reusable runtime packages that can be used
across local development, self-hosted deployments, and embedded application scenarios.

## Repository Layout

Canonical runtime package surfaces now live under the root `langbridge.*`
namespace:

- `langbridge.contracts`
- `langbridge.runtime`
- `langbridge.federation`
- `langbridge.semantic`
- `langbridge.orchestrator`
- `langbridge.hosting`
- `langbridge.plugins`

The legacy `langbridge.packages.*` layout remains in place as an incremental
compatibility layer while the monolith namespace is normalized.

Important areas in this repository:

- `langbridge/packages/runtime/` - runtime services, providers, and execution logic
- `langbridge/packages/federation/` - federated planning and execution engine
- `langbridge/packages/semantic/` - semantic execution and semantic model logic
- `langbridge/packages/connectors/` - official connector implementations published as the separate `langbridge-connectors` package
- `langbridge/packages/contracts/` - transitional compatibility layout behind the canonical `langbridge.contracts` surface
- `langbridge/apps/runtime_worker/` - thin runtime worker assembly for local, self-hosted, and hybrid execution
- `docs/` - architecture, deployment, and development documentation

## Getting Started

### Run The Runtime Worker

For local runtime development:

```bash
python -m langbridge.apps.runtime_worker.main
```

### Run The Runtime Host

If you want to host the portable runtime as an HTTP API directly, use the CLI entrypoint:

```bash
langbridge serve --config /path/to/langbridge_config.yml --host 0.0.0.0 --port 8000
```

If you installed the package with `pip install langbridge`, the module entrypoint works too:

```bash
python -m langbridge serve --config /path/to/langbridge_config.yml --host 0.0.0.0 --port 8000
```

There is a Dockerized example in
`examples/runtime_host/README.md` that mounts a runtime config into the container and
starts the same host command.

### Run The Local Runtime Stack

From this repository:

```bash
docker compose up --build db redis worker
```

### Run The Runtime Host Example

Seed the shared configured-local example:

```bash
python examples/sdk/semantic_query/setup.py
```

Then either start the host with the CLI:

```bash
pip install -e .
langbridge serve --config examples/sdk/semantic_query/langbridge_config.yml --host 127.0.0.1 --port 8000
```

Or start the same host with Docker:

```bash
docker compose --profile host up --build runtime-host
```

The runtime API docs are served at `http://127.0.0.1:8000/api/runtime/docs`. The full
example walkthrough lives in `examples/runtime_host/README.md`.

For a sync-focused self-hosted walkthrough, including connector discovery, hosted sync,
sync state inspection, and previewing the managed dataset that gets materialized at
runtime, use `examples/runtime_host_sync/README.md`.

## Development

Useful docs:

- `docs/architecture/runtime-boundary.md`
- `docs/architecture/execution-plane.md`
- `docs/development/local-dev.md`
- `docs/development/worker-dev.md`
- `docs/features/semantic.md`
- `docs/features/federation.md`
- `docs/features/agents.md`

## Design Principles

Langbridge is being shaped around a few rules:

- keep execution portable
- prefer explicit contracts between systems
- support self-hosted and hybrid use cases as first-class deployment models
- make connectors and execution capabilities reusable as packages, not just app code

## Status

Langbridge is actively evolving toward a cleaner package-oriented runtime architecture.

The direction is:

- thin assembly apps only where needed
- one canonical `langbridge.*` runtime namespace with modular internal boundaries
- `langbridge-connectors` for official installable connectors
- versioned runtime artifacts for downstream consumers

## License

See the license file in this repository for licensing terms.
