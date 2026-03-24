# Langbridge

Langbridge is an open source, self-hostable analytics runtime for structured data workloads.

It gives you a single runtime for connectors, datasets, semantic models, federated query, SQL execution, SaaS sync, and agent-style analytical workflows. You can run it in process through Python, expose it over HTTP, serve a lightweight UI, and optionally mount an MCP endpoint from the same runtime host.

The current product center is a strong single-node runtime host. Distributed coordinator/worker scale-out remains preview groundwork in this repo, not the primary v1 deployment path.

## What You Get

- A runtime host with a documented HTTP API under `/api/runtime/v1/*`
- A Python SDK for local and remote access
- Built-in connectors for SQL, SaaS/API, NoSQL, and vector systems
- Semantic query and federated query execution
- Dataset preview and connector sync flows
- Optional runtime UI served by the host
- Optional MCP surface mounted at `/mcp`

## Repository Layout

The main runtime modules live under `langbridge.*`:

- `langbridge.runtime`: runtime context, host construction, auth, services, persistence
- `langbridge.client`: Python SDK
- `langbridge.connectors`: built-in connector implementations
- `langbridge.plugins`: connector registry and extension surface
- `langbridge.semantic`: semantic model loading and semantic query support
- `langbridge.federation`: federated planning and execution
- `langbridge.orchestrator`: runtime-safe agent and tool orchestration
- `langbridge.mcp`: MCP server assembly
- `langbridge.ui`: packaged UI bundle served by the runtime host

Supporting project areas:

- `apps/runtime_ui`: React source for the runtime UI
- `packages/sdk`: packaging for the standalone SDK distribution
- `examples/`: runnable host and SDK examples
- `docs/`: architecture, deployment, and development docs

## Quick Start

Create an environment and install the runtime:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements/dev.txt
pip install -e .
```

Seed the local demo data:

```bash
python examples/sdk/semantic_query/setup.py
```

Start the runtime host:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --host 127.0.0.1 --port 8000
```

Open the runtime API docs at `http://127.0.0.1:8000/api/runtime/docs`.

Enable the runtime UI:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features ui
```

Enable the MCP endpoint:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features mcp
```

Enable both:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features ui,mcp
```

## Runtime UI

The runtime UI is source-controlled in `apps/runtime_ui` and built into `langbridge/ui/static`, which the Python host serves when `ui` is enabled.

For local UI development:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features ui
cd apps/runtime_ui
npm install
npm run dev
```

To build the production UI bundle:

```bash
cd apps/runtime_ui
npm install
npm run build
```

## Docker

Build and run the runtime host container:

```bash
docker compose --profile host up --build runtime-host
```

The runtime image is defined in `docker/Dockerfile`.

The top-level Docker path is intentionally single-node. Preview distributed execution internals remain in the federation layer, but this repo does not currently position coordinator/worker packaging as the main release-ready self-hosted path.

## Identity And Auth

Langbridge runtime execution is workspace-scoped. The core identity carried through the runtime is:

- `workspace_id`
- `actor_id`
- `roles`
- `request_id`

The runtime host supports thin auth modes:

- `none`
- `static_token`
- `jwt`

## Examples

- `examples/runtime_host/`: self-hosted runtime host over a local config
- `examples/runtime_host_sync/`: connector sync example
- `examples/sdk/semantic_query/`: local SDK + semantic query walkthrough
- `examples/sdk/federated_query/`: local SDK + federated query walkthrough

## Documentation

Start with:

- `docs/README.md`
- `docs/deployment/self-hosted.md`
- `docs/api.md`
- `docs/development/local-dev.md`
