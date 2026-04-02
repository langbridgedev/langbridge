# Langbridge

Langbridge is an open source, self-hostable analytics runtime for structured data workloads, built for agentic analytics applications. It provides a unified runtime for connecting to data sources, defining semantic models, executing federated queries, and orchestrating agent-style analytical workflows. The runtime is accessible through a Python SDK and a documented HTTP API, and it can optionally serve a lightweight UI and mount an MCP endpoint.

## What You Get

- A runtime host for executing agentic analytics workloads, with support for data source connectors, semantic modeling, federated query planning and execution, and agent orchestration.
- A Python SDK for interacting with the runtime, including tools for defining connectors, semantic models, and agents.
- A built-in UI for visualizing and interacting with runtime execution.
- An MCP endpoint for integrating with external agentic systems.

## Repository Layout

The repository is organized into the following key areas:
- `langbridge/`: core runtime host implementation, including API, execution engine, connector framework, and MCP endpoint
- `langbridge-connectors/`: collection of pre-built connectors for common data sources and services (e.g. Shopify, Salesforce, Stripe, etc.)
- `apps/runtime-ui/`: React source for the runtime UI
- `packages/sdk/`: packaging for the standalone SDK distribution
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

Apply runtime metadata migrations explicitly when you need a controlled rollout:

```bash
langbridge migrate --config examples/runtime_host/langbridge_config.yml
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

Enable the BI / ODBC endpoint:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features odbc --odbc-port 15432
```

Enable both:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features ui,mcp
```

You can also combine all runtime-facing surfaces:

```bash
langbridge serve --config examples/runtime_host/langbridge_config.yml --features ui,mcp,odbc --odbc-port 15432
```

SQLite metadata stores auto-apply Alembic migrations by default on startup. For
managed/self-hosted production deployments, switch `runtime.metadata_store` to
Postgres, run `langbridge migrate --config ...`, and set
`runtime.migrations.auto_apply: false` if you want startup to fail until the DB
is upgraded explicitly.

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

The top-level Docker path is intentionally single-node. The runtime host is designed to be horizontally scalable, and can be run in multiple instances behind a load balancer. The runtime does not currently have any built-in distributed execution capabilities, but it can be configured to use external services for distributed query execution and agent orchestration.

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
- `examples/shopify_sync/`: live Shopify declarative connector sync example
- `examples/hubspot_sync/`: live HubSpot declarative connector sync example
- `examples/sdk/semantic_query/`: local SDK + semantic query walkthrough
- `examples/sdk/federated_query/`: local SDK + federated query walkthrough

# Supported Connectors

## SQL
- Postgres
- MySQL
- Snowflake
- BigQuery
- Redshift
- Databricks
- SQLite
More SQL connectors are in progress, and the connector framework is designed to make it easy to add new ones.
## SaaS/API
- Shopify
- Stripe
- Salesforce
- HubSpot
- Zendesk
- Google Analytics
More SaaS connectors are in progress, and the connector framework is designed to make it easy to add new ones.
## NoSQL
- MongoDB
More NoSQL connectors are in progress, and the connector framework is designed to make it easy to add new ones.
## Vector
- Pinecone
- Faiss
More vector connectors are in progress, and the connector framework is designed to make it easy to add new ones.

# Future Work

## Scale-out architecture with distributed coordinator and worker nodes

Note: the current product center is a strong single-node runtime host. Distributed coordinator/worker scale-out remains preview groundwork in this repo, not the primary v1 deployment path.

## Documentation

Start with:

- `docs/README.md`
- `docs/deployment/self-hosted.md`
- `docs/api.md`
- `docs/development/local-dev.md`
