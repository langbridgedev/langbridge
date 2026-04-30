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

The dev requirements also include packaging tools for release work. To build the
runtime package locally:

```bash
python -m build --no-isolation
```

Seed the local demo data:

```bash
python examples/sdk/semantic_query/setup.py
```

Start the runtime host:

```bash
langbridge serve --config examples/deployment/runtime_host/langbridge_config.yml --host 127.0.0.1 --port 8000
```

Apply runtime metadata migrations explicitly when you need a controlled rollout:

```bash
langbridge migrate --config examples/deployment/runtime_host/langbridge_config.yml
```

Open the runtime API docs at `http://127.0.0.1:8000/api/runtime/docs`.

Enable the runtime UI:

```bash
langbridge serve --config examples/deployment/runtime_host/langbridge_config.yml --features ui
```

Enable the MCP endpoint:

```bash
langbridge serve --config examples/deployment/runtime_host/langbridge_config.yml --features mcp
```

Enable the BI / ODBC endpoint:

```bash
langbridge serve --config examples/deployment/runtime_host/langbridge_config.yml --features odbc --odbc-port 15432
```

Enable both:

```bash
langbridge serve --config examples/deployment/runtime_host/langbridge_config.yml --features ui,mcp
```

You can also combine all runtime-facing surfaces:

```bash
langbridge serve --config examples/deployment/runtime_host/langbridge_config.yml --features ui,mcp,odbc --odbc-port 15432
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
langbridge serve --config examples/deployment/runtime_host/langbridge_config.yml --features ui
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

## Packaging And CodeArtifact

Build both Python distributions from the repo root:

```bash
make build
```

Build only the runtime package:

```bash
make build-runtime
```

Build only the SDK package:

```bash
make build-sdk
```

Publish the runtime package to AWS CodeArtifact with GNU Make after configuring
an IAM user or role that can call `codeartifact:GetAuthorizationToken`,
`codeartifact:PublishPackageVersion`, and `sts:GetServiceBearerToken`. The
`aws` CLI command must already be installed on your machine:

```bash
make publish-codeartifact \
  CODEARTIFACT_DOMAIN=langbridge \
  CODEARTIFACT_DOMAIN_OWNER=060795918689 \
  CODEARTIFACT_REPOSITORY=langbridge \
  AWS_REGION=eu-west-2
```

Use `aws codeartifact login --tool twine` for publishing. `--tool pip` rewrites
your pip index configuration for package installs, so if the CodeArtifact
repository does not proxy public PyPI, local installs such as `pip install -r
requirements/dev.txt` will fail until pip is pointed back at PyPI or the
repository is configured with an upstream. Once the dev environment is already
provisioned, `python -m build --no-isolation` avoids that isolated-env fetch.

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

- `examples/golden/`: golden production-style runtime demo surface
- `examples/deployment/runtime_host/`: self-hosted runtime host over a local config
- `examples/deployment/runtime_host_sync/`: connector sync example
- `examples/deployment/customer_connector_image/`: customer-specific Docker image with selected connector packages
- `examples/connectors/shopify_sync/`: live Shopify declarative connector sync example
- `examples/connectors/hubspot_sync/`: live HubSpot declarative connector sync example
- `examples/sdk/semantic_query/`: local SDK + semantic query walkthrough
- `examples/sdk/federated_query/`: local SDK + federated query walkthrough
- `examples/legacy/complex_runtime/`: legacy complex local runtime example retained while `golden/` evolves

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
