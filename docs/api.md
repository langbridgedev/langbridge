# Runtime Interfaces

Langbridge exposes runtime functionality through a small set of public surfaces.

These docs describe runtime interfaces owned by this repository. Hosted
control-plane APIs belong to `langbridge-cloud`.

## Main Interfaces

- Python package: `langbridge.*`
- Python SDK: `langbridge.client.LangbridgeClient`
- runtime host HTTP API: `/api/runtime/v1/*`
- runtime contracts: `langbridge.contracts.*`
- semantic model contract: `docs/semantic-model.md`
- dataset contract: `docs/datasets.md`

## Python Runtime Surface

Use the Python package when you want to:

- embed Langbridge inside an application
- build a configured local runtime
- register connectors or plugins
- compose runtime services directly

Important namespaces:

- `langbridge.runtime`
- `langbridge.client`
- `langbridge.plugins`
- `langbridge.semantic`
- `langbridge.federation`

## Runtime Host HTTP API

The self-hosted runtime host serves runtime-owned endpoints for:

- `GET /api/runtime/v1/health`
- `GET /api/runtime/v1/info`
- `GET /api/runtime/v1/datasets`
- `POST /api/runtime/v1/datasets/{dataset_ref}/preview`
- `POST /api/runtime/v1/semantic/query`
- `POST /api/runtime/v1/sql/query`
- `POST /api/runtime/v1/agents/ask`
- `GET /api/runtime/v1/connectors`
- `GET /api/runtime/v1/connectors/{connector_name}/sync/resources`
- `GET /api/runtime/v1/connectors/{connector_name}/sync/states`
- `POST /api/runtime/v1/connectors/{connector_name}/sync`

The current host serves configured local runtimes.

## SDK Access Patterns

`LangbridgeClient` supports three main runtime-facing modes:

- `LangbridgeClient.local(...)` for in-process configured runtimes
- `LangbridgeClient.for_runtime_host(...)` for the self-hosted runtime host
- `LangbridgeClient.remote(...)` for automatic detection of runtime host versus remote API

The runtime host path is the main self-hosted SDK story in this repo.

## Identity Model

Runtime interfaces are workspace-scoped. Runtime-core identity uses:

- `workspace_id`
- `actor_id`
- `roles`
- `request_id`

If a cloud or product surface carries richer identity, it should be translated at
the boundary into this runtime context.
