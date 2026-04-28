# Runtime Interfaces

Langbridge exposes runtime functionality through a small set of public surfaces.

## Main Interfaces

- Python package: `langbridge.*`
- Python SDK: `langbridge.client.LangbridgeClient`
- runtime host HTTP API: `/api/runtime/v1/*`
- optional runtime UI: `/` and `/ui`
- optional MCP endpoint: `/mcp`
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
- `langbridge.mcp`
- `langbridge.ui`

## Runtime Host HTTP API

The self-hosted runtime host serves:

- `GET /api/runtime/v1/health`
- `GET /api/runtime/v1/info`
- `GET /api/runtime/v1/datasets`
- `GET /api/runtime/v1/datasets/{dataset_ref}/sync`
- `POST /api/runtime/v1/datasets/{dataset_ref}/sync`
- `POST /api/runtime/v1/datasets/{dataset_ref}/preview`
- `POST /api/runtime/v1/semantic/query`
- `POST /api/runtime/v1/sql/query`
- `POST /api/runtime/v1/agents/ask`
- `GET /api/runtime/v1/connectors`
- `GET /api/runtime/v1/connectors/{connector_name}/sync/resources`
- `GET /api/runtime/v1/connectors/{connector_name}/sync/states`

When the `ui` feature is enabled, the host also serves:

- `GET /`
- `GET /ui`
- `GET /api/runtime/ui/v1/summary`

When the `mcp` feature is enabled, the host also mounts the streamable MCP endpoint at `/mcp`.

The current host serves configured local runtimes.

### SQL Query Scopes

`POST /api/runtime/v1/sql/query` uses an explicit `query_scope` field:

- `semantic`: governed SQL over one runtime semantic model. The SQL `FROM` target must be a semantic model name, and the request compiles through the semantic query path rather than direct source SQL.
- `dataset`: dataset-backed runtime SQL over runtime datasets. `selected_datasets` is optional and narrows planner scope.
- `source`: direct connector or source SQL. This scope requires `connection_name` or `connection_id`.

## SDK Access Patterns

`LangbridgeClient` supports three main runtime-facing modes:

- `LangbridgeClient.local(...)` for in-process configured runtimes
- `LangbridgeClient.for_runtime_host(...)` for the runtime host
- `LangbridgeClient.remote(...)` for automatic detection of the runtime host surface

## Identity Model

Runtime interfaces are workspace-scoped. Runtime-core identity uses:

- `workspace_id`
- `actor_id`
- `roles`
- `request_id`
