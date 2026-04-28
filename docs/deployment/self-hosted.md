# Self-Hosted Deployment

Self-hosted deployment means running the Langbridge runtime in your own environment.

The primary supported shape in this release is a single runtime host process. Distributed coordinator/worker scale-out remains preview direction and is not the main self-hosted deployment contract yet.

## Runtime Host

Start the runtime host from an installed environment:

```bash
langbridge serve --config /path/to/langbridge_config.yml --host 0.0.0.0 --port 8000
```

Or:

```bash
python -m langbridge serve --config /path/to/langbridge_config.yml --host 0.0.0.0 --port 8000
```

The current host serves configured local runtimes and exposes:

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
- interactive docs at `/api/runtime/docs`

`POST /api/runtime/v1/sql/query` is an explicit scoped SQL surface:

- `query_scope: "semantic"` runs governed SQL against one semantic model and delegates into the semantic execution path
- `query_scope: "dataset"` runs dataset-backed runtime SQL over runtime datasets
- `query_scope: "source"` runs direct connector or source SQL and requires `connection_name` or `connection_id`

## Runtime Metadata Migrations

Runtime metadata schema changes are now managed by Alembic revisions in the
runtime repo root.

Apply them explicitly with:

```bash
langbridge migrate --config /path/to/langbridge_config.yml
```

## Runtime Metadata Store

Configured local runtimes now treat metadata persistence as an explicit deployment choice:

- `sqlite`: default for normal local and self-hosted runtime deployments
- `postgres`: recommended for production and self-managed hosted runtime metadata
- `in_memory`: explicit ephemeral mode for tests, notebooks, and throwaway local sessions

Supported config shape:

```yaml
runtime:
  migrations:
    auto_apply: true
  metadata_store:
    type: sqlite | postgres | in_memory
    path: .langbridge/metadata.db   # sqlite only
    url: postgresql://...           # postgres only
```

If `runtime.metadata_store` is omitted, the configured runtime uses SQLite at
`.langbridge/metadata.db` relative to the config file directory.

### SQLite

```yaml
runtime:
  metadata_store:
    type: sqlite
    path: .langbridge/metadata.db
```

This is the default self-hosted path and the runtime initializes the metadata
schema with Alembic automatically on `langbridge serve` unless you disable:

```yaml
runtime:
  migrations:
    auto_apply: false
  metadata_store:
    type: sqlite
    path: .langbridge/metadata.db
```

### Postgres

```yaml
runtime:
  metadata_store:
    type: postgres
    url: postgresql://langbridge:secret@db.example.com:5432/langbridge
```

The runtime normalizes the configured Postgres URL for its sync and async
SQLAlchemy engines. For controlled deployments, run `langbridge migrate` first
and then start the host with `runtime.migrations.auto_apply: false`.

### In-Memory

```yaml
runtime:
  metadata_store:
    type: in_memory
```

Use this only when you explicitly want metadata to disappear when the runtime
process exits.

## Startup Behavior

- `runtime.migrations.auto_apply: true` is the default and will upgrade sqlite
  or postgres metadata stores before host startup.
- `runtime.migrations.auto_apply: false` disables startup migration. If the
  schema is behind, `langbridge serve` fails with a clear message telling the
  operator to run `langbridge migrate --config ...`.
- Existing unversioned runtime metadata databases that already match the current
  schema are stamped into Alembic on the first explicit migrate or auto-apply.

## Scheduled Dataset Sync

The self-hosted runtime host can run dataset-owned sync in-process through the
existing runtime background task manager. This slice does not add a separate
worker or queue system.

Example:

```yaml
datasets:
  - name: billing_customers
    connector: billing_demo
    materialization_mode: synced
    sync:
      source:
        resource: customers
      cadence: 1h
      sync_on_start: true
```

Rules:

- the dataset must be `materialization_mode: synced`
- the dataset must have a valid dataset-owned sync contract
- supported `sync.cadence` values are interval shorthands such as `30s`, `5m`,
  `1h`, and `1d`
- `sync.sync_on_start: true` runs one sync during runtime host startup
- scheduled tasks are registered with names like `dataset-sync:billing_customers`

## Optional Runtime Features

Enable the runtime UI:

```bash
langbridge serve --config /path/to/langbridge_config.yml --features ui
```

Enable the MCP endpoint:

```bash
langbridge serve --config /path/to/langbridge_config.yml --features mcp
```

Enable both:

```bash
langbridge serve --config /path/to/langbridge_config.yml --features ui,mcp
```

When enabled:

- the runtime UI is served at `/` and `/ui`
- the MCP endpoint is mounted at `/mcp`

## Runtime Identity

Runtime requests execute with a workspace-scoped context:

- `workspace_id`
- `actor_id`
- `roles`
- `request_id`

`request_id` is sourced from `X-Request-Id` or `X-Correlation-Id` when present.

## Thin Runtime Auth

The runtime host supports three auth modes:

- `none`
- `static_token`
- `jwt`

### No Auth

```bash
export LANGBRIDGE_RUNTIME_AUTH_MODE=none
```

### Static Token

```bash
export LANGBRIDGE_RUNTIME_AUTH_MODE=static_token
export LANGBRIDGE_RUNTIME_AUTH_STATIC_TOKEN=runtime-token
export LANGBRIDGE_RUNTIME_AUTH_STATIC_WORKSPACE_ID=<workspace-uuid>
export LANGBRIDGE_RUNTIME_AUTH_STATIC_ACTOR_ID=<actor-uuid>
export LANGBRIDGE_RUNTIME_AUTH_STATIC_ROLES=runtime:viewer,dataset:preview
```

Clients must send:

```text
Authorization: Bearer runtime-token
```

### JWT

```bash
export LANGBRIDGE_RUNTIME_AUTH_MODE=jwt
export LANGBRIDGE_RUNTIME_AUTH_JWT_SECRET=<shared-secret>
```

Or configure JWKS:

```bash
export LANGBRIDGE_RUNTIME_AUTH_MODE=jwt
export LANGBRIDGE_RUNTIME_AUTH_JWT_JWKS_URL=https://issuer.example/.well-known/jwks.json
```

Supported JWT mapping settings:

- `LANGBRIDGE_RUNTIME_AUTH_JWT_ALGORITHMS`
- `LANGBRIDGE_RUNTIME_AUTH_JWT_ISSUER`
- `LANGBRIDGE_RUNTIME_AUTH_JWT_AUDIENCE`
- `LANGBRIDGE_RUNTIME_AUTH_JWT_WORKSPACE_CLAIM`
- `LANGBRIDGE_RUNTIME_AUTH_JWT_ACTOR_CLAIM`
- `LANGBRIDGE_RUNTIME_AUTH_JWT_ROLES_CLAIM`
- `LANGBRIDGE_RUNTIME_AUTH_JWT_SUBJECT_CLAIM`

### Local Operator Bootstrap And Browser Login

When runtime auth is enabled, the self-hosted UI can also use a runtime-owned local operator session.
This stays single-workspace and runtime-first:

- no public signup
- no org, project, or membership model
- first login on a fresh secured runtime bootstraps the first admin
- local auth persists in the configured `runtime.metadata_store`
- `in_memory` metadata keeps auth/session state ephemeral
- `sqlite` and `postgres` metadata persist auth/session state in the runtime metadata DB
- bearer clients can keep using `static_token` or `jwt`

Optional settings:

- `LANGBRIDGE_RUNTIME_AUTH_LOCAL_ENABLED`
- `LANGBRIDGE_RUNTIME_AUTH_LOCAL_COOKIE_NAME`
- `LANGBRIDGE_RUNTIME_AUTH_LOCAL_SESSION_MAX_AGE_SECONDS`
- `LANGBRIDGE_RUNTIME_AUTH_LOCAL_SESSION_SECRET`

## Local And Docker Start Paths

From this repo:

```bash
python examples/sdk/semantic_query/setup.py
langbridge serve --config examples/runtime_host/langbridge_config.yml --host 127.0.0.1 --port 8000
```

Or with Docker:

```bash
docker compose --profile host up --build runtime-host
```

For runnable walkthroughs, use:

- `examples/runtime_host/`
- `examples/runtime_host_sync/`
- `examples/shopify_sync/`
- `examples/hubspot_sync/`
