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
- `POST /api/runtime/v1/datasets/{dataset_ref}/preview`
- `POST /api/runtime/v1/semantic/query`
- `POST /api/runtime/v1/sql/query`
- `POST /api/runtime/v1/agents/ask`
- `GET /api/runtime/v1/connectors`
- `GET /api/runtime/v1/connectors/{connector_name}/sync/resources`
- `GET /api/runtime/v1/connectors/{connector_name}/sync/states`
- `POST /api/runtime/v1/connectors/{connector_name}/sync`
- interactive docs at `/api/runtime/docs`

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
