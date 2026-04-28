# Runtime Host Example

This example shows the main self-hosted Langbridge runtime surface: the runtime
host serving a configured local runtime over HTTP.

## What This Example Gives You

- a Dockerized runtime host
- mounted runtime config at `/examples/runtime_host/langbridge_config.yml`
- mounted demo SQLite warehouse from `examples/sdk/semantic_query/example.db`
- persistent runtime state under `/examples/runtime_host/.langbridge`
- runtime-owned HTTP endpoints for datasets, semantic query, scoped SQL, agents, and runtime info

The example config keeps runtime metadata in SQLite at
`examples/runtime_host/.langbridge/metadata.db`. For production or
self-managed hosted deployments, switch `runtime.metadata_store` to `postgres`.
This example leaves `runtime.migrations.auto_apply: true`, so `langbridge serve`
upgrades the runtime metadata schema automatically before startup.

## Prerequisites

From the repository root, seed the demo database once:

```bash
python examples/sdk/semantic_query/setup.py
```

If you want to call the default agent, export an LLM key before starting:

```bash
export OPENAI_API_KEY=...
```

## Start The Runtime Host

From this directory:

```bash
docker compose up --build
```

The host will listen on `http://localhost:8000`.

## Try The API

Health:

```bash
curl http://localhost:8000/api/runtime/v1/health
```

Runtime info:

```bash
curl http://localhost:8000/api/runtime/v1/info
```

List datasets:

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

The configured dataset in this example is explicitly `materialization_mode: live`.

Preview a dataset:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_orders/preview \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

Run a semantic query:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/semantic/query \
  -H "Content-Type: application/json" \
  -d '{
    "semantic_models": ["commerce_performance"],
    "measures": ["shopify_orders.net_sales"],
    "dimensions": ["shopify_orders.country"],
    "order": [{"member": "shopify_orders.net_sales", "direction": "desc"}],
    "limit": 5
  }'
```

Run semantic SQL:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/sql/query \
  -H "Content-Type: application/json" \
  -d '{
    "query_scope": "semantic",
    "query": "SELECT country, net_sales FROM commerce_performance ORDER BY net_sales DESC LIMIT 5"
  }'
```

Run dataset SQL:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/sql/query \
  -H "Content-Type: application/json" \
  -d '{
    "query_scope": "dataset",
    "query": "SELECT country, COUNT(*) AS order_count FROM shopify_orders GROUP BY country ORDER BY order_count DESC LIMIT 5"
  }'
```

Run source SQL:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/sql/query \
  -H "Content-Type: application/json" \
  -d '{
    "query_scope": "source",
    "query": "SELECT country, SUM(net_revenue) AS net_sales FROM orders_enriched GROUP BY country ORDER BY net_sales DESC LIMIT 5",
    "connection_name": "commerce_demo"
  }'
```

Ask the default analytics agent:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/agents/ask \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Which countries have the highest net sales this quarter?"
  }'
```

The agent in [`examples/runtime_host/langbridge_config.yml`](/home/callumwhi/langbridgedev/langbridge/examples/runtime_host/langbridge_config.yml)
is configured through `agents[].definition.tools`, which is the canonical local
runtime authoring model for SQL and semantic tool bindings.

## Runtime Identity And Auth

By default this example runs without host auth. If you enable host auth, send a
bearer token and let the host map that request into runtime identity:

- `workspace_id`
- `actor_id`
- `roles`
- `request_id`

See `docs/deployment/self-hosted.md` for the exact auth environment variables.

## Run The Same Host Without Docker

```bash
pip install -e .
python examples/sdk/semantic_query/setup.py
langbridge serve --config examples/runtime_host/langbridge_config.yml --host 0.0.0.0 --port 8000
```

Or:

```bash
python -m langbridge serve --config examples/runtime_host/langbridge_config.yml --host 0.0.0.0 --port 8000
```

If you want the production-style migration flow instead, run:

```bash
langbridge migrate --config examples/runtime_host/langbridge_config.yml
```

Then set `runtime.migrations.auto_apply: false` in the config before starting
the host.

Once the host is up, the CLI can call the same runtime-owned endpoints:

```bash
langbridge info --url http://localhost:8000
langbridge datasets list --url http://localhost:8000
langbridge semantic query --url http://localhost:8000 --model commerce_performance --measure shopify_orders.net_sales --dimension shopify_orders.country --limit 5
```
