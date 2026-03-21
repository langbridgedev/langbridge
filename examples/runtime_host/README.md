# Runtime Host Example

This example shows the main self-hosted Langbridge runtime surface: the runtime
host serving a configured local runtime over HTTP.

It does not depend on `langbridge-cloud`.

## What This Example Gives You

- a Dockerized runtime host
- mounted runtime config at `/examples/runtime_host/langbridge_config.yml`
- mounted demo SQLite warehouse from `examples/sdk/semantic_query/example.db`
- persistent runtime state under `/examples/runtime_host/.langbridge`
- runtime-owned HTTP endpoints for datasets, semantic query, SQL, agents, and runtime info

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

Run direct SQL:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/sql/query \
  -H "Content-Type: application/json" \
  -d '{
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

Once the host is up, the CLI can call the same runtime-owned endpoints:

```bash
langbridge info --url http://localhost:8000
langbridge datasets list --url http://localhost:8000
langbridge semantic query --url http://localhost:8000 --model commerce_performance --measure shopify_orders.net_sales --dimension shopify_orders.country --limit 5
```
