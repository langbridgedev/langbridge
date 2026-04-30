# Shopify Sync Example

This example runs a self-hosted Langbridge runtime host configured with the
declarative Shopify connector and syncs live Shopify Admin API resources into
declared synced datasets. It now showcases dataset-owned sync configuration:
each dataset declares its own `materialization_mode` and `sync.source.resource`.

## What This Example Covers

- a configured Shopify connector under `examples/connectors/shopify_sync/langbridge_config.yml`
- predeclared synced datasets that choose the Shopify resource at the dataset layer
- runtime-managed sync state persisted under `.langbridge/metadata.db`
- synced datasets materialized into the local DuckDB execution store
- a manifest-listed parent dataset resource: `customers`
- an explicit child resource path dataset: `products.options`
- a dynamic dataset-selected resource: `price_rules`
- explicit 1:1 flattening with `sync.source.flatten`

## Prerequisites

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements/dev.txt
pip install -e .
```

When you run from this source checkout, Langbridge auto-discovers connector
packages under `langbridge-connectors/`, so you do not need a separate
`pip install -e ./langbridge-connectors/langbridge-connector-shopify`.

Export the Shopify credentials that the example config resolves through runtime
secret references:

```bash
export SHOPIFY_SHOP_DOMAIN=acme.myshopify.com
export SHOPIFY_ACCESS_TOKEN=shpat_...
```

If you are using a Shopify app created in the Dev Dashboard, you can fetch the
Admin API access token directly with the helper script in this example. The
script uses Shopify's current client-credentials token endpoint and can update
`examples/connectors/shopify_sync/.env` for you:

```bash
export SHOPIFY_SHOP_DOMAIN=acme.myshopify.com
export SHOPIFY_CLIENT_ID=your_shopify_client_id
export SHOPIFY_CLIENT_SECRET=your_shopify_client_secret

python examples/connectors/shopify_sync/get_shopify_access_token.py --write-env
```

The script will print shell exports and write `SHOPIFY_ACCESS_TOKEN` into the
example `.env` file. If you only want the token value, use:

```bash
python examples/connectors/shopify_sync/get_shopify_access_token.py --raw
```

Notes:

- this helper is for Dev Dashboard apps installed on your target shop
- Shopify returns short-lived access tokens for this flow, so rerun the script when the token expires
- admin-created custom apps still generate their Admin API token inside Shopify admin
- if you are building for another merchant rather than your own store, this is not the right auth flow
- the app must already be installed on the shop with the Admin API scopes you need

## Start The Runtime Host

From the repository root:

```bash
langbridge serve --config examples/connectors/shopify_sync/langbridge_config.yml --host 127.0.0.1 --port 8000
```

The host will listen on `http://localhost:8000`.

## Discover The Connector

List configured connectors:

```bash
curl http://localhost:8000/api/runtime/v1/connectors
```

The connector payload includes explicit capability flags. `shopify_demo`
is an API sync connector, but the dataset decides whether a given dataset is
declared as `synced`.

List syncable Shopify resources:

```bash
curl http://localhost:8000/api/runtime/v1/connectors/shopify_demo/sync/resources
```

Because this example predeclares datasets, the resource list will include both:

- `customers`, which comes from the packaged connector manifest
- `products.options`, which is surfaced because a dataset explicitly owns that child resource path
- `price_rules`, which is surfaced because the dataset requested it even though it is not statically listed

List the configured datasets:

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

You should see:

- `shopify_customers`
- `shopify_product_options`
- `shopify_price_rules`

## Run A Sync

Sync the manifest-listed `customers` dataset incrementally:

```bash
SYNC_RESPONSE=$(curl -s -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_customers/sync \
  -H "Content-Type: application/json" \
  -d '{
    "sync_mode": "INCREMENTAL"
  }')

printf '%s\n' "$SYNC_RESPONSE"
```

Sync the dataset-selected dynamic `price_rules` resource:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_price_rules/sync \
  -H "Content-Type: application/json" \
  -d '{
    "sync_mode": "INCREMENTAL"
  }'
```

This works because the dataset controls `sync.source.resource`, and the Shopify
connector resolves `/admin/api/2025-01/price_rules.json` dynamically at sync time.

Sync the explicit child resource path dataset:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_product_options/sync \
  -H "Content-Type: application/json" \
  -d '{
    "sync_mode": "INCREMENTAL"
  }'
```

## Inspect The Resulting Dataset

List datasets before or after sync:

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

All declared datasets report `materialization_mode: synced` because that is
owned by the dataset config, not inferred from the connector.

Read the explicit dataset name returned by the sync:

```bash
DATASET_NAME=$(printf '%s' "$SYNC_RESPONSE" | python -c "import json,sys; print(json.load(sys.stdin)['resources'][0]['dataset_names'][0])")
```

Preview the synced dataset:

```bash
curl -X POST "http://localhost:8000/api/runtime/v1/datasets/${DATASET_NAME}/preview" \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

Inspect stored sync state:

```bash
curl http://localhost:8000/api/runtime/v1/connectors/shopify_demo/sync/states
```

## Use The CLI

```bash
langbridge connectors list --url http://localhost:8000
langbridge sync resources --url http://localhost:8000 --connector shopify_demo
langbridge sync run --url http://localhost:8000 --dataset shopify_customers
langbridge sync states --url http://localhost:8000 --connector shopify_demo
```

## Notes

- this example is meant for a live Shopify shop, not a local mock API
- connector credentials stay on the runtime side through env-backed secret references
- synced datasets are declared by the dataset config, and dataset sync only populates those explicit datasets
- nested child resources do not silently create datasets during sync
- `price_rules` demonstrates dataset-driven dynamic resource resolution for Shopify
- `shopify_product_options` demonstrates dataset-owned child resource paths
- `shopify_customers` demonstrates explicit 1:1 flattening with `sync.source.flatten`
- live materialization is also dataset-owned in the runtime, but this example stays focused on API sync datasets
- remove local persisted runtime state by deleting `examples/connectors/shopify_sync/.langbridge`
