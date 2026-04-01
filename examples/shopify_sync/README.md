# Shopify Sync Example

This example runs a self-hosted Langbridge runtime host configured with the
declarative Shopify connector and syncs live Shopify Admin API resources into
runtime-managed datasets.

It stays fully runtime-scoped and does not depend on `langbridge-cloud`.

## What This Example Covers

- a configured Shopify connector under `examples/shopify_sync/langbridge_config.yml`
- runtime-managed sync state persisted under `.langbridge/metadata.db`
- synced datasets materialized into the local DuckDB execution store
- the packaged declarative Shopify resources currently exposed by Langbridge:
  - `customers`
  - `draft_orders`
  - `locations`

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
`examples/shopify_sync/.env` for you:

```bash
export SHOPIFY_SHOP_DOMAIN=acme.myshopify.com
export SHOPIFY_CLIENT_ID=your_shopify_client_id
export SHOPIFY_CLIENT_SECRET=your_shopify_client_secret

python examples/shopify_sync/get_shopify_access_token.py --write-env
```

The script will print shell exports and write `SHOPIFY_ACCESS_TOKEN` into the
example `.env` file. If you only want the token value, use:

```bash
python examples/shopify_sync/get_shopify_access_token.py --raw
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
langbridge serve --config examples/shopify_sync/langbridge_config.yml --host 127.0.0.1 --port 8000
```

The host will listen on `http://localhost:8000`.

## Discover The Connector

List configured connectors:

```bash
curl http://localhost:8000/api/runtime/v1/connectors
```

The connector payload includes explicit capability flags. `shopify_demo`
currently supports synced datasets, not live datasets.

List syncable Shopify resources:

```bash
curl http://localhost:8000/api/runtime/v1/connectors/shopify_demo/sync/resources
```

## Run A Sync

Sync customers incrementally:

```bash
SYNC_RESPONSE=$(curl -s -X POST http://localhost:8000/api/runtime/v1/connectors/shopify_demo/sync \
  -H "Content-Type: application/json" \
  -d '{
    "resource_names": ["customers"],
    "sync_mode": "INCREMENTAL"
  }')

printf '%s\n' "$SYNC_RESPONSE"
```

You can switch the resource list to `draft_orders` or `locations`. The Shopify
manifest marks `locations` as `FULL_REFRESH`.

## Inspect The Resulting Dataset

List datasets:

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

The synced dataset returned by this flow reports `materialization_mode: synced`.

Read the runtime-managed dataset name returned by the sync:

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
langbridge sync run --url http://localhost:8000 --connector shopify_demo --resource customers
langbridge sync states --url http://localhost:8000 --connector shopify_demo
```

## Notes

- this example is meant for a live Shopify shop, not a local mock API
- connector credentials stay on the runtime side through env-backed secret references
- synced datasets are runtime-managed materializations created by connector sync
- remove local persisted runtime state by deleting `examples/shopify_sync/.langbridge`
