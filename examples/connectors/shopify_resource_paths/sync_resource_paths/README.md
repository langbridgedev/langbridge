# Shopify Sync Resource Paths

This example shows the V1 synced API dataset model for Shopify.

## What It Demonstrates

- a synced parent dataset using `sync.source.resource: customers`
- explicit `sync.source.flatten` for the 1:1 `default_address` child
- an explicit 1:many child-path synced dataset: `products.options`
- a dynamic Shopify resource path dataset: `price_rules`
- no silent dataset creation during dataset sync

## Start The Runtime

From the repository root:

```bash
langbridge serve --config examples/connectors/shopify_resource_paths/sync_resource_paths/langbridge_config.yml --host 127.0.0.1 --port 8000
```

## Inspect Declared Datasets

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

You should see three declared synced datasets before any sync runs:

- `shopify_synced_customers`
- `shopify_synced_product_options`
- `shopify_synced_price_rules`

## Inspect Syncable Resources

```bash
curl http://localhost:8000/api/runtime/v1/connectors/shopify_demo/sync/resources
```

The list should include the declared resource paths:

- `customers`
- `products.options`
- `price_rules`

## Run Syncs

Sync the flattened parent dataset incrementally:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_synced_customers/sync \
  -H "Content-Type: application/json" \
  -d '{
    "sync_mode": "INCREMENTAL"
  }'
```

Sync the explicit 1:many child dataset incrementally:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_synced_product_options/sync \
  -H "Content-Type: application/json" \
  -d '{
    "sync_mode": "INCREMENTAL"
  }'
```

Sync the dynamic resource with a full refresh:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_synced_price_rules/sync \
  -H "Content-Type: application/json" \
  -d '{
    "sync_mode": "FULL_REFRESH"
  }'
```

## Preview Results

Preview the flattened parent dataset:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_synced_customers/preview \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

Preview the explicit child dataset:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_synced_product_options/preview \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

Preview the dynamic resource dataset:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_synced_price_rules/preview \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

## Notes

- datasets must exist before sync; dataset sync will not invent them
- `customers` shows explicit 1:1 flattening
- `products.options` shows explicit 1:many child dataset materialization without flattening
- `price_rules` shows a readable dynamic Shopify resource path without opaque generated dataset names
