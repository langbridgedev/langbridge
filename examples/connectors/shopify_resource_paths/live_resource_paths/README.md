# Shopify Live Resource Paths

This example shows the V1 live API dataset model for Shopify.

## What It Demonstrates

- a live parent dataset using `source.resource: customers`
- explicit `source.flatten` for the 1:1 `default_address` child
- an explicit 1:1 child-path dataset: `customers.default_address`
- an explicit 1:many child-path dataset: `products.options`

## Start The Runtime

From the repository root:

```bash
langbridge serve --config examples/connectors/shopify_resource_paths/live_resource_paths/langbridge_config.yml --host 127.0.0.1 --port 8000
```

## Inspect Datasets

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

You should see:

- `shopify_live_customers`
- `shopify_live_customer_addresses`
- `shopify_live_product_options`

## Preview The Flattened Parent Dataset

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_live_customers/preview \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

The parent rows should include flattened columns from `default_address`, such as
`default_address__city` or similar address fields returned by Shopify.

## Preview The Explicit Child Datasets

Preview the 1:1 child path:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_live_customer_addresses/preview \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

Preview the 1:many child path:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/shopify_live_product_options/preview \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

## Notes

- flattening is explicit and dataset-owned
- `customers.default_address` is addressable as its own dataset because the dataset declares that path
- `products.options` is not flattened into `shopify_live_customers`; it is queried through its own dataset
