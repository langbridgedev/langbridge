# Runtime Host Sync Example

This example runs a self-hosted Langbridge runtime host next to a local
Stripe-like mock API and syncs the `customers` resource into a runtime-managed
dataset.

It is fully runtime-scoped and does not depend on `langbridge-cloud`.

## What This Example Starts

- `runtime-host`: self-hosted runtime host
- `mock-stripe`: local HTTP API exposing `/v1/account` and `/v1/customers`

## Start The Stack

From this directory:

```bash
docker compose up --build -d
```

Services:

- runtime host: `http://localhost:8000`
- mock API: `http://localhost:12111`

## Check Health

```bash
curl http://localhost:12111/health
curl http://localhost:8000/api/runtime/v1/health
```

## Discover Connectors And Sync Resources

List configured connectors:

```bash
curl http://localhost:8000/api/runtime/v1/connectors
```

List syncable resources:

```bash
curl http://localhost:8000/api/runtime/v1/connectors/billing_demo/sync/resources
```

You should see `customers` with `status` set to `never_synced`.

## Run A Sync

```bash
SYNC_RESPONSE=$(curl -s -X POST http://localhost:8000/api/runtime/v1/connectors/billing_demo/sync \
  -H "Content-Type: application/json" \
  -d '{
    "resource_names": ["customers"],
    "sync_mode": "INCREMENTAL"
  }')

printf '%s\n' "$SYNC_RESPONSE"
```

The sync response returns the runtime-managed dataset name in
`resources[0].dataset_names[0]`.

## Inspect Sync State

```bash
curl http://localhost:8000/api/runtime/v1/connectors/billing_demo/sync/states
```

## List And Preview The Synced Dataset

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

```bash
DATASET_NAME=$(printf '%s' "$SYNC_RESPONSE" | python -c "import json,sys; print(json.load(sys.stdin)['resources'][0]['dataset_names'][0])")

curl -X POST "http://localhost:8000/api/runtime/v1/datasets/${DATASET_NAME}/preview" \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

## Use The CLI Against The Hosted Runtime

```bash
langbridge connectors list --url http://localhost:8000
langbridge sync resources --url http://localhost:8000 --connector billing_demo
langbridge sync run --url http://localhost:8000 --connector billing_demo --resource customers
langbridge sync states --url http://localhost:8000 --connector billing_demo
langbridge datasets list --url http://localhost:8000
langbridge datasets preview --url http://localhost:8000 --dataset "$DATASET_NAME" --limit 5
```

## Notes

- runtime sync state is workspace-scoped inside the runtime
- the resulting synced dataset is owned by the runtime, not by a cloud control plane
- if you later enable host auth, send a bearer token and see `docs/deployment/self-hosted.md`
- remove all persisted example state with `docker compose down -v`
