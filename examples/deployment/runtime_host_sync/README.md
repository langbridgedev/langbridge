# Runtime Host Sync Example

This example runs a self-hosted Langbridge runtime host next to a local
Stripe-like mock API and syncs the declared `billing_customers` dataset from the
`customers` resource.
The declared synced dataset uses `materialization_mode: synced` with
`sync.source.resource: customers`, a scheduled `sync.cadence: 5m`, and
`sync.sync_on_start: true`.

## What This Example Starts

- `runtime-host`: self-hosted runtime host
- `mock-stripe`: local HTTP API exposing `/v1/account` and `/v1/customers`

The runtime host metadata store is configured explicitly in
`langbridge_config.yml`. This example uses SQLite for local durability under
`.langbridge/metadata.db`; move that block to `postgres` for production-style
deployments. It also leaves `runtime.migrations.auto_apply: true`, so the host
upgrades runtime metadata schema on startup by default.

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

The connector payload now includes explicit capability flags. In this example
`billing_demo` supports both live and synced datasets, while the dataset
contract explicitly chooses `synced`.

List syncable resources:

```bash
curl http://localhost:8000/api/runtime/v1/connectors/billing_demo/sync/resources
```

You should see `customers` with `status` set to `never_synced` and
`dataset_names` including `billing_customers`.

## Run Dataset Sync

```bash
SYNC_RESPONSE=$(curl -s -X POST http://localhost:8000/api/runtime/v1/datasets/billing_customers/sync \
  -H "Content-Type: application/json" \
  -d '{
    "sync_mode": "INCREMENTAL"
  }')

printf '%s\n' "$SYNC_RESPONSE"
```

The sync response returns the declared dataset name in `resources[0].dataset_names[0]`.

## Scheduled Sync Behavior

This example also enables dataset-owned background sync in the runtime host.

- `sync.cadence: 5m` registers a runtime background task named
  `dataset-sync:billing_customers`
- `sync.sync_on_start: true` runs the same dataset sync once during runtime host startup
- supported cadence values in this slice use interval shorthands such as `30s`,
  `5m`, `1h`, and `1d`

## Inspect Sync State

```bash
curl http://localhost:8000/api/runtime/v1/connectors/billing_demo/sync/states
```

## List And Preview The Synced Dataset

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

The synced dataset returned by this example will report `materialization_mode`
as `synced`, and before the first sync it will appear with `status:
pending_sync`.

```bash
DATASET_NAME=billing_customers

curl -X POST "http://localhost:8000/api/runtime/v1/datasets/${DATASET_NAME}/preview" \
  -H "Content-Type: application/json" \
  -d '{"limit": 5}'
```

## Use The CLI Against The Hosted Runtime

```bash
langbridge connectors list --url http://localhost:8000
langbridge sync resources --url http://localhost:8000 --connector billing_demo
langbridge sync run --url http://localhost:8000 --dataset "$DATASET_NAME"
langbridge sync states --url http://localhost:8000 --connector billing_demo
langbridge datasets list --url http://localhost:8000
langbridge datasets preview --url http://localhost:8000 --dataset "$DATASET_NAME" --limit 5
```

## Notes

- runtime sync state is workspace-scoped inside the runtime
- the resulting synced dataset is owned by the runtime, not by a cloud control plane
- this example shows a config-defined synced dataset that is materialized and refreshed from the dataset surface
- preview/query will fail honestly until the first sync populates the dataset
- if you later enable host auth, send a bearer token and see `docs/deployment/self-hosted.md`
- remove all persisted example state with `docker compose down -v`
