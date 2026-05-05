# HubSpot Sync Example

This example runs a self-hosted Langbridge runtime host configured with the
declarative HubSpot connector and syncs live HubSpot CRM resources into
runtime-managed datasets. It now showcases dataset-owned sync configuration:
each dataset declares its own `materialization_mode` and `sync.source.resource`.

## What This Example Covers

- a configured HubSpot connector under `examples/connectors/hubspot_sync/langbridge_config.yml`
- predeclared synced datasets that choose the HubSpot resource at the dataset layer
- runtime-managed sync state persisted under `.langbridge/metadata.db`
- synced datasets materialized into the local DuckDB execution store
- a manifest-listed dataset resource: `contacts`
- a dynamic dataset-selected resource: `custom_objects`

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
`pip install -e ./langbridge-connectors/langbridge-connector-hubspot`.

Export the HubSpot private app token used by the example config:

```bash
export HUBSPOT_ACCESS_TOKEN=pat-...
```

## Start The Runtime Host

From the repository root:

```bash
langbridge serve --config examples/connectors/hubspot_sync/langbridge_config.yml --host 127.0.0.1 --port 8000
```

The host will listen on `http://localhost:8000`.

## Discover The Connector

List configured connectors:

```bash
curl http://localhost:8000/api/runtime/v1/connectors
```

The connector payload includes explicit capability flags. `hubspot_demo`
is an API sync connector, but the dataset decides whether a given dataset is
declared as `synced`.

List syncable HubSpot resources:

```bash
curl http://localhost:8000/api/runtime/v1/connectors/hubspot_demo/sync/resources
```

Because this example predeclares datasets, the resource list will include both:

- `contacts`, which comes from the packaged connector manifest
- `custom_objects`, which is surfaced because the dataset requested it even though it is not statically listed

List the configured datasets:

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

You should see:

- `hubspot_contacts`
- `hubspot_custom_objects`

## Run A Sync

Sync the manifest-listed `contacts` dataset incrementally:

```bash
SYNC_RESPONSE=$(curl -s -X POST http://localhost:8000/api/runtime/v1/datasets/hubspot_contacts/sync \
  -H "Content-Type: application/json" \
  -d '{
    "sync_mode": "INCREMENTAL"
  }')

printf '%s\n' "$SYNC_RESPONSE"
```

Sync the dataset-selected dynamic `custom_objects` resource:

```bash
curl -X POST http://localhost:8000/api/runtime/v1/datasets/hubspot_custom_objects/sync \
  -H "Content-Type: application/json" \
  -d '{
    "sync_mode": "INCREMENTAL"
  }'
```

This works because the dataset controls `sync.source.resource`, and the HubSpot
connector resolves `/crm/v3/objects/custom_objects` dynamically at sync time.

## Inspect The Resulting Dataset

List datasets before or after sync:

```bash
curl http://localhost:8000/api/runtime/v1/datasets
```

Both declared datasets report `materialization_mode: synced` because that is
owned by the dataset config, not inferred from the connector.

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
curl http://localhost:8000/api/runtime/v1/connectors/hubspot_demo/sync/states
```

## Use The CLI

```bash
langbridge connectors list --url http://localhost:8000
langbridge sync resources --url http://localhost:8000 --connector hubspot_demo
langbridge sync run --url http://localhost:8000 --dataset hubspot_contacts
langbridge sync states --url http://localhost:8000 --connector hubspot_demo
```

## Notes

- this example is meant for a live HubSpot account, not a local mock API
- connector credentials stay on the runtime side through env-backed secret references
- synced datasets are declared by the dataset config, and dataset sync populates them
- `custom_objects` demonstrates dataset-driven dynamic resource resolution for HubSpot
- live materialization is also dataset-owned in the runtime, but this example stays focused on API sync datasets
- remove local persisted runtime state by deleting `examples/connectors/hubspot_sync/.langbridge`
