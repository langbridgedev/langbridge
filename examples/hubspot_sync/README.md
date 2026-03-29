# HubSpot Sync Example

This example runs a self-hosted Langbridge runtime host configured with the
declarative HubSpot connector and syncs live HubSpot CRM resources into
runtime-managed datasets.

It stays fully runtime-scoped and does not depend on `langbridge-cloud`.

## What This Example Covers

- a configured HubSpot connector under `examples/hubspot_sync/langbridge_config.yml`
- runtime-managed sync state persisted under `.langbridge/metadata.db`
- synced datasets materialized into the local DuckDB execution store
- the packaged declarative HubSpot resources currently exposed by Langbridge:
  - `contacts`
  - `companies`
  - `deals`

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
langbridge serve --config examples/hubspot_sync/langbridge_config.yml --host 127.0.0.1 --port 8000
```

The host will listen on `http://localhost:8000`.

## Discover The Connector

List configured connectors:

```bash
curl http://localhost:8000/api/runtime/v1/connectors
```

The connector payload includes explicit capability flags. `hubspot_demo`
currently supports synced datasets, not live datasets.

List syncable HubSpot resources:

```bash
curl http://localhost:8000/api/runtime/v1/connectors/hubspot_demo/sync/resources
```

## Run A Sync

Sync contacts incrementally:

```bash
SYNC_RESPONSE=$(curl -s -X POST http://localhost:8000/api/runtime/v1/connectors/hubspot_demo/sync \
  -H "Content-Type: application/json" \
  -d '{
    "resource_names": ["contacts"],
    "sync_mode": "INCREMENTAL"
  }')

printf '%s\n' "$SYNC_RESPONSE"
```

You can switch the resource list to `companies` or `deals`.

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
curl http://localhost:8000/api/runtime/v1/connectors/hubspot_demo/sync/states
```

## Use The CLI

```bash
langbridge connectors list --url http://localhost:8000
langbridge sync resources --url http://localhost:8000 --connector hubspot_demo
langbridge sync run --url http://localhost:8000 --connector hubspot_demo --resource contacts
langbridge sync states --url http://localhost:8000 --connector hubspot_demo
```

## Notes

- this example is meant for a live HubSpot account, not a local mock API
- connector credentials stay on the runtime side through env-backed secret references
- synced datasets are runtime-managed materializations created by connector sync
- remove local persisted runtime state by deleting `examples/hubspot_sync/.langbridge`
