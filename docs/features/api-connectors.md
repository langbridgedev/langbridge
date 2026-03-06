# API Connector Sync

Langbridge API connectors ingest SaaS data into managed `FILE` datasets instead of querying third-party APIs live from the federated planner. This keeps semantic models, SQL, federation, and agents operating on governed dataset assets.

## Architecture

`API Connector -> Connector Sync Job -> Normalize / flatten -> Managed datasets -> Semantic / SQL / agents`

Key implementation points:

- Sync execution runs in the worker through `CONNECTOR_SYNC` jobs.
- Each resource keeps resumable state in `connector_sync_states`.
- Normalized outputs are written as parquet-backed managed datasets under `.cache/datasets/api-connectors/...`.
- Child arrays become child datasets with preserved parent keys.
- Dataset metadata includes connector sync provenance in `file_config.connector_sync`.

## Sync Modes

- `FULL_REFRESH`: replace the materialized dataset contents for the selected resource.
- `INCREMENTAL`: resume from the last successful cursor when the connector exposes one.
- Unsupported resources automatically fall back to full refresh for safety.

## Supported v1 Connectors

- Shopify: recommended first-class v1 connector, cursor-based incremental via `updated_at`.
- Stripe: recommended first-class v1 connector, cursor-based incremental via `created`.
- HubSpot: acceptable v1 connector with checkpointed incremental filtering.
- Salesforce: scaffolded extension point; works but is a heavier operational surface.
- Google Analytics: scaffolded/report-style connector; currently best treated as full refresh.

Recommended production priority:

1. Shopify
2. Stripe
3. HubSpot

## API Surface

Connector sync stays under existing `/api/v1/connectors/{organization_id}` routes:

- `POST /{connector_id}/test`
- `GET /{connector_id}/resources`
- `POST /{connector_id}/sync`
- `GET /{connector_id}/sync-state`
- `GET /{connector_id}/sync-history`

`POST /sync` accepts:

```json
{
  "resources": ["orders", "customers"],
  "syncMode": "INCREMENTAL",
  "forceFullRefresh": false
}
```

## Rollout Notes

- Apply Alembic migrations through `alembic upgrade head`.
- Existing connector APIs remain backward-compatible; no `/v2` routes were introduced.
- Scheduling and webhook-assisted sync are left as extension points on top of the same state model and worker job path.
