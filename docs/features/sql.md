# SQL Feature

Langbridge SQL is a first-class in-product SQL workbench for native SQL execution through the Worker execution plane.

## Product Scope

- UI-native SQL editor (`/sql/{organizationId}`).
- Dataset-first workbench mode for governed SQL-ready datasets.
- Explicit direct SQL mode for real database connectors only.
- Parameterized SQL, explain, query history, saved queries.
- Result preview with server-enforced limits.
- Export controls and workspace policy enforcement.
- Optional AI assistance for SQL generation/fix/explain.

## Architecture

1. User picks a workbench mode:
   - `Datasets` (default)
   - `Direct SQL`
2. Control plane validates workspace scope, policy bounds, and source selection metadata.
3. Dataset mode resolves selected datasets into execution descriptors and routes through the federated worker pipeline.
4. Direct SQL mode validates the chosen connector is a SQL-capable database and routes to single-source execution.
5. Worker applies safety checks and limit/timeout enforcement.
6. Results and artifacts are persisted and retrieved via SQL job APIs.

No direct execution occurs in UI or API process.

## API Surface

Base: `/api/v1/sql`

- `POST /execute`
- `POST /cancel`
- `GET /jobs/{sql_job_id}`
- `GET /jobs/{sql_job_id}/results`
- `GET /jobs/{sql_job_id}/results/download?format=csv|parquet`
- `GET /history`
- `POST /saved`
- `GET /saved`
- `GET /saved/{saved_query_id}`
- `PUT /saved/{saved_query_id}`
- `DELETE /saved/{saved_query_id}`
- `GET /policies`
- `PUT /policies`
- `POST /assist`

## Policy and Guardrails

- Read-only by default; DML requires explicit policy enablement.
- Enforced preview/export row caps.
- Enforced runtime and concurrency limits.
- Workspace schema/table allowlists.
- Result redaction rules where configured.
- Correlation IDs and job IDs exposed for supportability.
- Worker execution, job lifecycle, and artifact-backed results are preserved in federated mode.

## Dataset Mode

Dataset mode is the primary SQL workbench experience.

- The SQL workspace defaults to `Datasets` when workspace policy allows dataset execution.
- Users browse governed datasets, assign aliases, and write SQL against those aliases.
- The backend persists `workbench_mode=dataset` plus `selected_datasets` metadata on jobs and saved queries.
- Worker execution resolves dataset aliases to dataset descriptors, then to connectors/files/physical objects through the federated planning layer.
- Structured file-backed datasets such as parquet Shopify syncs participate in joins the same way as database datasets.
- Dataset mode is the canonical `Dataset -> Connector -> Query` execution path.

## Direct SQL Mode

Direct SQL mode is an explicit lower-level path for SQL databases.

- The workbench exposes only SQL-capable database connectors in this mode.
- API connectors, file uploads, CSV uploads, and non-database sources are excluded.
- The backend persists `workbench_mode=direct_sql` and validates connector capability before execution.
- Direct mode is intended for power users who need raw database access without the dataset abstraction.

Query pattern:

```sql
SELECT TOP 100
  a.id,
  b.id
FROM shop.api_connector.shopify_orders AS a
JOIN warehouse.public.customers AS b
  ON a.id = b.id
ORDER BY a.id DESC;
```

Where `shop` and `warehouse` are dataset aliases configured in the SQL sidebar.

This allows examples like:

- join Shopify parquet syncs with Postgres/MySQL/Snowflake tables
- query CSV/parquet uploads together with warehouse tables
- route multi-dataset structured questions through federation-first agent execution instead of bespoke source SQL
