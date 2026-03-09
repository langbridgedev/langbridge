# SQL Feature

Langbridge SQL is a first-class in-product SQL workbench for native SQL execution through the Worker execution plane.

## Product Scope

- UI-native SQL editor (`/sql/{organizationId}`).
- Connection selector and dataset-oriented federated source builder.
- Parameterized SQL, explain, query history, saved queries.
- Result preview with server-enforced limits.
- Export controls and workspace policy enforcement.
- Optional AI assistance for SQL generation/fix/explain.

## Architecture

1. User writes SQL in UI (default T-SQL or connector dialect).
2. Control plane validates request and policy bounds.
3. SQL job is enqueued and executed by worker handler.
4. Worker applies safety checks and limit/timeout enforcement.
5. Federated execution resolves dataset descriptors into the existing worker federation pipeline.
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

## Federated SQL Authoring

Federated mode is now dataset-first for structured datasets.

- The SQL workspace defaults federated mode on when the workspace allows federation and structured federatable datasets exist.
- Users select structured datasets, assign aliases, and write SQL against those aliases.
- The backend sends `federated_datasets` to the worker as the only federated execution contract.
- Structured file-backed datasets such as parquet Shopify syncs participate in joins the same way as database datasets.
- Composite virtual datasets keep their normalized metadata/capabilities, but direct table-style SQL authoring over virtual datasets remains a follow-up.

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
