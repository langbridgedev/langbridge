# Datasets

Datasets are a governed virtual dataset abstraction between connectors and semantic models.

- Connectors expose raw physical sources.
- Datasets define reusable technical contracts over those sources.
- Semantic models provide business meaning on top of datasets.
- Agents query semantic models or SQL experiences that compile into dataset plans.

## Why Datasets Exist

Datasets solve control-plane and runtime concerns that should not live in semantic models:

- source binding (`connection_id`, table/sql/federated/file metadata)
- governed column allowlist and explicit schema
- preview/export policy enforcement
- row-filter templates and result redaction
- execution-oriented revision history and profiling stats

This keeps semantic models focused on business meaning (dimensions, measures, relationships, metrics).

## Data Model

Core records:

- `datasets`
- `dataset_columns`
- `dataset_policies`
- `dataset_revisions`
- `lineage_edges`

Indexes:

- `datasets.workspace_id`
- `datasets.name` (with workspace uniqueness)
- `datasets.updated_at`
- `dataset_revisions.dataset_id`
- `dataset_revisions.created_at`
- `lineage_edges.workspace_id`
- `lineage_edges.source_type/source_id`
- `lineage_edges.target_type/target_id`
- composite indexes on `(workspace_id, name)` and `(workspace_id, updated_at)`

## Versioning And Lineage

Datasets are append-only from a governance perspective:

- every create/update/restore writes a new `dataset_revisions` row
- `datasets.revision_id` points at the active revision
- restore is implemented as a new revision created from an older snapshot
- change summaries and `created_by` preserve the audit trail

Each revision stores:

- dataset definition snapshot
- schema snapshot
- policy snapshot
- source binding snapshot
- optional execution characteristics

Lineage is stored relationally in `lineage_edges` so it can be queried without a separate graph service.

Tracked nodes currently include:

- connections
- source tables
- file resources
- datasets
- semantic models
- unified semantic models
- saved queries
- dashboards

Tracked edge types currently include:

- `FEEDS`
- `DERIVES_FROM`
- `REFERENCES`
- `MATERIALIZES_FROM`
- `GENERATED_BY`

## API Surface

Dataset governance extends the existing `/v1/datasets` surface without introducing `/v2`.

- `GET /v1/datasets/{id}/versions`
- `GET /v1/datasets/{id}/versions/{revision_id}`
- `GET /v1/datasets/{id}/diff?from_revision=...&to_revision=...`
- `POST /v1/datasets/{id}/restore`
- `GET /v1/datasets/{id}/lineage`
- `GET /v1/datasets/{id}/impact`

The existing create and update endpoints now:

- create dataset revisions automatically
- update `datasets.revision_id`
- refresh lineage edges for the dataset definition

Semantic model, unified semantic model, saved query, and dashboard save flows also register lineage edges so downstream impact analysis stays current.

## Execution Architecture

- API persists dataset metadata and dispatches jobs.
- Worker resolves dataset definitions, enforces policy server-side, and executes via federated planner.
- Connector secrets remain in connector/runtime secret stores; datasets only store non-secret metadata.

### Create Flow

```mermaid
flowchart LR
  U[User UI] --> API[API /v1/datasets]
  API --> DB[(Dataset metadata tables)]
  API --> J[Queue Dataset Job]
  J --> W[Worker]
  W --> F[Federated Planner]
  F --> C[Connectors]
  C --> W
  W --> API
  API --> U
```

### Execute Preview/Profile Flow

```mermaid
sequenceDiagram
  participant UI as UI (Datasets/Semantic/SQL)
  participant API as API Control Plane
  participant Worker as Worker Runtime
  participant Planner as Federated Planner
  participant Conn as Connectors

  UI->>API: POST /v1/datasets/{id}/preview or /profile
  API->>API: enforce RBAC + workspace scope
  API->>API: compute effective limits
  API->>Worker: dataset_job_request
  Worker->>Worker: load dataset/columns/policy
  Worker->>Worker: apply allowlist/redaction/RLS templates
  Worker->>Planner: logical plan from dataset type
  Planner->>Conn: source query execution
  Conn-->>Planner: rows/stats
  Planner-->>Worker: result
  Worker-->>API: job result
  API-->>UI: preview/profile payload
```

## Dataset Type Compilation

- `TABLE`: scan + projection + filter + limit pushdown.
- `SQL`: read-only SQL subquery + outer projection/filter/limit.
- `FEDERATED`: feature-flagged; reference-based composition preferred.
- `FILE`: feature-flagged/stub for managed ingest-backed datasets.

## Migration Plan

No breaking changes:

1. Existing semantic models with physical table bindings continue to execute.
2. New semantic models should prefer `dataset_id` in table definitions.
3. Existing semantic models can be migrated incrementally table-by-table.
4. Worker semantic execution supports mixed models (dataset-backed and legacy physical tables).

Recommended direction:

- Treat dataset as mandatory source contract for all new semantic models.
- Keep business semantics in semantic model only; do not duplicate business logic into dataset objects.
