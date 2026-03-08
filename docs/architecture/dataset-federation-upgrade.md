# Dataset Federation Upgrade Review

## What exists now

- Langbridge already routes structured execution through the worker and built-in federated planner/executor.
- Datasets already support versions, lineage, preview/profile jobs, virtual datasets, CSV/file ingest, and connector-synced parquet materialization.
- The main gap is that dataset execution still branches on legacy `dataset_type` (`TABLE`, `SQL`, `FILE`, `FEDERATED`) instead of a normalized dataset contract.
- SQL workspace federated mode is now dataset-backed, and analyst execution routes unified semantic queries through the same federation-first worker path.

## What is changing

- Add a first-class dataset contract across backend and worker metadata:
  - `source_kind`
  - `connector_kind`
  - `storage_kind`
  - canonical `relation_identity`
  - explicit `execution_capabilities`
- Keep `dataset_type` as a compatibility field while deriving the richer descriptor for both new and existing records.
- Refactor worker/federation entrypoints to consume normalized dataset descriptors before building runtime bindings.
- Extend SQL workspace and SQL job contracts so federated execution targets datasets directly.
- Prefer federation-first structured execution in analyst paths when the federation runtime is available.

## Why the new model is needed

- A parquet-backed Shopify sync is structurally joinable data, but the current model can only label it as `FILE`.
- That coarse typing leaks into planning, lineage, UI, and agent routing, forcing source-specific behavior where the platform should be dataset-first.
- The richer contract lets Langbridge treat database tables, uploads, parquet-backed SaaS syncs, and virtual datasets as one structured federation surface.

## Expected improvement

- SQL workspace can default to federated execution for structured datasets and join synced Shopify parquet with Postgres/MySQL/Snowflake tables without special handling.
- Worker planning stays incremental: policy enforcement, limits, job lifecycle, and artifact-backed results remain in place.
- Agents can target the federation abstraction first, using connector-native execution only for direct single-source work.

## Risks and tradeoffs

- The main remaining regression risk is the legacy `dataset_type` branch surface that still underpins older dataset records.
- `dataset_type` still remains as a compatibility field while the richer dataset descriptor continues to normalize old records.
- Virtual datasets remain incremental: this upgrade focuses on normalizing execution metadata and federation entrypoints instead of replacing the planner or worker model.
