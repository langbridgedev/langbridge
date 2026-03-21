# Datasets

Datasets are Langbridge's structured execution contract.

They sit between connectors and higher-level query surfaces:

- connectors expose source-specific access
- datasets normalize those sources into workspace-scoped runtime metadata
- semantic, SQL, sync, and agent workloads resolve through datasets

## Why Datasets Exist

Datasets let the runtime work with one structured concept even when the backing
source differs.

They capture:

- workspace ownership
- source binding
- relation identity
- execution capabilities
- schema and policy metadata
- lineage and revision history

## Runtime Shape

The runtime dataset metadata model lives in `langbridge/runtime/models/metadata.py`
and centers on:

- `workspace_id`
- `connection_id`
- `name`
- `sql_alias`
- `dataset_type`
- `source_kind`
- `connector_kind`
- `storage_kind`
- `relation_identity`
- `execution_capabilities`
- `columns`
- `policy`

Important supporting records:

- `DatasetMetadata`
- `DatasetColumnMetadata`
- `DatasetPolicyMetadata`
- `DatasetRelationIdentity`
- `DatasetExecutionCapabilities`

## Workspace Scope

Datasets are resolved per workspace. That is the runtime boundary that matters
for execution. Runtime-core dataset resolution does not depend on upstream
product-account identity claims.

## Source Types

Datasets may represent:

- database tables
- SQL-defined virtual datasets
- file-backed datasets
- sync-materialized datasets
- federated or derived datasets

## Policies And Guardrails

Dataset policy is where the runtime applies:

- preview row limits
- export limits
- redaction rules
- row filters
- DML permissions

That policy is reused across dataset preview, SQL, semantic, and agent-driven
execution.

## Current Direction

Langbridge is moving toward a richer dataset-first execution model driven by
relation identity and execution capabilities rather than coarse dataset types
alone.
