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
- `materialization_mode`
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

## Dataset Mode

Dataset behavior is now dataset-owned through `materialization_mode`:

- `live`
- `synced`

That field is part of the canonical runtime dataset model and the configured
runtime config model.

The runtime validates each dataset definition against connector capabilities
instead of assuming behavior from connector family alone.

Examples:

- a SQLite or Postgres dataset can be `live`
- a runtime-managed API sync dataset is `synced`
- a config-defined synced connector resource can be declared in YAML and populated later by connector sync
- a connector may eventually support both modes, but only if the runtime has a real execution path for each

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

## Connector Capabilities

The runtime keeps connector kind separate from capability flags. Dataset mode
validation currently relies on connector capability metadata such as:

- `supports_live_datasets`
- `supports_synced_datasets`
- `supports_incremental_sync`
- `supports_query_pushdown`
- `supports_preview`
- `supports_federated_execution`

A dataset requesting `materialization_mode: live` must use a connector that
supports live datasets. A dataset requesting `materialization_mode: synced`
must use a connector that supports synced datasets. Config-defined synced
datasets currently require a runtime sync-capable connector and use
`source.resource` as the declared connector resource name to materialize.

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
relation identity, explicit materialization mode, and execution capabilities
rather than coarse dataset types or connector families alone.
