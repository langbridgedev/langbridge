# Semantic Model Guide

The standard semantic model contract lives in:

- `langbridge/semantic/model.py`
- `langbridge/semantic/loader.py`

The runtime uses these modules as the canonical semantic schema and normalization
path.

## Standard Semantic Model

Top-level fields:

- `version`
- `name`
- `connector`
- `dialect`
- `description`
- `tags`
- `datasets`
- `relationships`
- `metrics`

`datasets` is the canonical field name. `tables` is still accepted as a legacy
alias and normalized by the loader.

## Dataset Fields

Each dataset entry can define:

- `dataset_id`
- `relation_name`
- `schema_name`
- `catalog_name`
- `description`
- `synonyms`
- `dimensions`
- `measures`
- `filters`

Useful compatibility aliases that are still normalized:

- `datasetId` -> `dataset_id`
- `relationName` or `name` -> `relation_name`
- `schemaName` or `schema` -> `schema_name`
- `catalogName` or `catalog` -> `catalog_name`

For runtime-backed models, `dataset_id` is the preferred binding. `relation_name`
is still used as the relation name exposed to compilation and execution.

## Dimension

Fields:

- `name`
- `expression`
- `type`
- `primary_key`
- `alias`
- `description`
- `synonyms`
- `vector`

`vector` is the canonical semantic dimension vector-search contract:

```yaml
dimensions:
  - name: country
    expression: country
    type: string
    vector:
      enabled: true
      refresh_interval: 1d
      max_values: 5000
      store:
        type: managed_faiss
```

`vector.store` supports:

- `type: managed_faiss`
- `type: connector` with `connector_name`
- optional `index_name` for an explicit runtime index namespace

Legacy dimension fields are still normalized on load:

- `vectorized`
- `vector_reference`
- `vector_index`

## Measure

Fields:

- `name`
- `expression`
- `type`
- `description`
- `aggregation`
- `synonyms`

## Relationship

Canonical relationship fields:

- `name`
- `source_dataset`
- `source_field`
- `target_dataset`
- `target_field`
- `operator`
- `type`

Legacy forms such as `from_`, `to`, `join_on`, `joinOn`, `on`, and `condition`
are still normalized into the canonical relationship shape.

## Metric

Fields:

- `expression`
- `description`

## Unified Semantic Models

Unified semantic models are a separate contract loaded through:

- `load_unified_semantic_model(...)`
- `parse_unified_semantic_model_payload(...)`

Those models live in `langbridge.semantic.unified_model` and use `source_models`
instead of embedded datasets.

## Rules

- use the loader to normalize incoming semantic payloads
- prefer `datasets` over `tables` in new documentation and examples
- treat runtime dataset bindings as the primary execution path
