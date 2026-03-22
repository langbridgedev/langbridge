# Semantic Feature

Langbridge semantic modeling provides business-facing analytical structure on top
of runtime datasets.

## Current Shape

- the standard semantic model lives in `langbridge.semantic.model`
- loaders live in `langbridge.semantic.loader`
- semantic execution is performed by `langbridge.runtime.services.semantic_query_execution_service`
- structured execution routes through federation when needed

## What Semantic Models Do

- map business members to runtime datasets
- define dimensions, measures, filters, relationships, and metrics
- normalize legacy payload aliases into the current contract
- support unified semantic models separately from dataset-backed standard models

## Runtime Relationship

Semantic models are workspace-scoped runtime metadata.

The runtime resolves semantic models to datasets, then uses the same execution
substrate as SQL and dataset query paths.

## Related Docs

- `docs/semantic-model.md`
- `docs/datasets.md`
- `docs/features/federation.md`
