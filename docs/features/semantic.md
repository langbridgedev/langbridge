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
- support semantic graphs as a composition layer that compiles into dataset-backed executable semantic models

## Semantic Graph Direction

- `SemanticModel` is the concrete executable model used by the query engine
- `SemanticGraph` is the composition layer for source semantic models, graph relationships, and graph metrics
- semantic graphs compile into executable `SemanticModel` instances before query execution
- runtime execution stays on the existing semantic-model path; semantic graphs do not introduce a second execution engine

## Runtime Relationship

Semantic models are workspace-scoped runtime metadata.

The runtime resolves semantic models to datasets, then uses the same execution
substrate as SQL and dataset query paths. When a semantic graph is involved, the
runtime compiles it into an executable semantic model first and then executes
that compiled model through the normal semantic query flow.

## Related Docs

- `docs/semantic-model.md`
- `docs/datasets.md`
- `docs/features/federation.md`
