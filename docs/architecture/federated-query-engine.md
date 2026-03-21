# Federated Query Engine

The federated query engine is Langbridge's structured execution engine for
cross-source workloads.

It runs inside the runtime, not beside it. Runtime services resolve datasets and
connectors, then hand structured execution off to federation when a workload
needs local planning, pushdown analysis, or cross-source execution.

## Main Code Paths

- service facade: `langbridge/federation/service.py`
- planner: `langbridge/federation/planner/planner.py`
- SQL parsing: `langbridge/federation/planner/parser.py`
- semantic query compilation: `langbridge/federation/planner/smq_compiler.py`
- optimizer: `langbridge/federation/planner/optimizer.py`
- physical planner: `langbridge/federation/planner/physical_planner.py`
- scheduler: `langbridge/federation/executor/scheduler.py`
- stage executor: `langbridge/federation/executor/stage_executor.py`
- artifact store: `langbridge/federation/executor/artifact_store.py`
- runtime bridge: `langbridge/runtime/execution/federated_query_tool.py`

## Inputs

Federation works from runtime-resolved datasets and connector metadata. Those
datasets may represent:

- database tables
- SQL-backed virtual datasets
- file-backed datasets
- sync-materialized datasets
- federated combinations of other datasets

## Query Lifecycle

1. A SQL or semantic request enters the runtime.
2. Runtime services resolve workspace-scoped datasets, policies, and connectors.
3. Federation builds a logical plan.
4. The optimizer decides which work can be pushed down and which work runs locally.
5. The executor runs remote scans plus local compute stages and returns rows and metadata.

## Why It Matters

- SQL and semantic query share one structured execution substrate.
- Connectors stay inside the runtime boundary.
- Cross-source joins and transformations stay inside the runtime.
- The same engine works in embedded, self-hosted, and hybrid runtime shapes.
