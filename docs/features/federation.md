# Federation Feature

Federation is Langbridge's built-in planning and execution capability for
cross-source structured workloads.

## What Federation Does

- resolves runtime datasets into structured execution inputs
- parses SQL and compiled semantic SQL into logical plans
- optimizes pushdown versus local compute
- executes remote scans and local stages
- returns rows plus execution metadata

## Where It Lives

- `langbridge.federation`
- `langbridge.runtime.execution.FederatedQueryTool`
- runtime services that call federation for SQL, semantic, and dataset workloads

Federation is runtime-owned. It is not a separate gateway or cloud-only service.

## Why It Matters

- SQL and semantic query share one structured execution substrate
- connectors stay inside the runtime boundary
- cross-source joins and transformations work in embedded, self-hosted, and hybrid runtime modes
