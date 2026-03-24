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

Federation is runtime-owned. It is part of the runtime itself, not a separate service boundary.

For product positioning, the primary v1 story is still single-node runtime execution. The scheduler and dispatch seams in `langbridge.federation` are technical groundwork for preview scale-out, not a claim that coordinator/worker deployment is already the default release surface.

## Why It Matters

- SQL and semantic query share one structured execution substrate
- connectors stay inside the runtime boundary
- cross-source joins and transformations work in embedded, self-hosted, and hybrid runtime modes
- federated SQL can target all eligible workspace datasets by default, with `selected_datasets` used only to narrow scope
