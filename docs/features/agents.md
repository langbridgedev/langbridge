# Agent Features

Langbridge includes runtime primitives for agent-style analytical execution.

## Agent Stack

- planner-style routing for workload selection
- supervisor and orchestration flow for multi-step execution
- tools for semantic, SQL, retrieval, and analytical operations

## Relationship To Data Execution

- agents use the same runtime guardrails as direct workloads
- semantic and structured queries still go through the runtime execution path
- federation, limits, and result policies apply consistently to agent-initiated work

## Core Value

Agents are consumers of the same runtime primitives as the rest of the system:

- semantic layer
- SQL execution
- federated execution engine
- runtime-managed datasets
