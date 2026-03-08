# Architecture Overview

Langbridge is **Agentic Analytics Infrastructure with a Distributed Federated Query Engine**.

It is organized into three runtime domains:
- **Control Plane**: SaaS/API/UI/orchestration/policy and runtime registry.
- **Execution Plane**: Worker runtime where jobs execute with connector access.
- **Federated Query Engine**: planner + optimizer + stage executor used by workers.

## Primary System Flow

```mermaid
flowchart TD
    USER[User / Agent] --> UI[Web UI]
    UI --> CP[Control Plane<br/>API + Orchestrator + Policy]
    CP --> DISPATCH[Task Dispatch]
    DISPATCH --> EP[Execution Plane Worker]
    EP --> FQE[Federated Query Engine]
    FQE --> SOURCES[Remote Data Sources]
    FQE --> ARTIFACTS[Result Artifacts]
    ARTIFACTS --> CP
    CP --> UI
```

## Hosted Mode

In hosted mode, Langbridge operates both control and execution planes.

```mermaid
flowchart LR
    U[User] --> CP[Hosted Control Plane]
    CP --> HW[Hosted Worker Pool]
    HW --> FQE[Federated Query Engine]
    FQE --> DS[(Customer Data Sources)]
```

## Hybrid Mode

In hybrid mode, the control plane is hosted while worker runtime executes in customer infrastructure.

```mermaid
flowchart LR
    U[User] --> HCP[Hosted Control Plane]
    HCP --> EQ[Edge Task Queue / Pull API]
    EQ --> CR[Customer Runtime Worker]
    CR --> FQE[Federated Query Engine]
    FQE --> CDS[(Customer Data Sources)]
```

## Core Principles

- All structured query execution is Worker-mediated.
- SQL and semantic workloads share a common federated planning and execution substrate.
- Control plane and execution plane are independently deployable.
- Runtime registration and edge task transport are authenticated and auditable.
- External SQL gateway and Trino infrastructure are no longer part of the release architecture.

## Related Docs

- `docs/architecture/control-plane.md`
- `docs/architecture/execution-plane.md`
- `docs/architecture/federated-query-engine.md`
- `docs/architecture/hybrid-deployment.md`
- `docs/architecture/deprecations.md`
