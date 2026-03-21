# Execution Plane

The execution plane is where Langbridge runtime requests actually run.

Today that primarily means the configured runtime host plus the runtime services
it composes. The queued worker is another execution shape that reuses the same
runtime-owned execution primitives.

## Responsibilities

- build a runtime context for each request
- resolve datasets, connectors, semantic models, and secrets
- execute dataset preview, SQL, semantic, sync, and agent workloads
- route structured work through federation when needed
- enforce runtime limits, redaction, and guardrails
- return rows, summaries, artifacts, and sync state

## Main Components

- host API: `langbridge/runtime/hosting/app.py`
- host auth: `langbridge/runtime/hosting/auth.py`
- configured local runtime builder: `langbridge/runtime/local_config.py`
- runtime host facade: `langbridge/runtime/services/runtime_host.py`
- dataset query service: `langbridge/runtime/services/dataset_query_service.py`
- SQL query service: `langbridge/runtime/services/sql_query_service.py`
- semantic query service: `langbridge/runtime/services/semantic_query_execution_service.py`
- connector sync runtime: `langbridge/runtime/services/dataset_sync_service.py`
- agent execution service: `langbridge/runtime/services/agent_execution_service.py`
- federated execution bridge: `langbridge/runtime/execution/federated_query_tool.py`
- federated engine: `langbridge/federation/*`
- queued worker assembly: `apps/runtime_worker/main.py`

## Execution Modes

- Embedded runtime: `LangbridgeClient.local(...)` or direct runtime composition
- Self-hosted runtime host: `langbridge serve --config ...`
- Queued worker: `python -m langbridge.apps.runtime_worker.main`
- Hybrid runtime: runtime executes in customer infrastructure while integrating with a separate control layer

The current HTTP host only serves configured local runtimes in this release.

## Request Lifecycle

```mermaid
flowchart TD
    R[HTTP Request or SDK Call] --> A[Auth and Runtime Context]
    A --> D[Dataset and Connector Resolution]
    D --> S[Runtime Service]
    S --> F[Federated Query Tool when needed]
    F --> C[Connectors and Source Systems]
    C --> O[Rows, Sync Results, Agent Output]
    O --> R2[Runtime Response]
```
