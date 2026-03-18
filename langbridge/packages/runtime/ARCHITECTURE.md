# Runtime Architecture Notes

## Direction

The runtime package is being migrated toward runtime-owned boundaries:

- runtime-native models in `packages/runtime/models`
- runtime-native ports in `packages/runtime/ports`
- backend adapters that translate legacy repository and API payloads into those runtime models

This is intended to reduce direct coupling from `packages/runtime` into:

- `packages/contracts`
- `packages/common/langbridge_common`

## Modes

The target runtime modes are:

- `local_ephemeral`
  - config-backed metadata
  - in-memory runtime state
- `local_persistent`
  - local persistent control store, initially SQLite-backed
- `hybrid`
  - control-plane API backed metadata/state
- managed cloud
  - composed in `langbridge-cloud`, not in the portable runtime package

## Transitional Rules

During migration:

- read-oriented runtime services should prefer runtime ports
- write-heavy services may continue using legacy repositories until explicit command ports are introduced
- compatibility adapters in `packages/runtime/adapters` are allowed to depend on legacy repository and API shapes
- new runtime internals should not add fresh dependencies on `packages/contracts` unless they are true external API contracts

## Current First Slice

The first migration slice introduces:

- runtime-native metadata/state models
- runtime-owned job/request models that preserve worker payload compatibility
- runtime ports replacing contract-typed provider boundaries
- repository-backed and API-backed providers returning runtime-native models
- real in-memory providers for ephemeral mode work

## Current Dataset Slice

The dataset catalog and sync slice now also uses runtime-owned mutable boundaries:

- runtime-owned dataset catalog, column, policy, revision, lineage, and sync-state store ports
- repository adapters in `packages/runtime/adapters/stores.py` that keep SQLAlchemy repositories below the runtime service boundary
- `DatasetQueryService` and `ConnectorSyncRuntime` operating on runtime models instead of ORM records
- configured local runtime in-memory stores holding runtime models instead of fake ORM rows
- explicit `save(...)` semantics for mutable runtime stores so persistence does not rely on SQLAlchemy identity tracking

## Current Agent Slice

The agent execution path now also uses runtime-owned contracts for mutable state and lookup dependencies:

- runtime-owned agent definition, thread, thread-message, and conversation-memory models
- runtime store ports for agent definitions, LLM connections, threads, thread messages, semantic models, and conversation memory
- repository adapters in `packages/runtime/adapters/stores.py` that keep `common` repositories behind the runtime service boundary
- `AgentExecutionService` and `MemoryManager` operating on runtime models instead of `common` thread and memory records
- configured local runtime in-memory agent/thread/memory stores holding runtime models instead of ORM-shaped records
- runtime-owned agent event and embedding helper modules now used by the runtime service, orchestrator runtime, and worker event emitters instead of `common` helper types

## Current SQL Slice

The SQL execution boundary now also uses runtime-owned contracts:

- runtime-owned `SqlJob` state plus runtime SQL artifact models
- runtime store ports for SQL jobs and SQL result artifacts
- `SqlQueryService` operating on runtime SQL job models instead of `common` SQL ORM records
- worker-side SQL job handling adapting legacy repositories into runtime stores at the app boundary
- repository `save(...)` exposed as a public compatibility API so runtime adapters no longer reach into repository `_session` internals directly

## Current Semantic Slice

The semantic query execution service now also sits on runtime-owned boundaries:

- `SemanticQueryExecutionService` consuming runtime semantic-model metadata providers instead of `common` semantic repositories
- repository-backed semantic metadata lookups staying in bootstrap and worker/app composition
- worker-side semantic query handling adapting legacy semantic repositories into runtime providers at the app boundary
- worker-side semantic request parsing and semantic result shaping using runtime-owned semantic job/request models instead of `common` semantic contracts

## Current Federation Slice

The federated execution entrypoint now also sits on runtime-owned connector metadata:

- `FederatedQueryTool` consuming runtime connector metadata providers instead of `common` connector repositories
- SQL-capability checks derived from runtime connector metadata instead of ORM subtype checks
- repository-backed connector lookup staying in bootstrap and worker/app composition
- file-backed federation sources remaining unchanged and continuing to bypass connector lookup entirely

## Current Runtime Utility Slice

The dataset, SQL, and semantic execution paths now also own their core helper layer:

- runtime-owned `BusinessValidationError` in `packages/runtime/errors.py`
- runtime-owned dataset identity/capability models in `packages/runtime/models/metadata.py`
- runtime-owned SQL, dataset, lineage, and storage helpers in `packages/runtime/utils`
- `DatasetExecutionResolver`, `DatasetQueryService`, `ConnectorSyncRuntime`, `SqlQueryService`, and `SemanticQueryExecutionService` no longer importing `common` utility or error modules for their core runtime mechanics
- worker query handlers and semantic-query-builder orchestration helpers now also use the runtime-owned validation and SQL helper modules instead of `common`

## Current Connector Sync Edge Slice

The connector sync worker edge now also uses runtime-owned request and helper surfaces:

- runtime-owned `CreateConnectorSyncJobRequest` in `packages/runtime/models/jobs.py`
- runtime-owned connector payload resolution helper in `packages/runtime/utils/connector_runtime.py`
- worker-side connector sync request parsing and validation now using runtime models and runtime `BusinessValidationError`
- local config and worker sync handling no longer importing the legacy connector runtime helper from `common`

## Current Orchestrator Contract Slice

The remaining contract-typed orchestration helpers are also being pulled onto runtime-owned models:

- semantic-query-builder copilot request/preview flow now using runtime semantic request/meta/response models instead of `packages.contracts.semantic`
- `llm_tester` now using runtime-owned `LLMProvider` values instead of `packages.contracts.llm_connections`
- `packages.contracts` is no longer imported by runtime-local orchestration helpers in this slice

## Next Slice

Follow-on slices should move more services to runtime-native models and shrink the remaining direct `common` and `contracts` imports, especially:

- any remaining runtime-local imports of `common` repository types outside adapter/bootstrap boundaries
- any remaining worker app-edge imports of legacy request/contract models that can move to runtime-owned request types
- any cloud-facing broker or edge-runtime contracts that should remain app-edge or move to `langbridge-cloud`
