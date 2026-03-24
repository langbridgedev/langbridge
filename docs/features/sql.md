# SQL Feature

Langbridge supports SQL execution across direct connectors and federated
datasets.

## Runtime Scope

- direct SQL execution for SQL-capable connectors
- federated SQL over workspace datasets
- parameter handling
- read-only guardrails by default
- preview limits, timeouts, and redaction

## Execution Model

1. A SQL request enters the runtime.
2. An explicit `connection_name` or `connection_id` uses direct connector SQL.
3. Otherwise the runtime uses dataset-backed federated SQL.
4. `selected_datasets` is an optional subset selector that narrows planner scope by dataset id.
5. The SQL service resolves workspace-scoped datasets, derives SQL-safe aliases from dataset metadata, and applies limits.
6. The runtime returns rows and execution metadata.

## Guardrails

- DML is disabled by default
- preview and timeout limits are enforced in the runtime
- redaction is applied from dataset policy where configured
- the same runtime identity model is used as the rest of the system
