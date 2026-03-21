# SQL Feature

Langbridge supports SQL execution across direct connectors and federated
datasets.

## Runtime Scope

- direct SQL execution for SQL-capable connectors
- federated SQL over selected datasets
- parameter handling
- read-only guardrails by default
- preview limits, timeouts, and redaction

## Execution Model

1. A SQL request enters the runtime.
2. The runtime determines whether it is direct connector SQL or dataset-backed federated SQL.
3. The SQL service resolves workspace-scoped datasets, connectors, and limits.
4. Federation is used when the workload spans runtime datasets or requires local compute.
5. The runtime returns rows and execution metadata.

## Guardrails

- DML is disabled by default
- preview and timeout limits are enforced in the runtime
- redaction is applied from dataset policy where configured
- the same runtime identity model is used as the rest of the system
