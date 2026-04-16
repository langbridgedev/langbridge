# SQL Feature

Langbridge supports an explicit three-scope SQL model in the runtime.

## Runtime Scope

- governed semantic SQL over runtime semantic models
- dataset SQL over workspace datasets
- source SQL for SQL-capable connectors
- parameter handling
- read-only guardrails by default
- preview limits, timeouts, and redaction

## Execution Model

1. A SQL request enters the runtime with an explicit `query_scope`.
2. `query_scope: "semantic"` parses a governed SQL subset whose `FROM` target is one semantic model name.
3. Semantic SQL compiles into the existing semantic query path rather than falling through to direct connector SQL.
4. Semantic graphs are not direct semantic SQL targets in V1; semantic SQL still requires an executable semantic model.
5. `query_scope: "dataset"` uses dataset-backed runtime SQL over runtime datasets.
6. `selected_datasets` is only valid for dataset scope and narrows planner scope by dataset id.
7. `query_scope: "source"` uses direct connector or source SQL and requires `connection_name` or `connection_id`.
8. The runtime returns rows plus scope-aware execution metadata.

## Semantic SQL Shape

- one runtime semantic model in `FROM`
- semantic members and semantic metrics in `SELECT`
- semantic member filters in `WHERE`
- `GROUP BY` entries that match the selected semantic dimensions and time buckets
- `ORDER BY` entries that reference semantic members, selected aliases or ordinals, and semantic time buckets
- time buckets through `DATE_TRUNC(...)` or `TIMESTAMP_TRUNC(...)`
- governed compilation through the semantic execution service

Semantic SQL is intentionally constrained. It does not accept joins, CTEs,
`HAVING`, or `DISTINCT`, and it does not treat semantic scope as raw source SQL
in disguise.

### Supported SELECT Surface

Semantic SQL `SELECT` is intentionally narrow for V1:

- semantic members
- semantic metrics
- `DATE_TRUNC(...)` and `TIMESTAMP_TRUNC(...)` over semantic time dimensions

Semantic SQL `SELECT` does not allow arbitrary SQL expressions such as `MIN(...)`,
`MAX(...)`, `CASE`, scalar arithmetic, or mixed raw SQL function calls. If a
calculation should be governed, add it as a semantic metric. If the request is
free-form SQL, run it in dataset SQL scope instead.

### Feedback And Recovery Guidance

When semantic SQL rejects a query, the runtime returns product-facing guidance
that explains:

- what construct was rejected
- why semantic SQL rejects it
- what semantic SQL supports instead
- when to add a semantic metric or semantic member to the model
- when to use dataset SQL scope for free-form SQL

Common examples:

- joins are rejected because governed relationships belong to the semantic model
- free-form aggregates in `SELECT` should become semantic metrics
- `GROUP BY` must match the selected semantic dimensions and time buckets
- LIKE and ILIKE patterns are limited to exact, prefix, suffix, or contains shapes

## Guardrails

- DML is disabled by default
- preview and timeout limits are enforced in the runtime
- redaction is applied from dataset policy where configured
- the same runtime identity model is used as the rest of the system
