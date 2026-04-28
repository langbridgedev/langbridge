import uuid
from collections.abc import Mapping
from typing import Any

import sqlglot
from sqlglot import exp

from langbridge.runtime.execution.federated_query_tool import FederatedQueryTool
from langbridge.runtime.services.errors import ExecutionValidationError
from langbridge.semantic.model import Dimension


class SemanticVectorDistinctValueReader:
    """Builds and executes distinct-value queries for vector index refresh."""

    def __init__(self, *, federated_query_tool: FederatedQueryTool | None) -> None:
        self._federated_query_tool = federated_query_tool

    async def fetch_distinct_values(
        self,
        *,
        workspace_id: uuid.UUID,
        workflow: dict[str, Any],
        workflow_dialect: str,
        dataset_key: str,
        dimension: Dimension,
        max_values: int | None,
    ) -> list[str]:
        if self._federated_query_tool is None:
            raise ExecutionValidationError(
                "Federated query tool is required for semantic vector refresh."
            )

        query_sql = self.build_distinct_query(
            dataset_key=dataset_key,
            dimension=dimension,
            dialect=workflow_dialect,
            max_values=max_values,
        )
        execution = await self._federated_query_tool.execute_federated_query(
            {
                "workspace_id": str(workspace_id),
                "query": query_sql,
                "dialect": workflow_dialect,
                "workflow": workflow,
            }
        )
        return self.extract_distinct_values(execution)

    def build_distinct_query(
        self,
        *,
        dataset_key: str,
        dimension: Dimension,
        dialect: str,
        max_values: int | None,
    ) -> str:
        expression_sql = str(dimension.expression or dimension.name).strip()
        if not expression_sql:
            raise ExecutionValidationError(f"Dimension '{dimension.name}' is missing an expression.")
        try:
            expression = sqlglot.parse_one(expression_sql, read=dialect)
        except sqlglot.ParseError:
            expression = exp.Column(this=exp.Identifier(this=dimension.name, quoted=True))

        query = (
            exp.select(exp.alias_(expression.copy(), "value", quoted=True))
            .distinct()
            .from_(exp.table_(dataset_key, quoted=False))
            .where(exp.Not(this=exp.Is(this=expression.copy(), expression=exp.Null())))
        )
        if max_values is not None and max_values > 0:
            query = query.limit(max_values)
        return query.sql(dialect=dialect)

    def extract_distinct_values(self, execution: dict[str, Any]) -> list[str]:
        rows_payload = execution.get("rows") or []
        values: list[str] = []
        seen: set[str] = set()
        for row in rows_payload:
            if isinstance(row, Mapping):
                raw_value = row.get("value")
                if raw_value is None and row:
                    raw_value = next(iter(row.values()))
            elif isinstance(row, (list, tuple)):
                raw_value = row[0] if row else None
            else:
                raw_value = row
            normalized = str(raw_value or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            values.append(normalized)
        values.sort()
        return values
