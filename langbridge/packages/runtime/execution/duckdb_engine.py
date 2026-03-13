from __future__ import annotations

from typing import Any

import duckdb

from langbridge.packages.runtime.execution.engine import ExecutionEngine, ExecutionResult


class DuckDbExecutionEngine(ExecutionEngine):
    """DuckDB adapter for local runtime execution and dataset materialization."""

    def __init__(self, *, database: str = ":memory:") -> None:
        self._database = database

    def open_connection(self) -> Any:
        return duckdb.connect(self._database)

    async def execute(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> ExecutionResult:
        connection = self.open_connection()
        try:
            relation = connection.execute(sql, parameters=params or {})
            columns = [item[0] for item in relation.description or []]
            rows = relation.fetchall()
            return ExecutionResult(
                columns=columns,
                rows=[tuple(row) for row in rows],
                rowcount=len(rows),
                sql=sql,
            )
        finally:
            connection.close()
