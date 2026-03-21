from __future__ import annotations

import logging
import math
from typing import Any

import pyarrow as pa

from langbridge.connectors.base.connector import QueryResult, SqlConnector
from langbridge.federation.connectors.base import RemoteExecutionResult, RemoteSource, SourceCapabilities
from langbridge.federation.models.plans import SourceSubplan
from langbridge.federation.models.virtual_dataset import TableStatistics, VirtualTableBinding


def _rows_to_arrow(result: QueryResult) -> pa.Table:
    if not result.columns:
        return pa.table({})
    data: dict[str, list[Any]] = {column: [] for column in result.columns}
    for row in result.rows:
        for index, column in enumerate(result.columns):
            value = row[index] if index < len(row) else None
            data[column].append(value)
    return pa.table(data)


class SqlConnectorRemoteSource(RemoteSource):
    def __init__(
        self,
        *,
        source_id: str,
        connector: SqlConnector,
        dialect: str,
        logger: logging.Logger | None = None,
    ) -> None:
        self.source_id = source_id
        self._connector = connector
        self._dialect = dialect
        self._logger = logger or logging.getLogger(__name__)

    def capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(
            pushdown_filter=True,
            pushdown_projection=True,
            pushdown_aggregation=True,
            pushdown_limit=True,
            pushdown_join=False,
        )

    def dialect(self) -> str:
        return self._dialect

    async def execute(self, subplan: SourceSubplan) -> RemoteExecutionResult:
        self._logger.debug("Executing remote subplan stage=%s source=%s", subplan.stage_id, self.source_id)
        result = await self._connector.execute(subplan.sql)
        return RemoteExecutionResult(table=_rows_to_arrow(result), elapsed_ms=result.elapsed_ms)

    async def estimate_table_stats(self, binding: VirtualTableBinding) -> TableStatistics:
        if binding.stats is not None:
            return binding.stats

        metadata = binding.metadata if isinstance(binding.metadata, dict) else {}
        physical_sql = metadata.get("physical_sql")
        if isinstance(physical_sql, str) and physical_sql.strip():
            query = f"SELECT COUNT(*) AS row_count FROM ({physical_sql.strip().rstrip(';')}) AS dataset_stats"
        else:
            table_name = self._format_qualified_name(binding)
            query = f"SELECT COUNT(*) AS row_count FROM {table_name}"
        try:
            result = await self._connector.execute(query, timeout_s=10)
            row_count = float(result.rows[0][0]) if result.rows else None
            return TableStatistics(row_count_estimate=row_count, bytes_per_row=128.0)
        except Exception:
            table_name = self._format_qualified_name(binding)
            self._logger.warning("Falling back to heuristic stats for source=%s table=%s", self.source_id, table_name)
            return TableStatistics(row_count_estimate=1_000_000.0, bytes_per_row=128.0)

    @staticmethod
    def _format_qualified_name(binding: VirtualTableBinding) -> str:
        metadata = binding.metadata if isinstance(binding.metadata, dict) else {}
        parts = [
            metadata.get("physical_catalog", binding.catalog),
            metadata.get("physical_schema", binding.schema_name),
            metadata.get("physical_table", binding.table),
        ]
        return ".".join(part for part in parts if part)


def estimate_bytes(*, rows: float | None, bytes_per_row: float) -> float | None:
    if rows is None:
        return None
    if rows < 0:
        return None
    return float(math.ceil(rows * bytes_per_row))
