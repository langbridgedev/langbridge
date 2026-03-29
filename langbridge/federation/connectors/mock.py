
import time

import pyarrow as pa

from langbridge.federation.connectors.base import RemoteExecutionResult, RemoteSource, SourceCapabilities
from langbridge.federation.models.plans import SourceSubplan
from langbridge.federation.models.virtual_dataset import TableStatistics, VirtualTableBinding


class MockArrowRemoteSource(RemoteSource):
    def __init__(self, *, source_id: str, tables: dict[str, pa.Table], dialect: str = "postgres") -> None:
        self.source_id = source_id
        self._tables = tables
        self._dialect = dialect

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
        stage_start = time.perf_counter()
        table = self._tables.get(subplan.table_key)
        if table is None:
            raise ValueError(f"Unknown mock table key '{subplan.table_key}'.")

        projected = table
        if subplan.projected_columns:
            columns = [col for col in subplan.projected_columns if col in projected.column_names]
            if columns:
                projected = projected.select(columns)

        if subplan.pushed_limit is not None and subplan.pushed_limit >= 0:
            projected = projected.slice(0, subplan.pushed_limit)

        elapsed = int((time.perf_counter() - stage_start) * 1000)
        return RemoteExecutionResult(table=projected, elapsed_ms=elapsed)

    async def estimate_table_stats(self, binding: VirtualTableBinding) -> TableStatistics:
        if binding.stats is not None:
            return binding.stats
        table = self._tables.get(binding.table_key)
        if table is None:
            return TableStatistics(row_count_estimate=1000.0, bytes_per_row=128.0)
        row_count = float(table.num_rows)
        avg_bytes = float(table.nbytes / max(table.num_rows, 1))
        return TableStatistics(row_count_estimate=row_count, bytes_per_row=avg_bytes)
