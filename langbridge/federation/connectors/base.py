
from dataclasses import dataclass

import pyarrow as pa

from langbridge.federation.models.plans import SourceSubplan
from langbridge.federation.models.virtual_dataset import TableStatistics, VirtualTableBinding


@dataclass(slots=True)
class SourceCapabilities:
    pushdown_filter: bool = True
    pushdown_projection: bool = True
    pushdown_aggregation: bool = True
    pushdown_limit: bool = True
    pushdown_join: bool = False


@dataclass(slots=True)
class RemoteExecutionResult:
    table: pa.Table
    elapsed_ms: int


class RemoteSource:
    source_id: str

    def capabilities(self) -> SourceCapabilities:
        raise NotImplementedError

    def dialect(self) -> str:
        raise NotImplementedError

    async def execute(self, subplan: SourceSubplan) -> RemoteExecutionResult:
        raise NotImplementedError

    async def estimate_table_stats(self, binding: VirtualTableBinding) -> TableStatistics:
        raise NotImplementedError
