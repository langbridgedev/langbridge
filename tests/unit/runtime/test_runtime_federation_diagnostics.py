import uuid

import pyarrow as pa
import pytest

from langbridge.federation.connectors.base import RemoteExecutionResult, RemoteSource, SourceCapabilities
from langbridge.federation.executor import ArtifactStore
from langbridge.federation.models import (
    DatasetExecutionDescriptor,
    DatasetFreshnessDescriptor,
    DatasetFreshnessPolicy,
    FederationWorkflow,
    VirtualDataset,
    VirtualTableBinding,
)
from langbridge.federation.models.plans import SourceSubplan
from langbridge.federation.models.virtual_dataset import TableStatistics
from langbridge.federation.service import FederatedQueryService
from langbridge.runtime.services.federation_diagnostics import build_runtime_federation_diagnostics
from tests.helpers.federation_mock import MockArrowRemoteSource


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FixedLatencyRemoteSource(RemoteSource):
    def __init__(self, *, source_id: str, table: pa.Table, elapsed_ms: int = 7) -> None:
        self.source_id = source_id
        self._table = table
        self._elapsed_ms = elapsed_ms

    def capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(
            pushdown_filter=True,
            pushdown_projection=True,
            pushdown_aggregation=True,
            pushdown_limit=True,
            pushdown_join=False,
        )

    def dialect(self) -> str:
        return "duckdb"

    async def execute(self, subplan: SourceSubplan) -> RemoteExecutionResult:
        projected = self._table
        if subplan.projected_columns:
            columns = [column for column in subplan.projected_columns if column in projected.column_names]
            if columns:
                projected = projected.select(columns)
        return RemoteExecutionResult(table=projected, elapsed_ms=self._elapsed_ms)

    async def estimate_table_stats(self, binding: VirtualTableBinding) -> TableStatistics:
        return TableStatistics(
            row_count_estimate=float(self._table.num_rows),
            bytes_per_row=float(self._table.nbytes / max(self._table.num_rows, 1)),
        )


def _dataset_binding(
    *,
    table_key: str,
    source_id: str,
    dataset_id: uuid.UUID,
    freshness: DatasetFreshnessDescriptor,
) -> VirtualTableBinding:
    return VirtualTableBinding(
        table_key=table_key,
        source_id=source_id,
        table=table_key,
        metadata={"dataset_id": str(dataset_id), "materialization_mode": "synced"},
        dataset_descriptor=DatasetExecutionDescriptor(
            dataset_id=dataset_id,
            connector_id=None,
            name=table_key,
            materialization_mode="synced",
            source_kind="file",
            storage_kind="parquet",
            relation_identity={"canonical_reference": f"dataset:{dataset_id}"},
            execution_capabilities={"supports_structured_scan": True, "supports_sql_federation": True},
            freshness=freshness,
        ),
    )


@pytest.mark.anyio
async def test_runtime_federation_diagnostics_explain_surface_reports_plan_and_pushdown_reasons(tmp_path) -> None:
    workspace_id = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-diagnostics-explain",
        workspace_id=workspace_id,
        stage_parallelism=2,
        dataset=VirtualDataset(
            id="ds-diagnostics-explain",
            name="diagnostics explain",
            workspace_id=workspace_id,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="src_orders",
                    connector_id=uuid.uuid4(),
                    schema="public",
                    table="orders",
                ),
                "customers": VirtualTableBinding(
                    table_key="customers",
                    source_id="src_customers",
                    connector_id=uuid.uuid4(),
                    schema="public",
                    table="customers",
                ),
            },
        ),
    )
    service = FederatedQueryService(artifact_store=ArtifactStore(base_dir=str(tmp_path / "artifacts")))

    explain = await service.explain(
        query=(
            "SELECT c.name, SUM(o.amount) AS total_amount "
            "FROM public.orders AS o "
            "JOIN public.customers AS c ON o.customer_id = c.id "
            "GROUP BY c.name"
        ),
        dialect="postgres",
        workspace_id=workspace_id,
        workflow=workflow,
        sources={
            "src_orders": MockArrowRemoteSource(source_id="src_orders", tables={"orders": pa.table({"id": [1]})}),
            "src_customers": MockArrowRemoteSource(source_id="src_customers", tables={"customers": pa.table({"id": [1]})}),
        },
    )

    diagnostics = build_runtime_federation_diagnostics(
        workflow=workflow,
        logical_plan=explain.logical_plan,
        physical_plan=explain.physical_plan,
        execution=None,
    )

    assert diagnostics.summary.query_type == "sql"
    assert diagnostics.summary.full_query_pushdown is False
    assert diagnostics.logical_plan.joins[0].strategy is not None
    assert any("cross-source query" in reason.lower() for reason in diagnostics.pushdown.reasons)
    assert diagnostics.physical_plan.stages[-1].stage_type == "local_compute"
    assert diagnostics.stages[-1].cache.status is None


@pytest.mark.anyio
async def test_runtime_federation_diagnostics_execution_surface_reports_cache_timing_and_movement(tmp_path) -> None:
    workspace_id = str(uuid.uuid4())
    dataset_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    workflow = FederationWorkflow(
        id="wf-diagnostics-execution",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="ds-diagnostics-execution",
            name="diagnostics execution",
            workspace_id=workspace_id,
            tables={
                "orders": _dataset_binding(
                    table_key="orders",
                    source_id="src_orders",
                    dataset_id=dataset_id,
                    freshness=DatasetFreshnessDescriptor(
                        policy=DatasetFreshnessPolicy.REVISION,
                        freshness_key=f"dataset-revision:{revision_id}",
                        revision_id=revision_id,
                    ),
                )
            },
        ),
    )
    source = FixedLatencyRemoteSource(
        source_id="src_orders",
        table=pa.table({"id": [1, 2, 3], "amount": [100, 120, 140]}),
        elapsed_ms=9,
    )
    service = FederatedQueryService(artifact_store=ArtifactStore(base_dir=str(tmp_path / "artifacts")))

    await service.execute(
        query="SELECT id, amount FROM orders ORDER BY id",
        dialect="duckdb",
        workspace_id=workspace_id,
        workflow=workflow,
        sources={"src_orders": source},
    )
    handle_two = await service.execute(
        query="SELECT id, amount FROM orders ORDER BY id",
        dialect="duckdb",
        workspace_id=workspace_id,
        workflow=workflow,
        sources={"src_orders": source},
    )

    assert handle_two.logical_plan is not None
    assert handle_two.physical_plan is not None
    diagnostics = build_runtime_federation_diagnostics(
        workflow=workflow,
        logical_plan=handle_two.logical_plan,
        physical_plan=handle_two.physical_plan,
        execution=handle_two.execution,
    )

    assert diagnostics.summary.cache_hits == 1
    assert diagnostics.summary.final_rows == 3
    assert diagnostics.summary.final_bytes is not None
    assert diagnostics.stages[0].runtime_ms is not None
    assert diagnostics.stages[0].cache.status == "hit"
    assert "revision freshness matched" in str(diagnostics.stages[0].cache.reason or "").lower()
    assert diagnostics.stages[0].movement.rows == 3
    assert diagnostics.sources[0].total_rows == 3
    assert diagnostics.sources[0].total_bytes_written is not None
