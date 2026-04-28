import uuid

import pyarrow as pa
import pytest

from langbridge.federation.connectors.base import RemoteExecutionResult, RemoteSource, SourceCapabilities
from langbridge.federation.executor import (
    ArtifactStore,
    StageCacheDescriptor,
    StageCacheInput,
    StageCacheInputKind,
    StageCacheInputPolicy,
)
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


@pytest.fixture
def anyio_backend():
    return "asyncio"


class CountingRemoteSource(RemoteSource):
    def __init__(
        self,
        *,
        source_id: str,
        tables: dict[str, pa.Table],
        dialect: str = "duckdb",
    ) -> None:
        self.source_id = source_id
        self._tables = dict(tables)
        self._dialect = dialect
        self.execute_count = 0

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
        self.execute_count += 1
        table = self._tables.get(subplan.table_key)
        if table is None:
            raise ValueError(f"Unknown mock table key '{subplan.table_key}'.")

        projected = table
        if subplan.projected_columns:
            columns = [column for column in subplan.projected_columns if column in projected.column_names]
            if columns:
                projected = projected.select(columns)

        if subplan.pushed_limit is not None and subplan.pushed_limit >= 0:
            projected = projected.slice(0, subplan.pushed_limit)

        return RemoteExecutionResult(table=projected, elapsed_ms=1)

    async def estimate_table_stats(self, binding: VirtualTableBinding) -> TableStatistics:
        table = self._tables.get(binding.table_key)
        if table is None:
            return TableStatistics(row_count_estimate=1000.0, bytes_per_row=128.0)
        return TableStatistics(
            row_count_estimate=float(table.num_rows),
            bytes_per_row=float(table.nbytes / max(table.num_rows, 1)),
        )


def _stage_metrics_by_id(handle) -> dict[str, object]:
    return {
        metric.stage_id: metric
        for metric in handle.execution.stage_metrics
    }


def _binding(
    *,
    table_key: str,
    source_id: str,
    dataset_id: uuid.UUID,
    freshness: DatasetFreshnessDescriptor,
    materialization_mode: str,
    source_kind: str = "file",
    storage_kind: str = "parquet",
) -> VirtualTableBinding:
    return VirtualTableBinding(
        table_key=table_key,
        source_id=source_id,
        table=table_key,
        metadata={
            "dataset_id": str(dataset_id),
            "materialization_mode": materialization_mode,
        },
        dataset_descriptor=DatasetExecutionDescriptor(
            dataset_id=dataset_id,
            connector_id=None,
            name=table_key,
            materialization_mode=materialization_mode,
            source_kind=source_kind,
            storage_kind=storage_kind,
            relation_identity={"canonical_reference": f"dataset:{dataset_id}"},
            execution_capabilities={"supports_structured_scan": True, "supports_sql_federation": True},
            freshness=freshness,
        ),
    )


def _single_table_workflow(
    *,
    workspace_id: str,
    table_key: str,
    source_id: str,
    dataset_id: uuid.UUID,
    freshness: DatasetFreshnessDescriptor,
    materialization_mode: str,
    source_kind: str = "file",
    storage_kind: str = "parquet",
) -> FederationWorkflow:
    return FederationWorkflow(
        id=f"wf-{table_key}",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id=f"ds-{table_key}",
            name=table_key,
            workspace_id=workspace_id,
            tables={
                table_key: _binding(
                    table_key=table_key,
                    source_id=source_id,
                    dataset_id=dataset_id,
                    freshness=freshness,
                    materialization_mode=materialization_mode,
                    source_kind=source_kind,
                    storage_kind=storage_kind,
                )
            },
        ),
    )


@pytest.mark.anyio
async def test_synced_stage_cache_reuses_when_revision_is_unchanged(tmp_path) -> None:
    workspace_id = str(uuid.uuid4())
    dataset_id = uuid.uuid4()
    revision_id = uuid.uuid4()
    artifact_store = ArtifactStore(base_dir=str(tmp_path / "artifacts"))
    service = FederatedQueryService(artifact_store=artifact_store)
    source = CountingRemoteSource(
        source_id="file_orders",
        tables={"orders": pa.table({"id": [1, 2, 3]})},
    )
    workflow = _single_table_workflow(
        workspace_id=workspace_id,
        table_key="orders",
        source_id="file_orders",
        dataset_id=dataset_id,
        freshness=DatasetFreshnessDescriptor(
            policy=DatasetFreshnessPolicy.REVISION,
            freshness_key=f"dataset-revision:{revision_id}",
            revision_id=revision_id,
        ),
        materialization_mode="synced",
    )

    handle_one = await service.execute(
        query="SELECT id FROM orders ORDER BY id",
        dialect="duckdb",
        workspace_id=workspace_id,
        workflow=workflow,
        sources={"file_orders": source},
    )
    handle_two = await service.execute(
        query="SELECT id FROM orders ORDER BY id",
        dialect="duckdb",
        workspace_id=workspace_id,
        workflow=workflow,
        sources={"file_orders": source},
    )

    assert source.execute_count == 1
    assert (await service.fetch_arrow(handle_two)).to_pylist() == [{"id": 1}, {"id": 2}, {"id": 3}]
    first_metric = _stage_metrics_by_id(handle_one)["scan_full_query"]
    second_metric = _stage_metrics_by_id(handle_two)["scan_full_query"]
    assert first_metric.cached is False
    assert first_metric.stage_type.value == "remote_full_query"
    assert first_metric.source_id == "file_orders"
    assert first_metric.cache_status.value == "miss"
    assert "dataset revision freshness" in str(first_metric.cache_reason or "").lower()
    assert second_metric.cached is True
    assert second_metric.cache_status.value == "hit"
    assert "dataset revision freshness matched" in str(second_metric.cache_reason or "").lower()


@pytest.mark.anyio
async def test_synced_stage_cache_invalidates_when_revision_changes(tmp_path) -> None:
    workspace_id = str(uuid.uuid4())
    dataset_id = uuid.uuid4()
    revision_one = uuid.uuid4()
    revision_two = uuid.uuid4()
    artifact_store = ArtifactStore(base_dir=str(tmp_path / "artifacts"))
    service = FederatedQueryService(artifact_store=artifact_store)
    source = CountingRemoteSource(
        source_id="file_orders",
        tables={"orders": pa.table({"id": [1, 2, 3]})},
    )

    workflow_one = _single_table_workflow(
        workspace_id=workspace_id,
        table_key="orders",
        source_id="file_orders",
        dataset_id=dataset_id,
        freshness=DatasetFreshnessDescriptor(
            policy=DatasetFreshnessPolicy.REVISION,
            freshness_key=f"dataset-revision:{revision_one}",
            revision_id=revision_one,
        ),
        materialization_mode="synced",
    )
    handle_one = await service.execute(
        query="SELECT id FROM orders ORDER BY id",
        dialect="duckdb",
        workspace_id=workspace_id,
        workflow=workflow_one,
        sources={"file_orders": source},
    )

    workflow_two = _single_table_workflow(
        workspace_id=workspace_id,
        table_key="orders",
        source_id="file_orders",
        dataset_id=dataset_id,
        freshness=DatasetFreshnessDescriptor(
            policy=DatasetFreshnessPolicy.REVISION,
            freshness_key=f"dataset-revision:{revision_two}",
            revision_id=revision_two,
        ),
        materialization_mode="synced",
    )
    handle_two = await service.execute(
        query="SELECT id FROM orders ORDER BY id",
        dialect="duckdb",
        workspace_id=workspace_id,
        workflow=workflow_two,
        sources={"file_orders": source},
    )

    assert handle_one.plan_id == handle_two.plan_id
    assert source.execute_count == 2
    second_metric = _stage_metrics_by_id(handle_two)["scan_full_query"]
    assert second_metric.cached is False
    assert second_metric.cache_status.value == "miss"
    assert second_metric.cache_inputs[0].freshness_key_present is True

    manifest = artifact_store.get_stage_output_manifest(
        workspace_id=workspace_id,
        plan_id=handle_two.plan_id,
        stage_id=handle_two.result_stage_id,
    )
    assert manifest is not None
    assert manifest.cache is not None
    assert manifest.cache.cacheable is True
    assert manifest.cache.inputs[0].revision_id == revision_two
    assert manifest.cache.inputs[0].freshness_key == f"dataset-revision:{revision_two}"


@pytest.mark.anyio
async def test_live_dataset_stage_cache_is_bypassed(tmp_path) -> None:
    workspace_id = str(uuid.uuid4())
    artifact_store = ArtifactStore(base_dir=str(tmp_path / "artifacts"))
    service = FederatedQueryService(artifact_store=artifact_store)
    source = CountingRemoteSource(
        source_id="api_orders",
        tables={"orders": pa.table({"id": [1, 2]})},
    )
    workflow = _single_table_workflow(
        workspace_id=workspace_id,
        table_key="orders",
        source_id="api_orders",
        dataset_id=uuid.uuid4(),
        freshness=DatasetFreshnessDescriptor(
            policy=DatasetFreshnessPolicy.VOLATILE,
            reason="Live datasets bypass federation stage cache.",
        ),
        materialization_mode="live",
        source_kind="api",
        storage_kind="json",
    )

    handle_one = await service.execute(
        query="SELECT id FROM orders ORDER BY id",
        dialect="duckdb",
        workspace_id=workspace_id,
        workflow=workflow,
        sources={"api_orders": source},
    )
    handle_two = await service.execute(
        query="SELECT id FROM orders ORDER BY id",
        dialect="duckdb",
        workspace_id=workspace_id,
        workflow=workflow,
        sources={"api_orders": source},
    )

    assert source.execute_count == 2
    first_metric = _stage_metrics_by_id(handle_one)["scan_full_query"]
    second_metric = _stage_metrics_by_id(handle_two)["scan_full_query"]
    assert first_metric.cached is False
    assert first_metric.cache_status.value == "bypass"
    assert "bypass" in str(first_metric.cache_reason or "").lower()
    assert second_metric.cached is False
    assert second_metric.cache_status.value == "bypass"


@pytest.mark.anyio
async def test_local_stage_cache_invalidates_when_dependency_revision_changes(tmp_path) -> None:
    workspace_id = str(uuid.uuid4())
    artifact_store = ArtifactStore(base_dir=str(tmp_path / "artifacts"))
    service = FederatedQueryService(artifact_store=artifact_store)
    orders_dataset_id = uuid.uuid4()
    customers_dataset_id = uuid.uuid4()
    orders_revision_one = uuid.uuid4()
    orders_revision_two = uuid.uuid4()
    customers_revision = uuid.uuid4()

    orders_source = CountingRemoteSource(
        source_id="src_orders",
        tables={
            "orders": pa.table(
                {
                    "id": [1, 2],
                    "customer_id": [10, 11],
                }
            )
        },
    )
    customers_source = CountingRemoteSource(
        source_id="src_customers",
        tables={
            "customers": pa.table(
                {
                    "id": [10, 11],
                    "name": ["Acme", "Globex"],
                }
            )
        },
    )

    def build_join_workflow(*, orders_revision: uuid.UUID) -> FederationWorkflow:
        return FederationWorkflow(
            id="wf-join",
            workspace_id=workspace_id,
            stage_parallelism=2,
            dataset=VirtualDataset(
                id="ds-join",
                name="join",
                workspace_id=workspace_id,
                tables={
                    "orders": _binding(
                        table_key="orders",
                        source_id="src_orders",
                        dataset_id=orders_dataset_id,
                        freshness=DatasetFreshnessDescriptor(
                            policy=DatasetFreshnessPolicy.REVISION,
                            freshness_key=f"dataset-revision:{orders_revision}",
                            revision_id=orders_revision,
                        ),
                        materialization_mode="synced",
                    ),
                    "customers": _binding(
                        table_key="customers",
                        source_id="src_customers",
                        dataset_id=customers_dataset_id,
                        freshness=DatasetFreshnessDescriptor(
                            policy=DatasetFreshnessPolicy.REVISION,
                            freshness_key=f"dataset-revision:{customers_revision}",
                            revision_id=customers_revision,
                        ),
                        materialization_mode="synced",
                    ),
                },
            ),
        )

    query = (
        "SELECT o.id, c.name "
        "FROM orders o "
        "JOIN customers c ON o.customer_id = c.id "
        "ORDER BY o.id"
    )

    handle_one = await service.execute(
        query=query,
        dialect="duckdb", 
        workspace_id=workspace_id,
        workflow=build_join_workflow(orders_revision=orders_revision_one),
        sources={
            "src_orders": orders_source,
            "src_customers": customers_source,
        }
    )
    handle_two = await service.execute(
        query=query, 
        dialect="duckdb", 
        workspace_id=workspace_id,
        workflow=build_join_workflow(orders_revision=orders_revision_one),
        sources={
            "src_orders": orders_source,
            "src_customers": customers_source,
        }
    )
    handle_three = await service.execute(
        query=query, 
        dialect="duckdb", 
        workspace_id=workspace_id,
        workflow=build_join_workflow(orders_revision=orders_revision_two),
        sources={
            "src_orders": orders_source,
            "src_customers": customers_source,
        },
    )

    metrics_two = _stage_metrics_by_id(handle_two)
    metrics_three = _stage_metrics_by_id(handle_three)

    assert orders_source.execute_count == 2
    assert customers_source.execute_count == 1
    assert metrics_two["scan_o"].cached is True
    assert metrics_two["scan_c"].cached is True
    assert metrics_two["local_compute_final"].cached is True
    assert metrics_two["local_compute_final"].cache_status.value == "hit"
    assert metrics_three["scan_o"].cached is False
    assert metrics_three["scan_c"].cached is True
    assert metrics_three["local_compute_final"].cached is False
    assert metrics_three["scan_o"].cache_status.value == "miss"
    assert metrics_three["scan_c"].cache_status.value == "hit"
    assert metrics_three["local_compute_final"].cache_status.value == "miss"
    assert (await service.fetch_arrow(handle_one)).to_pylist() == [
        {"id": 1, "name": "Acme"},
        {"id": 2, "name": "Globex"},
    ]


def test_artifact_manifest_persists_cache_context_and_rejects_mismatched_fingerprints(tmp_path) -> None:
    workspace_id = str(uuid.uuid4())
    plan_id = "plan-cache-test"
    stage_id = "scan_orders"
    dataset_id = uuid.uuid4()
    revision_one = uuid.uuid4()
    revision_two = uuid.uuid4()
    artifact_store = ArtifactStore(base_dir=str(tmp_path / "artifacts"))
    table = pa.table({"id": [1]})
    cache_one = StageCacheDescriptor.from_inputs(
        inputs=[
            StageCacheInput(
                kind=StageCacheInputKind.DATASET,
                cache_policy=StageCacheInputPolicy.REVISION,
                source_id="file_orders",
                table_key="orders",
                dataset_id=dataset_id,
                canonical_reference=f"dataset:{dataset_id}",
                materialization_mode="synced",
                freshness_key=f"dataset-revision:{revision_one}",
                revision_id=revision_one,
            )
        ]
    )
    cache_two = StageCacheDescriptor.from_inputs(
        inputs=[
            StageCacheInput(
                kind=StageCacheInputKind.DATASET,
                cache_policy=StageCacheInputPolicy.REVISION,
                source_id="file_orders",
                table_key="orders",
                dataset_id=dataset_id,
                canonical_reference=f"dataset:{dataset_id}",
                materialization_mode="synced",
                freshness_key=f"dataset-revision:{revision_two}",
                revision_id=revision_two,
            )
        ]
    )

    artifact = artifact_store.write_stage_output(
        workspace_id=workspace_id,
        plan_id=plan_id,
        stage_id=stage_id,
        table=table,
        cache=cache_one,
    )
    manifest = artifact_store.get_stage_output_manifest(
        workspace_id=workspace_id,
        plan_id=plan_id,
        stage_id=stage_id,
    )

    assert manifest is not None
    assert manifest.artifact.artifact_key == artifact.artifact_key
    assert manifest.cache is not None
    assert manifest.cache.cacheable is True
    assert manifest.cache.inputs[0].freshness_key == f"dataset-revision:{revision_one}"
    assert artifact_store.get_cached_stage_output(
        workspace_id=workspace_id,
        plan_id=plan_id,
        stage_id=stage_id,
        expected_cache=cache_one,
    ) is not None
    assert artifact_store.get_cached_stage_output(
        workspace_id=workspace_id,
        plan_id=plan_id,
        stage_id=stage_id,
        expected_cache=cache_two,
    ) is None
