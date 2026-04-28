import asyncio
import time
import uuid

import pyarrow as pa
import pytest

from langbridge.connectors.base.connector import ApiExtractResult
from langbridge.federation.connectors.api import ApiConnectorRemoteSource
from langbridge.federation.executor import ArtifactStore, StageExecutor
from langbridge.federation.models import (
    DatasetExecutionDescriptor,
    FederationWorkflow,
    VirtualDataset,
    VirtualTableBinding,
)
from langbridge.federation.models.plans import SourceSubplan
from langbridge.federation.service import FederatedQueryService
from tests.helpers.federation_mock import MockArrowRemoteSource


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class _FakeApiConnector:
    def __init__(self, payloads: dict[str, list[dict[str, object]]]) -> None:
        self._payloads = payloads

    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiExtractResult:
        return ApiExtractResult(
            resource=resource_name,
            records=list(self._payloads.get(resource_name, [])),
        )


def _build_join_workflow(*, workspace_id: str) -> FederationWorkflow:
    return FederationWorkflow(
        id="wf-concurrency-join",
        workspace_id=workspace_id,
        stage_parallelism=2,
        dataset=VirtualDataset(
            id="ds-concurrency-join",
            name="concurrency_join",
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


def _build_single_table_workflow(*, workspace_id: str) -> FederationWorkflow:
    return FederationWorkflow(
        id="wf-concurrency-single",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="ds-concurrency-single",
            name="concurrency_single",
            workspace_id=workspace_id,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="src_orders",
                    connector_id=uuid.uuid4(),
                    table="orders",
                )
            },
        ),
    )


@pytest.mark.anyio
async def test_federated_query_service_offloads_local_compute_stage(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = str(uuid.uuid4())
    service = FederatedQueryService(
        artifact_store=ArtifactStore(base_dir=str(tmp_path / "artifacts")),
    )
    orders = pa.table({"id": [1, 2], "customer_id": [10, 11]})
    customers = pa.table({"id": [10, 11], "name": ["Acme", "Globex"]})
    query = (
        "SELECT o.id, c.name "
        "FROM public.orders AS o "
        "JOIN public.customers AS c ON o.customer_id = c.id "
        "ORDER BY o.id"
    )

    original = StageExecutor._execute_local_compute_stage_blocking

    def _slow_local_compute(self, stage, context, cache_descriptor):
        time.sleep(0.2)
        return original(self, stage, context, cache_descriptor)

    monkeypatch.setattr(StageExecutor, "_execute_local_compute_stage_blocking", _slow_local_compute)

    try:
        execution_task = asyncio.create_task(
            service.execute(
                query=query,
                dialect="tsql",
                workspace_id=workspace_id,
                workflow=_build_join_workflow(workspace_id=workspace_id),
                sources={
                    "src_orders": MockArrowRemoteSource(
                        source_id="src_orders",
                        tables={"orders": orders},
                    ),
                    "src_customers": MockArrowRemoteSource(
                        source_id="src_customers",
                        tables={"customers": customers},
                    ),
                },
            )
        )
        probe_task = asyncio.create_task(asyncio.sleep(0.02))
        done, _ = await asyncio.wait(
            {execution_task, probe_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        assert probe_task in done
        assert not execution_task.done()

        handle = await execution_task
        table = await service.fetch_arrow(handle)
        assert table.to_pylist() == [{"id": 1, "name": "Acme"}, {"id": 2, "name": "Globex"}]
    finally:
        await service.aclose()


@pytest.mark.anyio
async def test_federated_query_service_offloads_artifact_reads(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = str(uuid.uuid4())
    artifact_store = ArtifactStore(base_dir=str(tmp_path / "artifacts"))
    service = FederatedQueryService(artifact_store=artifact_store)

    try:
        handle = await service.execute(
            query="SELECT id FROM orders ORDER BY id",
            dialect="duckdb",
            workspace_id=workspace_id,
            workflow=_build_single_table_workflow(workspace_id=workspace_id),
            sources={
                "src_orders": MockArrowRemoteSource(
                    source_id="src_orders",
                    tables={"orders": pa.table({"id": [1, 2, 3]})},
                    dialect="duckdb",
                )
            },
        )

        original_read_artifact = artifact_store.read_artifact

        def _slow_read_artifact(artifact_key: str):
            time.sleep(0.2)
            return original_read_artifact(artifact_key)

        monkeypatch.setattr(artifact_store, "read_artifact", _slow_read_artifact)

        fetch_task = asyncio.create_task(service.fetch_arrow(handle))
        probe_task = asyncio.create_task(asyncio.sleep(0.02))
        done, _ = await asyncio.wait(
            {fetch_task, probe_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        assert probe_task in done
        assert not fetch_task.done()

        table = await fetch_task
        assert table.to_pylist() == [{"id": 1}, {"id": 2}, {"id": 3}]
    finally:
        await service.aclose()


@pytest.mark.anyio
async def test_api_connector_remote_source_offloads_materialization_and_duckdb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connector = _FakeApiConnector(
        {
            "customers": [
                {"id": "cus_001", "email": "ada@example.com"},
                {"id": "cus_002", "email": "grace@example.com"},
            ],
            "orders": [
                {"id": "ord_001", "customer_id": "cus_001", "amount": 42.0},
                {"id": "ord_002", "customer_id": "cus_002", "amount": 7.0},
            ],
        }
    )
    source = ApiConnectorRemoteSource(
        source_id="api_source",
        connector=connector,
        bindings=[
            VirtualTableBinding(
                table_key="customers",
                source_id="api_source",
                connector_id=uuid.uuid4(),
                table="customers",
                metadata={"api_resource": "customers"},
                dataset_descriptor=DatasetExecutionDescriptor(
                    source_kind="api",
                    connector_kind="stripe",
                    storage_kind="memory",
                    materialization_mode="live",
                    source={"resource": "customers"},
                    relation_identity={},
                    execution_capabilities={},
                ),
            ),
            VirtualTableBinding(
                table_key="orders",
                source_id="api_source",
                connector_id=uuid.uuid4(),
                table="orders",
                metadata={"api_resource": "orders"},
                dataset_descriptor=DatasetExecutionDescriptor(
                    source_kind="api",
                    connector_kind="stripe",
                    storage_kind="memory",
                    materialization_mode="live",
                    source={"resource": "orders"},
                    relation_identity={},
                    execution_capabilities={},
                ),
            ),
        ],
    )

    original = ApiConnectorRemoteSource._execute_subplan_blocking

    def _slow_execute(self, sql: str, binding_payloads):
        time.sleep(0.2)
        return original(self, sql, binding_payloads)

    monkeypatch.setattr(ApiConnectorRemoteSource, "_execute_subplan_blocking", _slow_execute)

    execute_task = asyncio.create_task(
        source.execute(
            SourceSubplan(
                stage_id="scan_full_query",
                source_id="api_source",
                alias="customers",
                table_key="customers",
                sql=(
                    "SELECT c.email, o.amount "
                    "FROM customers AS c "
                    "JOIN orders AS o ON o.customer_id = c.id "
                    "WHERE o.amount > 10 "
                    "ORDER BY o.amount DESC"
                ),
            )
        )
    )
    probe_task = asyncio.create_task(asyncio.sleep(0.02))
    done, _ = await asyncio.wait(
        {execute_task, probe_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    assert probe_task in done
    assert not execute_task.done()

    result = await execute_task
    assert result.table.to_pylist() == [{"email": "ada@example.com", "amount": 42.0}]
