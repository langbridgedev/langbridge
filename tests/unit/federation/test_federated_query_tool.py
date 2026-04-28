
import uuid

import pytest

from langbridge.connectors.base.connector import ApiExtractResult
from langbridge.connectors.base import BaseConnectorConfig, StorageConnector
from langbridge.runtime.models import (
    ConnectorCapabilities,
    ConnectorMetadata,
    LifecycleState,
    ManagementMode,
)
from langbridge.runtime.execution.federated_query_tool import (
    FederatedQueryTool,
)
from langbridge.runtime.providers import MemoryConnectorProvider
from langbridge.federation.connectors import (
    ApiConnectorRemoteSource,
    DuckDbFileRemoteSource,
    DuckDbParquetRemoteSource,
)
from langbridge.federation.models import (
    DatasetExecutionDescriptor,
    FederationWorkflow,
    SourceSubplan,
    VirtualDataset,
    VirtualTableBinding,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FakeApiConnector:
    def __init__(self, payloads: dict[str, list[dict[str, object]]]) -> None:
        self._payloads = payloads
        self.calls: list[str] = []

    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiExtractResult:
        self.calls.append(resource_name)
        return ApiExtractResult(
            resource=resource_name,
            records=list(self._payloads.get(resource_name, [])),
        )


class StubStorageConnector(StorageConnector):
    def __init__(self) -> None:
        super().__init__(config=BaseConnectorConfig())

    async def list_buckets(self) -> list[str]:
        return []

    async def list_objects(self, bucket: str) -> list[str]:
        return []

    async def get_object(self, bucket: str, key: str) -> bytes:
        raise NotImplementedError


def _storage_connector_metadata(*, workspace_id: str, connector_id: uuid.UUID) -> ConnectorMetadata:
    return ConnectorMetadata(
        id=connector_id,
        name="storage_demo",
        connector_type="LOCAL_FILESYSTEM",
        connector_family="storage",
        workspace_id=uuid.UUID(workspace_id),
        config={"config": {"root_path": "/tmp"}},
        capabilities=ConnectorCapabilities(
            supports_live_datasets=False,
            supports_synced_datasets=False,
            supports_incremental_sync=False,
            supports_federated_execution=False,
        ),
        management_mode=ManagementMode.CONFIG_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )


@pytest.mark.anyio
async def test_build_sources_uses_file_bindings_for_file_backed_workflow() -> None:
    workspace_id = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="workflow_file_test",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="dataset_file_test",
            name="File Dataset",
            workspace_id=workspace_id,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="file_source_orders",
                    connector_id=None,
                    table="orders",
                    metadata={
                        "source_kind": "file",
                        "storage_uri": "file:///tmp/orders.parquet",
                        "file_format": "parquet",
                    },
                )
            },
            relationships=[],
        ),
    )
    tool = FederatedQueryTool(connector_provider=MemoryConnectorProvider())

    sources = await tool._build_sources(workflow)

    assert "file_source_orders" in sources
    source = sources["file_source_orders"]
    assert isinstance(source, DuckDbFileRemoteSource)
    assert source.source_id == "file_source_orders"


@pytest.mark.anyio
async def test_build_sources_uses_descriptor_for_saas_parquet_dataset() -> None:
    workspace_id = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="workflow_descriptor_test",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="dataset_descriptor_test",
            name="Shopify Orders",
            workspace_id=workspace_id,
            tables={
                "shop.orders": VirtualTableBinding(
                    table_key="shop.orders",
                    source_id="file_source_shop_orders",
                    connector_id=None,
                    catalog="shop",
                    table="orders",
                    metadata={"dataset_id": str(uuid.uuid4())},
                    dataset_descriptor=DatasetExecutionDescriptor(
                        source_kind="saas",
                        connector_kind="shopify",
                        storage_kind="parquet",
                        relation_identity={},
                        execution_capabilities={"supports_sql_federation": True, "supports_structured_scan": True},
                    ),
                )
            },
            relationships=[],
        ),
    )
    tool = FederatedQueryTool(connector_provider=MemoryConnectorProvider())

    sources = await tool._build_sources(workflow)

    assert "file_source_shop_orders" in sources
    assert isinstance(sources["file_source_shop_orders"], DuckDbFileRemoteSource)


@pytest.mark.anyio
async def test_build_sources_uses_parquet_remote_source_for_distributed_parquet_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = str(uuid.uuid4())
    connector_id = uuid.uuid4()
    workflow = FederationWorkflow(
        id="workflow_remote_parquet_test",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="dataset_remote_parquet_test",
            name="Remote Parquet Orders",
            workspace_id=workspace_id,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="parquet_source_orders",
                    connector_id=connector_id,
                    table="orders",
                    metadata={
                        "source_kind": "file",
                        "storage_kind": "parquet",
                        "file_format": "parquet",
                        "storage_uri": "s3://acme-bucket/orders/*.parquet",
                    },
                )
            },
            relationships=[],
        ),
    )
    tool = FederatedQueryTool(
        connector_provider=MemoryConnectorProvider(
            {connector_id: _storage_connector_metadata(workspace_id=workspace_id, connector_id=connector_id)}
        )
    )

    async def _fake_create_storage_connector(*, connector_type, connector_config):
        return StubStorageConnector()

    monkeypatch.setattr(tool, "_create_storage_connector", _fake_create_storage_connector)

    sources = await tool._build_sources(workflow)

    assert "parquet_source_orders" in sources
    assert isinstance(sources["parquet_source_orders"], DuckDbParquetRemoteSource)


@pytest.mark.anyio
async def test_build_sources_passes_storage_connector_to_parquet_remote_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace_id = str(uuid.uuid4())
    connector_id = uuid.uuid4()
    workflow = FederationWorkflow(
        id="workflow_remote_parquet_with_storage_test",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="dataset_remote_parquet_with_storage_test",
            name="Remote Parquet Orders",
            workspace_id=workspace_id,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="parquet_source_orders",
                    connector_id=connector_id,
                    table="orders",
                    metadata={
                        "source_kind": "file",
                        "storage_kind": "parquet",
                        "file_format": "parquet",
                        "storage_uri": "s3://acme-bucket/orders/*.parquet",
                    },
                )
            },
            relationships=[],
        ),
    )
    storage_connector = StubStorageConnector()
    tool = FederatedQueryTool(
        connector_provider=MemoryConnectorProvider(
            {connector_id: _storage_connector_metadata(workspace_id=workspace_id, connector_id=connector_id)}
        ),
    )

    async def _fake_create_storage_connector(*, connector_type, connector_config):
        return storage_connector

    monkeypatch.setattr(tool, "_create_storage_connector", _fake_create_storage_connector)

    sources = await tool._build_sources(workflow)
    source = sources["parquet_source_orders"]

    assert isinstance(source, DuckDbParquetRemoteSource)
    assert source._storage_connector is storage_connector


@pytest.mark.anyio
async def test_build_sources_rejects_non_sql_connector_metadata() -> None:
    workspace_id = str(uuid.uuid4())
    connector_id = uuid.uuid4()
    workflow = FederationWorkflow(
        id="workflow_non_sql_connector_test",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="dataset_non_sql_connector_test",
            name="Shopify Orders",
            workspace_id=workspace_id,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="shopify_source",
                    connector_id=connector_id,
                    table="orders",
                    metadata={},
                )
            },
            relationships=[],
        ),
    )
    tool = FederatedQueryTool(
        connector_provider=MemoryConnectorProvider(
            {
                connector_id: ConnectorMetadata(
                    id=connector_id,
                    name="shopify",
                    connector_type="SHOPIFY",
                    connector_family="api",
                    config={},
                    capabilities=ConnectorCapabilities(supports_synced_datasets=True),
                    management_mode=ManagementMode.CONFIG_MANAGED,
                    lifecycle_state=LifecycleState.ACTIVE,
                )
            }
        )
    )

    with pytest.raises(ValueError, match="does not support SQL federation"):
        await tool._build_sources(workflow)


@pytest.mark.anyio
async def test_build_sources_requires_connector_in_same_workspace() -> None:
    workspace_id = str(uuid.uuid4())
    connector_id = uuid.uuid4()
    workflow = FederationWorkflow(
        id="workflow_workspace_scoped_connector_test",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="dataset_workspace_scoped_connector_test",
            name="Orders",
            workspace_id=workspace_id,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="warehouse_source",
                    connector_id=connector_id,
                    table="orders",
                    metadata={},
                )
            },
            relationships=[],
        ),
    )
    tool = FederatedQueryTool(
        connector_provider=MemoryConnectorProvider(
            {
                connector_id: ConnectorMetadata(
                    id=connector_id,
                    name="warehouse",
                    connector_type="POSTGRES",
                    connector_family="database",
                    workspace_id=uuid.uuid4(),
                    config={"config": {"database": "analytics"}},
                    capabilities=ConnectorCapabilities(
                        supports_live_datasets=True,
                        supports_query_pushdown=True,
                        supports_federated_execution=True,
                    ),
                    management_mode=ManagementMode.CONFIG_MANAGED,
                    lifecycle_state=LifecycleState.ACTIVE,
                )
            }
        )
    )

    with pytest.raises(ValueError, match="not found"):
        await tool._build_sources(workflow)


@pytest.mark.anyio
async def test_build_sources_uses_api_remote_source_for_live_api_descriptor(monkeypatch: pytest.MonkeyPatch) -> None:
    workspace_id = str(uuid.uuid4())
    connector_id = uuid.uuid4()
    workflow = FederationWorkflow(
        id="workflow_live_api_test",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="dataset_live_api_test",
            name="Stripe Customers",
            workspace_id=workspace_id,
            tables={
                "customers": VirtualTableBinding(
                    table_key="customers",
                    source_id=f"api_{connector_id.hex[:12]}",
                    connector_id=connector_id,
                    table="customers",
                    metadata={"api_resource": "customers"},
                    dataset_descriptor=DatasetExecutionDescriptor(
                        dataset_id=uuid.uuid4(),
                        connector_id=connector_id,
                        name="customers",
                        materialization_mode="live",
                        source_kind="api",
                        connector_kind="stripe",
                        storage_kind="memory",
                        source={"resource": "customers"},
                        relation_identity={},
                        execution_capabilities={},
                    ),
                )
            },
            relationships=[],
        ),
    )
    tool = FederatedQueryTool(
        connector_provider=MemoryConnectorProvider(
            {
                connector_id: ConnectorMetadata(
                    id=connector_id,
                    name="billing_demo",
                    connector_type="STRIPE",
                    connector_family="api",
                    workspace_id=uuid.UUID(workspace_id),
                    config={"config": {"api_key": "test-key"}},
                    capabilities=ConnectorCapabilities(
                        supports_live_datasets=True,
                        supports_synced_datasets=True,
                        supports_incremental_sync=True,
                        supports_federated_execution=True,
                    ),
                    management_mode=ManagementMode.CONFIG_MANAGED,
                    lifecycle_state=LifecycleState.ACTIVE,
                )
            }
        )
    )
    fake_connector = _FakeApiConnector({"customers": [{"id": "cus_001", "email": "ada@example.com"}]})

    def _fake_create_api_connector(*, connector_type, connector_config):
        return fake_connector

    monkeypatch.setattr(tool, "_create_api_connector", _fake_create_api_connector)

    sources = await tool._build_sources(workflow)

    assert len(sources) == 1
    source = next(iter(sources.values()))
    assert isinstance(source, ApiConnectorRemoteSource)


@pytest.mark.anyio
async def test_api_connector_remote_source_executes_query_in_duckdb() -> None:
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

    result = await source.execute(
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

    assert result.table.to_pylist() == [{"email": "ada@example.com", "amount": 42.0}]
    assert sorted(connector.calls) == ["customers", "orders"]
