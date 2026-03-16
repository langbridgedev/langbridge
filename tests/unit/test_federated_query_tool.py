from __future__ import annotations

import uuid

import pytest

from langbridge.packages.runtime.execution.federated_query_tool import (
    FederatedQueryTool,
)
from langbridge.packages.federation.connectors import DuckDbFileRemoteSource
from langbridge.packages.federation.models import (
    DatasetExecutionDescriptor,
    FederationWorkflow,
    VirtualDataset,
    VirtualTableBinding,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _UnusedConnectorRepository:
    async def get_by_id(self, connector_id):
        return None


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
    tool = FederatedQueryTool(connector_repository=_UnusedConnectorRepository())

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
    tool = FederatedQueryTool(connector_repository=_UnusedConnectorRepository())

    sources = await tool._build_sources(workflow)

    assert "file_source_shop_orders" in sources
    assert isinstance(sources["file_source_shop_orders"], DuckDbFileRemoteSource)
