from __future__ import annotations

import uuid

import pytest

from langbridge.apps.worker.langbridge_worker.tools.federated_query_tool import (
    FederatedQueryTool,
)
from langbridge.packages.federation.connectors import DuckDbFileRemoteSource
from langbridge.packages.federation.models import FederationWorkflow, VirtualDataset, VirtualTableBinding


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
