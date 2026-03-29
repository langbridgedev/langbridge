
import uuid

import pyarrow as pa
import pytest

from langbridge.federation.connectors import MockArrowRemoteSource
from langbridge.federation.executor import ArtifactStore
from langbridge.federation.models import FederationWorkflow, VirtualDataset, VirtualTableBinding
from langbridge.federation.service import FederatedQueryService


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_cross_source_join_executes_with_arrow(tmp_path) -> None:
    workspace_id = str(uuid.uuid4())

    workflow = FederationWorkflow(
        id="wf-integration",
        workspace_id=workspace_id,
        stage_parallelism=2,
        dataset=VirtualDataset(
            id="ds-integration",
            name="integration",
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

    orders = pa.table(
        {
            "id": [1, 2, 3],
            "customer_id": [10, 11, 12],
            "amount": [100, 200, 300],
        }
    )
    customers = pa.table(
        {
            "id": [10, 11, 13],
            "name": ["Acme", "Globex", "Umbrella"],
        }
    )

    service = FederatedQueryService(
        artifact_store=ArtifactStore(base_dir=str(tmp_path / "artifacts")),
    )
    service.register_workspace(
        workspace_id=workspace_id,
        workflow=workflow,
        sources={
            "src_orders": MockArrowRemoteSource(source_id="src_orders", tables={"orders": orders}),
            "src_customers": MockArrowRemoteSource(source_id="src_customers", tables={"customers": customers}),
        },
    )

    handle = await service.execute(
        query=(
            "SELECT o.id, c.name "
            "FROM public.orders o "
            "JOIN public.customers c ON o.customer_id = c.id "
            "ORDER BY o.id"
        ),
        dialect="tsql",
        workspace_id=workspace_id,
    )
    table = await service.fetch_arrow(handle)

    assert table.column_names == ["id", "name"]
    assert table.to_pylist() == [{"id": 1, "name": "Acme"}, {"id": 2, "name": "Globex"}]
