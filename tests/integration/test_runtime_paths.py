
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from langbridge.federation.models import FederationWorkflow, VirtualDataset, VirtualTableBinding
from langbridge.runtime.execution.federated_query_tool import FederatedQueryTool
from langbridge.runtime.services.semantic_query_execution_service import SemanticQueryExecutionService

from tests.helpers.semantic_harness import SemanticHarness
from tests.helpers.sql_normalize import normalize_sql


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@dataclass
class _ModelRecord:
    id: uuid.UUID
    name: str
    content_yaml: str
    content_json: dict[str, Any]
    connector_id: uuid.UUID | None = None
    description: str | None = None


class _FakeSemanticModelProvider:
    def __init__(self, models: dict[uuid.UUID, _ModelRecord]) -> None:
        self._models = models

    async def get_semantic_model(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
    ) -> _ModelRecord | None:
        _ = workspace_id
        return self._models.get(semantic_model_id)


class _FakeFederatedQueryTool:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[dict[str, Any]] = []

    async def execute_federated_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(payload)
        return {"rows": self.rows}


class _FakeDatasetRepository:
    def __init__(self, datasets: dict[uuid.UUID, Any]) -> None:
        self._datasets = datasets

    async def get_for_workspace(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> Any | None:
        dataset = self._datasets.get(dataset_id)
        if dataset is None or dataset.workspace_id != workspace_id:
            return None
        return dataset


class _DummyConnectorProvider:
    async def get_connector(self, *, workspace_id: uuid.UUID, connector_id: uuid.UUID) -> Any:
        _ = workspace_id
        _ = connector_id
        raise AssertionError("File-backed runtime federation should not request connector metadata.")


def _dataset_stub(
    *,
    dataset_id: uuid.UUID,
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    name: str,
    schema_name: str,
    table_name: str,
    dialect: str,
) -> Any:
    now = datetime.now(timezone.utc)
    return SimpleNamespace(
        id=dataset_id,
        workspace_id=workspace_id,
        connection_id=connection_id,
        created_by=None,
        updated_by=None,
        name=name,
        sql_alias=name,
        description=None,
        tags_json=[],
        dataset_type="TABLE",
        source_kind="database",
        connector_kind="postgres",
        storage_kind="table",
        dialect=dialect,
        catalog_name=None,
        schema_name=schema_name,
        table_name=table_name,
        storage_uri=None,
        sql_text=None,
        relation_identity_json={},
        execution_capabilities_json={},
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.anyio
async def test_runtime_semantic_query_service_path_builds_dataset_backed_workflow() -> None:
    harness = SemanticHarness()
    workspace_id = uuid.uuid4()
    semantic_model_id = uuid.uuid4()
    warehouse_connector_id = uuid.uuid4()

    model_record = _ModelRecord(
        id=semantic_model_id,
        name="Commerce",
        content_yaml=harness.read_text("semantic_models", "commerce.yml"),
        content_json=harness.read_yaml("semantic_models", "commerce.yml"),
        connector_id=warehouse_connector_id,
    )
    dataset_repository = _FakeDatasetRepository(
        {
            uuid.UUID("11111111-1111-1111-1111-111111111111"): _dataset_stub(
                dataset_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
                workspace_id=workspace_id,
                connection_id=warehouse_connector_id,
                name="orders",
                schema_name="analytics",
                table_name="orders",
                dialect="postgres",
            ),
            uuid.UUID("22222222-2222-2222-2222-222222222222"): _dataset_stub(
                dataset_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
                workspace_id=workspace_id,
                connection_id=warehouse_connector_id,
                name="customers",
                schema_name="analytics",
                table_name="customers",
                dialect="postgres",
            ),
        }
    )
    tool = _FakeFederatedQueryTool(rows=harness.expected_rows("grouped_revenue_by_region"))
    service = SemanticQueryExecutionService(
        dataset_repository=dataset_repository,
        federated_query_tool=tool,
        logger=logging.getLogger(__name__),
        semantic_model_provider=_FakeSemanticModelProvider({semantic_model_id: model_record}),
    )

    result = await service.execute_standard_query(
        workspace_id=workspace_id,
        semantic_model_id=semantic_model_id,
        semantic_query=harness.load_query_fixture("grouped_revenue_by_region"),
    )

    assert result.response.data == harness.expected_rows("grouped_revenue_by_region")
    assert normalize_sql(
        result.compiled_sql,
        read_dialect="postgres",
        write_dialect="postgres",
    ) == harness.compile_model_fixture(
        model_name="commerce",
        query_name="grouped_revenue_by_region",
        dialect="postgres",
    )


@pytest.mark.anyio
async def test_runtime_semantic_graph_query_path_builds_multi_model_workflow() -> None:
    harness = SemanticHarness()
    workspace_id = uuid.uuid4()
    commerce_model_id = uuid.uuid4()
    marketing_model_id = uuid.uuid4()
    warehouse_connector_id = uuid.uuid4()
    marketing_connector_id = uuid.uuid4()

    model_records = {
        commerce_model_id: _ModelRecord(
            id=commerce_model_id,
            name="Commerce",
            content_yaml=harness.read_text("semantic_models", "commerce.yml"),
            content_json=harness.read_yaml("semantic_models", "commerce.yml"),
            connector_id=warehouse_connector_id,
        ),
        marketing_model_id: _ModelRecord(
            id=marketing_model_id,
            name="Marketing",
            content_yaml=harness.read_text("semantic_models", "marketing.yml"),
            content_json=harness.read_yaml("semantic_models", "marketing.yml"),
            connector_id=marketing_connector_id,
        ),
    }
    dataset_repository = _FakeDatasetRepository(
        {
            uuid.UUID("11111111-1111-1111-1111-111111111111"): _dataset_stub(
                dataset_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
                workspace_id=workspace_id,
                connection_id=warehouse_connector_id,
                name="orders",
                schema_name="analytics",
                table_name="orders",
                dialect="postgres",
            ),
            uuid.UUID("22222222-2222-2222-2222-222222222222"): _dataset_stub(
                dataset_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
                workspace_id=workspace_id,
                connection_id=warehouse_connector_id,
                name="customers",
                schema_name="analytics",
                table_name="customers",
                dialect="postgres",
            ),
            uuid.UUID("33333333-3333-3333-3333-333333333333"): _dataset_stub(
                dataset_id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
                workspace_id=workspace_id,
                connection_id=marketing_connector_id,
                name="campaigns",
                schema_name="marketing",
                table_name="campaigns",
                dialect="snowflake",
            ),
        }
    )
    tool = _FakeFederatedQueryTool(rows=harness.expected_rows("federated_three_dataset_smq"))
    service = SemanticQueryExecutionService(
        dataset_repository=dataset_repository,
        federated_query_tool=tool,
        logger=logging.getLogger(__name__),
        semantic_model_provider=_FakeSemanticModelProvider(model_records),
    )

    result = await service.execute_semantic_graph_query(
        workspace_id=workspace_id,
        semantic_model_ids=[commerce_model_id, marketing_model_id],
        semantic_query=harness.load_query_fixture(
            "three_dataset_revenue_and_spend",
            kind="federation",
        ),
        relationships=[
            {
                "name": "commerce_to_marketing",
                "source_semantic_model_id": str(commerce_model_id),
                "source_field": "orders.customer_id",
                "target_semantic_model_id": str(marketing_model_id),
                "target_field": "campaigns.customer_id",
                "relationship_type": "left",
            }
        ],
    )

    assert result.response.data == harness.expected_rows("federated_three_dataset_smq")
    assert result.response.semantic_model_ids == [commerce_model_id, marketing_model_id]
    assert result.response.connector_id == SemanticQueryExecutionService.build_semantic_graph_execution_connector_id(
        workspace_id=workspace_id
    )
    assert "Marketing__campaigns__spend" in result.compiled_sql
    assert len(tool.calls) == 1
    workflow = tool.calls[0]["workflow"]
    assert sorted(workflow["dataset"]["tables"]) == [
        "Commerce__customers",
        "Commerce__orders",
        "Marketing__campaigns",
    ]
    assert [relationship["name"] for relationship in workflow["dataset"]["relationships"]] == [
        "Commerce__orders_to_customers",
        "commerce_to_marketing",
    ]


@pytest.mark.anyio
async def test_runtime_federated_sql_path_executes_file_backed_workflow() -> None:
    harness = SemanticHarness()
    fixture_root = harness.fixture_root / "datasets"
    workspace_id = "00000000-0000-0000-0000-000000000123"
    tool = FederatedQueryTool(connector_provider=_DummyConnectorProvider())
    workflow = FederationWorkflow(
        id="wf-files",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="ds-files",
            name="file fixtures",
            workspace_id=workspace_id,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="file_orders",
                    schema="analytics",
                    table="orders",
                    metadata={
                        "source_kind": "file",
                        "storage_uri": str(Path(fixture_root, "orders.csv")),
                        "file_format": "csv",
                        "header": True,
                    },
                ),
                "customers": VirtualTableBinding(
                    table_key="customers",
                    source_id="file_customers",
                    schema="analytics",
                    table="customers",
                    metadata={
                        "source_kind": "file",
                        "storage_uri": str(Path(fixture_root, "customers.csv")),
                        "file_format": "csv",
                        "header": True,
                    },
                ),
            },
        ),
    )

    result = await tool.execute_federated_query(
        {
            "workspace_id": workspace_id,
            "query": (
                "SELECT o.order_id, c.name "
                "FROM analytics.orders AS o "
                "JOIN analytics.customers AS c ON o.customer_id = c.customer_id "
                "ORDER BY o.order_id"
            ),
            "dialect": "postgres",
            "workflow": workflow.model_dump(mode="json"),
        }
    )

    assert result["rows"] == [
        {"order_id": 1, "name": "Acme Corp"},
        {"order_id": 2, "name": "Globex"},
        {"order_id": 3, "name": "Acme Corp"},
        {"order_id": 4, "name": "Initech"},
    ]
