from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from langbridge.apps.api.langbridge_api.services.lineage_service import LineageService
from langbridge.packages.common.langbridge_common.db import agent as _agent  # noqa: F401
from langbridge.packages.common.langbridge_common.db.dataset import DatasetRecord
from langbridge.packages.common.langbridge_common.db.lineage import LineageEdgeRecord
from langbridge.packages.common.langbridge_common.db.semantic import SemanticModelEntry
from langbridge.packages.common.langbridge_common.db.sql import SqlSavedQueryRecord


@pytest.fixture
def anyio_backend():
    return "asyncio"


@dataclass
class _FakeConnector:
    id: uuid.UUID
    name: str
    connector_type: str


class _FakeDatasetRepository:
    def __init__(self, *datasets: DatasetRecord) -> None:
        self.items = {dataset.id: dataset for dataset in datasets}

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None = None,
        limit: int = 5000,
    ) -> list[DatasetRecord]:
        rows = [item for item in self.items.values() if item.workspace_id == workspace_id]
        if project_id is not None:
            rows = [item for item in rows if item.project_id == project_id]
        return rows[:limit]

    async def get_by_id(self, dataset_id: uuid.UUID) -> DatasetRecord | None:
        return self.items.get(dataset_id)


class _FakeSemanticModelRepository:
    def __init__(self) -> None:
        self.items: dict[uuid.UUID, SemanticModelEntry] = {}

    def add(self, model: SemanticModelEntry) -> None:
        self.items[model.id] = model

    async def get_by_id(self, model_id: uuid.UUID) -> SemanticModelEntry | None:
        return self.items.get(model_id)


class _FakeSqlSavedQueryRepository:
    async def get_by_id(self, query_id: uuid.UUID):
        return None


class _FakeDashboardRepository:
    async def get_by_id(self, dashboard_id: uuid.UUID):
        return None


class _FakeConnectorRepository:
    def __init__(self, connector: _FakeConnector) -> None:
        self._connector = connector

    async def get_by_id(self, connector_id: uuid.UUID):
        if connector_id == self._connector.id:
            return self._connector
        return None


class _FakeLineageEdgeRepository:
    def __init__(self) -> None:
        self.items: list[LineageEdgeRecord] = []

    def add(self, edge: LineageEdgeRecord) -> None:
        self.items.append(edge)

    async def delete_for_target(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> None:
        self.items = [
            edge
            for edge in self.items
            if not (
                edge.workspace_id == workspace_id
                and edge.target_type == target_type
                and edge.target_id == target_id
            )
        ]

    async def delete_for_node(
        self,
        *,
        workspace_id: uuid.UUID,
        node_type: str,
        node_id: str,
    ) -> None:
        self.items = [
            edge
            for edge in self.items
            if not (
                edge.workspace_id == workspace_id
                and (
                    (edge.source_type == node_type and edge.source_id == node_id)
                    or (edge.target_type == node_type and edge.target_id == node_id)
                )
            )
        ]

    async def list_inbound(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> list[LineageEdgeRecord]:
        return [
            edge
            for edge in self.items
            if edge.workspace_id == workspace_id
            and edge.target_type == target_type
            and edge.target_id == target_id
        ]

    async def list_outbound(
        self,
        *,
        workspace_id: uuid.UUID,
        source_type: str,
        source_id: str,
    ) -> list[LineageEdgeRecord]:
        return [
            edge
            for edge in self.items
            if edge.workspace_id == workspace_id
            and edge.source_type == source_type
            and edge.source_id == source_id
        ]


def _build_dataset(
    *,
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    name: str,
    dataset_type: str,
    table_name: str | None = None,
    schema_name: str | None = None,
    sql_text: str | None = None,
    referenced_dataset_ids: list[str] | None = None,
) -> DatasetRecord:
    now = datetime.now(timezone.utc)
    return DatasetRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        project_id=None,
        connection_id=connection_id,
        created_by=None,
        updated_by=None,
        name=name,
        description=None,
        tags_json=[],
        dataset_type=dataset_type,
        dialect="tsql" if dataset_type != "FILE" else "duckdb",
        catalog_name=None,
        schema_name=schema_name,
        table_name=table_name,
        storage_uri=None,
        sql_text=sql_text,
        referenced_dataset_ids_json=list(referenced_dataset_ids or []),
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


def _build_service(
    *,
    connector: _FakeConnector,
    dataset_repository: _FakeDatasetRepository,
    semantic_model_repository: _FakeSemanticModelRepository,
    lineage_edge_repository: _FakeLineageEdgeRepository,
) -> LineageService:
    return LineageService(
        lineage_edge_repository=lineage_edge_repository,
        dataset_repository=dataset_repository,
        semantic_model_repository=semantic_model_repository,
        sql_saved_query_repository=_FakeSqlSavedQueryRepository(),
        dashboard_repository=_FakeDashboardRepository(),
        connector_repository=_FakeConnectorRepository(connector),
    )


@pytest.mark.anyio
async def test_register_dataset_lineage_creates_table_sql_and_federated_edges() -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector = _FakeConnector(id=connection_id, name="warehouse", connector_type="POSTGRES")

    base_dataset = _build_dataset(
        workspace_id=workspace_id,
        connection_id=connection_id,
        name="orders_base",
        dataset_type="TABLE",
        schema_name="public",
        table_name="orders",
    )
    sql_dataset = _build_dataset(
        workspace_id=workspace_id,
        connection_id=connection_id,
        name="orders_rollup",
        dataset_type="SQL",
        sql_text="select * from orders_base",
    )
    federated_dataset = _build_dataset(
        workspace_id=workspace_id,
        connection_id=connection_id,
        name="orders_federated",
        dataset_type="FEDERATED",
        referenced_dataset_ids=[str(base_dataset.id), str(sql_dataset.id)],
    )

    dataset_repository = _FakeDatasetRepository(base_dataset, sql_dataset, federated_dataset)
    semantic_model_repository = _FakeSemanticModelRepository()
    lineage_edge_repository = _FakeLineageEdgeRepository()
    service = _build_service(
        connector=connector,
        dataset_repository=dataset_repository,
        semantic_model_repository=semantic_model_repository,
        lineage_edge_repository=lineage_edge_repository,
    )

    await service.register_dataset_lineage(dataset=base_dataset)
    await service.register_dataset_lineage(dataset=sql_dataset)
    await service.register_dataset_lineage(dataset=federated_dataset)

    base_edges = await lineage_edge_repository.list_inbound(
        workspace_id=workspace_id,
        target_type="dataset",
        target_id=str(base_dataset.id),
    )
    assert {
        (edge.source_type, edge.edge_type)
        for edge in base_edges
    } == {
        ("connection", "FEEDS"),
        ("source_table", "MATERIALIZES_FROM"),
    }

    sql_edges = await lineage_edge_repository.list_inbound(
        workspace_id=workspace_id,
        target_type="dataset",
        target_id=str(sql_dataset.id),
    )
    assert ("connection", str(connection_id), "FEEDS") in {
        (edge.source_type, edge.source_id, edge.edge_type)
        for edge in sql_edges
    }
    assert ("dataset", str(base_dataset.id), "DERIVES_FROM") in {
        (edge.source_type, edge.source_id, edge.edge_type)
        for edge in sql_edges
    }

    federated_edges = await lineage_edge_repository.list_inbound(
        workspace_id=workspace_id,
        target_type="dataset",
        target_id=str(federated_dataset.id),
    )
    dataset_inputs = sorted(
        edge.source_id
        for edge in federated_edges
        if edge.source_type == "dataset" and edge.edge_type == "DERIVES_FROM"
    )
    assert dataset_inputs == sorted([str(base_dataset.id), str(sql_dataset.id)])


@pytest.mark.anyio
async def test_build_dataset_impact_distinguishes_direct_and_indirect_dependents() -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector = _FakeConnector(id=connection_id, name="warehouse", connector_type="POSTGRES")

    source_dataset = _build_dataset(
        workspace_id=workspace_id,
        connection_id=connection_id,
        name="orders_base",
        dataset_type="TABLE",
        schema_name="public",
        table_name="orders",
    )
    derived_dataset = _build_dataset(
        workspace_id=workspace_id,
        connection_id=connection_id,
        name="orders_federated",
        dataset_type="FEDERATED",
        referenced_dataset_ids=[str(source_dataset.id)],
    )

    dataset_repository = _FakeDatasetRepository(source_dataset, derived_dataset)
    semantic_model_repository = _FakeSemanticModelRepository()
    lineage_edge_repository = _FakeLineageEdgeRepository()
    service = _build_service(
        connector=connector,
        dataset_repository=dataset_repository,
        semantic_model_repository=semantic_model_repository,
        lineage_edge_repository=lineage_edge_repository,
    )

    await service.register_dataset_lineage(dataset=source_dataset)
    await service.register_dataset_lineage(dataset=derived_dataset)

    model = SemanticModelEntry(
        id=uuid.uuid4(),
        connector_id=connection_id,
        organization_id=workspace_id,
        project_id=None,
        name="Orders semantic model",
        description=None,
        content_yaml="name: Orders semantic model",
        content_json=json.dumps(
            {
                "name": "Orders semantic model",
                "tables": {
                    "orders": {
                        "dataset_id": str(derived_dataset.id),
                    }
                },
            }
        ),
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    semantic_model_repository.add(model)
    await service.register_semantic_model_lineage(model=model)

    impact = await service.build_dataset_impact(
        workspace_id=workspace_id,
        dataset_id=source_dataset.id,
    )

    assert impact["total_downstream_assets"] == 2
    assert [item["label"] for item in impact["direct_dependents"]] == ["orders_federated"]
    assert impact["dependent_datasets"][0]["direct"] is True
    assert impact["semantic_models"][0]["label"] == "Orders semantic model"
    assert impact["semantic_models"][0]["direct"] is False


@pytest.mark.anyio
async def test_register_saved_query_lineage_prefers_selected_dataset_metadata() -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector = _FakeConnector(id=connection_id, name="warehouse", connector_type="POSTGRES")
    dataset = _build_dataset(
        workspace_id=workspace_id,
        connection_id=connection_id,
        name="orders_base",
        dataset_type="TABLE",
        schema_name="public",
        table_name="orders",
    )

    dataset_repository = _FakeDatasetRepository(dataset)
    semantic_model_repository = _FakeSemanticModelRepository()
    lineage_edge_repository = _FakeLineageEdgeRepository()
    service = _build_service(
        connector=connector,
        dataset_repository=dataset_repository,
        semantic_model_repository=semantic_model_repository,
        lineage_edge_repository=lineage_edge_repository,
    )

    now = datetime.now(timezone.utc)
    record = SqlSavedQueryRecord(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        project_id=None,
        created_by=uuid.uuid4(),
        updated_by=uuid.uuid4(),
        connection_id=None,
        workbench_mode="dataset",
        selected_datasets_json=[
            {
                "alias": "orders",
                "dataset_id": str(dataset.id),
                "dataset_name": dataset.name,
            }
        ],
        name="Orders query",
        description=None,
        query_text="SELECT * FROM orders.public.orders",
        query_hash="hash",
        tags_json=[],
        default_params_json={},
        is_shared=False,
        last_sql_job_id=None,
        last_result_artifact_id=None,
        created_at=now,
        updated_at=now,
    )

    await service.register_saved_query_lineage(record=record)

    inbound_edges = await lineage_edge_repository.list_inbound(
        workspace_id=workspace_id,
        target_type="saved_query",
        target_id=str(record.id),
    )
    dataset_edges = [
        edge for edge in inbound_edges if edge.source_type == "dataset" and edge.edge_type == "REFERENCES"
    ]
    source_table_edges = [edge for edge in inbound_edges if edge.source_type == "source_table"]

    assert len(dataset_edges) == 1
    assert dataset_edges[0].source_id == str(dataset.id)
    assert dataset_edges[0].metadata_json["alias"] == "orders"
    assert source_table_edges == []
