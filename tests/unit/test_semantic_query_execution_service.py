from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from langbridge.runtime.services.semantic_query_execution_service import (
    SemanticQueryExecutionService,
    _normalize_unified_relationship_payload,
)
from langbridge.runtime.models import (
    UnifiedSemanticRelationshipRequest,
)
from langbridge.runtime.errors import BusinessValidationError
from langbridge.semantic.model import Dimension, SemanticModel, Table
from langbridge.semantic.query import SemanticQuery


@pytest.fixture
def anyio_backend():
    return "asyncio"


@dataclass
class _ModelRecord:
    id: uuid.UUID
    name: str = 'model'
    description: str | None = None
    content_json: str | None = None
    content_yaml: str | None = None
    connector_id: uuid.UUID | None = None


def test_parse_unified_model_config_from_record_reads_source_models() -> None:
    source_model_id = uuid.uuid4()
    target_model_id = uuid.uuid4()
    payload = {
        "version": "1.0",
        "source_models": [{"id": str(source_model_id), "alias": "Sales"}],
        "relationships": [
            {
                "name": "join_1",
                "source_semantic_model_id": str(source_model_id),
                "source_field": "customer_id",
                "target_semantic_model_id": str(target_model_id),
                "target_field": "customer_id",
                "relationship_type": "inner",
            }
        ],
        "metrics": {"gross_margin": {"expression": "Sales.revenue - Sales.cost"}},
    }
    record = _ModelRecord(id=uuid.uuid4(), content_json=json.dumps(payload))

    config = SemanticQueryExecutionService.parse_unified_model_config_from_record(record)

    assert config is not None
    assert config.semantic_model_ids == [source_model_id]
    assert config.relationships == [
        {
            **payload["relationships"][0],
            "operator": "=",
        }
    ]
    assert config.metrics == payload["metrics"]


def test_parse_unified_model_config_from_record_returns_none_for_standard_model() -> None:
    record = _ModelRecord(id=uuid.uuid4(), content_json=json.dumps({"version": "1.0", "tables": {}}))
    assert SemanticQueryExecutionService.parse_unified_model_config_from_record(record) is None


def test_parse_unified_model_config_from_record_requires_source_models_metadata() -> None:
    payload = {"version": "1.0", "semantic_models": [{"version": "1.0", "tables": {}}]}
    record = _ModelRecord(id=uuid.uuid4(), content_json=json.dumps(payload))

    with pytest.raises(BusinessValidationError, match="source_models"):
        SemanticQueryExecutionService.parse_unified_model_config_from_record(record)


def test_normalize_unified_relationship_payload_uses_snake_case_field_names() -> None:
    relationship = UnifiedSemanticRelationshipRequest(
        source_semantic_model_id=uuid.uuid4(),
        source_field="sales.customer_id",
        target_semantic_model_id=uuid.uuid4(),
        target_field="marketing.customer_id",
        relationship_type="inner",
    )

    normalized = _normalize_unified_relationship_payload(relationship)

    assert "source_semantic_model_id" in normalized
    assert "sourceSemanticModelId" not in normalized
    assert normalized["target_field"] == "marketing.customer_id"

def test_build_widget_query_payload_translates_filters_and_time_range() -> None:
    widget = {
        "measures": ["orders.total"],
        "dimensions": ["orders.region"],
        "timeDimension": "orders.created_at",
        "timeGrain": "month",
        "timeRangePreset": "custom_before",
        "timeRangeFrom": "2026-02-01",
        "orderBys": [{"member": "orders.total", "direction": "desc"}],
        "limit": 100,
        "filters": [{"member": "orders.channel", "operator": "in", "values": "online,retail"}],
    }
    global_filters = [
        {"member": "orders.country", "operator": "equals", "values": "US"},
        {"member": "orders.region", "operator": "set", "values": ""},
    ]

    payload = SemanticQueryExecutionService.build_widget_query_payload(
        widget=widget,
        global_filters=global_filters,
    )

    assert payload["timeDimensions"] == [
        {
            "dimension": "orders.created_at",
            "granularity": "month",
            "dateRange": "before:2026-02-01",
        }
    ]
    assert payload["filters"] == [
        {"member": "orders.country", "operator": "equals", "values": ["US"]},
        {"member": "orders.region", "operator": "set"},
        {"member": "orders.channel", "operator": "in", "values": ["online", "retail"]},
    ]
    assert payload["order"] == [{"orders.total": "desc"}]
    assert payload["limit"] == 100


def test_build_widget_query_payload_normalizes_year_to_date_equals_filter() -> None:
    widget = {
        "measures": ["orders.total"],
        "dimensions": [],
        "timeDimension": "orders.order_ts",
        "timeGrain": "month",
        "timeRangePreset": "year_to_date",
        "filters": [{"member": "orders.order_ts", "operator": "equals", "values": "year_to_date"}],
    }

    payload = SemanticQueryExecutionService.build_widget_query_payload(
        widget=widget,
        global_filters=[],
    )

    assert payload["timeDimensions"] == [
        {
            "dimension": "orders.order_ts",
            "granularity": "month",
            "dateRange": "year_to_date",
        }
    ]
    assert payload["filters"] == [
        {"member": "orders.order_ts", "operator": "indaterange", "values": ["year_to_date"]}
    ]


def test_to_semantic_filters_keeps_non_date_member_equals_unchanged() -> None:
    payload = SemanticQueryExecutionService.to_semantic_filters(
        [{"member": "orders.total", "operator": "equals", "values": "2026"}]
    )

    assert payload == [{"member": "orders.total", "operator": "equals", "values": ["2026"]}]


def test_to_semantic_filters_normalizes_iso_dot_date_range_for_date_member() -> None:
    payload = SemanticQueryExecutionService.to_semantic_filters(
        [{"member": "orders.order_ts", "operator": "equals", "values": "2026-01-01..2026-12-31"}]
    )

    assert payload == [
        {
            "member": "orders.order_ts",
            "operator": "indaterange",
            "values": ["2026-01-01", "2026-12-31"],
        }
    ]


class _FakeSemanticModelProvider:
    def __init__(self, models: dict[uuid.UUID, _ModelRecord]) -> None:
        self._models = models

    async def get_semantic_model(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
    ) -> _ModelRecord | None:
        return self._models.get(semantic_model_id)


class _FakeFederatedQueryTool:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.calls: list[dict[str, Any]] = []

    async def execute_federated_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(payload)
        return {"rows": self._rows}


class _FakeDatasetRepository:
    def __init__(self, datasets: dict[uuid.UUID, Any]) -> None:
        self._datasets = datasets

    async def get_for_workspace(self, *, dataset_id: uuid.UUID, workspace_id: uuid.UUID) -> DatasetRecord | None:
        dataset = self._datasets.get(dataset_id)
        if dataset is None or dataset.workspace_id != workspace_id:
            return None
        return dataset


def _dataset_stub(
    *,
    dataset_id: uuid.UUID,
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID | None,
    name: str,
    dataset_type: str,
    dialect: str,
    schema_name: str | None,
    table_name: str | None,
    storage_uri: str | None,
    file_config_json: dict[str, Any] | None,
    source_kind: str | None = None,
    connector_kind: str | None = None,
    storage_kind: str | None = None,
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
        dataset_type=dataset_type,
        source_kind=source_kind,
        connector_kind=connector_kind,
        storage_kind=storage_kind,
        dialect=dialect,
        catalog_name=None,
        schema_name=schema_name,
        table_name=table_name,
        storage_uri=storage_uri,
        sql_text=None,
        relation_identity_json={},
        execution_capabilities_json={},
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json=file_config_json,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.anyio
async def test_execute_unified_query_routes_through_federated_tool() -> None:
    pytest.importorskip("pyarrow")

    workspace_id = uuid.uuid4()
    model_id = uuid.uuid4()
    connector_id = uuid.uuid4()
    dataset_id = uuid.uuid4()

    source_model = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                dataset_id=str(dataset_id),
                schema="public",
                name="orders",
                dimensions=[Dimension(name="id", type="integer", primary_key=True)],
            )
        },
    )
    provider = _FakeSemanticModelProvider(
        {
            model_id: _ModelRecord(
                id=model_id,
                name='Orders',
                content_yaml=source_model.yml_dump(),
                connector_id=connector_id,
            )
        }
    )
    tool = _FakeFederatedQueryTool(rows=[{"orders__id": 1}])
    service = SemanticQueryExecutionService(
        dataset_repository=_FakeDatasetRepository(
            {
                dataset_id: _dataset_stub(
                    dataset_id=dataset_id,
                    workspace_id=workspace_id,
                    connection_id=connector_id,
                    name="orders_table",
                    dataset_type="TABLE",
                    source_kind="database",
                    connector_kind="postgres",
                    storage_kind="table",
                    dialect="postgres",
                    schema_name="public",
                    table_name="orders",
                    storage_uri=None,
                    file_config_json=None,
                )
            }
        ),
        federated_query_tool=tool,
        logger=logging.getLogger(__name__),
        semantic_model_provider=provider,
    )

    result = await service.execute_unified_query(
        workspace_id=workspace_id,
        semantic_query=SemanticQuery(dimensions=["Orders__orders.id"], limit=10),
        semantic_model_ids=[model_id],
        relationships=None,
        metrics=None,
    )

    assert isinstance(result.response.data, list)
    assert result.response.data == [{"orders__id": 1}]
    assert result.compiled_sql
    assert len(tool.calls) == 1


@pytest.mark.anyio
async def test_execute_unified_query_resolves_dataset_backed_tables_per_table() -> None:
    pytest.importorskip("pyarrow")

    workspace_id = uuid.uuid4()
    model_id = uuid.uuid4()
    legacy_connector_id = uuid.uuid4()
    warehouse_connector_id = uuid.uuid4()
    file_dataset_id = uuid.uuid4()
    table_dataset_id = uuid.uuid4()

    source_model = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                dataset_id=str(file_dataset_id),
                schema="analytics",
                name="orders",
                dimensions=[Dimension(name="id", type="integer", primary_key=True)],
            ),
            "inventory": Table(
                dataset_id=str(table_dataset_id),
                schema="warehouse",
                name="inventory",
                dimensions=[Dimension(name="sku", type="string", primary_key=True)],
            ),
        },
    )
    provider = _FakeSemanticModelProvider(
        {
            model_id: _ModelRecord(
                id=model_id,
                name='Inventory',
                content_yaml=source_model.yml_dump(),
                content_json=source_model.model_dump_json(exclude_none=True),
                connector_id=legacy_connector_id,
            )
        }
    )
    dataset_repo = _FakeDatasetRepository(
        {
            file_dataset_id: _dataset_stub(
                dataset_id=file_dataset_id,
                workspace_id=workspace_id,
                connection_id=None,
                name="orders_file",
                dataset_type="FILE",
                dialect="duckdb",
                schema_name=None,
                table_name="orders",
                storage_uri="file:///tmp/orders.parquet",
                file_config_json={"format": "parquet"},
            ),
            table_dataset_id: _dataset_stub(
                dataset_id=table_dataset_id,
                workspace_id=workspace_id,
                connection_id=warehouse_connector_id,
                name="inventory_table",
                dataset_type="TABLE",
                source_kind="database",
                connector_kind="postgres",
                storage_kind="table",
                dialect="postgres",
                schema_name="warehouse",
                table_name="inventory",
                storage_uri=None,
                file_config_json=None,
            ),
        }
    )
    tool = _FakeFederatedQueryTool(rows=[{"orders__id": 1}])
    service = SemanticQueryExecutionService(
        dataset_repository=dataset_repo,
        federated_query_tool=tool,
        logger=logging.getLogger(__name__),
        semantic_model_provider=provider,
    )

    result = await service.execute_unified_query(
        workspace_id=workspace_id,
        semantic_query=SemanticQuery(dimensions=["Inventory__orders.id"], limit=10),
        semantic_model_ids=[model_id],
        relationships=None,
        metrics=None,
    )

    assert result.response.data == [{"orders__id": 1}]
    workflow = tool.calls[0]["workflow"]
    orders_binding = workflow["dataset"]["tables"]["Inventory__orders"]
    inventory_binding = workflow["dataset"]["tables"]["Inventory__inventory"]
    assert orders_binding["metadata"]["source_kind"] == "file"
    assert inventory_binding["connector_id"] == str(warehouse_connector_id)





