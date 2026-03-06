from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from langbridge.apps.worker.langbridge_worker.semantic_query_execution_service import (
    SemanticQueryExecutionService,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.db.dataset import DatasetRecord
from langbridge.packages.semantic.langbridge_semantic.model import Dimension, SemanticModel, Table
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery


@pytest.fixture
def anyio_backend():
    return "asyncio"


@dataclass
class _ModelRecord:
    content_json: str | None = None
    content_yaml: str | None = None
    connector_id: uuid.UUID | None = None


def test_parse_unified_model_config_from_record_reads_source_models() -> None:
    source_model_id = uuid.uuid4()
    payload = {
        "version": "1.0",
        "source_models": [{"id": str(source_model_id)}],
        "relationships": [{"name": "join_1", "from": "orders", "to": "customers", "on": "orders.id = customers.id"}],
        "metrics": {"gross_margin": {"expression": "orders.revenue - orders.cost"}},
    }
    record = _ModelRecord(content_json=json.dumps(payload))

    config = SemanticQueryExecutionService.parse_unified_model_config_from_record(record)

    assert config is not None
    assert config.semantic_model_ids == [source_model_id]
    assert config.joins == payload["relationships"]
    assert config.metrics == payload["metrics"]


def test_parse_unified_model_config_from_record_returns_none_for_standard_model() -> None:
    record = _ModelRecord(content_json=json.dumps({"version": "1.0", "tables": {}}))
    assert SemanticQueryExecutionService.parse_unified_model_config_from_record(record) is None


def test_parse_unified_model_config_from_record_requires_source_models_metadata() -> None:
    payload = {"version": "1.0", "semantic_models": [{"version": "1.0", "tables": {}}]}
    record = _ModelRecord(content_json=json.dumps(payload))

    with pytest.raises(BusinessValidationError, match="source_models"):
        SemanticQueryExecutionService.parse_unified_model_config_from_record(record)


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


class _FakeSemanticModelRepository:
    def __init__(self, models: dict[uuid.UUID, _ModelRecord]) -> None:
        self._models = models

    async def get_for_scope(self, *, model_id: uuid.UUID, organization_id: uuid.UUID) -> _ModelRecord | None:
        return self._models.get(model_id)


class _FakeFederatedQueryTool:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.calls: list[dict[str, Any]] = []

    async def execute_federated_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(payload)
        return {"rows": self._rows}


class _FakeDatasetRepository:
    def __init__(self, datasets: dict[uuid.UUID, DatasetRecord]) -> None:
        self._datasets = datasets

    async def get_for_workspace(self, *, dataset_id: uuid.UUID, workspace_id: uuid.UUID) -> DatasetRecord | None:
        dataset = self._datasets.get(dataset_id)
        if dataset is None or dataset.workspace_id != workspace_id:
            return None
        return dataset


@pytest.mark.anyio
async def test_execute_unified_query_routes_through_federated_tool() -> None:
    pytest.importorskip("pyarrow")

    organization_id = uuid.uuid4()
    model_id = uuid.uuid4()
    connector_id = uuid.uuid4()

    source_model = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                schema="public",
                name="orders",
                dimensions=[Dimension(name="id", type="integer", primary_key=True)],
            )
        },
    )
    repo = _FakeSemanticModelRepository(
        {
            model_id: _ModelRecord(
                content_yaml=source_model.yml_dump(),
                connector_id=connector_id,
            )
        }
    )
    tool = _FakeFederatedQueryTool(rows=[{"orders__id": 1}])
    service = SemanticQueryExecutionService(
        semantic_model_repository=repo,
        dataset_repository=_FakeDatasetRepository({}),
        federated_query_tool=tool,
        logger=logging.getLogger(__name__),
    )

    result = await service.execute_unified_query(
        organization_id=organization_id,
        project_id=None,
        semantic_query=SemanticQuery(dimensions=["orders.id"], limit=10),
        semantic_model_ids=[model_id],
        joins=None,
        metrics=None,
    )

    assert isinstance(result.response.data, list)
    assert result.response.data == [{"orders__id": 1}]
    assert result.compiled_sql
    assert len(tool.calls) == 1


@pytest.mark.anyio
async def test_execute_unified_query_resolves_dataset_backed_tables_per_table() -> None:
    pytest.importorskip("pyarrow")

    organization_id = uuid.uuid4()
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
    repo = _FakeSemanticModelRepository(
        {
            model_id: _ModelRecord(
                content_yaml=source_model.yml_dump(),
                content_json=source_model.model_dump_json(exclude_none=True),
                connector_id=legacy_connector_id,
            )
        }
    )
    dataset_repo = _FakeDatasetRepository(
        {
            file_dataset_id: DatasetRecord(
                id=file_dataset_id,
                workspace_id=organization_id,
                project_id=None,
                connection_id=None,
                created_by=None,
                updated_by=None,
                name="orders_file",
                description=None,
                tags_json=[],
                dataset_type="FILE",
                dialect="duckdb",
                catalog_name=None,
                schema_name=None,
                table_name="orders",
                storage_uri="file:///tmp/orders.parquet",
                sql_text=None,
                referenced_dataset_ids_json=[],
                federated_plan_json=None,
                file_config_json={"format": "parquet"},
                status="published",
                revision_id=None,
                row_count_estimate=None,
                bytes_estimate=None,
                last_profiled_at=None,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            ),
            table_dataset_id: DatasetRecord(
                id=table_dataset_id,
                workspace_id=organization_id,
                project_id=None,
                connection_id=warehouse_connector_id,
                created_by=None,
                updated_by=None,
                name="inventory_table",
                description=None,
                tags_json=[],
                dataset_type="TABLE",
                dialect="postgres",
                catalog_name=None,
                schema_name="warehouse",
                table_name="inventory",
                storage_uri=None,
                sql_text=None,
                referenced_dataset_ids_json=[],
                federated_plan_json=None,
                file_config_json=None,
                status="published",
                revision_id=None,
                row_count_estimate=None,
                bytes_estimate=None,
                last_profiled_at=None,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            ),
        }
    )
    tool = _FakeFederatedQueryTool(rows=[{"orders__id": 1}])
    service = SemanticQueryExecutionService(
        semantic_model_repository=repo,
        dataset_repository=dataset_repo,
        federated_query_tool=tool,
        logger=logging.getLogger(__name__),
    )

    result = await service.execute_unified_query(
        organization_id=organization_id,
        project_id=None,
        semantic_query=SemanticQuery(dimensions=["orders.id"], limit=10),
        semantic_model_ids=[model_id],
        joins=None,
        metrics=None,
    )

    assert result.response.data == [{"orders__id": 1}]
    workflow = tool.calls[0]["workflow"]
    orders_binding = workflow["dataset"]["tables"]["orders"]
    inventory_binding = workflow["dataset"]["tables"]["inventory"]
    assert orders_binding["metadata"]["source_kind"] == "file"
    assert inventory_binding["connector_id"] == str(warehouse_connector_id)
