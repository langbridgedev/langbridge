from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any

import pytest

from langbridge.apps.worker.langbridge_worker.semantic_query_execution_service import (
    SemanticQueryExecutionService,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
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
