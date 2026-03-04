from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import pytest
import yaml

from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticModelRecordResponse,
)
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst.interfaces import (
    AnalystQueryRequest,
)
from langbridge.packages.orchestrator.langbridge_orchestrator.runtime.agent_orchestrator_factory import (
    AgentOrchestratorFactory,
    AgentToolConfig,
)
from langbridge.packages.semantic.langbridge_semantic.model import Dimension, SemanticModel, Table


class _StaticLLM:
    def __init__(self, sql: str) -> None:
        self._sql = sql

    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        _ = (prompt, temperature, max_tokens)
        return self._sql


class _SemanticModelStore:
    def __init__(self, entries: dict[uuid.UUID, SemanticModelRecordResponse]) -> None:
        self._entries = entries

    async def get_by_ids(self, model_ids: list[uuid.UUID]) -> list[SemanticModelRecordResponse]:
        return [self._entries[model_id] for model_id in model_ids if model_id in self._entries]


class _ConnectorStore:
    async def get_by_ids(self, connector_ids: list[uuid.UUID]) -> list[Any]:
        _ = connector_ids
        return []


class _FederatedQueryTool:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def execute_federated_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(payload)
        return {
            "columns": ["order_id", "customer_id"],
            "rows": [{"order_id": 1, "customer_id": 10}],
            "execution": {"total_runtime_ms": 13},
        }


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _build_source_entry(
    *,
    model_id: uuid.UUID,
    connector_id: uuid.UUID,
    organization_id: uuid.UUID,
    table_key: str,
    table_name: str,
) -> SemanticModelRecordResponse:
    model = SemanticModel(
        version="1.0",
        name=table_key,
        tables={
            table_key: Table(
                schema="public",
                name=table_name,
                dimensions=[Dimension(name=f"{table_key}_id", type="integer", primary_key=True)],
            )
        },
    )
    now = datetime.now(timezone.utc)
    return SemanticModelRecordResponse(
        id=model_id,
        organization_id=organization_id,
        project_id=None,
        name=table_key,
        description=None,
        content_yaml=model.yml_dump(),
        created_at=now,
        updated_at=now,
        connector_id=connector_id,
    )


@pytest.mark.anyio
async def test_agent_orchestrator_factory_routes_unified_sql_tool_to_federation() -> None:
    organization_id = uuid.uuid4()
    connector_a = uuid.uuid4()
    connector_b = uuid.uuid4()
    source_model_a = uuid.uuid4()
    source_model_b = uuid.uuid4()
    unified_model_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    source_a_entry = _build_source_entry(
        model_id=source_model_a,
        connector_id=connector_a,
        organization_id=organization_id,
        table_key="orders",
        table_name="orders",
    )
    source_b_entry = _build_source_entry(
        model_id=source_model_b,
        connector_id=connector_b,
        organization_id=organization_id,
        table_key="customers",
        table_name="customers",
    )

    unified_payload = {
        "version": "1.0",
        "name": "unified_orders_customers",
        "source_models": [{"id": str(source_model_a)}, {"id": str(source_model_b)}],
        "relationships": [
            {
                "name": "orders_to_customers",
                "from_": "orders",
                "to": "customers",
                "type": "inner",
                "join_on": "orders.orders_id = customers.customers_id",
            }
        ],
    }
    unified_entry = SemanticModelRecordResponse(
        id=unified_model_id,
        organization_id=organization_id,
        project_id=None,
        name="unified_orders_customers",
        description=None,
        content_yaml=yaml.safe_dump(unified_payload, sort_keys=False),
        created_at=now,
        updated_at=now,
        connector_id=connector_a,
    )

    store = _SemanticModelStore(
        entries={
            unified_model_id: unified_entry,
            source_model_a: source_a_entry,
            source_model_b: source_b_entry,
        }
    )
    federated_tool = _FederatedQueryTool()
    factory = AgentOrchestratorFactory(
        semantic_model_store=store,
        connector_store=_ConnectorStore(),
        federated_query_tool=federated_tool,
    )

    catalog_a = f"org_{organization_id.hex[:12]}__src_{connector_a.hex[:12]}"
    catalog_b = f"org_{organization_id.hex[:12]}__src_{connector_b.hex[:12]}"
    llm = _StaticLLM(
        "SELECT o.orders_id, c.customers_id "
        f'FROM "{catalog_a}"."public"."orders" AS o '
        f'JOIN "{catalog_b}"."public"."customers" AS c ON o.orders_id = c.customers_id'
    )
    sql_tools, _ = await factory._build_analyst_tools(  # noqa: SLF001 - intentional white-box test
        AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            sql_model_ids={unified_model_id},
        ),
        llm_provider=llm,  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )

    assert len(sql_tools) == 1

    response = await sql_tools[0].arun(AnalystQueryRequest(question="Join orders and customers"))

    assert response.error is None
    assert response.result is not None
    assert response.result.rows == [(1, 10)]
    assert len(federated_tool.calls) == 1
