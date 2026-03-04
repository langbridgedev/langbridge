from __future__ import annotations

from typing import Any

from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst.interfaces import (
    AnalystQueryRequest,
    QueryResult,
)
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst.tool import (
    SqlAnalystTool,
)
from langbridge.packages.semantic.langbridge_semantic.model import Dimension, SemanticModel, Table


class _StaticLLM:
    def __init__(self, sql: str) -> None:
        self._sql = sql

    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        _ = (prompt, temperature, max_tokens)
        return self._sql


class _RecordingConnector:
    DIALECT = type("D", (), {"name": "POSTGRES"})

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def execute(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        max_rows: int | None = None,
        timeout_s: int | None = None,
    ) -> Any:
        _ = (params, max_rows, timeout_s)
        self.calls.append(sql)

        class _Result:
            def __init__(self, sql_text: str) -> None:
                self.columns = ["value"]
                self.rows = [(42,)]
                self.rowcount = 1
                self.elapsed_ms = 7
                self.sql = sql_text

        return _Result(sql)


class _FakeFederatedExecutor:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def execute_sql(
        self,
        *,
        sql: str,
        dialect: str,
        max_rows: int | None = None,
    ) -> QueryResult:
        self.calls.append({"sql": sql, "dialect": dialect, "max_rows": max_rows})
        return QueryResult(
            columns=["order_id", "customer_id"],
            rows=[(1, 100)],
            rowcount=1,
            elapsed_ms=11,
            source_sql=sql,
        )


def _multi_source_model() -> SemanticModel:
    return SemanticModel(
        version="1.0",
        name="unified_orders",
        dialect="postgres",
        tables={
            "orders_a": Table(
                catalog="org_abc__src_111",
                schema="public",
                name="orders",
                dimensions=[Dimension(name="order_id", type="integer", primary_key=True)],
            ),
            "customers_b": Table(
                catalog="org_abc__src_222",
                schema="public",
                name="customers",
                dimensions=[Dimension(name="customer_id", type="integer", primary_key=True)],
            ),
        },
    )


def test_sql_analyst_tool_detects_cross_source_without_federation() -> None:
    connector = _RecordingConnector()
    llm = _StaticLLM(
        'SELECT o.order_id, c.customer_id '
        'FROM "org_abc__src_111"."public"."orders" AS o '
        'JOIN "org_abc__src_222"."public"."customers" AS c ON o.order_id = c.customer_id'
    )
    tool = SqlAnalystTool(
        llm=llm,
        semantic_model=_multi_source_model(),
        connector=connector,
        dialect="postgres",
        table_source_map={"orders_a": "source_a", "customers_b": "source_b"},
    )

    response = tool.run(AnalystQueryRequest(question="Join orders and customers"))

    assert response.error is not None
    assert "Cross-source query detected" in response.error
    assert connector.calls == []


def test_sql_analyst_tool_routes_cross_source_to_federation() -> None:
    connector = _RecordingConnector()
    federated_executor = _FakeFederatedExecutor()
    llm = _StaticLLM(
        'SELECT o.order_id, c.customer_id '
        'FROM "org_abc__src_111"."public"."orders" AS o '
        'JOIN "org_abc__src_222"."public"."customers" AS c ON o.order_id = c.customer_id'
    )
    tool = SqlAnalystTool(
        llm=llm,
        semantic_model=_multi_source_model(),
        connector=connector,
        dialect="postgres",
        table_source_map={"orders_a": "source_a", "customers_b": "source_b"},
        federated_sql_executor=federated_executor,
    )

    response = tool.run(AnalystQueryRequest(question="Join orders and customers", limit=50))

    assert response.error is None
    assert response.result is not None
    assert response.result.rows == [(1, 100)]
    assert connector.calls == []
    assert len(federated_executor.calls) == 1
    assert federated_executor.calls[0]["dialect"] == "postgres"


def test_sql_analyst_tool_single_source_uses_connector_execution() -> None:
    connector = _RecordingConnector()
    llm = _StaticLLM("SELECT order_id FROM public.orders")
    model = SemanticModel(
        version="1.0",
        name="orders",
        dialect="postgres",
        tables={
            "orders": Table(
                schema="public",
                name="orders",
                dimensions=[Dimension(name="order_id", type="integer", primary_key=True)],
            )
        },
    )
    tool = SqlAnalystTool(
        llm=llm,
        semantic_model=model,
        connector=connector,
        dialect="postgres",
        table_source_map={"orders": "source_a"},
    )

    response = tool.run(AnalystQueryRequest(question="List orders", limit=10))

    assert response.error is None
    assert response.result is not None
    assert response.result.rows == [(42,)]
    assert len(connector.calls) == 1


def test_sql_analyst_tool_routes_catalog_qualified_single_source_sql_to_federation() -> None:
    connector = _RecordingConnector()
    federated_executor = _FakeFederatedExecutor()
    llm = _StaticLLM(
        'SELECT "org_abc__src_111"."public"."orders"."order_id" '
        'FROM "org_abc__src_111"."public"."orders"'
    )
    model = SemanticModel(
        version="1.0",
        name="orders",
        dialect="postgres",
        tables={
            "orders": Table(
                catalog="org_abc__src_111",
                schema="public",
                name="orders",
                dimensions=[Dimension(name="order_id", type="integer", primary_key=True)],
            )
        },
    )
    tool = SqlAnalystTool(
        llm=llm,
        semantic_model=model,
        connector=connector,
        dialect="postgres",
        table_source_map={"orders": "source_a"},
        federated_sql_executor=federated_executor,
    )

    response = tool.run(AnalystQueryRequest(question="List orders"))

    assert response.error is None
    assert response.result is not None
    assert connector.calls == []
    assert len(federated_executor.calls) == 1
