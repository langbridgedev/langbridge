import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).resolve().parents[5] / "langbridge" / "langbridge"))

from typing import Any


from langbridge.packages.orchestrator.langbridge_orchestrator.agents.analyst.agent import AnalystAgent
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst.interfaces import AnalystQueryRequest, SemanticModel
from langbridge.packages.orchestrator.langbridge_orchestrator.tools.sql_analyst.tool import SqlAnalystTool


class StaticLLM:
    def __init__(self, sql: str) -> None:
        self._sql = sql

    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        return self._sql


class RecorderConnector:
    DIALECT = type("D", (), {"name": "POSTGRES"})

    def __init__(self, label: str) -> None:
        self.label = label
        self.calls: list[str] = []

    async def execute(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        max_rows: int | None = None,
        timeout_s: int | None = None,
    ) -> Any:
        self.calls.append(sql)

        class Result:
            def __init__(self, sql_text: str) -> None:
                self.columns = ["value"]
                self.rows = [(label,)]
                self.rowcount = 1
                self.elapsed_ms = 0
                self.sql = sql_text

        label = self.label
        return Result(sql)


def _model(name: str, entity: str, tags: list[str] | None = None) -> SemanticModel:
    return SemanticModel(
        version="1.0",
        name=name,
        tags=tags or [],
        tables={
            entity: {
                "name": entity,
                "dimensions": [{"name": "id", "type": "integer"}],
            },
        },
        relationships=[],
        metrics={"total": {"expression": "COUNT(*)"}},
    )


def _tool(name: str, entity: str, sql: str, connector_label: str, priority: int = 0, tags: list[str] | None = None) -> tuple[SqlAnalystTool, RecorderConnector]:
    connector = RecorderConnector(connector_label)
    tool = SqlAnalystTool(
        llm=StaticLLM(sql),
        semantic_model=_model(name, entity, tags=tags),
        connector=connector,
        dialect="postgres",
        priority=priority,
    )
    return tool, connector


def test_analyst_agent_selects_tool_by_keywords() -> None:
    customers_tool, customers_connector = _tool(
        "customers_model",
        "customers",
        "SELECT COUNT(*) FROM customers",
        "customers",
    )
    sales_tool, sales_connector = _tool(
        "sales_model",
        "orders",
        "SELECT COUNT(*) FROM orders",
        "sales",
        tags=["revenue", "orders"],
    )

    agent = AnalystAgent(StaticLLM(""), [], [customers_tool, sales_tool])
    response = agent.answer("Show revenue by orders")

    assert response.error is None
    assert sales_connector.calls, "Sales connector should have been invoked"
    assert not customers_connector.calls, "Customers connector should not have been invoked"
    assert response.model_name == "sales_model"


def test_analyst_agent_uses_priority_on_tie() -> None:
    tool_a, conn_a = _tool(
        "model_a",
        "entity_a",
        "SELECT 1",
        "a",
        priority=1,
    )
    tool_b, conn_b = _tool(
        "model_b",
        "entity_b",
        "SELECT 1",
        "b",
        priority=5,
    )

    agent = AnalystAgent(StaticLLM(""), [], [tool_a, tool_b])
    response = agent.answer("General question with no keywords")

    assert response.model_name == "model_b"
    assert conn_b.calls
    assert not conn_a.calls


def test_semantic_tool_selector_handles_filters() -> None:
    tool_a, conn_a = _tool(
        "metrics_model",
        "metrics_table",
        "SELECT COUNT(*) FROM metrics_table",
        "metrics",
        tags=["kpi"],
    )
    tool_b, conn_b = _tool(
        "inventory_model",
        "inventory",
        "SELECT COUNT(*) FROM inventory",
        "inventory",
    )

    agent = AnalystAgent(StaticLLM(""), [], [tool_a, tool_b])
    response = agent.answer_with_request(
        AnalystQueryRequest(
            question="Give me KPI results",
            filters={"kpi": "retention"},
        )
    )

    assert response.model_name == "metrics_model"
    assert conn_a.calls
    assert not conn_b.calls
