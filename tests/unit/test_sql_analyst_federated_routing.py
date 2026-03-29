
from typing import Any

from langbridge.orchestrator.tools.sql_analyst.interfaces import (
    AnalyticalColumn,
    AnalyticalContext,
    AnalyticalDatasetBinding,
    AnalyticalField,
    AnalyticalMetric,
    AnalystQueryRequest,
    QueryResult,
)
from langbridge.orchestrator.tools.sql_analyst.tool import (
    SqlAnalystTool,
)
from langbridge.semantic.model import Dataset, Dimension, Measure, SemanticModel


class _StaticLLM:
    def __init__(self, sql: str) -> None:
        self._sql = sql

    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        _ = (prompt, temperature, max_tokens)
        return self._sql


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


def _semantic_context() -> AnalyticalContext:
    return AnalyticalContext(
        asset_type="semantic_model",
        asset_id="semantic-1",
        asset_name="orders_model",
        description="Governed orders model",
        datasets=[
            AnalyticalDatasetBinding(
                dataset_id="dataset-1",
                dataset_name="orders_dataset",
                sql_alias="orders",
                source_kind="connector",
                storage_kind="table",
                columns=[AnalyticalColumn(name="order_id", data_type="integer")],
            ),
            AnalyticalDatasetBinding(
                dataset_id="dataset-2",
                dataset_name="customers_dataset",
                sql_alias="customers",
                source_kind="connector",
                storage_kind="table",
                columns=[AnalyticalColumn(name="customer_id", data_type="integer")],
            ),
        ],
        tables=["orders", "customers"],
        dimensions=[
            AnalyticalField(name="order_id"),
            AnalyticalField(name="customer_id"),
        ],
        metrics=[AnalyticalMetric(name="total_orders", expression="COUNT(*)")],
        relationships=["INNER join orders -> customers on orders.customer_id = customers.customer_id"],
    )


def test_sql_analyst_tool_executes_semantic_model_context_through_federation() -> None:
    executor = _FakeFederatedExecutor()
    tool = SqlAnalystTool(
        llm=_StaticLLM(
            "SELECT orders.order_id, customers.customer_id "
            "FROM orders JOIN customers ON orders.customer_id = customers.customer_id"
        ),
        context=_semantic_context(),
        federated_sql_executor=executor,
    )

    response = tool.run(AnalystQueryRequest(question="Join orders and customers", limit=50))

    assert response.error is None
    assert response.asset_type == "semantic_model"
    assert response.execution_mode == "federated"
    assert response.result is not None
    assert response.result.rows == [(1, 100)]
    assert len(response.selected_datasets) == 2
    assert executor.calls == [
        {
            "sql": (
                "SELECT orders.order_id, customers.customer_id "
                "FROM orders JOIN customers ON orders.customer_id = customers.customer_id LIMIT 50"
            ),
            "dialect": "postgres",
            "max_rows": 50,
        }
    ]


def test_sql_analyst_tool_returns_parse_error_for_invalid_sql() -> None:
    executor = _FakeFederatedExecutor()
    tool = SqlAnalystTool(
        llm=_StaticLLM("SELECT FROM"),
        context=_semantic_context(),
        federated_sql_executor=executor,
    )

    response = tool.run(AnalystQueryRequest(question="Break the parser"))

    assert response.error is not None
    assert "failed to parse" in response.error.lower()
    assert executor.calls == []


def test_sql_analyst_tool_casts_temporal_semantic_dimensions_in_predicates() -> None:
    executor = _FakeFederatedExecutor()
    tool = SqlAnalystTool(
        llm=_StaticLLM(
            "SELECT orders.country, SUM(orders.net_sales) AS total_net_sales "
            "FROM orders "
            "WHERE orders.order_date >= DATE_TRUNC('QUARTER', CURRENT_DATE) "
            "AND orders.order_date < DATE_TRUNC('QUARTER', CURRENT_DATE) + INTERVAL '3 MONTHS' "
            "GROUP BY orders.country"
        ),
        context=_semantic_context(),
        federated_sql_executor=executor,
        semantic_model=SemanticModel(
            version="1",
            datasets={
                "orders": Dataset(
                    relation_name="orders_enriched",
                    dimensions=[
                        Dimension(name="country", type="string"),
                        Dimension(name="order_date", type="time"),
                    ],
                    measures=[
                        Measure(name="net_sales", expression="net_revenue", type="number", aggregation="sum"),
                    ],
                )
            },
        ),
    )

    response = tool.run(AnalystQueryRequest(question="Quarterly sales by country"))

    assert response.error is None
    assert response.sql_canonical.count('CAST(orders.order_date AS TIMESTAMP)') == 2
    assert "SUM(orders.net_revenue) AS total_net_sales" in response.sql_canonical
    assert executor.calls[0]["sql"].count('CAST(orders.order_date AS TIMESTAMP)') == 2


def test_sql_analyst_tool_expands_semantic_measure_expressions() -> None:
    executor = _FakeFederatedExecutor()
    tool = SqlAnalystTool(
        llm=_StaticLLM(
            "SELECT orders.country, SUM(orders.net_sales) AS total_net_sales "
            "FROM orders GROUP BY orders.country"
        ),
        context=_semantic_context(),
        federated_sql_executor=executor,
        semantic_model=SemanticModel(
            version="1",
            datasets={
                "orders": Dataset(
                    relation_name="orders_enriched",
                    dimensions=[Dimension(name="country", type="string")],
                    measures=[
                        Measure(name="net_sales", expression="net_revenue", type="number", aggregation="sum"),
                    ],
                )
            },
        ),
    )

    response = tool.run(AnalystQueryRequest(question="Top countries by net sales"))

    assert response.error is None
    assert "orders.net_sales" not in response.sql_canonical
    assert "SUM(orders.net_revenue) AS total_net_sales" in response.sql_canonical


def test_sql_analyst_tool_casts_temporal_dataset_columns_in_predicates() -> None:
    executor = _FakeFederatedExecutor()
    context = AnalyticalContext(
        asset_type="dataset",
        asset_id="dataset-1",
        asset_name="orders_dataset",
        datasets=[
            AnalyticalDatasetBinding(
                dataset_id="dataset-1",
                dataset_name="orders_dataset",
                sql_alias="orders",
                columns=[
                    AnalyticalColumn(name="order_date", data_type="timestamp"),
                    AnalyticalColumn(name="country", data_type="string"),
                ],
            )
        ],
        tables=["orders"],
    )
    tool = SqlAnalystTool(
        llm=_StaticLLM(
            "SELECT orders.country FROM orders "
            "WHERE orders.order_date >= DATE_TRUNC('MONTH', CURRENT_DATE)"
        ),
        context=context,
        federated_sql_executor=executor,
    )

    response = tool.run(AnalystQueryRequest(question="Current month countries"))

    assert response.error is None
    assert 'CAST(orders.order_date AS TIMESTAMP) >= DATE_TRUNC(\'MONTH\', CURRENT_DATE)' in response.sql_canonical
