from langbridge.ai.tools.sql.interfaces import AnalyticalContext, SqlQueryScope
from langbridge.ai.tools.sql.renderer import render_analysis_context
from langbridge.semantic.loader import load_semantic_model


def test_semantic_model_preserves_sql_instructions() -> None:
    model = load_semantic_model(
        {
            "version": "1",
            "name": "commerce",
            "description": "Commerce model",
            "sql_instructions": "Always use orders.net_revenue for revenue questions.",
            "datasets": {
                "orders": {
                    "relation_name": "sales_orders",
                    "dimensions": [{"name": "order_id", "type": "string"}],
                    "measures": [{"name": "net_revenue", "type": "number", "aggregation": "sum"}],
                }
            },
            "metrics": {
                "average_order_value": {
                    "expression": "SUM(orders.net_revenue) / NULLIF(COUNT(orders.order_id), 0)",
                    "description": "Average value per order",
                }
            },
        }
    )

    assert model.sql_instructions == "Always use orders.net_revenue for revenue questions."
    assert model.metrics is not None
    assert model.metrics["average_order_value"].description == "Average value per order"


def test_semantic_model_uses_orchestration_instructions_as_sql_instructions() -> None:
    model = load_semantic_model(
        {
            "version": "1",
            "name": "performance",
            "orchestration": {
                "orchestration": "sql_generation",
                "steps": [
                    {
                        "name": "sql_generation",
                        "instructions": "Use ILIKE for product lookups.",
                    },
                    {
                        "name": "relative_metrics",
                        "instructions": "Use aligned_returns for alpha and beta.",
                    },
                ],
            },
            "datasets": {
                "aligned_returns": {
                    "relation_name": "aligned_returns",
                    "dimensions": [{"name": "product_id", "type": "string"}],
                }
            },
        }
    )

    assert model.sql_instructions == (
        "Use ILIKE for product lookups.\n\nUse aligned_returns for alpha and beta."
    )


def test_analysis_context_renders_sql_instructions() -> None:
    rendered = render_analysis_context(
        AnalyticalContext(
            query_scope=SqlQueryScope.semantic,
            asset_type="semantic_model",
            asset_id="model-1",
            asset_name="commerce",
            description="Commerce model",
            sql_instructions="Prefer order_date for time analysis.",
        )
    )

    assert "SQL instructions:" in rendered
    assert "Prefer order_date for time analysis." in rendered
