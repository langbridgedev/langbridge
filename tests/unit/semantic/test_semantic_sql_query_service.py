import pytest

from langbridge.runtime.services.semantic_sql_query_service import SemanticSqlQueryService
from langbridge.semantic.errors import (
    SemanticSqlAmbiguousMemberError,
    SemanticSqlInvalidFilterError,
    SemanticSqlInvalidGroupingError,
    SemanticSqlInvalidTimeBucketError,
    SemanticSqlParseError,
    SemanticSqlUnsupportedConstructError,
    SemanticSqlUnsupportedExpressionError,
)
from langbridge.semantic.model import SemanticModel


def _semantic_model() -> SemanticModel:
    return SemanticModel.model_validate(
        {
            "version": "1",
            "name": "commerce_performance",
            "datasets": {
                "orders": {
                    "dimensions": [
                        {"name": "region", "type": "string"},
                        {"name": "order_status", "type": "string"},
                        {"name": "order_date", "type": "time"},
                        {"name": "customer_id", "type": "integer"},
                    ],
                    "measures": [
                        {
                            "name": "net_sales",
                            "expression": "net_revenue",
                            "type": "number",
                            "aggregation": "sum",
                        }
                    ],
                },
                "customers": {
                    "dimensions": [
                        {"name": "region", "type": "string"},
                        {"name": "customer_id", "type": "integer"},
                    ],
                },
            },
            "metrics": {
                "net_sales_metric": {
                    "expression": "SUM(orders.net_revenue)",
                }
            },
        }
    )


def test_semantic_sql_query_service_builds_semantic_query_plan() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query=(
            "SELECT orders.region, net_sales "
            "FROM commerce_performance "
            "WHERE order_status = 'fulfilled' "
            "ORDER BY net_sales DESC "
            "LIMIT 5"
        ),
        query_dialect="postgres",
    )

    plan = service.build_query_plan(
        parsed_query=parsed,
        semantic_model=_semantic_model(),
    )

    assert parsed.semantic_model_ref == "commerce_performance"
    assert [projection.output_name for projection in plan.projections] == ["region", "net_sales"]
    assert plan.semantic_query.dimensions == ["orders.region"]
    assert plan.semantic_query.measures == ["orders.net_sales"]
    assert plan.semantic_query.filters[0].member == "orders.order_status"
    assert plan.semantic_query.filters[0].operator == "equals"
    assert plan.semantic_query.filters[0].values == ["fulfilled"]
    assert plan.semantic_query.order == {"orders.net_sales": "desc"}
    assert plan.semantic_query.limit == 5


def test_semantic_sql_query_service_supports_time_buckets_and_group_ordinals() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query=(
            "SELECT DATE_TRUNC('month', order_date) AS month, net_sales "
            "FROM commerce_performance "
            "GROUP BY 1 "
            "ORDER BY month DESC"
        ),
        query_dialect="postgres",
    )

    plan = service.build_query_plan(
        parsed_query=parsed,
        semantic_model=_semantic_model(),
    )

    assert plan.semantic_query.time_dimensions[0].dimension == "orders.order_date"
    assert plan.semantic_query.time_dimensions[0].granularity == "month"
    assert plan.semantic_query.order == {"orders.order_date": "desc"}
    assert [projection.output_name for projection in plan.projections] == ["month", "net_sales"]


def test_semantic_sql_query_service_preserves_ilike_filters() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query=(
            "SELECT orders.region "
            "FROM commerce_performance "
            "WHERE order_status ILIKE 'Fulfilled%'"
        ),
        query_dialect="postgres",
    )

    plan = service.build_query_plan(
        parsed_query=parsed,
        semantic_model=_semantic_model(),
    )

    assert len(plan.semantic_query.filters) == 1
    assert plan.semantic_query.filters[0].member == "orders.order_status"
    assert plan.semantic_query.filters[0].operator == "ilike"
    assert plan.semantic_query.filters[0].values == ["Fulfilled%"]


def test_semantic_sql_query_service_normalizes_year_filter_for_time_dimension() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query=(
            "SELECT DATE_TRUNC('month', order_date) AS month, net_sales "
            "FROM commerce_performance "
            "WHERE order_date = 2025 "
            "GROUP BY 1 "
            "ORDER BY month ASC"
        ),
        query_dialect="postgres",
    )

    plan = service.build_query_plan(
        parsed_query=parsed,
        semantic_model=_semantic_model(),
    )

    assert len(plan.semantic_query.filters) == 1
    assert plan.semantic_query.filters[0].member == "orders.order_date"
    assert plan.semantic_query.filters[0].operator == "indaterange"
    assert plan.semantic_query.filters[0].values == ["2025-01-01", "2025-12-31"]


def test_semantic_sql_query_service_surfaces_actionable_parse_errors() -> None:
    service = SemanticSqlQueryService()

    with pytest.raises(SemanticSqlParseError, match="could not parse this query") as exc_info:
        service.parse_query(
            query="SELECT orders.region FROM commerce_performance WHERE (",
            query_dialect="postgres",
        )

    message = str(exc_info.value)
    assert "single semantic model" in message
    assert "dataset SQL scope" in message


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "SELECT orders.region FROM commerce_performance AS o JOIN other_model AS x ON o.region = x.region",
            "governed relationships",
        ),
        (
            "WITH scoped AS (SELECT orders.region FROM commerce_performance) "
            "SELECT region FROM scoped",
            "does not support CTEs",
        ),
        (
            "SELECT DISTINCT orders.region FROM commerce_performance",
            "does not support DISTINCT",
        ),
        (
            "SELECT orders.region, net_sales FROM commerce_performance GROUP BY 1 HAVING net_sales > 10",
            "does not support HAVING",
        ),
    ],
)
def test_semantic_sql_query_service_rejects_unsupported_constructs_with_guidance(
    query: str,
    expected: str,
) -> None:
    service = SemanticSqlQueryService()

    with pytest.raises(SemanticSqlUnsupportedConstructError, match=expected) as exc_info:
        service.parse_query(
            query=query,
            query_dialect="postgres",
        )

    assert "dataset SQL scope" in str(exc_info.value)


def test_semantic_sql_query_service_guides_on_unsupported_aggregate_select_expression() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query=(
            "SELECT orders.customer_id, MIN(order_date) AS first_order_date "
            "FROM commerce_performance "
            "GROUP BY 1"
        ),
        query_dialect="postgres",
    )

    with pytest.raises(SemanticSqlUnsupportedExpressionError, match="free-form aggregate expressions") as exc_info:
        service.build_query_plan(
            parsed_query=parsed,
            semantic_model=_semantic_model(),
        )

    message = str(exc_info.value)
    assert "semantic metric" in message
    assert "dataset SQL scope" in message


def test_semantic_sql_query_service_rejects_raw_scalar_select_expressions() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query="SELECT order_date + 1 FROM commerce_performance",
        query_dialect="postgres",
    )

    with pytest.raises(SemanticSqlUnsupportedExpressionError, match="does not allow raw SQL expressions in SELECT"):
        service.build_query_plan(
            parsed_query=parsed,
            semantic_model=_semantic_model(),
        )


def test_semantic_sql_query_service_rejects_or_filters_with_recovery_guidance() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query=(
            "SELECT orders.region "
            "FROM commerce_performance "
            "WHERE order_status = 'fulfilled' OR order_status = 'pending'"
        ),
        query_dialect="postgres",
    )

    with pytest.raises(SemanticSqlInvalidFilterError, match="AND-combined predicates") as exc_info:
        service.build_query_plan(
            parsed_query=parsed,
            semantic_model=_semantic_model(),
        )

    assert "dataset SQL scope" in str(exc_info.value)


def test_semantic_sql_query_service_rejects_unsupported_like_patterns() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query=(
            "SELECT orders.region "
            "FROM commerce_performance "
            "WHERE order_status LIKE 'fulfill_d%'"
        ),
        query_dialect="postgres",
    )

    with pytest.raises(SemanticSqlInvalidFilterError, match="Single-character `_` wildcards"):
        service.build_query_plan(
            parsed_query=parsed,
            semantic_model=_semantic_model(),
        )


def test_semantic_sql_query_service_rejects_order_by_raw_sql_expressions() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query=(
            "SELECT orders.region "
            "FROM commerce_performance "
            "ORDER BY LOWER(region)"
        ),
        query_dialect="postgres",
    )

    with pytest.raises(SemanticSqlInvalidGroupingError, match="ORDER BY only supports semantic members"):
        service.build_query_plan(
            parsed_query=parsed,
            semantic_model=_semantic_model(),
        )


def test_semantic_sql_query_service_rejects_group_by_mismatch_with_specific_guidance() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query=(
            "SELECT DATE_TRUNC('month', order_date) AS month, orders.region, net_sales "
            "FROM commerce_performance "
            "GROUP BY 2"
        ),
        query_dialect="postgres",
    )

    with pytest.raises(SemanticSqlInvalidGroupingError, match="must match the selected semantic dimensions and time buckets exactly") as exc_info:
        service.build_query_plan(
            parsed_query=parsed,
            semantic_model=_semantic_model(),
        )

    assert "missing:" in str(exc_info.value)
    assert "`month`" in str(exc_info.value)


def test_semantic_sql_query_service_guides_on_ambiguous_members() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query="SELECT region FROM commerce_performance",
        query_dialect="postgres",
    )

    with pytest.raises(SemanticSqlAmbiguousMemberError, match="ambiguous") as exc_info:
        service.build_query_plan(
            parsed_query=parsed,
            semantic_model=_semantic_model(),
        )

    message = str(exc_info.value)
    assert "dataset name" in message
    assert "`orders.region`" in message


def test_semantic_sql_query_service_guides_on_invalid_time_bucket_usage() -> None:
    service = SemanticSqlQueryService()
    parsed = service.parse_query(
        query="SELECT DATE_TRUNC('month', order_status) FROM commerce_performance",
        query_dialect="postgres",
    )

    with pytest.raises(SemanticSqlInvalidTimeBucketError, match="semantic time dimensions") as exc_info:
        service.build_query_plan(
            parsed_query=parsed,
            semantic_model=_semantic_model(),
        )

    assert "dataset SQL scope" in str(exc_info.value)
