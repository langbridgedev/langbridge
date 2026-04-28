from langbridge.semantic.model import Dimension, Measure, SemanticModel, Table
from langbridge.semantic.query import SemanticQuery, SemanticQueryEngine


def _build_orders_model() -> SemanticModel:
    return SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                schema="public",
                name="orders",
                dimensions=[Dimension(name="created_at", type="timestamp")],
                measures=[Measure(name="amount", type="number", aggregation="sum")],
            )
        },
    )


def test_measure_with_time_dimension_is_selected_and_grouped_when_no_dimensions() -> None:
    model = _build_orders_model()
    query = SemanticQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "timeDimensions": [{"dimension": "public.orders.created_at", "granularity": "day"}],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert (
        'SELECT DATE_TRUNC(\'DAY\', CAST(t0."created_at" AS TIMESTAMP)) AS "orders__created_at_day", '
        'SUM(t0."amount")'
    ) in sql
    assert 'GROUP BY DATE_TRUNC(\'DAY\', CAST(t0."created_at" AS TIMESTAMP))' in sql


def test_ordering_by_schema_table_time_dimension_uses_projected_time_dimension_alias() -> None:
    model = _build_orders_model()
    query = SemanticQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "timeDimensions": [{"dimension": "public.orders.created_at", "granularity": "day"}],
            "order": [{"public.orders.created_at": "desc"}],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert 'ORDER BY "orders__created_at_day" DESC' in sql
    assert "ORDER BY created_at DESC" not in sql


def test_relative_time_preset_builds_timestamp_window_not_literal_value_filter() -> None:
    model = _build_orders_model()
    query = SemanticQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "timeDimensions": [{"dimension": "orders.created_at", "dateRange": "last_30_days"}],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert "last_30_days" not in sql
    assert 'WHERE CAST(t0."created_at" AS TIMESTAMP) >=' in sql
    assert 'CAST(t0."created_at" AS TIMESTAMP) <' in sql


def test_custom_time_dimension_before_date_filter_builds_before_condition() -> None:
    model = _build_orders_model()
    query = SemanticQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "timeDimensions": [{"dimension": "orders.created_at", "dateRange": "before:2026-01-01"}],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert 'WHERE CAST(t0."created_at" AS TIMESTAMP) < \'2026-01-01\'' in sql


def test_custom_between_dates_uses_inclusive_day_window_for_timestamp_dimensions() -> None:
    model = _build_orders_model()
    query = SemanticQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "timeDimensions": [{"dimension": "orders.created_at", "dateRange": ["2026-01-01", "2026-01-31"]}],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert 'CAST(t0."created_at" AS TIMESTAMP) >= \'2026-01-01\'' in sql
    assert 'CAST(t0."created_at" AS TIMESTAMP) < CAST(\'2026-01-31\' AS DATE) + INTERVAL \'1 DAY\'' in sql


def test_custom_on_date_uses_casted_day_window_for_timestamp_dimensions() -> None:
    model = _build_orders_model()
    query = SemanticQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "timeDimensions": [{"dimension": "orders.created_at", "dateRange": "on:2026-01-01"}],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert 'CAST(t0."created_at" AS TIMESTAMP) >= \'2026-01-01\'' in sql
    assert 'CAST(t0."created_at" AS TIMESTAMP) < CAST(\'2026-01-01\' AS DATE) + INTERVAL \'1 DAY\'' in sql


def test_time_typed_dimension_is_cast_before_date_comparison() -> None:
    model = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                name="orders",
                dimensions=[Dimension(name="order_date", type="time")],
                measures=[Measure(name="amount", type="number", aggregation="sum")],
            )
        },
    )
    query = SemanticQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "timeDimensions": [{"dimension": "orders.order_date", "dateRange": "this_quarter"}],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert 'CAST(t0."order_date" AS TIMESTAMP) >=' in sql
    assert 'CAST(t0."order_date" AS TIMESTAMP) <' in sql
    assert "0 + INTERVAL" not in sql


def test_sqlite_time_dimension_year_bucket_uses_portable_date_trunc() -> None:
    model = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                name="orders",
                dimensions=[Dimension(name="order_date", type="time")],
                measures=[Measure(name="amount", type="number", aggregation="sum")],
            )
        },
    )
    query = SemanticQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "timeDimensions": [{"dimension": "orders.order_date", "granularity": "year"}],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="sqlite").sql

    assert 'DATE_TRUNC(\'YEAR\', CAST(t0."order_date" AS TIMESTAMP))' in sql
    assert "JULIANDAY" not in sql
    assert "DATE(0" not in sql


def test_time_dimension_uses_underlying_column_expression_in_snowflake() -> None:
    model = SemanticModel.model_validate(
        {
            "version": "1.0",
            "name": "product_performance",
            "datasets": {
                "product_returns": {
                    "relation_name": "product_returns",
                    "dimensions": [
                        {
                            "name": "strike_date",
                            "expression": "STRIKE_DATE",
                            "type": "time",
                        }
                    ],
                    "measures": [
                        {
                            "name": "periodic_return",
                            "expression": "RETURN",
                            "type": "number",
                            "aggregation": "avg",
                        }
                    ],
                }
            },
        }
    )
    query = SemanticQuery.model_validate(
        {
            "measures": ["product_returns.periodic_return"],
            "timeDimensions": [{"dimension": "product_returns.strike_date", "granularity": "year"}],
            "filters": [
                {
                    "member": "product_returns.strike_date",
                    "operator": "lte",
                    "values": ["2025-12-31"],
                }
            ],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="snowflake").sql

    assert 'DATE_TRUNC(\'YEAR\', CAST(t0."STRIKE_DATE" AS TIMESTAMP))' in sql
    assert 'CAST(t0."STRIKE_DATE" AS TIMESTAMP)' in sql
    assert 't0."STRIKE_DATE" <= \'2025-12-31\'' in sql
    assert 't0."strike_date"' not in sql


def test_snowflake_year_bucket_compiles_portably_for_benchmark_annualised_metric() -> None:
    model = SemanticModel.model_validate(
        {
            "version": "1.0",
            "name": "product_performance",
            "datasets": {
                "benchmark_stream": {
                    "relation_name": "benchmark_stream",
                    "dimensions": [
                        {
                            "name": "benchmark_stream_bk_key",
                            "expression": "BENCHMARK_STREAM_BK_KEY",
                            "type": "number",
                        },
                        {
                            "name": "benchmark_stream_name",
                            "expression": "BENCHMARK_STREAM_NAME",
                            "type": "string",
                        },
                    ],
                },
                "benchmark_returns": {
                    "relation_name": "benchmark_returns",
                    "dimensions": [
                        {
                            "name": "benchmark_stream_bk_key",
                            "expression": "BENCHMARK_STREAM_BK_KEY",
                            "type": "number",
                        },
                        {
                            "name": "strike_date",
                            "expression": "STRIKE_DATE",
                            "type": "time",
                        },
                    ],
                    "measures": [
                        {
                            "name": "benchmark_return",
                            "expression": "RETURN",
                            "type": "number",
                            "aggregation": "avg",
                        }
                    ],
                },
            },
            "relationships": [
                {
                    "name": "benchmark_returns_to_benchmark_stream",
                    "source_dataset": "benchmark_returns",
                    "source_field": "benchmark_stream_bk_key",
                    "target_dataset": "benchmark_stream",
                    "target_field": "benchmark_stream_bk_key",
                    "type": "many_to_one",
                }
            ],
            "metrics": {
                "benchmark_annualised_return": {
                    "expression": (
                        "CASE "
                        "WHEN DATEDIFF('month', MIN(benchmark_returns.STRIKE_DATE), MAX(benchmark_returns.STRIKE_DATE)) + 1 > 0 "
                        "THEN POWER(1 + (EXP(SUM(LN(1 + benchmark_returns.RETURN))) - 1), "
                        "12.0 / (DATEDIFF('month', MIN(benchmark_returns.STRIKE_DATE), MAX(benchmark_returns.STRIKE_DATE)) + 1)) - 1 "
                        "ELSE NULL END"
                    )
                }
            },
        }
    )
    query = SemanticQuery.model_validate(
        {
            "measures": ["benchmark_annualised_return"],
            "timeDimensions": [{"dimension": "benchmark_returns.strike_date", "granularity": "year"}],
            "filters": [
                {
                    "member": "benchmark_stream.benchmark_stream_name",
                    "operator": "ilike",
                    "values": ["MSCI India NR USD"],
                },
                {
                    "member": "benchmark_returns.strike_date",
                    "operator": "gte",
                    "values": ["2020-01-01"],
                },
                {
                    "member": "benchmark_returns.strike_date",
                    "operator": "lt",
                    "values": ["2026-01-01"],
                },
            ],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="snowflake").sql

    assert 'DATE_TRUNC(\'YEAR\', CAST(t0."STRIKE_DATE" AS TIMESTAMP))' in sql
    assert "DATEADD(YEAR, DATEDIFF(YEAR, 0" not in sql
    assert "CASE WHEN DATEDIFF(MONTH, MIN(t0.STRIKE_DATE), MAX(t0.STRIKE_DATE)) + 1 > 0" in sql


def test_measure_expression_uses_underlying_column_name_not_measure_name() -> None:
    model = SemanticModel(
        version="1.0",
        tables={
            "shopify_orders": Table(
                name="orders_enriched",
                dimensions=[Dimension(name="country", expression="country", type="string")],
                measures=[
                    Measure(
                        name="net_sales",
                        expression="net_revenue",
                        type="number",
                        aggregation="sum",
                    )
                ],
            )
        },
    )
    query = SemanticQuery.model_validate(
        {
            "measures": ["shopify_orders.net_sales"],
            "dimensions": ["shopify_orders.country"],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert 'SUM(t0."net_revenue") AS "shopify_orders__net_sales"' in sql
    assert 'SUM(t0."net_sales")' not in sql


def test_metric_can_use_group_safe_dimension_lookup_via_any_value() -> None:
    model = SemanticModel.model_validate(
        {
            "version": "1.0",
            "name": "returns",
            "datasets": {
                "returns": {
                    "relation_name": "returns",
                    "dimensions": [
                        {"name": "fund_id", "expression": "fund_id", "type": "string"},
                        {"name": "frequency", "expression": "frequency", "type": "string"},
                    ],
                    "measures": [
                        {
                            "name": "amount",
                            "expression": "amount",
                            "type": "number",
                            "aggregation": "sum",
                        }
                    ],
                }
            },
            "metrics": {
                "annualised_amount": {
                    "expression": (
                        "CASE "
                        "WHEN ANY_VALUE(returns.frequency) ILIKE 'MONTHLY' THEN SUM(returns.amount) * 12 "
                        "ELSE SUM(returns.amount) "
                        "END"
                    )
                }
            },
        }
    )
    query = SemanticQuery.model_validate(
        {
            "measures": ["annualised_amount"],
            "dimensions": ["returns.fund_id"],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="snowflake").sql

    assert "ANY_VALUE(t0.frequency) ILIKE 'MONTHLY'" in sql
    assert 'GROUP BY t0."fund_id"' in sql
    assert 'GROUP BY t0."fund_id", t0."frequency"' not in sql


def test_ilike_filter_compiles_to_portable_case_insensitive_like() -> None:
    model = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                name="orders",
                dimensions=[Dimension(name="status", expression="status", type="string")],
                measures=[Measure(name="amount", expression="amount", type="number", aggregation="sum")],
            )
        },
    )
    query = SemanticQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "filters": [{"member": "orders.status", "operator": "ilike", "values": ["Fulfilled%"]}],
        }
    )

    sql = SemanticQueryEngine().compile(query, model, dialect="snowflake").sql

    assert "LOWER(" in sql
    assert "LIKE LOWER('Fulfilled%')" in sql
