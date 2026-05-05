from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from langbridge.federation.connectors import SourceCapabilities
from langbridge.federation.models.plans import StageType
from tests.helpers.federation_dialect_harness import FederationDialectHarness, SqliteTableFixture


@dataclass(slots=True, frozen=True)
class DialectPlanCase:
    name: str
    sql: str
    input_dialect: str
    source_by_table: Mapping[str, str]
    source_dialects: Mapping[str, str]
    expected_stage_types: Sequence[StageType]
    expect_full_query_pushdown: bool | None = None
    table_names: Mapping[str, str] = field(default_factory=dict)
    required_remote_tokens: Sequence[str] = ()
    forbidden_remote_tokens: Sequence[str] = ()
    required_local_tokens: Sequence[str] = ()
    forbidden_local_tokens: Sequence[str] = ()


@dataclass(slots=True, frozen=True)
class DialectExecutionCase:
    name: str
    sql: str
    input_dialect: str
    source_by_table: Mapping[str, str]
    sqlite_tables_by_source: Mapping[str, Sequence[SqliteTableFixture]]
    expected_rows: Sequence[Mapping[str, Any]]
    table_names: Mapping[str, str] = field(default_factory=dict)


def _orders_fixture() -> SqliteTableFixture:
    return SqliteTableFixture(
        name="orders",
        ddl=(
            "CREATE TABLE orders ("
            "order_id INTEGER PRIMARY KEY, "
            "customer_id INTEGER NOT NULL, "
            "order_date TEXT NOT NULL, "
            "net_revenue REAL NOT NULL"
            ")"
        ),
        columns=("order_id", "customer_id", "order_date", "net_revenue"),
        rows=(
            (1, 10, "2025-01-02", 120.0),
            (2, 11, "2025-01-03", 80.0),
            (3, 10, "2025-02-04", 250.0),
        ),
    )


def _customers_fixture() -> SqliteTableFixture:
    return SqliteTableFixture(
        name="customers",
        ddl=(
            "CREATE TABLE customers ("
            "customer_id INTEGER PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "region TEXT NOT NULL"
            ")"
        ),
        columns=("customer_id", "name", "region"),
        rows=(
            (10, "Acme Corp", "EMEA"),
            (11, "Globex", "AMER"),
        ),
    )


PLAN_MATRIX: tuple[DialectPlanCase, ...] = (
    DialectPlanCase(
        name="tsql_top_to_sqlite_full_query",
        sql="SELECT TOP 2 o.order_id, o.net_revenue FROM orders AS o ORDER BY o.order_id",
        input_dialect="tsql",
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "sqlite"},
        expected_stage_types=(StageType.REMOTE_FULL_QUERY,),
        expect_full_query_pushdown=True,
        required_remote_tokens=("LIMIT 2",),
        forbidden_remote_tokens=("TOP 2",),
    ),
    DialectPlanCase(
        name="postgres_limit_to_mysql_full_query",
        sql=(
            "SELECT o.order_id, o.net_revenue "
            "FROM orders AS o "
            "WHERE o.net_revenue >= 80 "
            "ORDER BY o.order_id "
            "LIMIT 2"
        ),
        input_dialect="postgres",
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "mysql"},
        expected_stage_types=(StageType.REMOTE_FULL_QUERY,),
        expect_full_query_pushdown=True,
        required_remote_tokens=("LIMIT 2",),
        forbidden_remote_tokens=("TOP ",),
    ),
    DialectPlanCase(
        name="postgres_btrim_to_duckdb_local_compute",
        sql=(
            "SELECT o.order_id, btrim(c.name) AS customer_name "
            "FROM orders AS o "
            "JOIN customers AS c ON o.customer_id = c.customer_id "
            "WHERE o.net_revenue > 100 "
            "ORDER BY o.order_id"
        ),
        input_dialect="postgres",
        source_by_table={
            "orders": "src_orders",
            "customers": "src_customers",
        },
        source_dialects={
            "src_orders": "sqlite",
            "src_customers": "postgres",
        },
        expected_stage_types=(
            StageType.REMOTE_SCAN,
            StageType.REMOTE_SCAN,
            StageType.LOCAL_COMPUTE,
        ),
        expect_full_query_pushdown=False,
        required_local_tokens=("TRIM",),
        forbidden_local_tokens=("BTRIM",),
    ),
    DialectPlanCase(
        name="quoted_identifiers_to_snowflake_full_query",
        sql=(
            'SELECT "o"."order_id" AS "Order Id" '
            'FROM "orders" AS "o" '
            'WHERE "o"."net_revenue" > 100 '
            'ORDER BY "o"."order_id"'
        ),
        input_dialect="postgres",
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "snowflake"},
        expected_stage_types=(StageType.REMOTE_FULL_QUERY,),
        expect_full_query_pushdown=True,
    ),
    DialectPlanCase(
        name="postgres_extract_to_bigquery_full_query",
        sql=(
            "SELECT EXTRACT(YEAR FROM o.order_date) AS order_year, COUNT(*) AS order_count "
            "FROM orders AS o "
            "GROUP BY order_year "
            "ORDER BY order_year"
        ),
        input_dialect="postgres",
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "bigquery"},
        expected_stage_types=(StageType.REMOTE_FULL_QUERY,),
        expect_full_query_pushdown=True,
        required_remote_tokens=("EXTRACT",),
    ),
    DialectPlanCase(
        name="snowflake_bigquery_remote_scans_local_join",
        sql=(
            "SELECT o.order_id, c.region "
            "FROM orders AS o "
            "JOIN customers AS c ON o.customer_id = c.customer_id "
            "WHERE o.order_date >= '2025-01-01' "
            "ORDER BY o.order_id"
        ),
        input_dialect="postgres",
        source_by_table={
            "orders": "src_orders",
            "customers": "src_customers",
        },
        source_dialects={
            "src_orders": "snowflake",
            "src_customers": "bigquery",
        },
        expected_stage_types=(
            StageType.REMOTE_SCAN,
            StageType.REMOTE_SCAN,
            StageType.LOCAL_COMPUTE,
        ),
        expect_full_query_pushdown=False,
        forbidden_remote_tokens=("TOP ",),
    ),
)


EXECUTION_MATRIX: tuple[DialectExecutionCase, ...] = (
    DialectExecutionCase(
        name="tsql_top_to_sqlite_executes",
        sql="SELECT TOP 2 o.order_id, o.net_revenue FROM orders AS o ORDER BY o.order_id",
        input_dialect="tsql",
        source_by_table={"orders": "src_orders"},
        sqlite_tables_by_source={"src_orders": (_orders_fixture(),)},
        expected_rows=(
            {"order_id": 1, "net_revenue": 120.0},
            {"order_id": 2, "net_revenue": 80.0},
        ),
    ),
    DialectExecutionCase(
        name="postgres_filter_projection_limit_sqlite_executes",
        sql=(
            "SELECT o.order_id, o.net_revenue "
            "FROM orders AS o "
            "WHERE o.net_revenue >= 80 "
            "ORDER BY o.order_id "
            "LIMIT 2"
        ),
        input_dialect="postgres",
        source_by_table={"orders": "src_orders"},
        sqlite_tables_by_source={"src_orders": (_orders_fixture(),)},
        expected_rows=(
            {"order_id": 1, "net_revenue": 120.0},
            {"order_id": 2, "net_revenue": 80.0},
        ),
    ),
    DialectExecutionCase(
        name="postgres_grouped_aggregate_sqlite_executes",
        sql=(
            "SELECT o.customer_id, SUM(o.net_revenue) AS total_revenue "
            "FROM orders AS o "
            "GROUP BY o.customer_id "
            "ORDER BY o.customer_id"
        ),
        input_dialect="postgres",
        source_by_table={"orders": "src_orders"},
        sqlite_tables_by_source={"src_orders": (_orders_fixture(),)},
        expected_rows=(
            {"customer_id": 10, "total_revenue": 370.0},
            {"customer_id": 11, "total_revenue": 80.0},
        ),
    ),
    DialectExecutionCase(
        name="postgres_cross_source_join_sqlite_executes",
        sql=(
            "SELECT o.order_id, c.name "
            "FROM orders AS o "
            "JOIN customers AS c ON o.customer_id = c.customer_id "
            "WHERE o.net_revenue > 100 "
            "ORDER BY o.order_id"
        ),
        input_dialect="postgres",
        source_by_table={
            "orders": "src_orders",
            "customers": "src_customers",
        },
        sqlite_tables_by_source={
            "src_orders": (_orders_fixture(),),
            "src_customers": (_customers_fixture(),),
        },
        expected_rows=(
            {"order_id": 1, "name": "Acme Corp"},
            {"order_id": 3, "name": "Acme Corp"},
        ),
    ),
    DialectExecutionCase(
        name="physical_table_rewrite_sqlite_executes",
        sql=(
            "SELECT shopify_orders.order_id, shopify_orders.net_revenue "
            "FROM shopify_orders "
            "WHERE shopify_orders.net_revenue >= 100 "
            "ORDER BY shopify_orders.order_id"
        ),
        input_dialect="postgres",
        source_by_table={"shopify_orders": "src_orders"},
        table_names={"shopify_orders": "orders"},
        sqlite_tables_by_source={"src_orders": (_orders_fixture(),)},
        expected_rows=(
            {"order_id": 1, "net_revenue": 120.0},
            {"order_id": 3, "net_revenue": 250.0},
        ),
    ),
)


def _sql_capabilities_for_sources(source_ids: Mapping[str, str]) -> dict[str, SourceCapabilities]:
    return {
        source_id: SourceCapabilities(pushdown_full_query=True, pushdown_join=True)
        for source_id in set(source_ids.values())
    }


@pytest.mark.parametrize("case", PLAN_MATRIX, ids=[case.name for case in PLAN_MATRIX])
def test_federation_dialect_plan_matrix(tmp_path: Path, case: DialectPlanCase) -> None:
    harness = FederationDialectHarness(tmp_path)
    workflow = harness.workflow(
        source_by_table=case.source_by_table,
        table_names=case.table_names,
    )

    output = harness.plan_sql(
        sql=case.sql,
        workflow=workflow,
        input_dialect=case.input_dialect,
        source_dialects=case.source_dialects,
        source_capabilities=_sql_capabilities_for_sources(case.source_by_table),
    )

    harness.assert_stage_types(
        stages=output.physical_plan.stages,
        expected=case.expected_stage_types,
    )
    harness.assert_stage_sql_parses(
        stages=output.physical_plan.stages,
        source_dialects=case.source_dialects,
    )
    if case.expect_full_query_pushdown is not None:
        assert output.physical_plan.pushdown_full_query is case.expect_full_query_pushdown

    harness.assert_sql_fragments(
        sql_fragments=tuple(harness.remote_sql_by_stage(output.physical_plan.stages).values()),
        required=case.required_remote_tokens,
        forbidden=case.forbidden_remote_tokens,
    )
    harness.assert_sql_fragments(
        sql_fragments=tuple(harness.local_sql_by_stage(output.physical_plan.stages).values()),
        required=case.required_local_tokens,
        forbidden=case.forbidden_local_tokens,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("case", EXECUTION_MATRIX, ids=[case.name for case in EXECUTION_MATRIX])
async def test_federation_dialect_execution_matrix(
    tmp_path: Path,
    case: DialectExecutionCase,
) -> None:
    harness = FederationDialectHarness(tmp_path)
    sources = {
        source_id: harness.sqlite_source(
            source_id=source_id,
            database_name=f"{case.name}_{source_id}.db",
            tables=tables,
        )
        for source_id, tables in case.sqlite_tables_by_source.items()
    }
    workflow = harness.workflow(
        source_by_table=case.source_by_table,
        table_names=case.table_names,
    )

    rows = await harness.execute_sql(
        sql=case.sql,
        workflow=workflow,
        input_dialect=case.input_dialect,
        sources=sources,
    )

    assert rows == list(case.expected_rows)
