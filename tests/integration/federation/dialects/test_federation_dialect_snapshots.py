from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import pytest

from tests.helpers.federation_dialect_harness import FederationDialectHarness


@dataclass(slots=True, frozen=True)
class DialectSnapshotCase:
    name: str
    sql: str
    input_dialect: str
    source_by_table: Mapping[str, str]
    source_dialects: Mapping[str, str]


SNAPSHOT_CASES: tuple[DialectSnapshotCase, ...] = (
    DialectSnapshotCase(
        name="tsql_top_to_sqlite_full_query",
        sql="SELECT TOP 2 o.order_id, o.net_revenue FROM orders AS o ORDER BY o.order_id",
        input_dialect="tsql",
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "sqlite"},
    ),
    DialectSnapshotCase(
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
    ),
    DialectSnapshotCase(
        name="postgres_btrim_to_duckdb_local_compute",
        sql=(
            "SELECT o.order_id, btrim(o.customer_name) AS customer_name "
            "FROM orders AS o "
            "ORDER BY o.order_id"
        ),
        input_dialect="postgres",
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "sqlite"},
    ),
)


@pytest.mark.parametrize("case", SNAPSHOT_CASES, ids=[case.name for case in SNAPSHOT_CASES])
def test_federation_dialect_plan_contract_snapshots(
    tmp_path: Path,
    case: DialectSnapshotCase,
) -> None:
    harness = FederationDialectHarness(tmp_path)
    workflow = harness.workflow(source_by_table=case.source_by_table)

    output = harness.plan_sql(
        sql=case.sql,
        workflow=workflow,
        input_dialect=case.input_dialect,
        source_dialects=case.source_dialects,
    )

    actual = harness.plan_contract(
        output=output,
        input_dialect=case.input_dialect,
        source_dialects=case.source_dialects,
    )
    assert actual == _expected_contracts()[case.name]


def _expected_contracts() -> dict[str, object]:
    path = Path("tests/fixtures/expected/federation_plans/dialect_matrix_contracts.json")
    return json.loads(path.read_text(encoding="utf-8"))
