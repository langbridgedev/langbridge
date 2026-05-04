from __future__ import annotations

import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from langbridge.federation.connectors import RemoteExecutionResult, RemoteSource, SourceCapabilities
from langbridge.federation.executor import ArtifactStore
from langbridge.federation.models.plans import SourceSubplan
from langbridge.federation.models.virtual_dataset import TableStatistics, VirtualTableBinding
from langbridge.federation.service import FederatedQueryService
from langbridge.federation.utils.sql import enforce_read_only_sql, sanitize_sql_error_message
from tests.helpers.federation_dialect_harness import FederationDialectHarness


@dataclass(slots=True, frozen=True)
class PlanningFailureCase:
    name: str
    sql: str
    expected_message: str


class StaticArrowSource(RemoteSource):
    def __init__(
        self,
        *,
        source_id: str,
        table: pa.Table | None = None,
        error: Exception | None = None,
    ) -> None:
        self.source_id = source_id
        self._table = table or pa.table({})
        self._error = error

    def capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(pushdown_join=False)

    def dialect(self) -> str:
        return "duckdb"

    async def execute(self, subplan: SourceSubplan) -> RemoteExecutionResult:
        _ = subplan
        if self._error is not None:
            raise self._error
        return RemoteExecutionResult(table=self._table, elapsed_ms=1)

    async def estimate_table_stats(self, binding: VirtualTableBinding) -> TableStatistics:
        return binding.stats or TableStatistics(row_count_estimate=float(self._table.num_rows), bytes_per_row=64)


PLANNING_FAILURE_MATRIX: tuple[PlanningFailureCase, ...] = (
    PlanningFailureCase(
        name="invalid_sql",
        sql="SELECT * FROM",
        expected_message="Expected table name",
    ),
    PlanningFailureCase(
        name="dml_is_not_federation_select",
        sql="DELETE FROM orders WHERE order_id = 1",
        expected_message="Only SELECT/CTE queries are supported",
    ),
    PlanningFailureCase(
        name="unknown_table",
        sql="SELECT * FROM missing_orders",
        expected_message="not mapped in virtual dataset",
    ),
)


@pytest.mark.parametrize(
    "case",
    PLANNING_FAILURE_MATRIX,
    ids=[case.name for case in PLANNING_FAILURE_MATRIX],
)
def test_federation_planning_failure_matrix(tmp_path: Path, case: PlanningFailureCase) -> None:
    harness = FederationDialectHarness(tmp_path)
    workflow = harness.workflow(source_by_table={"orders": "src_orders"})

    with pytest.raises(ValueError) as exc_info:
        harness.plan_sql(
            sql=case.sql,
            workflow=workflow,
            input_dialect="postgres",
            source_dialects={"src_orders": "postgres"},
        )

    assert case.expected_message.lower() in str(exc_info.value).lower()


def test_read_only_sql_policy_blocks_ddl_and_dml() -> None:
    for sql in ("DROP TABLE orders", "UPDATE orders SET net_revenue = 0"):
        with pytest.raises(ValueError, match="only allows SELECT"):
            enforce_read_only_sql(sql, allow_dml=False, dialect="postgres")


@pytest.mark.anyio
async def test_federation_remote_execution_failure_surfaces_source_error(tmp_path: Path) -> None:
    harness = FederationDialectHarness(tmp_path)
    workflow = harness.workflow(source_by_table={"orders": "src_orders"})

    with pytest.raises(RuntimeError) as exc_info:
        await _execute_with_sources(
            sql="SELECT o.order_id FROM orders AS o",
            workflow=workflow,
            sources={
                "src_orders": StaticArrowSource(
                    source_id="src_orders",
                    error=RuntimeError("remote warehouse rejected SQL"),
                )
            },
        )

    assert "remote warehouse rejected sql" in str(exc_info.value).lower()


@pytest.mark.anyio
async def test_federation_local_compute_failure_surfaces_missing_column(tmp_path: Path) -> None:
    harness = FederationDialectHarness(tmp_path)
    workflow = harness.workflow(
        source_by_table={
            "orders": "src_orders",
            "customers": "src_customers",
        }
    )

    with pytest.raises(Exception) as exc_info:
        await _execute_with_sources(
            sql=(
                "SELECT o.order_id, c.name "
                "FROM orders AS o "
                "JOIN customers AS c ON o.customer_id = c.customer_id"
            ),
            workflow=workflow,
            sources={
                "src_orders": StaticArrowSource(
                    source_id="src_orders",
                    table=pa.table({"order_id": [1], "customer_id": [10]}),
                ),
                "src_customers": StaticArrowSource(
                    source_id="src_customers",
                    table=pa.table({"customer_id": [10]}),
                ),
            },
        )

    assert "name" in str(exc_info.value).lower()


def test_sql_error_sanitizer_redacts_secret_like_values() -> None:
    message = sanitize_sql_error_message(
        "connection failed password=hunter2 token=abc123 secret=my-secret query failed"
    )

    assert "hunter2" not in message
    assert "abc123" not in message
    assert "my-secret" not in message
    assert "password=***" in message
    assert "token=***" in message
    assert "secret=***" in message


async def _execute_with_sources(
    *,
    sql: str,
    workflow,
    sources: Mapping[str, RemoteSource],
) -> None:
    with tempfile.TemporaryDirectory() as artifact_dir:
        service = FederatedQueryService(artifact_store=ArtifactStore(base_dir=artifact_dir))
        try:
            await service.execute(
                query=sql,
                dialect="postgres",
                workspace_id=workflow.workspace_id,
                workflow=workflow,
                sources=dict(sources),
            )
        finally:
            await service.aclose()
