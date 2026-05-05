from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from langbridge.federation.connectors import SourceCapabilities
from langbridge.federation.models.plans import StageType
from tests.helpers.federation_dialect_harness import FederationDialectHarness


@dataclass(slots=True, frozen=True)
class PushdownMatrixCase:
    name: str
    sql: str
    source_by_table: Mapping[str, str]
    source_dialects: Mapping[str, str]
    expected_stage_types: Sequence[StageType]
    expected_pushdown_full_query: bool
    source_capabilities: Mapping[str, SourceCapabilities] = field(default_factory=dict)
    table_names: Mapping[str, str] = field(default_factory=dict)
    table_schemas: Mapping[str, str | None] = field(default_factory=dict)
    table_catalogs: Mapping[str, str | None] = field(default_factory=dict)
    table_metadata: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    required_remote_tokens: Sequence[str] = ()
    forbidden_remote_tokens: Sequence[str] = ()
    required_local_tokens: Sequence[str] = ()
    forbidden_local_tokens: Sequence[str] = ()
    expected_reason_fragments: Sequence[str] = ()
    expected_pushdown_by_alias: Mapping[str, Mapping[str, bool]] = field(default_factory=dict)


def _source_capabilities(**overrides: bool) -> SourceCapabilities:
    values = {
        "pushdown_full_query": False,
        "pushdown_filter": True,
        "pushdown_projection": True,
        "pushdown_aggregation": True,
        "pushdown_limit": True,
        "pushdown_join": False,
    }
    values.update(overrides)
    return SourceCapabilities(**values)


PUSHDOWN_MATRIX: tuple[PushdownMatrixCase, ...] = (
    PushdownMatrixCase(
        name="single_source_full_query_pushdown",
        sql=(
            "SELECT o.customer_id, SUM(o.net_revenue) AS total_revenue "
            "FROM orders AS o "
            "GROUP BY o.customer_id "
            "ORDER BY total_revenue DESC"
        ),
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "postgres"},
        source_capabilities={
            "src_orders": _source_capabilities(pushdown_full_query=True, pushdown_join=True)
        },
        expected_stage_types=(StageType.REMOTE_FULL_QUERY,),
        expected_pushdown_full_query=True,
        required_remote_tokens=("SUM", "GROUP BY", "ORDER BY"),
    ),
    PushdownMatrixCase(
        name="sql_full_query_ignores_scan_capabilities",
        sql=(
            "SELECT o.customer_id, SUM(o.net_revenue) AS total_revenue "
            "FROM orders AS o "
            "JOIN customers AS c ON o.customer_id = c.customer_id "
            "WHERE o.net_revenue > 100 "
            "GROUP BY o.customer_id "
            "ORDER BY total_revenue DESC "
            "LIMIT 5"
        ),
        source_by_table={"orders": "src_orders", "customers": "src_orders"},
        source_dialects={"src_orders": "postgres"},
        source_capabilities={
            "src_orders": _source_capabilities(
                pushdown_full_query=True,
                pushdown_filter=False,
                pushdown_projection=False,
                pushdown_aggregation=False,
                pushdown_limit=False,
                pushdown_join=False,
            )
        },
        expected_stage_types=(StageType.REMOTE_FULL_QUERY,),
        expected_pushdown_full_query=True,
        required_remote_tokens=("JOIN", "NET_REVENUE > 100", "GROUP BY", "LIMIT 5"),
    ),
    PushdownMatrixCase(
        name="scan_source_missing_filter_capability_keeps_filter_local",
        sql=(
            "SELECT o.order_id, o.net_revenue "
            "FROM orders AS o "
            "WHERE o.net_revenue > 100"
        ),
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "postgres"},
        source_capabilities={
            "src_orders": _source_capabilities(pushdown_filter=False)
        },
        expected_stage_types=(StageType.REMOTE_SCAN, StageType.LOCAL_COMPUTE),
        expected_pushdown_full_query=False,
        forbidden_remote_tokens=("NET_REVENUE > 100",),
        required_local_tokens=("NET_REVENUE > 100",),
        expected_reason_fragments=("full-query SQL pushdown is unavailable",),
        expected_pushdown_by_alias={"o": {"filter": False, "projection": True}},
    ),
    PushdownMatrixCase(
        name="scan_source_missing_projection_capability_scans_full_rows",
        sql="SELECT o.order_id FROM orders AS o",
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "postgres"},
        source_capabilities={
            "src_orders": _source_capabilities(pushdown_projection=False)
        },
        expected_stage_types=(StageType.REMOTE_SCAN, StageType.LOCAL_COMPUTE),
        expected_pushdown_full_query=False,
        required_remote_tokens=("SELECT *",),
        expected_reason_fragments=("full-query SQL pushdown is unavailable",),
        expected_pushdown_by_alias={"o": {"projection": False}},
    ),
    PushdownMatrixCase(
        name="scan_source_missing_limit_capability_keeps_limit_local",
        sql="SELECT o.order_id FROM orders AS o LIMIT 2",
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "postgres"},
        source_capabilities={
            "src_orders": _source_capabilities(pushdown_limit=False)
        },
        expected_stage_types=(StageType.REMOTE_SCAN, StageType.LOCAL_COMPUTE),
        expected_pushdown_full_query=False,
        forbidden_remote_tokens=("LIMIT 2",),
        required_local_tokens=("LIMIT 2",),
        expected_reason_fragments=("full-query SQL pushdown is unavailable",),
        expected_pushdown_by_alias={"o": {"limit": False}},
    ),
    PushdownMatrixCase(
        name="scan_source_missing_aggregation_capability_keeps_aggregate_local",
        sql=(
            "SELECT o.customer_id, SUM(o.net_revenue) AS total_revenue "
            "FROM orders AS o "
            "GROUP BY o.customer_id"
        ),
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "postgres"},
        source_capabilities={
            "src_orders": _source_capabilities(pushdown_aggregation=False)
        },
        expected_stage_types=(StageType.REMOTE_SCAN, StageType.LOCAL_COMPUTE),
        expected_pushdown_full_query=False,
        required_local_tokens=("SUM", "GROUP BY"),
        expected_reason_fragments=("full-query SQL pushdown is unavailable",),
        expected_pushdown_by_alias={"o": {"aggregation": False}},
    ),
    PushdownMatrixCase(
        name="cross_dialect_unknown_function_keeps_compute_local",
        sql=(
            "SELECT o.order_id, btrim(o.customer_name) AS customer_name "
            "FROM orders AS o "
            "ORDER BY o.order_id"
        ),
        source_by_table={"orders": "src_orders"},
        source_dialects={"src_orders": "sqlite"},
        source_capabilities={
            "src_orders": _source_capabilities(pushdown_full_query=True, pushdown_join=True)
        },
        expected_stage_types=(StageType.REMOTE_SCAN, StageType.LOCAL_COMPUTE),
        expected_pushdown_full_query=False,
        forbidden_remote_tokens=("BTRIM",),
        required_local_tokens=("TRIM",),
        forbidden_local_tokens=("BTRIM",),
        expected_reason_fragments=("unrecognized function", "BTRIM"),
    ),
    PushdownMatrixCase(
        name="physical_table_rewrite_removes_logical_table_name",
        sql=(
            "SELECT shopify_orders.order_id "
            "FROM shopify_orders "
            "WHERE shopify_orders.order_id > 10"
        ),
        source_by_table={"shopify_orders": "src_orders"},
        source_dialects={"src_orders": "postgres"},
        source_capabilities={
            "src_orders": _source_capabilities(pushdown_full_query=True, pushdown_join=True)
        },
        table_names={"shopify_orders": "orders"},
        expected_stage_types=(StageType.REMOTE_FULL_QUERY,),
        expected_pushdown_full_query=True,
        required_remote_tokens=("FROM orders",),
        forbidden_remote_tokens=("FROM SHOPIFY_ORDERS",),
    ),
    PushdownMatrixCase(
        name="catalog_schema_rewrite_targets_physical_relation",
        sql='SELECT o.order_id FROM "org_abc"."semantic"."shopify_orders" AS o',
        source_by_table={"shopify_orders": "src_orders"},
        source_dialects={"src_orders": "postgres"},
        source_capabilities={
            "src_orders": _source_capabilities(pushdown_full_query=True, pushdown_join=True)
        },
        table_names={"shopify_orders": "shopify_orders"},
        table_schemas={"shopify_orders": "semantic"},
        table_catalogs={"shopify_orders": "org_abc"},
        table_metadata={
            "shopify_orders": {
                "physical_schema": "public",
                "physical_table": "orders",
                "skip_catalog_in_pushdown": True,
            }
        },
        expected_stage_types=(StageType.REMOTE_FULL_QUERY,),
        expected_pushdown_full_query=True,
        required_remote_tokens=("FROM public.orders",),
        forbidden_remote_tokens=("ORG_ABC", "SEMANTIC"),
    ),
    PushdownMatrixCase(
        name="physical_sql_scan_preserves_safe_filter",
        sql=(
            "SELECT aligned_returns.product_id "
            "FROM aligned_returns "
            "WHERE aligned_returns.product_id = 'P-1'"
        ),
        source_by_table={"aligned_returns": "src_products"},
        source_dialects={"src_products": "snowflake"},
        source_capabilities={
            "src_products": _source_capabilities(pushdown_full_query=True, pushdown_join=True)
        },
        table_metadata={
            "aligned_returns": {
                "physical_sql": (
                    "SELECT PRODUCT_ID AS product_id, "
                    "PRODUCT_NAME AS product_name "
                    "FROM DIM_PRODUCT"
                )
            }
        },
        expected_stage_types=(StageType.REMOTE_SCAN, StageType.LOCAL_COMPUTE),
        expected_pushdown_full_query=False,
        required_remote_tokens=("FROM (SELECT PRODUCT_ID AS product_id", "PRODUCT_ID = 'P-1'"),
        expected_reason_fragments=("physical SQL bindings",),
        expected_pushdown_by_alias={"aligned_returns": {"filter": True, "projection": True}},
    ),
)


@pytest.mark.parametrize("case", PUSHDOWN_MATRIX, ids=[case.name for case in PUSHDOWN_MATRIX])
def test_federation_pushdown_matrix(tmp_path: Path, case: PushdownMatrixCase) -> None:
    harness = FederationDialectHarness(tmp_path)
    workflow = harness.workflow(
        source_by_table=case.source_by_table,
        table_names=case.table_names,
        table_schemas=case.table_schemas,
        table_catalogs=case.table_catalogs,
        table_metadata=case.table_metadata,
    )

    output = harness.plan_sql(
        sql=case.sql,
        workflow=workflow,
        input_dialect="postgres",
        source_dialects=case.source_dialects,
        source_capabilities=case.source_capabilities,
    )

    harness.assert_stage_types(
        stages=output.physical_plan.stages,
        expected=case.expected_stage_types,
    )
    harness.assert_stage_sql_parses(
        stages=output.physical_plan.stages,
        source_dialects=case.source_dialects,
    )
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
    assert output.physical_plan.pushdown_full_query is case.expected_pushdown_full_query
    combined_reasons = "\n".join(output.physical_plan.pushdown_reasons)
    for reason in case.expected_reason_fragments:
        assert reason.lower() in combined_reasons.lower()
    _assert_pushdown_decisions(output.physical_plan.stages, case.expected_pushdown_by_alias)


def _assert_pushdown_decisions(
    stages,
    expected_by_alias: Mapping[str, Mapping[str, bool]],
) -> None:
    subplans = {
        stage.subplan.alias: stage.subplan
        for stage in stages
        if stage.stage_type == StageType.REMOTE_SCAN and stage.subplan is not None
    }
    for alias, expected_decisions in expected_by_alias.items():
        subplan = subplans[alias]
        for decision_name, expected in expected_decisions.items():
            decision = getattr(subplan.pushdown, decision_name)
            assert decision.pushed is expected
