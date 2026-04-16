
from langbridge.federation.models import TableStatistics

from tests.helpers.federation_harness import FederationHarness


def test_single_dataset_sql_plan_matches_select_regions_golden() -> None:
    harness = FederationHarness()
    model = harness.semantic.load_semantic_model_fixture("commerce")
    workflow = harness.build_workflow_for_model(
        model=model,
        source_by_dataset={"orders": "src_commerce", "customers": "src_commerce"},
        stats_by_dataset={
            "orders": TableStatistics(row_count_estimate=4, bytes_per_row=64),
            "customers": TableStatistics(row_count_estimate=3, bytes_per_row=64),
        },
        workspace_id="ws-commerce",
        workflow_id="wf-commerce",
    )
    sql = harness.semantic.read_text("queries", "sql", "single_dataset_select_regions.sql")

    output = harness.plan_sql(
        sql=sql,
        workflow=workflow,
        dialect="postgres",
        source_dialects={"src_commerce": "postgres"},
    )

    actual = harness.normalize_planning_output(
        output=output,
        input_dialect="postgres",
        source_dialects={"src_commerce": "postgres"},
    )
    assert actual == harness.expected_plan("sql_single_dataset_select_regions")


def test_two_dataset_sql_plan_matches_select_revenue_by_region_golden() -> None:
    harness = FederationHarness()
    model = harness.semantic.load_semantic_model_fixture("commerce")
    workflow = harness.build_workflow_for_model(
        model=model,
        source_by_dataset={"orders": "src_commerce", "customers": "src_commerce"},
        stats_by_dataset={
            "orders": TableStatistics(row_count_estimate=4, bytes_per_row=64),
            "customers": TableStatistics(row_count_estimate=3, bytes_per_row=64),
        },
        workspace_id="ws-commerce",
        workflow_id="wf-commerce",
    )
    sql = harness.semantic.read_text("queries", "sql", "two_dataset_select_revenue_by_region.sql")

    output = harness.plan_sql(
        sql=sql,
        workflow=workflow,
        dialect="postgres",
        source_dialects={"src_commerce": "postgres"},
    )

    actual = harness.normalize_planning_output(
        output=output,
        input_dialect="postgres",
        source_dialects={"src_commerce": "postgres"},
    )
    assert actual == harness.expected_plan("sql_two_dataset_select_revenue_by_region")
    
def test_two_dataset_union_sql_plan_matches_union_two_datasets_golden() -> None:
    harness = FederationHarness()
    model = harness.semantic.load_semantic_model_fixture("commerce")
    workflow = harness.build_workflow_for_model(
        model=model,
        source_by_dataset={"orders": "src_commerce", "customers": "src_commerce"},
        stats_by_dataset={
            "orders": TableStatistics(row_count_estimate=4, bytes_per_row=64),
            "customers": TableStatistics(row_count_estimate=3, bytes_per_row=64),
        },
        workspace_id="ws-commerce",
        workflow_id="wf-commerce",
    )
    sql = harness.semantic.read_text("queries", "sql", "union_two_datasets.sql")

    output = harness.plan_sql(
        sql=sql,
        workflow=workflow,
        dialect="postgres",
        source_dialects={"src_commerce": "postgres"},
    )

    actual = harness.normalize_planning_output(
        output=output,
        input_dialect="postgres",
        source_dialects={"src_commerce": "postgres"},
    )
    assert actual == harness.expected_plan("sql_union_two_datasets")
    
def test_two_dataset_union_sql_plan_matches_two_datasets_semantic_graph_golden() -> None:
    harness = FederationHarness()
    model = harness.semantic.load_semantic_graph_fixture("commerce_marketing_graph")
    workflow = harness.build_workflow_for_model(
        model=model,
        source_by_dataset={
            "Commerce__orders": "src_commerce",
            "Commerce__customers": "src_commerce",
            "Marketing__campaigns": "src_marketing",
        },
        stats_by_dataset={
            "Commerce__orders": TableStatistics(row_count_estimate=4, bytes_per_row=64),
            "Commerce__customers": TableStatistics(row_count_estimate=3, bytes_per_row=64),
            "Marketing__campaigns": TableStatistics(row_count_estimate=3, bytes_per_row=48),
        },
        workspace_id="ws-unified",
        workflow_id="wf-unified",
    )
    
    sql = harness.semantic.read_text("queries", "sql", "two_datasets_join_unified.sql")
    
    output = harness.plan_sql(
        sql=sql,
        workflow=workflow,
        dialect="postgres",
        source_dialects={
            "src_commerce": "postgres",
            "src_marketing": "postgres",
        },
    )
    
    actual = harness.normalize_planning_output(
        output=output,
        input_dialect="postgres",
        source_dialects={
            "src_commerce": "postgres",
            "src_marketing": "postgres",
        },
    )
    
    import json
    print(json.dumps(actual, indent=2))
    assert actual == harness.expected_plan("sql_two_datasets_join_unified")
