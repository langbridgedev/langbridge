
import pytest

from langbridge.semantic.loader import load_unified_semantic_model

from tests.helpers.semantic_harness import SemanticHarness


def test_commerce_model_fixture_loads_core_semantic_contract() -> None:
    harness = SemanticHarness()
    model = harness.load_semantic_model_fixture("commerce")

    assert sorted(model.datasets) == ["customers", "orders"]
    assert model.relationships is not None
    assert [relationship.name for relationship in model.relationships] == ["orders_to_customers"]
    assert model.metrics is not None
    assert sorted(model.metrics) == [
        "cancelled_revenue_per_customer",
        "completed_net_revenue",
        "open_pipeline_net_revenue",
        "revenue_delta",
        "revenue_per_customer",
    ]
    assert model.datasets["orders"].filters is not None
    assert "completed_only" in model.datasets["orders"].filters
    assert [measure.name for measure in model.datasets["orders"].measures or []] == [
        "revenue",
        "order_count",
        "net_revenue",
    ]


def test_vector_dimension_block_is_available_on_fixture_model() -> None:
    harness = SemanticHarness()
    model = harness.load_semantic_model_fixture("commerce")
    country = next(
        dimension
        for dimension in (model.datasets["orders"].dimensions or [])
        if dimension.name == "country"
    )

    assert country.vector is not None
    assert country.vector.enabled is True
    assert country.vector.refresh_interval == "1d"
    assert country.vector.store.type == "managed_faiss"


def test_unified_semantic_fixture_loads_and_materializes_joinable_runtime_model() -> None:
    harness = SemanticHarness()
    raw_unified = load_unified_semantic_model(
        harness.read_text("semantic_models", "commerce_marketing_unified.yml")
    )
    merged_model = harness.load_unified_model_fixture("commerce_marketing_unified")

    assert [source.alias for source in raw_unified.source_models] == ["Commerce", "Marketing"]
    assert sorted(merged_model.datasets) == [
        "Commerce__customers",
        "Commerce__orders",
        "Marketing__campaigns",
    ]
    assert merged_model.relationships is not None
    assert [relationship.name for relationship in merged_model.relationships] == [
        "Commerce__orders_to_customers",
        "commerce_to_marketing",
    ]
    assert merged_model.metrics is not None
    assert merged_model.metrics["revenue_minus_spend"].expression == (
        "Commerce__orders.revenue - Marketing__campaigns.spend"
    )


@pytest.mark.parametrize(
    "query_name",
    [
        "simple_dimension_select",
        "grouped_revenue_by_region",
        "filtered_revenue_by_region",
        "net_revenue_by_month",
        "top_regions_by_order_count",
        "net_revenue_by_country",
        "completed_net_revenue_by_region",
        "high_completed_net_revenue_regions",
        "revenue_per_customer_by_region",
        "open_pipeline_net_revenue_by_region",
        "cancelled_revenue_per_customer_by_region",
    ],
)
def test_semantic_translation_matches_postgres_goldens(query_name: str) -> None:
    harness = SemanticHarness()
    harness.assert_sql_fixture(
        model_name="commerce",
        query_name=query_name,
        dialect="postgres",
    )


def test_unified_semantic_translation_matches_postgres_golden() -> None:
    harness = SemanticHarness()
    harness.assert_sql_fixture(
        model_name="commerce_marketing_unified",
        query_name="unified_revenue_by_campaign",
        dialect="postgres",
        unified=True,
    )


@pytest.mark.parametrize(
    "query_name",
    [
        "grouped_revenue_by_region",
        "net_revenue_by_month",
        "revenue_per_customer_by_region",
    ],
)
@pytest.mark.parametrize("dialect", ["postgres", "snowflake", "bigquery", "duckdb"])
def test_semantic_translation_has_stable_cross_dialect_goldens(
    query_name: str,
    dialect: str,
) -> None:
    harness = SemanticHarness()
    harness.assert_sql_fixture(
        model_name="commerce",
        query_name=query_name,
        dialect=dialect,
    )
