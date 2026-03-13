import uuid

from langbridge.packages.semantic.langbridge_semantic.model import (
    Dimension,
    Measure,
    Relationship,
    SemanticModel,
    Table,
)
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery, SemanticQueryEngine
from langbridge.packages.semantic.langbridge_semantic.unified_query import (
    TenantAwareQueryContext,
    UnifiedSourceModel,
    apply_tenant_aware_context,
    build_unified_semantic_model,
)


def test_build_unified_semantic_model_resolves_graph_relationships_and_metric_references() -> None:
    sales_model_id = uuid.uuid4()
    marketing_model_id = uuid.uuid4()
    connector_a = uuid.uuid4()
    connector_b = uuid.uuid4()

    sales_model = SemanticModel(
        version="1.0",
        name="Sales",
        tables={
            "orders": Table(
                schema="public",
                name="orders",
                dimensions=[
                    Dimension(name="customer_id", type="integer"),
                    Dimension(name="order_id", type="integer", primary_key=True),
                ],
                measures=[Measure(name="revenue", type="number", aggregation="sum")],
            )
        },
    )
    marketing_model = SemanticModel(
        version="1.0",
        name="Marketing",
        tables={
            "campaigns": Table(
                schema="public",
                name="campaigns",
                dimensions=[Dimension(name="customer_id", type="integer")],
                measures=[Measure(name="spend", type="number", aggregation="sum")],
            )
        },
    )

    unified_model, table_connector_map = build_unified_semantic_model(
        source_models=[
            UnifiedSourceModel(model_id=sales_model_id, model=sales_model, connector_id=connector_a, key="Sales"),
            UnifiedSourceModel(model_id=marketing_model_id, model=marketing_model, connector_id=connector_b, key="Marketing"),
        ],
        relationships=[
            {
                "name": "sales_to_marketing",
                "source_semantic_model_id": str(sales_model_id),
                "source_field": "customer_id",
                "target_semantic_model_id": str(marketing_model_id),
                "target_field": "customer_id",
                "relationship_type": "left",
            }
        ],
        metrics={
            "marketing_roi": {
                "expression": "Sales.revenue / Marketing.spend",
                "description": "Cross-domain ROI",
            }
        },
    )

    assert sorted(unified_model.tables.keys()) == ["Marketing__campaigns", "Sales__orders"]
    assert unified_model.relationships == [
        Relationship(
            name="sales_to_marketing",
            source_dataset="Sales__orders",
            source_field="customer_id",
            target_dataset="Marketing__campaigns",
            target_field="customer_id",
            operator="=",
            type="left",
        )
    ]
    assert unified_model.metrics is not None
    assert unified_model.metrics["marketing_roi"].expression == "Sales__orders.revenue / Marketing__campaigns.spend"
    assert table_connector_map["Sales__orders"] == connector_a
    assert table_connector_map["Marketing__campaigns"] == connector_b


def test_apply_tenant_aware_context_sets_catalog_from_org_and_connector_tokens() -> None:
    organization_id = uuid.uuid4()
    execution_connector_id = uuid.uuid4()
    orders_connector_id = uuid.uuid4()

    base_model = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                schema="public",
                name="orders",
                dimensions=[Dimension(name="id", type="integer", primary_key=True)],
            ),
            "legacy_sales": Table(
                schema="legacy.sales",
                name="fact_sales",
                dimensions=[Dimension(name="id", type="integer", primary_key=True)],
            ),
        },
    )

    tenant_model = apply_tenant_aware_context(
        base_model,
        context=TenantAwareQueryContext(
            organization_id=organization_id,
            execution_connector_id=execution_connector_id,
        ),
        table_connector_map={"orders": orders_connector_id},
    )

    expected_catalog = f"org_{organization_id.hex[:12]}__src_{orders_connector_id.hex[:12]}"
    assert tenant_model.tables["orders"].catalog == expected_catalog
    assert tenant_model.tables["orders"].schema == "public"
    assert tenant_model.tables["legacy_sales"].catalog == "legacy"
    assert tenant_model.tables["legacy_sales"].schema == "sales"


def test_catalog_qualified_translation_uses_catalog_schema_table_when_available() -> None:
    model = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                catalog="tenant_catalog",
                schema="analytics",
                name="orders",
                dimensions=[Dimension(name="id", type="integer", primary_key=True)],
            )
        },
    )
    query = SemanticQuery(dimensions=["orders.id"], limit=10)

    plan = SemanticQueryEngine().compile(query, model, dialect="postgres")
    assert '"tenant_catalog"."analytics"."orders"' in plan.sql


def test_joined_dimensions_are_qualified_to_avoid_ambiguous_column_names() -> None:
    model = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                schema="public",
                name="orders",
                dimensions=[
                    Dimension(name="id", type="integer", primary_key=True),
                    Dimension(name="customer_id", type="integer"),
                ],
            ),
            "customers": Table(
                schema="public",
                name="customers",
                dimensions=[Dimension(name="id", type="integer", primary_key=True)],
            ),
        },
        relationships=[
            Relationship(
                name="orders_to_customers",
                source_dataset="orders",
                source_field="customer_id",
                target_dataset="customers",
                target_field="id",
                operator="=",
                type="inner",
            )
        ],
    )
    query = SemanticQuery(dimensions=["orders.id", "customers.id"])

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert 't0."id" AS "orders__id"' in sql
    assert 't1."id" AS "customers__id"' in sql
    assert "ON t0.customer_id = t1.id" in sql



