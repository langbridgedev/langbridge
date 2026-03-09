import uuid

from langbridge.packages.semantic.langbridge_semantic.model import Dimension, Relationship, SemanticModel, Table
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery, SemanticQueryEngine
from langbridge.packages.semantic.langbridge_semantic.unified_query import (
    TenantAwareQueryContext,
    UnifiedSourceModel,
    apply_tenant_aware_context,
    build_unified_semantic_model,
)


def test_build_unified_semantic_model_merges_tables_and_tracks_source_connector() -> None:
    connector_a = uuid.uuid4()
    connector_b = uuid.uuid4()

    model_a = SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                schema="public",
                name="orders",
                dimensions=[Dimension(name="id", type="integer", primary_key=True)],
            )
        },
    )
    model_b = SemanticModel(
        version="1.0",
        tables={
            "customers": Table(
                schema="public",
                name="customers",
                dimensions=[Dimension(name="id", type="integer", primary_key=True)],
            )
        },
    )

    unified_model, table_connector_map = build_unified_semantic_model(
        source_models=[
            UnifiedSourceModel(model=model_a, connector_id=connector_a),
            UnifiedSourceModel(model=model_b, connector_id=connector_b),
        ],
        joins=[
            {
                "name": "orders_to_customers",
                "from": "orders",
                "to": "customers",
                "type": "inner",
                "on": "orders.id = customers.id",
            }
        ],
    )

    assert sorted(unified_model.tables.keys()) == ["customers", "orders"]
    assert unified_model.relationships is not None
    assert len(unified_model.relationships) == 1
    assert table_connector_map["orders"] == connector_a
    assert table_connector_map["customers"] == connector_b


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

    # Existing catalog.schema notation is normalized and preserved.
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
                from_="orders",
                to="customers",
                type="inner",
                join_on="orders.customer_id = customers.id",
            )
        ],
    )
    query = SemanticQuery(dimensions=["orders.id", "customers.id"])

    sql = SemanticQueryEngine().compile(query, model, dialect="postgres").sql

    assert 't0."id" AS "orders__id"' in sql
    assert 't1."id" AS "customers__id"' in sql
    assert "ON t0.customer_id = t1.id" in sql
