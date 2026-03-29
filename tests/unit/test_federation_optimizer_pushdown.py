
import uuid

from langbridge.federation.models import FederationWorkflow, VirtualDataset, VirtualTableBinding
from langbridge.federation.models.plans import StageType
from langbridge.federation.planner import FederatedPlanner
from langbridge.federation.planner.parser import parse_sql


def _workflow() -> FederationWorkflow:
    workspace = str(uuid.uuid4())
    return FederationWorkflow(
        id="wf-opt",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt",
            name="optimizer",
            workspace_id=workspace,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="source_orders",
                    connector_id=uuid.uuid4(),
                    schema="dbo",
                    table="orders",
                ),
                "customers": VirtualTableBinding(
                    table_key="customers",
                    source_id="source_customers",
                    connector_id=uuid.uuid4(),
                    schema="dbo",
                    table="customers",
                ),
            },
        ),
    )


def test_optimizer_pushes_projection_and_filter() -> None:
    planner = FederatedPlanner()
    workflow = _workflow()

    sql = (
        "SELECT o.customer_id, c.name "
        "FROM dbo.orders o "
        "JOIN dbo.customers c ON o.customer_id = c.id "
        "WHERE o.amount > 100"
    )

    output = planner.plan_sql(
        sql=sql,
        dialect="tsql",
        workflow=workflow,
        source_dialects={"source_orders": "postgres", "source_customers": "snowflake"},
    )

    scan_stages = {
        stage.subplan.alias: stage.subplan
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.REMOTE_SCAN and stage.subplan is not None
    }

    orders_subplan = scan_stages["o"]
    customers_subplan = scan_stages["c"]

    assert "amount" in orders_subplan.projected_columns
    assert "customer_id" in orders_subplan.projected_columns
    assert any("amount" in predicate for predicate in orders_subplan.pushed_filters)

    assert "name" in customers_subplan.projected_columns
    assert "id" in customers_subplan.projected_columns


def test_optimizer_avoids_full_query_pushdown_for_synthetic_catalog_bindings() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-opt-synthetic-catalog",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-synthetic-catalog",
            name="optimizer synthetic catalog",
            workspace_id=workspace,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="source_orders",
                    connector_id=uuid.uuid4(),
                    schema="dbo",
                    table="orders",
                    catalog="org_abc__src_123",
                    metadata={
                        "physical_catalog": None,
                        "physical_schema": "dbo",
                        "physical_table": "orders",
                        "skip_catalog_in_pushdown": True,
                    },
                ),
            },
        ),
    )

    sql = 'SELECT o.id FROM "org_abc__src_123"."dbo"."orders" o WHERE o.id > 10'
    output = planner.plan_sql(
        sql=sql,
        dialect="tsql",
        workflow=workflow,
        source_dialects={"source_orders": "postgres"},
    )

    stage_ids = {stage.stage_id for stage in output.physical_plan.stages}
    assert "scan_full_query" not in stage_ids
    scan_stage = next(
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.REMOTE_SCAN
    )
    assert scan_stage.subplan is not None
    assert "org_abc__src_123" not in scan_stage.subplan.sql


def test_optimizer_rewrites_fully_qualified_columns_for_synthetic_catalog_bindings() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    connector_id = uuid.uuid4()
    workflow = FederationWorkflow(
        id="wf-opt-synthetic-catalog-qualified-columns",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-synthetic-catalog-qualified-columns",
            name="optimizer synthetic catalog qualified columns",
            workspace_id=workspace,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="source_orders",
                    connector_id=connector_id,
                    schema="public",
                    table="orders",
                    catalog="org_abc__src_123",
                    metadata={
                        "physical_catalog": None,
                        "physical_schema": "public",
                        "physical_table": "orders",
                        "skip_catalog_in_pushdown": True,
                    },
                ),
                "customers": VirtualTableBinding(
                    table_key="customers",
                    source_id="source_orders",
                    connector_id=connector_id,
                    schema="public",
                    table="customers",
                    catalog="org_abc__src_123",
                    metadata={
                        "physical_catalog": None,
                        "physical_schema": "public",
                        "physical_table": "customers",
                        "skip_catalog_in_pushdown": True,
                    },
                ),
            },
        ),
    )

    sql = (
        "SELECT org_abc__src_123.public.customers.id AS customer_id, "
        "SUM(org_abc__src_123.public.orders.total) AS total_order_value_2025 "
        "FROM org_abc__src_123.public.orders "
        "INNER JOIN org_abc__src_123.public.customers "
        "ON org_abc__src_123.public.orders.customer_id = org_abc__src_123.public.customers.id "
        "WHERE EXTRACT(YEAR FROM org_abc__src_123.public.orders.order_ts) = 2025 "
        "GROUP BY org_abc__src_123.public.customers.id "
        "ORDER BY total_order_value_2025 DESC LIMIT 20"
    )
    output = planner.plan_sql(
        sql=sql,
        dialect="postgres",
        workflow=workflow,
        source_dialects={"source_orders": "postgres"},
    )

    scan_stages = [
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.REMOTE_SCAN and stage.subplan is not None
    ]
    assert scan_stages
    for stage in scan_stages:
        assert "org_abc__src_123" not in stage.subplan.sql

    local_stage = next(
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.LOCAL_COMPUTE
    )
    assert local_stage.sql is not None
    assert "org_abc__src_123" not in local_stage.sql


def test_optimizer_supports_cte_references_without_mapping_cte_names() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    connector_a = uuid.uuid4()
    connector_b = uuid.uuid4()
    workflow = FederationWorkflow(
        id="wf-opt-cte",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-cte",
            name="optimizer cte",
            workspace_id=workspace,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="source_a",
                    connector_id=connector_a,
                    schema="public",
                    table="orders",
                    catalog="org_abc__src_111",
                    metadata={
                        "physical_catalog": None,
                        "physical_schema": "public",
                        "physical_table": "orders",
                        "skip_catalog_in_pushdown": True,
                    },
                ),
                "customers": VirtualTableBinding(
                    table_key="customers",
                    source_id="source_a",
                    connector_id=connector_a,
                    schema="public",
                    table="customers",
                    catalog="org_abc__src_111",
                    metadata={
                        "physical_catalog": None,
                        "physical_schema": "public",
                        "physical_table": "customers",
                        "skip_catalog_in_pushdown": True,
                    },
                ),
                "contacts": VirtualTableBinding(
                    table_key="contacts",
                    source_id="source_b",
                    connector_id=connector_b,
                    schema="public",
                    table="contacts",
                    catalog="org_abc__src_222",
                    metadata={
                        "physical_catalog": None,
                        "physical_schema": "public",
                        "physical_table": "contacts",
                        "skip_catalog_in_pushdown": True,
                    },
                ),
                "accounts": VirtualTableBinding(
                    table_key="accounts",
                    source_id="source_b",
                    connector_id=connector_b,
                    schema="public",
                    table="accounts",
                    catalog="org_abc__src_222",
                    metadata={
                        "physical_catalog": None,
                        "physical_schema": "public",
                        "physical_table": "accounts",
                        "skip_catalog_in_pushdown": True,
                    },
                ),
            },
        ),
    )

    sql = (
        "WITH base_fact AS ("
        "SELECT org_abc__src_111.public.customers.id AS customer_id, "
        "SUM(org_abc__src_111.public.orders.total) AS total_order_value_2025 "
        "FROM org_abc__src_111.public.orders "
        "INNER JOIN org_abc__src_111.public.customers "
        "ON org_abc__src_111.public.orders.customer_id = org_abc__src_111.public.customers.id "
        "WHERE EXTRACT(YEAR FROM org_abc__src_111.public.orders.order_ts) = 2025 "
        "GROUP BY org_abc__src_111.public.customers.id"
        "), top_20 AS ("
        "SELECT base_fact.customer_id, base_fact.total_order_value_2025 "
        "FROM base_fact ORDER BY base_fact.total_order_value_2025 DESC LIMIT 20"
        ") "
        "SELECT org_abc__src_111.public.customers.id AS customer_id, "
        "org_abc__src_111.public.customers.email AS customer_email, "
        "org_abc__src_222.public.contacts.lifecycle_stage AS lifecycle_stage, "
        "top_20.total_order_value_2025 AS total_order_value_2025, "
        "org_abc__src_111.public.customers.loyalty_tier AS loyalty_tier, "
        "org_abc__src_222.public.accounts.status AS account_status "
        "FROM top_20 "
        "INNER JOIN org_abc__src_111.public.customers "
        "ON top_20.customer_id = org_abc__src_111.public.customers.id "
        "INNER JOIN org_abc__src_222.public.contacts "
        "ON org_abc__src_222.public.contacts.contact_external_id = org_abc__src_111.public.customers.crm_contact_external_id "
        "LEFT JOIN org_abc__src_222.public.accounts "
        "ON org_abc__src_222.public.contacts.account_id = org_abc__src_222.public.accounts.id "
        "ORDER BY top_20.total_order_value_2025 DESC LIMIT 1000"
    )

    output = planner.plan_sql(
        sql=sql,
        dialect="postgres",
        workflow=workflow,
        source_dialects={"source_a": "postgres", "source_b": "postgres"},
    )

    scan_stages = [
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.REMOTE_SCAN and stage.subplan is not None
    ]
    assert scan_stages
    assert {stage.subplan.table_key for stage in scan_stages} == {
        "orders",
        "customers",
        "contacts",
        "accounts",
    }
    for stage in scan_stages:
        assert "org_abc__src_" not in stage.subplan.sql

    local_stage = next(
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.LOCAL_COMPUTE
    )
    assert local_stage.sql is not None
    assert "WITH base_fact AS" in local_stage.sql
    assert "FROM top_20" in local_stage.sql
    assert "org_abc__src_" not in local_stage.sql


def test_optimizer_sets_duckdb_as_local_stage_dialect() -> None:
    planner = FederatedPlanner()
    workflow = _workflow()

    output = planner.plan_sql(
        sql="SELECT o.id, c.name FROM dbo.orders o JOIN dbo.customers c ON o.customer_id = c.id",
        dialect="tsql",
        workflow=workflow,
        source_dialects={"source_orders": "postgres", "source_customers": "postgres"},
    )

    local_stage = next(
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.LOCAL_COMPUTE
    )
    assert local_stage.sql_dialect == "duckdb"


def test_optimizer_avoids_full_query_pushdown_when_logical_alias_differs_from_physical_table() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-opt-logical-physical-rewrite",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-logical-physical-rewrite",
            name="optimizer logical physical rewrite",
            workspace_id=workspace,
            tables={
                "shopify_orders": VirtualTableBinding(
                    table_key="shopify_orders",
                    source_id="source_orders",
                    connector_id=uuid.uuid4(),
                    table="orders_enriched",
                    metadata={
                        "physical_table": "orders_enriched",
                    },
                ),
            },
        ),
    )

    output = planner.plan_sql(
        sql="SELECT shopify_orders.country FROM shopify_orders",
        dialect="postgres",
        workflow=workflow,
        source_dialects={"source_orders": "sqlite"},
    )

    stage_ids = {stage.stage_id for stage in output.physical_plan.stages}
    assert "scan_full_query" not in stage_ids
    scan_stage = next(
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.REMOTE_SCAN
    )
    assert scan_stage.subplan is not None
    assert "FROM orders_enriched AS shopify_orders" in scan_stage.subplan.sql.replace('"', "")


def test_parser_normalizes_trunc_and_interval_syntax() -> None:
    parsed = parse_sql(
        "SELECT shopify_orders.country, SUM(shopify_orders.net_sales) AS total_net_sales "
        "FROM shopify_orders "
        "WHERE shopify_orders.order_date >= TIMESTAMP_TRUNC(CURRENT_DATE, QUARTER) "
        "AND shopify_orders.order_date < TIMESTAMP_TRUNC(CURRENT_DATE, QUARTER) + INTERVAL '3' MONTHS "
        "GROUP BY shopify_orders.country ORDER BY total_net_sales DESC LIMIT 1000",
        dialect="postgres",
    )

    normalized_sql = parsed.expression.sql(dialect="postgres")
    assert "DATE_TRUNC('QUARTER', CURRENT_DATE)" in normalized_sql
    assert "INTERVAL '3 MONTHS'" in normalized_sql
    assert "GROUP BY" in normalized_sql
    assert "MONTHS GROUP BY" not in normalized_sql


def test_optimizer_renders_postgres_btrim_for_duckdb_local_stage() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-opt-local-duckdb-btrim",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-local-duckdb-btrim",
            name="optimizer local duckdb btrim",
            workspace_id=workspace,
            tables={
                "shopify_orders": VirtualTableBinding(
                    table_key="shopify_orders",
                    source_id="source_orders",
                    connector_id=uuid.uuid4(),
                    table="orders_enriched",
                    metadata={
                        "physical_table": "orders_enriched",
                    },
                ),
            },
        ),
    )

    output = planner.plan_sql(
        sql=(
            "SELECT shopify_orders.country, COUNT(*) AS order_count "
            "FROM shopify_orders "
            "WHERE NOT shopify_orders.country IS NULL "
            "AND BTRIM(shopify_orders.country) <> '' "
            "GROUP BY shopify_orders.country"
        ),
        dialect="postgres",
        workflow=workflow,
        source_dialects={"source_orders": "sqlite"},
    )

    local_stage = next(
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.LOCAL_COMPUTE
    )
    assert local_stage.sql is not None
    assert local_stage.sql_dialect == "duckdb"
    assert "BTRIM" not in local_stage.sql.upper()
    assert "TRIM(" in local_stage.sql.upper()


def test_optimizer_renders_tsql_top_using_duckdb_limit_for_local_stage() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-opt-local-duckdb-top",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-local-duckdb-top",
            name="optimizer local duckdb top",
            workspace_id=workspace,
            tables={
                "shopify_orders": VirtualTableBinding(
                    table_key="shopify_orders",
                    source_id="source_orders",
                    connector_id=uuid.uuid4(),
                    schema="dbo",
                    table="orders_enriched",
                    metadata={
                        "physical_table": "orders_enriched",
                    },
                ),
            },
        ),
    )

    output = planner.plan_sql(
        sql="SELECT TOP 5 shopify_orders.country FROM shopify_orders ORDER BY shopify_orders.country",
        dialect="tsql",
        workflow=workflow,
        source_dialects={"source_orders": "sqlite"},
    )

    local_stage = next(
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.LOCAL_COMPUTE
    )
    assert local_stage.sql is not None
    assert local_stage.sql_dialect == "duckdb"
    assert "TOP 5" not in local_stage.sql.upper()
    assert "LIMIT 5" in local_stage.sql.upper()
