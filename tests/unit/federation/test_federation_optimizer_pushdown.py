
import uuid

from langbridge.federation.connectors import SourceCapabilities
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
    assert orders_subplan.pushdown.filter.pushed is True
    assert orders_subplan.pushdown.projection.pushed is True

    assert "name" in customers_subplan.projected_columns
    assert "id" in customers_subplan.projected_columns
    assert customers_subplan.pushdown.projection.pushed is True


def test_optimizer_pushes_full_query_for_synthetic_catalog_bindings_after_rewrite() -> None:
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

    assert output.physical_plan.pushdown_full_query is True
    scan_stage = next(
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.REMOTE_FULL_QUERY
    )
    assert scan_stage.subplan is not None
    assert "org_abc__src_123" not in scan_stage.subplan.sql


def test_optimizer_unquotes_lowercase_filters_for_snowflake_physical_sql_scans() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-opt-snowflake-physical-sql-filter-quoting",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-snowflake-physical-sql-filter-quoting",
            name="optimizer snowflake physical sql filter quoting",
            workspace_id=workspace,
            tables={
                "aligned_returns": VirtualTableBinding(
                    table_key="aligned_returns",
                    source_id="source_snowflake",
                    connector_id=uuid.uuid4(),
                    schema="semantic",
                    table="aligned_returns",
                    metadata={
                        "physical_sql": (
                            "SELECT PRODUCT_ID AS product_id, "
                            "PRODUCT_NAME AS product_name, "
                            "STRIKE_DATE AS strike_date "
                            "FROM DIM_PRODUCT"
                        ),
                    },
                ),
            },
        ),
    )

    output = planner.plan_sql(
        sql=(
            'SELECT aligned_returns.product_id, aligned_returns.product_name, aligned_returns.strike_date '
            'FROM aligned_returns '
            'WHERE LOWER(aligned_returns."product_id") LIKE LOWER(\'I5052724\') '
            'AND LOWER(aligned_returns."product_name") LIKE LOWER(\'140 Summer\') '
            'AND aligned_returns."strike_date" <= \'2025-12-31\''
        ),
        dialect="snowflake",
        workflow=workflow,
        source_dialects={"source_snowflake": "snowflake"},
    )

    assert output.physical_plan.pushdown_full_query is False

    scan_stage = next(
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.REMOTE_SCAN
    )
    assert scan_stage.subplan is not None
    assert scan_stage.subplan.pushdown.filter.pushed is True
    assert 'LOWER(aligned_returns.product_id) LIKE LOWER(\'I5052724\')' in scan_stage.subplan.sql
    assert 'LOWER(aligned_returns.product_name) LIKE LOWER(\'140 Summer\')' in scan_stage.subplan.sql
    assert "aligned_returns.strike_date <= '2025-12-31'" in scan_stage.subplan.sql
    assert '"product_id"' not in scan_stage.subplan.sql
    assert '"product_name"' not in scan_stage.subplan.sql
    assert '"strike_date"' not in scan_stage.subplan.sql


def test_optimizer_reports_pushdown_reason_when_single_source_cannot_push_join() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    connector_id = uuid.uuid4()
    workflow = FederationWorkflow(
        id="wf-opt-single-source-join",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-single-source-join",
            name="optimizer single source join",
            workspace_id=workspace,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="file_source",
                    connector_id=connector_id,
                    schema="public",
                    table="orders",
                ),
                "customers": VirtualTableBinding(
                    table_key="customers",
                    source_id="file_source",
                    connector_id=connector_id,
                    schema="public",
                    table="customers",
                ),
            },
        ),
    )

    output = planner.plan_sql(
        sql=(
            "SELECT o.id, c.name "
            "FROM public.orders AS o "
            "JOIN public.customers AS c ON o.customer_id = c.id"
        ),
        dialect="postgres",
        workflow=workflow,
        source_dialects={"file_source": "duckdb"},
        source_capabilities={"file_source": SourceCapabilities(pushdown_join=False)},
    )

    assert output.physical_plan.pushdown_full_query is False
    assert any("join pushdown is unavailable" in reason for reason in output.physical_plan.pushdown_reasons)
    scan_stage = next(
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.REMOTE_SCAN and stage.subplan is not None
    )
    assert scan_stage.subplan.pushdown.join.pushed is False
    assert "join pushdown is unavailable" in str(scan_stage.subplan.pushdown.join.reason or "")


def test_optimizer_pushes_single_source_full_query_across_dialects() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-opt-single-source-cross-dialect",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-single-source-cross-dialect",
            name="optimizer single source cross dialect",
            workspace_id=workspace,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="source_orders",
                    connector_id=uuid.uuid4(),
                    schema="dbo",
                    table="orders",
                ),
            },
        ),
    )

    output = planner.plan_sql(
        sql="SELECT TOP 5 o.id FROM dbo.orders AS o ORDER BY o.id",
        dialect="tsql",
        workflow=workflow,
        source_dialects={"source_orders": "postgres"},
    )

    assert output.physical_plan.pushdown_full_query is True
    scan_stage = next(
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.REMOTE_FULL_QUERY
    )
    assert scan_stage.subplan is not None
    assert "LIMIT 5" in scan_stage.subplan.sql
    assert "transpiling to the source dialect" in str(scan_stage.subplan.pushdown.full_query.reason or "")


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

    remote_stages = [
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type in {StageType.REMOTE_SCAN, StageType.REMOTE_FULL_QUERY}
        and stage.subplan is not None
    ]
    assert remote_stages
    for stage in remote_stages:
        assert "org_abc__src_123" not in stage.subplan.sql

    for local_stage in (
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.LOCAL_COMPUTE
    ):
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


def test_optimizer_pushes_full_query_down_when_logical_alias_differs_from_physical_table() -> None:
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
        source_dialects={"source_orders": "postgres"},
    )

    stage_ids = {stage.stage_id for stage in output.physical_plan.stages}
    assert "scan_full_query" in stage_ids
    assert output.physical_plan.pushdown_full_query is True
    scan_stage = next(
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.REMOTE_FULL_QUERY
    )
    assert scan_stage.subplan is not None
    assert "FROM orders_enriched AS shopify_orders" in scan_stage.subplan.sql.replace('"', "")


def test_optimizer_pushes_limit_in_remote_full_query_when_alias_rewrite_is_supported() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-opt-logical-physical-limit",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-logical-physical-limit",
            name="optimizer logical physical limit",
            workspace_id=workspace,
            tables={
                "product": VirtualTableBinding(
                    table_key="product",
                    source_id="source_products",
                    connector_id=uuid.uuid4(),
                    schema="transformed_scd",
                    table="dim_product",
                    metadata={
                        "physical_schema": "transformed_scd",
                        "physical_table": "dim_product",
                    },
                ),
            },
        ),
    )

    output = planner.plan_sql(
        sql="SELECT * FROM product LIMIT 10",
        dialect="snowflake",
        workflow=workflow,
        source_dialects={"source_products": "snowflake"},
    )

    assert output.physical_plan.pushdown_full_query is True

    scan_stage = next(
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.REMOTE_FULL_QUERY
    )
    assert scan_stage.subplan is not None
    assert scan_stage.subplan.pushed_limit is None
    assert scan_stage.subplan.pushdown.limit.pushed is True
    assert "LIMIT 10" in scan_stage.subplan.sql.upper()
    assert "FROM TRANSFORMED_SCD.DIM_PRODUCT AS PRODUCT" in scan_stage.subplan.sql.upper().replace('"', "")


def test_optimizer_pushes_ordered_limit_in_remote_full_query_when_alias_rewrite_is_supported() -> None:
    planner = FederatedPlanner()
    workspace = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-opt-logical-physical-ordered-limit",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-opt-logical-physical-ordered-limit",
            name="optimizer logical physical ordered limit",
            workspace_id=workspace,
            tables={
                "product": VirtualTableBinding(
                    table_key="product",
                    source_id="source_products",
                    connector_id=uuid.uuid4(),
                    schema="transformed_scd",
                    table="dim_product",
                    metadata={
                        "physical_schema": "transformed_scd",
                        "physical_table": "dim_product",
                    },
                ),
            },
        ),
    )

    output = planner.plan_sql(
        sql="SELECT product.id FROM product ORDER BY product.id LIMIT 10",
        dialect="snowflake",
        workflow=workflow,
        source_dialects={"source_products": "snowflake"},
    )

    scan_stage = next(
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.REMOTE_FULL_QUERY
    )
    assert scan_stage.subplan is not None
    assert scan_stage.subplan.pushed_limit is None
    assert scan_stage.subplan.pushdown.limit.pushed is True
    assert "ORDER BY" in scan_stage.subplan.sql.upper()
    assert "LIMIT 10" in scan_stage.subplan.sql.upper()


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

    assert output.physical_plan.pushdown_full_query is True
    scan_stage = next(
        stage for stage in output.physical_plan.stages if stage.stage_type == StageType.REMOTE_FULL_QUERY
    )
    assert scan_stage.subplan is not None
    assert "TOP 5" not in scan_stage.subplan.sql.upper()
    assert "LIMIT 5" in scan_stage.subplan.sql.upper()
