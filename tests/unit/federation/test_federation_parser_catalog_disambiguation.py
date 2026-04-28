
import uuid

from langbridge.federation.models import VirtualDataset, VirtualTableBinding
from langbridge.federation.models.plans import QueryType
from langbridge.federation.planner.parser import logical_plan_from_sql


def test_parser_resolves_duplicate_schema_table_using_catalog() -> None:
    workspace = str(uuid.uuid4())
    dataset = VirtualDataset(
        id="ds-catalog-disambiguation",
        name="catalog-disambiguation",
        workspace_id=workspace,
        tables={
            "shops_a": VirtualTableBinding(
                table_key="shops_a",
                source_id="src-a",
                connector_id=uuid.uuid4(),
                catalog="org_abc__src_111",
                schema="public",
                table="shops",
                metadata={
                    "physical_catalog": None,
                    "physical_schema": "public",
                    "physical_table": "shops",
                    "skip_catalog_in_pushdown": True,
                },
            ),
            "shops_b": VirtualTableBinding(
                table_key="shops_b",
                source_id="src-b",
                connector_id=uuid.uuid4(),
                catalog="org_abc__src_222",
                schema="public",
                table="shops",
                metadata={
                    "physical_catalog": None,
                    "physical_schema": "public",
                    "physical_table": "shops",
                    "skip_catalog_in_pushdown": True,
                },
            ),
        },
    )

    sql = (
        'SELECT t0.city FROM "org_abc__src_111"."public"."shops" AS t0 '
        "WHERE t0.city IS NOT NULL"
    )

    logical_plan, _ = logical_plan_from_sql(
        sql=sql,
        virtual_dataset=dataset,
        dialect="tsql",
        query_type=QueryType.SQL,
    )

    assert logical_plan.from_alias == "t0"
    assert logical_plan.tables["t0"].table_key == "shops_a"


def test_parser_resolves_physical_source_table_for_logical_dataset_alias() -> None:
    workspace = str(uuid.uuid4())
    dataset = VirtualDataset(
        id="ds-physical-source-name",
        name="physical source name",
        workspace_id=workspace,
        tables={
            "product_returns": VirtualTableBinding(
                table_key="product_returns",
                source_id="src-snowflake",
                connector_id=uuid.uuid4(),
                table="product_returns",
                metadata={
                    "dataset_alias": "product_returns",
                    "physical_table": "DT_FACT_PRODUCT_PERFORMANCE_RETURN",
                },
            ),
            "performance_stream": VirtualTableBinding(
                table_key="performance_stream",
                source_id="src-snowflake",
                connector_id=uuid.uuid4(),
                table="performance_stream",
                metadata={
                    "dataset_alias": "performance_stream",
                    "physical_table": "DIM_PERFORMANCE_STREAM",
                },
            ),
            "product_performance_bridge": VirtualTableBinding(
                table_key="product_performance_bridge",
                source_id="src-snowflake",
                connector_id=uuid.uuid4(),
                table="product_performance_bridge",
                metadata={
                    "dataset_alias": "product_performance_bridge",
                    "physical_table": "IFACT_PRODUCT_PERFORMANCE",
                },
            ),
            "product": VirtualTableBinding(
                table_key="product",
                source_id="src-snowflake",
                connector_id=uuid.uuid4(),
                table="product",
                metadata={
                    "dataset_alias": "product",
                    "physical_table": "DIM_PRODUCT",
                },
            ),
        },
    )

    sql = (
        'SELECT t3."PRODUCT_ID", AVG(t0."RETURN") '
        'FROM DT_FACT_PRODUCT_PERFORMANCE_RETURN AS t0 '
        "LEFT JOIN DIM_PERFORMANCE_STREAM AS t1 "
        "ON t0.performance_stream_bk_key = t1.performance_stream_bk_key "
        "LEFT JOIN IFACT_PRODUCT_PERFORMANCE AS t2 "
        "ON t2.performance_stream_bk_key = t1.performance_stream_bk_key "
        "LEFT JOIN DIM_PRODUCT AS t3 "
        "ON t2.product_bk_key = t3.product_bk_key "
        'WHERE t0."STRIKE_DATE" >= \'2024-06-01\' '
        'GROUP BY t3."PRODUCT_ID"'
    )

    logical_plan, _ = logical_plan_from_sql(
        sql=sql,
        virtual_dataset=dataset,
        dialect="snowflake",
        query_type=QueryType.SQL,
    )

    assert logical_plan.from_alias == "t0"
    assert logical_plan.tables["t0"].table_key == "product_returns"
    assert logical_plan.tables["t1"].table_key == "performance_stream"
    assert logical_plan.tables["t2"].table_key == "product_performance_bridge"
    assert logical_plan.tables["t3"].table_key == "product"
