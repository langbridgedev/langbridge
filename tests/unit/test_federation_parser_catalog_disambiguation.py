
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
