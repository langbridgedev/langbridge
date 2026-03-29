
import uuid

from langbridge.federation.models import VirtualDataset, VirtualTableBinding
from langbridge.federation.models.plans import QueryType
from langbridge.federation.planner.parser import logical_plan_from_sql


def _dataset() -> VirtualDataset:
    workspace = str(uuid.uuid4())
    return VirtualDataset(
        id="ds-parse",
        name="parse",
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
    )


def test_tsql_parse_to_logical_plan() -> None:
    sql = (
        "SELECT TOP 5 o.customer_id, SUM(o.amount) AS total "
        "FROM dbo.orders o "
        "JOIN dbo.customers c ON o.customer_id = c.id "
        "WHERE o.amount > 10 "
        "GROUP BY o.customer_id "
        "HAVING SUM(o.amount) > 100 "
        "ORDER BY total DESC"
    )

    logical_plan, _ = logical_plan_from_sql(
        sql=sql,
        virtual_dataset=_dataset(),
        dialect="tsql",
        query_type=QueryType.SQL,
    )

    assert logical_plan.query_type == QueryType.SQL
    assert len(logical_plan.tables) == 2
    assert len(logical_plan.joins) == 1
    assert logical_plan.where_sql is not None
    assert logical_plan.having_sql is not None
