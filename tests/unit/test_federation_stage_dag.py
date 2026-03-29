
import uuid

from langbridge.federation.models import FederationWorkflow, VirtualDataset, VirtualTableBinding
from langbridge.federation.models.plans import StageType
from langbridge.federation.planner import FederatedPlanner


def _workflow() -> FederationWorkflow:
    workspace = str(uuid.uuid4())
    return FederationWorkflow(
        id="wf-dag",
        workspace_id=workspace,
        dataset=VirtualDataset(
            id="ds-dag",
            name="dag",
            workspace_id=workspace,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="source_orders",
                    connector_id=uuid.uuid4(),
                    schema="public",
                    table="orders",
                ),
                "customers": VirtualTableBinding(
                    table_key="customers",
                    source_id="source_customers",
                    connector_id=uuid.uuid4(),
                    schema="public",
                    table="customers",
                ),
            },
        ),
    )


def test_physical_plan_dag_dependencies() -> None:
    planner = FederatedPlanner()
    workflow = _workflow()

    sql = (
        "SELECT o.id, c.name "
        "FROM public.orders o "
        "JOIN public.customers c ON o.customer_id = c.id"
    )

    output = planner.plan_sql(
        sql=sql,
        dialect="tsql",
        workflow=workflow,
        source_dialects={"source_orders": "postgres", "source_customers": "postgres"},
    )

    stages = {stage.stage_id: stage for stage in output.physical_plan.stages}
    final_stage = stages[output.physical_plan.result_stage_id]

    assert final_stage.stage_type == StageType.LOCAL_COMPUTE
    assert final_stage.sql_dialect == "duckdb"
    assert len(final_stage.dependencies) == 2
    for dependency in final_stage.dependencies:
        assert dependency in stages
        assert stages[dependency].stage_type == StageType.REMOTE_SCAN
