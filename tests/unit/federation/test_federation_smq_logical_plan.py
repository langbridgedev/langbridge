
import uuid

from langbridge.federation.connectors import SourceCapabilities
from langbridge.federation.models import FederationWorkflow, SMQQuery, VirtualDataset, VirtualRelationship, VirtualTableBinding
from langbridge.federation.models.plans import QueryType, StageType
from langbridge.federation.planner import FederatedPlanner
from langbridge.semantic.model import Dimension, Measure, Relationship, SemanticModel, Table


def _build_semantic_model() -> SemanticModel:
    return SemanticModel(
        version="1.0",
        tables={
            "orders": Table(
                schema="public",
                name="orders",
                dimensions=[
                    Dimension(name="id", type="integer", primary_key=True),
                    Dimension(name="customer_id", type="integer"),
                ],
                measures=[Measure(name="amount", type="number", aggregation="sum")],
            ),
            "customers": Table(
                schema="public",
                name="customers",
                dimensions=[
                    Dimension(name="id", type="integer", primary_key=True),
                    Dimension(name="name", type="string"),
                ],
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


def _build_workflow() -> FederationWorkflow:
    workspace_id = str(uuid.uuid4())
    return FederationWorkflow(
        id="wf-test",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="ds-test",
            name="test dataset",
            workspace_id=workspace_id,
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
            relationships=[
                VirtualRelationship(
                    name="orders_to_customers",
                    left_table="orders",
                    right_table="customers",
                    join_type="inner",
                    condition="orders.customer_id = customers.id",
                )
            ],
        ),
    )


def test_smq_compiles_to_logical_plan() -> None:
    planner = FederatedPlanner()
    workflow = _build_workflow()
    semantic_model = _build_semantic_model()

    query = SMQQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "dimensions": ["customers.name"],
            "filters": [
                {
                    "member": "orders.amount",
                    "operator": "gt",
                    "values": ["10"],
                }
            ],
            "limit": 25,
        }
    )

    output = planner.plan_smq(
        query=query,
        semantic_model=semantic_model,
        dialect="tsql",
        workflow=workflow,
        source_dialects={"source_orders": "postgres", "source_customers": "postgres"},
    )

    assert output.logical_plan.query_type == QueryType.SMQ
    assert len(output.logical_plan.tables) == 2
    assert output.logical_plan.joins
    assert output.physical_plan.result_stage_id


def test_smq_pushes_full_query_down_for_single_source_with_logical_relation_names() -> None:
    planner = FederatedPlanner()
    workspace_id = str(uuid.uuid4())
    workflow = FederationWorkflow(
        id="wf-smq-single-source-rewrite",
        workspace_id=workspace_id,
        dataset=VirtualDataset(
            id="ds-smq-single-source-rewrite",
            name="single source semantic rewrite",
            workspace_id=workspace_id,
            tables={
                "orders": VirtualTableBinding(
                    table_key="orders",
                    source_id="source_warehouse",
                    connector_id=uuid.uuid4(),
                    schema="public",
                    table="orders",
                    metadata={"physical_table": "fact_orders"},
                ),
                "customers": VirtualTableBinding(
                    table_key="customers",
                    source_id="source_warehouse",
                    connector_id=uuid.uuid4(),
                    schema="public",
                    table="customers",
                    metadata={"physical_table": "dim_customers"},
                ),
            },
            relationships=[
                VirtualRelationship(
                    name="orders_to_customers",
                    left_table="orders",
                    right_table="customers",
                    join_type="inner",
                    condition="orders.customer_id = customers.id",
                )
            ],
        ),
    )

    query = SMQQuery.model_validate(
        {
            "measures": ["orders.amount"],
            "dimensions": ["customers.name"],
            "filters": [
                {
                    "member": "orders.amount",
                    "operator": "gt",
                    "values": ["10"],
                }
            ],
            "limit": 25,
        }
    )

    output = planner.plan_smq(
        query=query,
        semantic_model=_build_semantic_model(),
        dialect="postgres",
        workflow=workflow,
        source_dialects={"source_warehouse": "postgres"},
        source_capabilities={"source_warehouse": SourceCapabilities(pushdown_join=True)},
    )

    assert output.physical_plan.pushdown_full_query is True
    remote_stage = next(
        stage
        for stage in output.physical_plan.stages
        if stage.stage_type == StageType.REMOTE_FULL_QUERY
    )
    assert remote_stage.subplan is not None
    rewritten_sql = remote_stage.subplan.sql.replace('"', "")
    assert "FROM public.fact_orders AS t0" in rewritten_sql
    assert "JOIN public.dim_customers AS t1" in rewritten_sql
