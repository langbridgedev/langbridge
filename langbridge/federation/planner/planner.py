
from dataclasses import dataclass

from langbridge.federation.models.plans import LogicalPlan, PhysicalPlan, QueryType
from langbridge.federation.models.smq import SMQQuery
from langbridge.federation.models.virtual_dataset import FederationWorkflow, TableStatistics
from langbridge.federation.planner.optimizer import FederatedOptimizer
from langbridge.federation.planner.parser import logical_plan_from_sql
from langbridge.federation.planner.physical_planner import PhysicalPlanner
from langbridge.federation.planner.smq_compiler import SMQCompiler
from langbridge.federation.planner.stats import StatsStore
from langbridge.semantic.model import SemanticModel


@dataclass(slots=True)
class PlanningOutput:
    logical_plan: LogicalPlan
    physical_plan: PhysicalPlan
    sql: str


class FederatedPlanner:
    def __init__(self, *, stats_store: StatsStore | None = None) -> None:
        self._stats_store = stats_store or StatsStore()
        self._smq_compiler = SMQCompiler()
        self._physical_planner = PhysicalPlanner()

    @property
    def stats_store(self) -> StatsStore:
        return self._stats_store

    def plan_sql(
        self,
        *,
        sql: str,
        dialect: str,
        workflow: FederationWorkflow,
        source_dialects: dict[str, str],
        local_dialect: str = "duckdb",
    ) -> PlanningOutput:
        logical_plan, expression = logical_plan_from_sql(
            sql=sql,
            virtual_dataset=workflow.dataset,
            dialect=dialect,
            query_type=QueryType.SQL,
        )

        optimizer = FederatedOptimizer(
            broadcast_threshold_bytes=workflow.broadcast_threshold_bytes,
        )
        optimized = optimizer.optimize(
            logical_plan=logical_plan,
            expression=expression,
            virtual_dataset=workflow.dataset,
            stats_by_table=self._resolve_stats(workflow),
            source_dialects=source_dialects,
            input_dialect=dialect,
            local_dialect=local_dialect,
        )
        physical_plan = self._physical_planner.build(optimized_plan=optimized)
        return PlanningOutput(logical_plan=logical_plan, physical_plan=physical_plan, sql=sql)

    def plan_smq(
        self,
        *,
        query: SMQQuery,
        semantic_model: SemanticModel,
        dialect: str,
        workflow: FederationWorkflow,
        source_dialects: dict[str, str],
        local_dialect: str = "duckdb",
    ) -> PlanningOutput:
        sql = self._smq_compiler.compile_to_sql(
            query=query,
            semantic_model=semantic_model,
            dialect=dialect,
        )
        logical_plan, expression = logical_plan_from_sql(
            sql=sql,
            virtual_dataset=workflow.dataset,
            dialect=dialect,
            query_type=QueryType.SMQ,
        )

        optimizer = FederatedOptimizer(
            broadcast_threshold_bytes=workflow.broadcast_threshold_bytes,
        )
        optimized = optimizer.optimize(
            logical_plan=logical_plan,
            expression=expression,
            virtual_dataset=workflow.dataset,
            stats_by_table=self._resolve_stats(workflow),
            source_dialects=source_dialects,
            input_dialect=dialect,
            local_dialect=local_dialect,
        )
        physical_plan = self._physical_planner.build(optimized_plan=optimized)
        return PlanningOutput(logical_plan=logical_plan, physical_plan=physical_plan, sql=sql)

    def _resolve_stats(self, workflow: FederationWorkflow) -> dict[str, TableStatistics]:
        self._stats_store.apply_overrides(
            workspace_id=workflow.workspace_id,
            overrides=workflow.dataset.stats_overrides,
        )
        resolved: dict[str, TableStatistics] = {}
        for table_key, binding in workflow.dataset.tables.items():
            from_store = self._stats_store.get(workspace_id=workflow.workspace_id, table_key=table_key)
            if from_store is not None:
                resolved[table_key] = from_store
                continue
            if binding.stats is not None:
                resolved[table_key] = binding.stats
                continue
            resolved[table_key] = TableStatistics(row_count_estimate=1_000_000.0, bytes_per_row=128.0)
        return resolved
