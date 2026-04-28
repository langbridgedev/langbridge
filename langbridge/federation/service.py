import asyncio
import uuid
from typing import Any

import pyarrow as pa

from langbridge.federation.connectors import RemoteSource
from langbridge.federation.executor import (
    ArtifactStore,
    FederationExecutionOffloader,
    LocalStageDispatcher,
    StageExecutor,
    StageScheduler,
    run_federation_blocking,
)
from langbridge.federation.executor.cache_context import StageCacheResolver
from langbridge.federation.models import (
    FederatedExplainPlan,
    FederationWorkflow,
    ResultHandle,
    SMQQuery,
    TableStatistics,
)
from langbridge.federation.planner import FederatedPlanner, PlanningOutput
from langbridge.semantic.model import SemanticModel


class FederatedQueryService:
    def __init__(
        self,
        *,
        artifact_store: ArtifactStore,
        planner: FederatedPlanner | None = None,
        blocking_executor: FederationExecutionOffloader | None = None,
    ) -> None:
        self._artifact_store = artifact_store
        self._planner = planner or FederatedPlanner()
        self._blocking_executor = blocking_executor or FederationExecutionOffloader()
        self._owns_blocking_executor = blocking_executor is None
        self._planner_lock = asyncio.Lock()

    @property
    def blocking_executor(self) -> FederationExecutionOffloader:
        return self._blocking_executor

    async def aclose(self) -> None:
        if self._owns_blocking_executor:
            await self._blocking_executor.aclose()

    def close(self) -> None:
        if self._owns_blocking_executor:
            self._blocking_executor.close()

    async def execute(
        self,
        query: SMQQuery | str | dict[str, Any],
        workspace_id: str,
        workflow: FederationWorkflow,
        sources: dict[str, RemoteSource],
        semantic_model: SemanticModel | None = None,
        dialect: str = "duckdb",
    ) -> ResultHandle:
        async with self._planner_lock:
            planning = await run_federation_blocking(
                self._blocking_executor,
                self._plan_query,
                query=query,
                workflow=workflow,
                sources=sources,
                semantic_model=semantic_model,
                dialect=dialect,
            )

        cache_resolver = StageCacheResolver(
            workflow=workflow,
            plan=planning.physical_plan,
        )
        stage_executor = StageExecutor(
            artifact_store=self._artifact_store,
            cache_resolver=cache_resolver,
            sources=sources,
            blocking_executor=self._blocking_executor,
        )
        dispatcher = LocalStageDispatcher(stage_executor=stage_executor)
        scheduler = StageScheduler(dispatcher=dispatcher, stage_parallelism=workflow.stage_parallelism)
        scheduler_result = await scheduler.run(plan=planning.physical_plan, workspace_id=workspace_id)

        result_stage_id = planning.physical_plan.result_stage_id
        result_artifact = scheduler_result.artifacts[result_stage_id]
        result_handle = ResultHandle(
            handle_id=str(uuid.uuid4()),
            workspace_id=workspace_id,
            plan_id=planning.physical_plan.plan_id,
            result_stage_id=result_stage_id,
            artifact_key=result_artifact.artifact_key,
            execution=scheduler_result.summary,
            logical_plan=planning.logical_plan,
            physical_plan=planning.physical_plan,
        )
        async with self._planner_lock:
            await run_federation_blocking(
                self._blocking_executor,
                self._record_runtime_stats,
                workspace_id=workspace_id,
                plan=planning.physical_plan,
                artifacts=scheduler_result.artifacts,
            )
        return result_handle

    async def fetch_arrow(self, result_handle: ResultHandle) -> pa.Table:
        return await run_federation_blocking(
            self._blocking_executor,
            self._artifact_store.read_artifact,
            result_handle.artifact_key,
        )

    async def explain(
        self,
        query: SMQQuery | str | dict[str, Any],
        workspace_id: str,
        workflow: FederationWorkflow,
        sources: dict[str, RemoteSource],
        semantic_model: SemanticModel | None = None,
        dialect: str = "tsql",
    ) -> FederatedExplainPlan:
        async with self._planner_lock:
            planning = await run_federation_blocking(
                self._blocking_executor,
                self._plan_query,
                query=query,
                workflow=workflow,
                sources=sources,
                semantic_model=semantic_model,
                dialect=dialect,
            )

        return FederatedExplainPlan(
            logical_plan=planning.logical_plan,
            physical_plan=planning.physical_plan,
        )

    def _plan_query(
        self,
        *,
        query: SMQQuery | str | dict[str, Any],
        workflow: FederationWorkflow,
        sources: dict[str, RemoteSource],
        semantic_model: SemanticModel | None,
        dialect: str,
    ) -> PlanningOutput:
        source_dialects = {source_id: source.dialect() for source_id, source in sources.items()}
        source_capabilities = {
            source_id: source.capabilities()
            for source_id, source in sources.items()
        }

        if isinstance(query, str):
            return self._planner.plan_sql(
                sql=query,
                dialect=dialect,
                local_dialect="duckdb",
                workflow=workflow,
                source_dialects=source_dialects,
                source_capabilities=source_capabilities,
            )
        smq = query if isinstance(query, SMQQuery) else SMQQuery.model_validate(query)
        if semantic_model is None:
            raise ValueError("SMQ execution requires a semantic model payload.")
        return self._planner.plan_smq(
            query=smq,
            semantic_model=semantic_model,
            dialect=dialect,
            local_dialect="duckdb",
            workflow=workflow,
            source_dialects=source_dialects,
            source_capabilities=source_capabilities,
        )

    def _record_runtime_stats(self, *, workspace_id: str, plan, artifacts: dict[str, Any]) -> None:
        for stage in plan.stages:
            if stage.subplan is None:
                continue
            artifact = artifacts.get(stage.stage_id)
            if artifact is None or artifact.rows <= 0:
                continue
            bytes_per_row = float(artifact.bytes_written / max(artifact.rows, 1))
            self._planner.stats_store.upsert(
                workspace_id=workspace_id,
                table_key=stage.subplan.table_key,
                stats=TableStatistics(
                    row_count_estimate=float(artifact.rows),
                    bytes_per_row=bytes_per_row,
                ),
            )


__all__ = ["FederatedQueryService"]
