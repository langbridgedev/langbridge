
import uuid
from typing import Any

import pyarrow as pa

from langbridge.federation.connectors import RemoteSource
from langbridge.federation.executor import ArtifactStore, LocalStageDispatcher, StageExecutor, StageScheduler
from langbridge.federation.models import (
    FederatedExplainPlan,
    FederationWorkflow,
    QueryType,
    ResultHandle,
    SMQQuery,
    TableStatistics,
)
from langbridge.federation.planner import FederatedPlanner
from langbridge.semantic.model import SemanticModel


class FederatedQueryService:
    def __init__(
        self,
        *,
        artifact_store: ArtifactStore,
        planner: FederatedPlanner | None = None,
    ) -> None:
        self._artifact_store = artifact_store
        self._planner = planner or FederatedPlanner()
        self._workflows: dict[str, FederationWorkflow] = {}
        self._sources: dict[str, dict[str, RemoteSource]] = {}
        self._semantic_models: dict[str, SemanticModel] = {}
        self._results: dict[str, ResultHandle] = {}

    def register_workspace(
        self,
        *,
        workspace_id: str,
        workflow: FederationWorkflow,
        sources: dict[str, RemoteSource],
        semantic_model: SemanticModel | None = None,
    ) -> None:
        if workflow.workspace_id != workspace_id:
            raise ValueError("Workflow workspace_id must match registration workspace_id.")
        self._workflows[workspace_id] = workflow
        self._sources[workspace_id] = sources
        if semantic_model is not None:
            self._semantic_models[workspace_id] = semantic_model

    async def execute(
        self,
        query: SMQQuery | str | dict[str, Any],
        dialect: str = "duckdb",
        workspace_id: str = "",
    ) -> ResultHandle:
        workflow = self._require_workflow(workspace_id)
        sources = self._require_sources(workspace_id)

        source_dialects = {source_id: source.dialect() for source_id, source in sources.items()}

        if isinstance(query, str):
            planning = self._planner.plan_sql(
                sql=query,
                dialect=dialect,
                local_dialect="duckdb",
                workflow=workflow,
                source_dialects=source_dialects,
            )
        else:
            smq = query if isinstance(query, SMQQuery) else SMQQuery.model_validate(query)
            semantic_model = self._semantic_models.get(workspace_id)
            if semantic_model is None:
                raise ValueError("SMQ execution requires a semantic model registered for the workspace.")
            planning = self._planner.plan_smq(
                query=smq,
                semantic_model=semantic_model,
                dialect=dialect,
                local_dialect="duckdb",
                workflow=workflow,
                source_dialects=source_dialects,
            )

        stage_executor = StageExecutor(artifact_store=self._artifact_store, sources=sources)
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
        )
        self._results[result_handle.handle_id] = result_handle
        self._record_runtime_stats(
            workspace_id=workspace_id,
            plan=planning.physical_plan,
            artifacts=scheduler_result.artifacts,
        )
        return result_handle

    async def fetch_arrow(self, result_handle: ResultHandle | str) -> pa.Table:
        handle = self._resolve_result_handle(result_handle)
        return self._artifact_store.read_artifact(handle.artifact_key)

    async def explain(
        self,
        query: SMQQuery | str | dict[str, Any],
        dialect: str = "tsql",
        workspace_id: str = "",
    ) -> FederatedExplainPlan:
        workflow = self._require_workflow(workspace_id)
        sources = self._require_sources(workspace_id)
        source_dialects = {source_id: source.dialect() for source_id, source in sources.items()}

        if isinstance(query, str):
            planning = self._planner.plan_sql(
                sql=query,
                dialect=dialect,
                local_dialect="duckdb",
                workflow=workflow,
                source_dialects=source_dialects,
            )
        else:
            smq = query if isinstance(query, SMQQuery) else SMQQuery.model_validate(query)
            semantic_model = self._semantic_models.get(workspace_id)
            if semantic_model is None:
                raise ValueError("SMQ explain requires a semantic model registered for the workspace.")
            planning = self._planner.plan_smq(
                query=smq,
                semantic_model=semantic_model,
                dialect=dialect,
                local_dialect="duckdb",
                workflow=workflow,
                source_dialects=source_dialects,
            )

        return FederatedExplainPlan(
            logical_plan=planning.logical_plan,
            physical_plan=planning.physical_plan,
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

    def _resolve_result_handle(self, result_handle: ResultHandle | str) -> ResultHandle:
        if isinstance(result_handle, ResultHandle):
            return result_handle
        handle = self._results.get(result_handle)
        if handle is None:
            raise KeyError(f"Unknown result handle '{result_handle}'.")
        return handle

    def _require_workflow(self, workspace_id: str) -> FederationWorkflow:
        workflow = self._workflows.get(workspace_id)
        if workflow is None:
            raise KeyError(f"No federation workflow registered for workspace '{workspace_id}'.")
        return workflow

    def _require_sources(self, workspace_id: str) -> dict[str, RemoteSource]:
        sources = self._sources.get(workspace_id)
        if sources is None:
            raise KeyError(f"No remote sources registered for workspace '{workspace_id}'.")
        return sources


__all__ = ["FederatedQueryService"]
