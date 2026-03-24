from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from langbridge.federation.executor.stage_executor import StageExecutionContext, StageExecutor
from langbridge.federation.models.plans import ExecutionSummary, PhysicalPlan, StageArtifact, StageDefinition, StageMetrics


class StageDispatcher:
    async def run(self, *, stage: StageDefinition, context: StageExecutionContext) -> tuple[StageArtifact, StageMetrics]:
        raise NotImplementedError


class LocalStageDispatcher(StageDispatcher):
    def __init__(self, *, stage_executor: StageExecutor) -> None:
        self._stage_executor = stage_executor

    async def run(self, *, stage: StageDefinition, context: StageExecutionContext) -> tuple[StageArtifact, StageMetrics]:
        return await self._stage_executor.execute_stage(stage=stage, context=context)


class CallbackStageDispatcher(StageDispatcher):
    """
    Preview distributed dispatch adapter.

    This dispatcher keeps a narrow callback seam for coordinator/worker style execution,
    but the primary v1 runtime path remains local stage dispatch on a single runtime node.
    """

    def __init__(
        self,
        *,
        dispatch_callback: Callable[
            [StageDefinition, StageExecutionContext],
            Awaitable[tuple[StageArtifact, StageMetrics]],
        ],
    ) -> None:
        self._dispatch_callback = dispatch_callback

    async def run(self, *, stage: StageDefinition, context: StageExecutionContext) -> tuple[StageArtifact, StageMetrics]:
        return await self._dispatch_callback(stage, context)


@dataclass(slots=True)
class SchedulerResult:
    summary: ExecutionSummary
    artifacts: dict[str, StageArtifact]


class StageScheduler:
    def __init__(
        self,
        *,
        dispatcher: StageDispatcher,
        stage_parallelism: int,
    ) -> None:
        self._dispatcher = dispatcher
        self._stage_parallelism = max(1, stage_parallelism)

    async def run(
        self,
        *,
        plan: PhysicalPlan,
        workspace_id: str,
    ) -> SchedulerResult:
        started = time.perf_counter()
        context = StageExecutionContext(workspace_id=workspace_id, plan_id=plan.plan_id)
        remaining: dict[str, StageDefinition] = {stage.stage_id: stage for stage in plan.stages}
        completed: set[str] = set()
        artifacts: dict[str, StageArtifact] = {}
        metrics: dict[str, StageMetrics] = {}

        while remaining:
            ready = [
                stage
                for stage in remaining.values()
                if all(dependency in completed for dependency in stage.dependencies)
            ]
            if not ready:
                unresolved = ", ".join(sorted(remaining.keys()))
                raise RuntimeError(f"Stage DAG contains unresolved dependencies: {unresolved}")

            for batch in _chunk(ready, self._stage_parallelism):
                tasks = [
                    asyncio.create_task(self._execute_with_retry(stage=stage, context=context))
                    for stage in batch
                ]
                batch_results = await asyncio.gather(*tasks)
                for stage, artifact, metric in batch_results:
                    artifacts[stage.stage_id] = artifact
                    metrics[stage.stage_id] = metric
                    completed.add(stage.stage_id)
                    remaining.pop(stage.stage_id, None)

        total_runtime_ms = int((time.perf_counter() - started) * 1000)
        summary = ExecutionSummary(
            plan_id=plan.plan_id,
            total_runtime_ms=total_runtime_ms,
            stage_metrics=[metrics[stage.stage_id] for stage in plan.stages if stage.stage_id in metrics],
        )
        return SchedulerResult(summary=summary, artifacts=artifacts)

    async def _execute_with_retry(
        self,
        *,
        stage: StageDefinition,
        context: StageExecutionContext,
    ) -> tuple[StageDefinition, StageArtifact, StageMetrics]:
        max_attempts = max(1, stage.retry_limit + 1)
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                artifact, metric = await self._dispatcher.run(stage=stage, context=context)
                metric.attempts = attempt
                return stage, artifact, metric
            except Exception as exc:
                last_error = exc
                if attempt >= max_attempts:
                    break
                await asyncio.sleep(min(0.25 * attempt, 1.0))
        assert last_error is not None
        raise last_error


def _chunk(items: list[StageDefinition], size: int) -> list[list[StageDefinition]]:
    return [items[index : index + size] for index in range(0, len(items), size)]
