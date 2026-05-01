import uuid
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.events import QueueingAgentStreamEmitter
from langbridge.runtime.models import RuntimeJobStatus, RuntimeJobStreamEvent
from langbridge.runtime.services.jobs.context import JobExecutionContext
from langbridge.runtime.services.jobs.handlers import RuntimeJobHandler

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


AGENT_RUN_JOB_TYPE = "agent.run"


@dataclass(slots=True, frozen=True)
class _AgentRunJobPayload:
    agent_definition_id: uuid.UUID | None
    agent_name: str | None
    thread_id: uuid.UUID
    user_message_id: uuid.UUID | None
    agent_mode: str
    mcp: bool


@dataclass(slots=True, frozen=True)
class _PublicProgressEvent:
    message: str
    stage: str
    status: str = "in_progress"


class _AgentRunPublicProgressPolicy:
    def __init__(self) -> None:
        self._events_by_type = {
            "MetaControllerStarted": _PublicProgressEvent(
                message="Selecting analyst.",
                stage="selecting_agent",
            ),
            "AgentRoutingStarted": _PublicProgressEvent(
                message="Selecting analyst.",
                stage="selecting_agent",
            ),
            "AgentRouteSelected": _PublicProgressEvent(
                message="Selecting analyst.",
                stage="selecting_agent",
            ),
            "AnalystModeSelectionStarted": _PublicProgressEvent(
                message="Selecting analyst.",
                stage="selecting_agent",
            ),
            "AgentToolStarted": _PublicProgressEvent(
                message="Selecting analyst.",
                stage="selecting_agent",
            ),
            "SqlGenerationStarted": _PublicProgressEvent(
                message="Generating SQL.",
                stage="generating_sql",
            ),
            "SqlExecutionStarted": _PublicProgressEvent(
                message="Running governed query.",
                stage="running_query",
            ),
            "AnalystEvidenceReviewStarted": _PublicProgressEvent(
                message="Reviewing evidence.",
                stage="reviewing_evidence",
            ),
            "DeepResearchStarted": _PublicProgressEvent(
                message="Reviewing evidence.",
                stage="reviewing_evidence",
            ),
            "SemanticSearchStarted": _PublicProgressEvent(
                message="Reviewing evidence.",
                stage="reviewing_evidence",
            ),
            "WebSearchStarted": _PublicProgressEvent(
                message="Reviewing evidence.",
                stage="reviewing_evidence",
            ),
            "FinalReviewStarted": _PublicProgressEvent(
                message="Reviewing evidence.",
                stage="reviewing_evidence",
            ),
            "PresentationStarted": _PublicProgressEvent(
                message="Composing final answer.",
                stage="composing_response",
            ),
            "PresentationCompositionStarted": _PublicProgressEvent(
                message="Composing final answer.",
                stage="composing_response",
            ),
        }

    def event_for(self, event: RuntimeJobStreamEvent) -> _PublicProgressEvent | None:
        raw_event_type = str(event.raw_event_type or "").strip()
        if not raw_event_type:
            return None
        return self._events_by_type.get(raw_event_type)


class AgentRunJobHandler(RuntimeJobHandler):
    def __init__(self, *, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host
        self._public_progress = _AgentRunPublicProgressPolicy()

    @property
    def job_type(self) -> str:
        return AGENT_RUN_JOB_TYPE

    async def handle(self, context: JobExecutionContext) -> dict[str, Any]:
        payload = self._parse_payload(context.job.payload)
        job_host = self._host.with_context(self._job_runtime_context(context))
        task = await context.upsert_task(
            task_key="agent_run",
            task_type=self.job_type,
            status=RuntimeJobStatus.running.value,
            attempt=int(context.job.attempt or 1),
            max_attempts=int(context.job.max_attempts or 1),
            input=self._task_input(payload),
            state={"stage": "planning"},
        )
        await context.emit(
            event_type="run.started",
            message="Agent run started.",
            status="in_progress",
            stage="planning",
            task_id=task.id,
            visibility="public",
            source="agent-run",
            details=self._event_details(payload),
        )
        public_progress_stages: set[str] = set()
        emitter = QueueingAgentStreamEmitter(
            thread_id=payload.thread_id,
            job_id=context.job.id,
            message_id=payload.user_message_id,
            enqueue=lambda event: self._publish_stream_event(
                context=context,
                task_id=task.id,
                event=event,
                public_progress_stages=public_progress_stages,
            ),
            initial_sequence=int(context.job.last_sequence or 0),
        )

        try:
            raw_result = await job_host.execute_agent_run(
                job_id=context.job.id,
                payload=dict(context.job.payload or {}),
                event_emitter=emitter,
            )
        except Exception as exc:
            error = {"type": type(exc).__name__, "message": str(exc)}
            await context.upsert_task(
                task_key="agent_run",
                task_type=self.job_type,
                status=RuntimeJobStatus.failed.value,
                attempt=int(context.job.attempt or 1),
                max_attempts=int(context.job.max_attempts or 1),
                input=self._task_input(payload),
                state={"stage": "failed"},
                error=error,
            )
            await context.emit(
                event_type="run.failed",
                message=str(exc) or "Agent run failed.",
                status=RuntimeJobStatus.failed.value,
                stage="failed",
                task_id=task.id,
                visibility="public",
                source="agent-run",
                details={"error": error, **self._event_details(payload)},
            )
            raise

        result = self._json_safe_mapping(raw_result)
        await self._record_artifacts(context=context, task_id=task.id, result=result)
        await context.upsert_task(
            task_key="agent_run",
            task_type=self.job_type,
            status=RuntimeJobStatus.succeeded.value,
            attempt=int(context.job.attempt or 1),
            max_attempts=int(context.job.max_attempts or 1),
            input=self._task_input(payload),
            state={"stage": "completed"},
            result=result,
            diagnostics=self._diagnostics(result),
        )
        await context.emit(
            event_type="run.completed",
            message=self._success_message(result),
            status="completed",
            stage=self._completion_stage(result),
            task_id=task.id,
            visibility="public",
            source="agent-run",
            details={**self._event_details(payload), **self._diagnostics(result)},
        )
        return result

    def _parse_payload(self, payload: dict[str, Any]) -> _AgentRunJobPayload:
        return _AgentRunJobPayload(
            agent_definition_id=self._uuid_or_none(payload.get("agent_definition_id")),
            agent_name=str(payload.get("agent_name") or "").strip() or None,
            thread_id=self._required_uuid(payload.get("thread_id"), "thread_id"),
            user_message_id=self._uuid_or_none(payload.get("user_message_id")),
            agent_mode=str(payload.get("agent_mode") or "auto").strip() or "auto",
            mcp=bool(payload.get("mcp")),
        )

    def _job_runtime_context(self, context: JobExecutionContext) -> RuntimeContext:
        return RuntimeContext.build(
            workspace_id=context.job.workspace_id,
            actor_id=context.job.actor_id,
            roles=self._host.context.roles,
            request_id=f"job:{context.job.id}",
        )

    def _task_input(self, payload: _AgentRunJobPayload) -> dict[str, Any]:
        return {
            "agent_definition_id": str(payload.agent_definition_id) if payload.agent_definition_id else None,
            "agent_name": payload.agent_name,
            "thread_id": str(payload.thread_id),
            "user_message_id": str(payload.user_message_id) if payload.user_message_id else None,
            "agent_mode": payload.agent_mode,
            "mcp": payload.mcp,
        }

    def _event_details(self, payload: _AgentRunJobPayload) -> dict[str, Any]:
        return {
            "job_type": self.job_type,
            "agent_definition_id": str(payload.agent_definition_id) if payload.agent_definition_id else None,
            "agent_name": payload.agent_name,
            "thread_id": str(payload.thread_id),
            "message_id": str(payload.user_message_id) if payload.user_message_id else None,
            "agent_mode": payload.agent_mode,
        }

    async def _publish_stream_event(
        self,
        *,
        context: JobExecutionContext,
        task_id: uuid.UUID,
        event: RuntimeJobStreamEvent,
        public_progress_stages: set[str],
    ) -> None:
        event_details = {
            **dict(event.details or {}),
            "job_type": self.job_type,
            "thread_id": str(event.thread_id) if event.thread_id else None,
            "message_id": str(event.message_id) if event.message_id else None,
        }
        await context.emit(
            event_type=event.event,
            message=event.message,
            status=event.status,
            stage=event.stage,
            task_id=task_id,
            visibility="internal",
            terminal=False,
            source=event.source,
            raw_event_type=event.raw_event_type,
            details=event_details,
        )
        public_event = self._public_progress.event_for(event)
        if public_event is not None and public_event.stage not in public_progress_stages:
            public_progress_stages.add(public_event.stage)
            await context.emit(
                event_type="run.progress",
                message=public_event.message,
                status=public_event.status,
                stage=public_event.stage,
                task_id=task_id,
                visibility="public",
                terminal=False,
                source="agent-progress",
                raw_event_type=event.raw_event_type,
                details={
                    "job_type": self.job_type,
                    "thread_id": str(event.thread_id) if event.thread_id else None,
                    "message_id": str(event.message_id) if event.message_id else None,
                    "raw_event_type": event.raw_event_type,
                },
            )
        await self._commit_current_unit_of_work()

    async def _commit_current_unit_of_work(self) -> None:
        controller = getattr(self._host, "persistence_controller", None)
        if controller is None:
            return
        uow = controller.current_uow()
        if uow is not None:
            await uow.commit()

    async def _record_artifacts(
        self,
        *,
        context: JobExecutionContext,
        task_id: uuid.UUID,
        result: dict[str, Any],
    ) -> None:
        await context.add_artifact(
            artifact_key="agent_response",
            artifact_type="json",
            title="Agent response",
            task_id=task_id,
            data=result,
            metadata=self._diagnostics(result),
        )
        diagnostics = result.get("diagnostics")
        if isinstance(diagnostics, dict):
            await context.add_artifact(
                artifact_key="agent_diagnostics",
                artifact_type="json",
                title="Agent diagnostics",
                task_id=task_id,
                data=diagnostics,
                metadata={"thread_id": result.get("thread_id")},
            )
        artifacts = result.get("artifacts")
        if isinstance(artifacts, list) and artifacts:
            await context.add_artifact(
                artifact_key="agent_artifacts",
                artifact_type="json",
                title="Agent artifacts",
                task_id=task_id,
                data=artifacts,
                metadata={"artifact_count": len(artifacts)},
            )

    def _diagnostics(self, result: dict[str, Any]) -> dict[str, Any]:
        return {
            "thread_id": result.get("thread_id"),
            "message_id": result.get("message_id"),
            "summary": result.get("summary"),
            "has_result": result.get("result") is not None,
            "has_visualization": result.get("visualization") is not None,
            "has_error": result.get("error") is not None,
        }

    def _completion_stage(self, result: dict[str, Any]) -> str:
        diagnostics = result.get("diagnostics")
        if isinstance(diagnostics, dict) and diagnostics.get("clarifying_question"):
            return "clarification"
        if result.get("error") is not None:
            return "completed_with_error"
        return "completed"

    def _success_message(self, result: dict[str, Any]) -> str:
        answer = result.get("answer")
        if isinstance(answer, str) and answer.strip():
            return answer.strip()
        summary = result.get("summary")
        if isinstance(summary, str) and summary.strip():
            return summary.strip()
        return "Agent run completed."

    def _required_uuid(self, value: Any, field_name: str) -> uuid.UUID:
        parsed = self._uuid_or_none(value)
        if parsed is None:
            raise ValueError(f"Agent run job payload requires {field_name}.")
        return parsed

    def _uuid_or_none(self, value: Any) -> uuid.UUID | None:
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except (TypeError, ValueError):
            return None

    def _json_safe_mapping(self, value: dict[str, Any]) -> dict[str, Any]:
        return {
            str(key): self._json_safe_value(item)
            for key, item in dict(value or {}).items()
        }

    def _json_safe_value(self, value: Any) -> Any:
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, dict):
            return {
                str(key): self._json_safe_value(item)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._json_safe_value(item) for item in value]
        if isinstance(value, tuple):
            return [self._json_safe_value(item) for item in value]
        return value
