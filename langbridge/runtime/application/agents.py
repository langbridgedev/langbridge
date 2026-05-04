import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from langbridge.runtime.application.job_handlers.agent_run import AGENT_RUN_JOB_TYPE
from langbridge.runtime.events import (
    CollectingAgentEventEmitter,
    QueueingAgentStreamEmitter,
)
from langbridge.runtime.models import (
    CreateAgentJobRequest,
    CreateRuntimeJobRequest,
    JobType,
    RuntimeJobStreamEvent,
    RuntimeMessageRole,
    RuntimeThread,
    RuntimeThreadMessage,
    RuntimeThreadState,
)

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


@dataclass(slots=True)
class PreparedAgentRun:
    agent: Any
    actor_id: uuid.UUID
    thread: RuntimeThread
    user_message: RuntimeThreadMessage
    job_id: uuid.UUID
    created_thread: bool = False


def _consume_detached_task_exception(task: asyncio.Task[Any]) -> None:
    if not task.done():
        return
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception:
        return


class AgentApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    async def create_agent(self, *args: Any, **kwargs: Any) -> Any:
        async with self._host._runtime_operation_scope() as uow:
            result = await self._host._runtime_host.create_agent(*args, **kwargs)
            if uow is not None:
                await uow.commit()
            return result

    async def ask_agent(
        self,
        *,
        prompt: str,
        agent_name: str | None = None,
        thread_id: uuid.UUID | None = None,
        title: str | None = None,
        agent_mode: str | None = None,
        metadata_json: dict[str, Any] | None = None,
        mcp: bool = False,
    ) -> dict[str, Any]:
        prepared = await self._prepare_agent_run(
            prompt=prompt,
            agent_name=agent_name,
            thread_id=thread_id,
            title=title,
            agent_mode=agent_mode,
            metadata_json=metadata_json,
            mcp=mcp,
            queue_name="inline",
        )
        collector = CollectingAgentEventEmitter()
        try:
            execution = await self._execute_prepared_agent_run(
                prepared=prepared,
                event_emitter=collector,
                agent_mode=agent_mode,
            )
        except Exception as exc:
            await self._reset_thread_after_failure(
                thread_id=prepared.thread.id,
                delete_thread=prepared.created_thread,
            )
            await self._host.services.jobs.fail_job(
                job_id=prepared.job_id,
                exc=exc,
                event_type="run.failed",
                stage="failed",
                message=str(exc) or "Agent run failed.",
                event_details=self._build_failure_terminal_details(
                    prepared=prepared,
                    exc=exc,
                ),
            )
            raise
        payload = self._build_agent_response_payload(
            prepared=prepared,
            execution=execution,
            events=collector.events,
        )
        terminal = self._build_agent_terminal_event(
            prepared=prepared,
            execution=execution,
            payload=payload,
        )
        await self._host.services.jobs.complete_job(
            job_id=prepared.job_id,
            result=payload,
            message=terminal["message"],
            event_type=terminal["event_type"],
            event_status=terminal["status"],
            stage=terminal["stage"],
            event_details=terminal["details"],
        )
        return payload

    async def ask_agent_stream(
        self,
        *,
        prompt: str,
        agent_name: str | None = None,
        thread_id: uuid.UUID | None = None,
        title: str | None = None,
        agent_mode: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> AsyncIterator[RuntimeJobStreamEvent | None]:
        prepared = await self._prepare_agent_run(
            prompt=prompt,
            agent_name=agent_name,
            thread_id=thread_id,
            title=title,
            agent_mode=agent_mode,
            metadata_json=metadata_json,
            queue_name="inline",
        )
        emitter = QueueingAgentStreamEmitter(
            thread_id=prepared.thread.id,
            job_id=prepared.job_id,
            message_id=prepared.user_message.id,
            enqueue=self._publish_agent_stream_event,
            initial_sequence=0,
        )

        async def run_execution() -> None:
            try:
                execution = await self._execute_prepared_agent_run(
                    prepared=prepared,
                    event_emitter=emitter,
                    agent_mode=agent_mode,
                )
                payload = self._build_agent_response_payload(prepared=prepared, execution=execution)
                terminal = self._build_agent_terminal_event(
                    prepared=prepared,
                    execution=execution,
                    payload=payload,
                )
                await self._host.services.jobs.complete_job(
                    job_id=prepared.job_id,
                    result=payload,
                    message=terminal["message"],
                    event_type=terminal["event_type"],
                    event_status=terminal["status"],
                    stage=terminal["stage"],
                    event_details=terminal["details"],
                )
            except Exception as exc:
                await self._reset_thread_after_failure(
                    thread_id=prepared.thread.id,
                    delete_thread=prepared.created_thread,
                )
                await self._host.services.jobs.fail_job(
                    job_id=prepared.job_id,
                    exc=exc,
                    event_type="run.failed",
                    stage="failed",
                    message=str(exc) or "Agent run failed.",
                    event_details=self._build_failure_terminal_details(
                        prepared=prepared,
                        exc=exc,
                    ),
                )
        task = asyncio.create_task(run_execution())
        task.add_done_callback(_consume_detached_task_exception)
        try:
            stream = await self.stream_job(job_id=prepared.job_id)
            async for event in stream:
                yield event
        finally:
            if task.done():
                try:
                    task.result()
                except Exception:
                    return

    async def create_agent_run_job(
        self,
        *,
        prompt: str,
        agent_name: str | None = None,
        thread_id: uuid.UUID | None = None,
        title: str | None = None,
        agent_mode: str | None = None,
        metadata_json: dict[str, Any] | None = None,
        mcp: bool = False,
    ) -> dict[str, Any]:
        prepared = await self._prepare_agent_run(
            prompt=prompt,
            agent_name=agent_name,
            thread_id=thread_id,
            title=title,
            agent_mode=agent_mode,
            metadata_json=metadata_json,
            mcp=mcp,
            queue_name="default",
        )
        self._host.wake_job_processor()
        return {
            "status": "queued",
            "job_id": prepared.job_id,
            "job_type": AGENT_RUN_JOB_TYPE,
            "thread_id": prepared.thread.id,
            "message_id": prepared.user_message.id,
            "agent_name": getattr(prepared.agent.config, "name", None),
            "stream_path": f"/api/runtime/v1/jobs/{prepared.job_id}/stream",
        }

    async def execute_agent_run(
        self,
        *,
        job_id: uuid.UUID,
        payload: dict[str, Any],
        event_emitter,
    ) -> dict[str, Any]:
        prepared = await self._load_prepared_agent_run(job_id=job_id, payload=payload)
        try:
            execution = await self._execute_prepared_agent_run(
                prepared=prepared,
                event_emitter=event_emitter,
                agent_mode=str(payload.get("agent_mode") or "auto").strip() or "auto",
                mcp=bool(payload.get("mcp")),
                record_start_event=False,
            )
        except Exception:
            await self._reset_thread_after_failure(
                thread_id=prepared.thread.id,
                delete_thread=prepared.created_thread,
            )
            raise
        return self._build_agent_response_payload(prepared=prepared, execution=execution)

    async def stream_job(
        self,
        *,
        job_id: uuid.UUID,
        after_sequence: int = 0,
        heartbeat_interval: float = 10.0,
    ) -> AsyncIterator[RuntimeJobStreamEvent | None]:
        return await self._host.services.jobs.stream_events(
            job_id=job_id,
            after_sequence=after_sequence,
            heartbeat_interval=heartbeat_interval,
        )

    async def _publish_agent_stream_event(self, event: RuntimeJobStreamEvent) -> None:
        if event.job_id is None:
            raise ValueError("Agent stream events must include a job_id.")
        await self._host.services.jobs.append_event(
            job_id=event.job_id,
            task_id=None,
            event_type=event.event,
            status=event.status,
            stage=event.stage,
            message=event.message,
            visibility=event.visibility,
            terminal=event.terminal,
            source=event.source,
            raw_event_type=event.raw_event_type,
            details={
                **dict(event.details or {}),
                "thread_id": str(event.thread_id) if event.thread_id is not None else None,
                "message_id": str(event.message_id) if event.message_id is not None else None,
            },
        )
        await self._commit_current_unit_of_work()

    async def list_agents(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for name, record in self._host._agents.items():
            definition = dict(record.agent_definition.definition or {})
            tools = self._agent_tools(definition)
            items.append(
                {
                    "id": record.id,
                    "name": name,
                    "description": record.config.description or record.agent_definition.description,
                    "default": self._host._default_agent is not None and self._host._default_agent.id == record.id,
                    "llm_connection": self._agent_llm_connection(record.config),
                    "tool_count": len(tools),
                    "tools": tools,
                }
            )
        items.sort(key=lambda item: (not bool(item["default"]), str(item["name"]).lower()))
        return items

    async def get_agent(
        self,
        *,
        agent_ref: str,
    ) -> dict[str, Any]:
        record = self._host._resolve_agent_record(agent_ref)
        items = await self.list_agents()
        summary = next((item for item in items if item["id"] == record.id), None) or {}
        return {
            **summary,
            "definition": dict(record.agent_definition.definition or {}),
            "semantic_models": list(record.config.analyst_scope.semantic_models),
            "datasets": list(record.config.analyst_scope.datasets),
            "instructions": record.config.prompts.user_prompt,
        }

    async def _prepare_agent_run(
        self,
        *,
        prompt: str,
        agent_name: str | None,
        thread_id: uuid.UUID | None,
        title: str | None,
        agent_mode: str | None,
        metadata_json: dict[str, Any] | None = None,
        mcp: bool = False,
        queue_name: str = "default",
    ) -> PreparedAgentRun:
        agent = self._host._resolve_agent(agent_name)
        actor_id = self._host._resolve_actor_id()
        job_id = uuid.uuid4()
        timestamp = datetime.now(timezone.utc)
        async with self._host._runtime_operation_scope() as uow:
            existing_thread = None
            if thread_id is not None:
                existing_thread = await self._host._thread_repository.get_by_id(thread_id)
                if existing_thread is None:
                    raise ValueError(f"Thread '{thread_id}' was not found.")
                if existing_thread.workspace_id != self._host.context.workspace_id:
                    raise ValueError("Thread does not belong to the current runtime workspace.")
            if existing_thread is None:
                thread_id = uuid.uuid4()
                thread = RuntimeThread(
                    id=thread_id,
                    workspace_id=self._host.context.workspace_id,
                    title=str(title or "").strip() or agent.config.name,
                    created_by=actor_id,
                    state=RuntimeThreadState.processing,
                    metadata={
                        "runtime_mode": "local_config",
                        "active_job_id": str(job_id),
                        "active_job_type": AGENT_RUN_JOB_TYPE,
                    },
                    created_at=timestamp,
                    updated_at=timestamp,
                )
                thread = self._host._thread_repository.add(thread)
            else:
                thread = existing_thread
                thread.state = RuntimeThreadState.processing
                thread.updated_at = timestamp
                metadata = dict(thread.metadata or {})
                metadata["active_job_id"] = str(job_id)
                metadata["active_job_type"] = AGENT_RUN_JOB_TYPE
                thread.metadata = metadata
                if str(title or "").strip():
                    thread.title = str(title).strip()
                await self._host._thread_repository.save(thread)

            metadata_payload = dict(metadata_json) if isinstance(metadata_json, dict) else None
            user_message = RuntimeThreadMessage(
                id=uuid.uuid4(),
                thread_id=thread_id,
                role=RuntimeMessageRole.user,
                content={
                    "text": str(prompt or "").strip(),
                    "agent_mode": str(agent_mode or "auto").strip() or "auto",
                    **({"metadata_json": metadata_payload} if metadata_payload is not None else {}),
                    **({"context": {"metadata_json": metadata_payload}} if metadata_payload is not None else {}),
                },
                model_snapshot={
                    "agent_mode": str(agent_mode or "auto").strip() or "auto",
                    **({"metadata_json": metadata_payload} if metadata_payload is not None else {}),
                },
                created_at=timestamp,
            )
            user_message = self._host._thread_message_repository.add(user_message)
            thread.last_message_id = user_message.id
            await self._host._thread_repository.save(thread)
            await self._host.services.jobs.create_job(
                workspace_id=self._host.context.workspace_id,
                actor_id=actor_id,
                job_id=job_id,
                request=CreateRuntimeJobRequest(
                    job_type=AGENT_RUN_JOB_TYPE,
                    queue_name=queue_name,
                    subject_type="thread",
                    subject_id=thread.id,
                    payload={
                        "agent_definition_id": str(agent.id),
                        "agent_name": getattr(agent.config, "name", None),
                        "thread_id": str(thread.id),
                        "user_message_id": str(user_message.id),
                        "agent_mode": str(agent_mode or "auto").strip() or "auto",
                        "mcp": bool(mcp),
                    },
                ),
            )
            if uow is not None:
                await uow.commit()

        return PreparedAgentRun(
            agent=agent,
            actor_id=actor_id,
            thread=thread,
            user_message=user_message,
            job_id=job_id,
            created_thread=existing_thread is None,
        )

    async def _load_prepared_agent_run(
        self,
        *,
        job_id: uuid.UUID,
        payload: dict[str, Any],
    ) -> PreparedAgentRun:
        agent_ref = str(payload.get("agent_definition_id") or payload.get("agent_name") or "").strip()
        agent = (
            self._host._resolve_agent_record(agent_ref)
            if agent_ref
            else self._host._resolve_agent(None)
        )
        thread_id = uuid.UUID(str(payload.get("thread_id")))
        raw_user_message_id = payload.get("user_message_id")
        user_message_id = uuid.UUID(str(raw_user_message_id)) if raw_user_message_id else None
        async with self._host._runtime_operation_scope():
            thread = await self._host._thread_repository.get_by_id(thread_id)
            if thread is None:
                raise ValueError(f"Thread '{thread_id}' was not found.")
            if thread.workspace_id != self._host.context.workspace_id:
                raise ValueError("Thread does not belong to the current runtime workspace.")
            messages = await self._host._thread_message_repository.list_for_thread(thread.id)

        user_message = None
        if user_message_id is not None:
            user_message = next((message for message in messages if message.id == user_message_id), None)
        if user_message is None:
            user_messages = [
                message
                for message in messages
                if str(getattr(message.role, "value", message.role)) == RuntimeMessageRole.user.value
            ]
            user_message = user_messages[-1] if user_messages else None
        if user_message is None:
            raise ValueError(f"Thread '{thread_id}' does not contain a user message for agent execution.")

        return PreparedAgentRun(
            agent=agent,
            actor_id=self._host._resolve_actor_id(),
            thread=thread,
            user_message=user_message,
            job_id=job_id,
            created_thread=False,
        )

    async def _execute_prepared_agent_run(
        self,
        *,
        prepared: PreparedAgentRun,
        event_emitter,
        agent_mode: str | None = None,
        mcp: bool = False,
        record_start_event: bool = True,
    ) -> Any:
        if record_start_event:
            await self._host.services.jobs.start_job(
                job_id=prepared.job_id,
                worker_id="agent-inline",
                event_type="run.started",
                event_status="in_progress",
                stage="planning",
                message="Agent run started.",
                event_details={
                    "job_type": AGENT_RUN_JOB_TYPE,
                    "thread_id": str(prepared.thread.id),
                    "message_id": str(prepared.user_message.id),
                    "worker_id": "agent-inline",
                },
            )
        async with self._host._runtime_operation_scope() as uow:
            execution = await self._host._runtime_host.create_agent(
                job_id=prepared.job_id,
                request=CreateAgentJobRequest(
                    job_type=JobType.AGENT,
                    agent_definition_id=prepared.agent.id,
                    workspace_id=self._host.context.workspace_id,
                    actor_id=prepared.actor_id,
                    thread_id=prepared.thread.id,
                    mcp=mcp,
                    agent_mode=agent_mode or "auto",
                ),
                event_emitter=event_emitter,
            )
            if uow is not None:
                await uow.commit()
            return execution

    async def _commit_current_unit_of_work(self) -> None:
        controller = self._host.persistence_controller
        if controller is None:
            return
        uow = controller.current_uow()
        if uow is not None:
            await uow.commit()

    async def _reset_thread_after_failure(self, *, thread_id: uuid.UUID, delete_thread: bool = False) -> None:
        async with self._host._runtime_operation_scope() as uow:
            reset = getattr(
                self._host._runtime_host.services.agent_execution,
                "reset_thread_after_failure",
                None,
            )
            if callable(reset):
                await reset(thread_id=thread_id)
            elif delete_thread:
                await self._host._thread_message_repository.delete_for_thread(thread_id)
                await self._host._thread_repository.delete(thread_id)
            if uow is not None:
                await uow.commit()

    def _build_agent_response_payload(
        self,
        *,
        prepared: PreparedAgentRun,
        execution: Any,
        events: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        response = getattr(execution, "response", {}) or {}
        assistant_message = getattr(execution, "assistant_message", None)
        return {
            "thread_id": prepared.thread.id,
            "job_id": prepared.job_id,
            "message_id": getattr(assistant_message, "id", None),
            "answer_markdown": response.get("answer_markdown"),
            "artifacts": response.get("artifacts"),
            "diagnostics": response.get("diagnostics"),
            "metadata": response.get("metadata"),
            "error": response.get("error"),
            "events": list(events or []),
        }

    def _build_agent_terminal_event(
        self,
        *,
        prepared: PreparedAgentRun,
        execution: Any,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        response = getattr(execution, "response", {}) or {}
        diagnostics = response.get("diagnostics") if isinstance(response, dict) else None
        analyst_outcome = diagnostics.get("analyst_outcome") if isinstance(diagnostics, dict) else None
        outcome_status = (
            str(analyst_outcome.get("status")).strip().lower()
            if isinstance(analyst_outcome, dict) and analyst_outcome.get("status")
            else ""
        )
        clarification_question = self._extract_clarifying_question(
            payload=payload,
            response=response,
        )
        if clarification_question:
            event_type = "run.completed"
            status = "completed"
            stage = "clarification"
        elif outcome_status in {"access_denied", "invalid_request", "query_error", "execution_error"}:
            event_type = "run.failed"
            status = "failed"
            stage = outcome_status
        elif outcome_status == "empty_result":
            event_type = "run.completed"
            status = "completed"
            stage = "empty_result"
        else:
            event_type = "run.completed"
            status = "completed"
            stage = "completed"

        return {
            "event_type": event_type,
            "status": status,
            "stage": stage,
            "message": self._terminal_message(payload=payload, response=response),
            "details": {
                "job_type": AGENT_RUN_JOB_TYPE,
                "thread_id": str(prepared.thread.id),
                "message_id": str(payload.get("message_id")) if payload.get("message_id") else None,
                "outcome_status": outcome_status or None,
                "artifact_count": self._artifact_count(payload),
                "table_available": self._has_artifact_type(payload, "table"),
                "chart_available": self._has_artifact_type(payload, "chart"),
                "error": payload.get("error"),
                "diagnostics": payload.get("diagnostics"),
                "clarifying_question": clarification_question,
            },
        }

    def _build_failure_terminal_details(
        self,
        *,
        prepared: PreparedAgentRun,
        exc: BaseException,
    ) -> dict[str, Any]:
        return {
            "job_type": AGENT_RUN_JOB_TYPE,
            "thread_id": str(prepared.thread.id),
            "message_id": str(prepared.user_message.id),
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }

    def _terminal_message(self, *, payload: dict[str, Any], response: dict[str, Any]) -> str:
        clarifying_question = self._extract_clarifying_question(
            payload=payload,
            response=response,
        )
        if clarifying_question:
            return clarifying_question
        answer_markdown = payload.get("answer_markdown")
        if isinstance(answer_markdown, str) and answer_markdown.strip():
            return answer_markdown.strip()
        return "Run completed."

    def _extract_clarifying_question(self, *, payload: dict[str, Any], response: dict[str, Any]) -> str | None:
        diagnostics = payload.get("diagnostics")
        if isinstance(diagnostics, dict):
            question = diagnostics.get("clarifying_question")
            if isinstance(question, str) and question.strip():
                return question.strip()
            ai_run = diagnostics.get("ai_run")
            if self._is_clarification_ai_run(ai_run):
                answer_markdown = payload.get("answer_markdown")
                if isinstance(answer_markdown, str) and answer_markdown.strip():
                    return answer_markdown.strip()

        answer_markdown = response.get("answer_markdown")
        if isinstance(answer_markdown, str) and answer_markdown.strip():
            response_diagnostics = response.get("diagnostics")
            if isinstance(response_diagnostics, dict):
                question = response_diagnostics.get("clarifying_question")
                if isinstance(question, str) and question.strip():
                    return question.strip()
                if self._is_clarification_ai_run(response_diagnostics.get("ai_run")):
                    return answer_markdown.strip()
        return None

    @staticmethod
    def _artifact_count(payload: dict[str, Any]) -> int:
        artifacts = payload.get("artifacts")
        return len(artifacts) if isinstance(artifacts, list) else 0

    @staticmethod
    def _has_artifact_type(payload: dict[str, Any], artifact_type: str) -> bool:
        artifacts = payload.get("artifacts")
        if not isinstance(artifacts, list):
            return False
        expected = artifact_type.strip().lower()
        return any(
            isinstance(artifact, dict)
            and str(artifact.get("type") or "").strip().lower() == expected
            for artifact in artifacts
        )

    def _is_clarification_ai_run(self, value: Any) -> bool:
        if not isinstance(value, dict):
            return False
        status = str(value.get("status") or "").strip().lower()
        route = str(value.get("route") or "").strip().lower()
        diagnostics = value.get("diagnostics")
        stop_reason = (
            str(diagnostics.get("stop_reason") or "").strip().lower()
            if isinstance(diagnostics, dict)
            else ""
        )
        return status == "clarification_needed" or stop_reason == "clarification" or "clarification" in route

    @staticmethod
    def _agent_llm_connection(config: Any) -> str | None:
        llm_scope = getattr(config, "llm_scope", None)
        if llm_scope is None:
            return None
        value = getattr(llm_scope, "llm_connection", None)
        return str(value).strip() or None if value is not None else None

    @staticmethod
    def _agent_tools(definition: dict[str, Any]) -> list[dict[str, Any]]:
        tools_payload = definition.get("tools")
        if isinstance(tools_payload, list):
            return [
                {
                    "name": item.get("name"),
                    "tool_type": item.get("tool_type"),
                    "description": item.get("description"),
                }
                for item in tools_payload
                if isinstance(item, dict)
            ]

        tools: list[dict[str, Any]] = []
        analyst_scope = definition.get("analyst_scope")
        if isinstance(analyst_scope, dict):
            semantic_models = analyst_scope.get("semantic_models")
            datasets = analyst_scope.get("datasets")
            if isinstance(semantic_models, list) and semantic_models:
                tools.append(
                    {
                        "name": "semantic_analysis",
                        "tool_type": "semantic",
                        "description": "Semantic-model analysis scope.",
                    }
                )
            if isinstance(datasets, list) and datasets:
                tools.append(
                    {
                        "name": "dataset_analysis",
                        "tool_type": "sql",
                        "description": "Dataset analysis scope.",
                    }
                )
        web_search_scope = definition.get("web_search_scope")
        if isinstance(web_search_scope, dict) and web_search_scope.get("enabled"):
            tools.append(
                {
                    "name": "web_search",
                    "tool_type": "web_search",
                    "description": "Web research scope.",
                }
            )
        return tools
