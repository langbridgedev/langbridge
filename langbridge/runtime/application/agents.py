import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from langbridge.runtime.events import (
    AgentEventVisibility,
    CollectingAgentEventEmitter,
    QueueingAgentStreamEmitter,
)
from langbridge.runtime.models import (
    CreateAgentJobRequest,
    JobType,
    RuntimeMessageRole,
    RuntimeRunStreamEvent,
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
        )
        collector = CollectingAgentEventEmitter()
        try:
            execution = await self._execute_prepared_agent_run(
                prepared=prepared,
                event_emitter=collector,
                agent_mode=agent_mode,
            )
        except Exception:
            await self._reset_thread_after_failure(
                thread_id=prepared.thread.id,
                delete_thread=prepared.created_thread,
            )
            raise
        return self._build_agent_response_payload(
            prepared=prepared,
            execution=execution,
            events=collector.events,
        )

    async def ask_agent_stream(
        self,
        *,
        prompt: str,
        agent_name: str | None = None,
        thread_id: uuid.UUID | None = None,
        title: str | None = None,
        agent_mode: str | None = None,
        metadata_json: dict[str, Any] | None = None,
    ) -> AsyncIterator[RuntimeRunStreamEvent | None]:
        prepared = await self._prepare_agent_run(
            prompt=prompt,
            agent_name=agent_name,
            thread_id=thread_id,
            title=title,
            agent_mode=agent_mode,
            metadata_json=metadata_json,
        )
        sequence = 1
        await self._host._run_streams.open_run(
            run_id=prepared.job_id,
            run_type="agent",
            thread_id=prepared.thread.id,
        )
        await self._host._run_streams.publish(
            RuntimeRunStreamEvent(
                sequence=sequence,
                event="run.started",
                status="in_progress",
                stage="planning",
                message="Run queued. Starting execution.",
                timestamp=datetime.now(timezone.utc),
                run_type="agent",
                run_id=prepared.job_id,
                thread_id=prepared.thread.id,
                job_id=prepared.job_id,
                message_id=prepared.user_message.id,
                visibility=AgentEventVisibility.public.value,
                terminal=False,
                source="runtime",
                details={
                    "agent_name": getattr(prepared.agent.config, "name", None),
                    "user_message_id": str(prepared.user_message.id),
                },
            )
        )
        emitter = QueueingAgentStreamEmitter(
            thread_id=prepared.thread.id,
            job_id=prepared.job_id,
            message_id=prepared.user_message.id,
            enqueue=self._host._run_streams.publish,
            initial_sequence=sequence,
        )

        async def run_execution() -> None:
            try:
                execution = await self._execute_prepared_agent_run(
                prepared=prepared,
                event_emitter=emitter,
                agent_mode=agent_mode,
            )
                payload = self._build_agent_response_payload(prepared=prepared, execution=execution)
                final_event = self._build_terminal_stream_event(
                    sequence=emitter.sequence + 1,
                    event="run.completed",
                    prepared=prepared,
                    execution=execution,
                    payload=payload,
                )
                await self._host._run_streams.publish(final_event)
            except Exception as exc:
                await self._reset_thread_after_failure(
                    thread_id=prepared.thread.id,
                    delete_thread=prepared.created_thread,
                )
                await self._host._run_streams.publish(
                    RuntimeRunStreamEvent(
                        sequence=emitter.sequence + 1,
                        event="run.failed",
                        status="failed",
                        stage="failed",
                        message=str(exc),
                        timestamp=datetime.now(timezone.utc),
                        run_type="agent",
                        run_id=prepared.job_id,
                        thread_id=prepared.thread.id,
                        job_id=prepared.job_id,
                        message_id=prepared.user_message.id,
                        visibility=AgentEventVisibility.public.value,
                        terminal=True,
                        source="runtime",
                        details={"error": str(exc)},
                    )
                )
        task = asyncio.create_task(run_execution())
        task.add_done_callback(_consume_detached_task_exception)
        try:
            stream = await self.stream_run(run_id=prepared.job_id)
            async for event in stream:
                yield event
        finally:
            if task.done():
                try:
                    task.result()
                except Exception:
                    return

    async def stream_run(
        self,
        *,
        run_id: uuid.UUID,
        after_sequence: int = 0,
        heartbeat_interval: float = 10.0,
    ) -> AsyncIterator[RuntimeRunStreamEvent | None]:
        return await self._host._run_streams.subscribe(
            run_id=run_id,
            after_sequence=after_sequence,
            heartbeat_interval=heartbeat_interval,
        )

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
                        "active_run_id": str(job_id),
                        "active_run_type": "agent",
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
                metadata["active_run_id"] = str(job_id)
                metadata["active_run_type"] = "agent"
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

    async def _execute_prepared_agent_run(
        self,
        *,
        prepared: PreparedAgentRun,
        event_emitter,
        agent_mode: str | None = None,
        mcp: bool = False,
    ) -> Any:
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
            "run_id": prepared.job_id,
            "job_id": prepared.job_id,
            "message_id": getattr(assistant_message, "id", None),
            "summary": response.get("summary"),
            "answer": response.get("answer"),
            "result": response.get("result"),
            "visualization": response.get("visualization"),
            "error": response.get("error"),
            "diagnostics": response.get("diagnostics"),
            "events": list(events or []),
        }

    def _build_terminal_stream_event(
        self,
        *,
        sequence: int,
        event: str,
        prepared: PreparedAgentRun,
        execution: Any,
        payload: dict[str, Any],
    ) -> RuntimeRunStreamEvent:
        response = getattr(execution, "response", {}) or {}
        diagnostics = response.get("diagnostics") if isinstance(response, dict) else None
        analyst_outcome = diagnostics.get("analyst_outcome") if isinstance(diagnostics, dict) else None
        clarification_question = self._extract_clarifying_question(
            payload=payload,
            response=response,
        )
        outcome_status = (
            str(analyst_outcome.get("status")).strip().lower()
            if isinstance(analyst_outcome, dict) and analyst_outcome.get("status")
            else ""
        )
        if clarification_question:
            status = "completed"
            stage = "clarification"
            event_name = event
        elif outcome_status in {"access_denied", "invalid_request", "query_error", "execution_error"}:
            status = "failed"
            stage = outcome_status
            event_name = "run.failed"
        elif outcome_status == "empty_result":
            status = "completed"
            stage = "empty_result"
            event_name = event
        else:
            status = "completed"
            stage = "completed"
            event_name = event
        return RuntimeRunStreamEvent(
            sequence=sequence,
            event=event_name,
            status=status,
            stage=stage,
            message=self._terminal_message(payload=payload, response=response),
            timestamp=datetime.now(timezone.utc),
            run_type="agent",
            run_id=prepared.job_id,
            thread_id=prepared.thread.id,
            job_id=prepared.job_id,
            message_id=payload.get("message_id"),
            visibility=AgentEventVisibility.public.value,
            terminal=True,
            source="runtime",
            details={
                "outcome_status": outcome_status or None,
                "result_available": payload.get("result") is not None,
                "visualization_available": payload.get("visualization") is not None,
                "error": payload.get("error"),
                "summary": payload.get("summary"),
                "answer": payload.get("answer"),
                "diagnostics": payload.get("diagnostics"),
                "clarifying_question": clarification_question,
            },
        )

    @staticmethod
    def _terminal_message(*, payload: dict[str, Any], response: dict[str, Any]) -> str:
        clarifying_question = AgentApplication._extract_clarifying_question(
            payload=payload,
            response=response,
        )
        if clarifying_question:
            return clarifying_question
        answer = payload.get("answer")
        if isinstance(answer, str) and answer.strip():
            return answer.strip()
        summary = payload.get("summary")
        if isinstance(summary, str) and summary.strip():
            return summary.strip()
        return "Run completed."

    @staticmethod
    def _extract_clarifying_question(*, payload: dict[str, Any], response: dict[str, Any]) -> str | None:
        diagnostics = payload.get("diagnostics")
        if isinstance(diagnostics, dict):
                question = diagnostics.get("clarifying_question")
                if isinstance(question, str) and question.strip():
                    return question.strip()
                ai_run = diagnostics.get("ai_run")
                if isinstance(ai_run, dict):
                    status = str(ai_run.get("status") or "").strip().lower()
                    route = str(ai_run.get("route") or "").strip().lower()
                    ai_run_diagnostics = ai_run.get("diagnostics")
                    stop_reason = (
                        str(ai_run_diagnostics.get("stop_reason") or "").strip().lower()
                        if isinstance(ai_run_diagnostics, dict)
                        else ""
                    )
                    if (
                        status == "clarification_needed"
                        or stop_reason == "clarification"
                        or "clarification" in route
                    ):
                        answer = payload.get("answer")
                        if isinstance(answer, str) and answer.strip():
                            return answer.strip()
        answer = response.get("answer")
        if isinstance(answer, str) and answer.strip():
            response_diagnostics = response.get("diagnostics")
            if isinstance(response_diagnostics, dict):
                question = response_diagnostics.get("clarifying_question")
                if isinstance(question, str) and question.strip():
                    return question.strip()
                ai_run = response_diagnostics.get("ai_run")
                if isinstance(ai_run, dict):
                    status = str(ai_run.get("status") or "").strip().lower()
                    route = str(ai_run.get("route") or "").strip().lower()
                    ai_run_diagnostics = ai_run.get("diagnostics")
                    stop_reason = (
                        str(ai_run_diagnostics.get("stop_reason") or "").strip().lower()
                        if isinstance(ai_run_diagnostics, dict)
                        else ""
                    )
                    if (
                        status == "clarification_needed"
                        or stop_reason == "clarification"
                        or "clarification" in route
                    ):
                        return answer.strip()
        return None

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
