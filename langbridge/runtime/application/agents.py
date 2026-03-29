
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from langbridge.runtime.models import (
    CreateAgentJobRequest,
    JobType,
    RuntimeMessageRole,
    RuntimeThread,
    RuntimeThreadMessage,
    RuntimeThreadState,
)

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


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
    ) -> dict[str, Any]:
        agent = self._host._resolve_agent(agent_name)
        actor_id = self._host._resolve_actor_id()
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
                    metadata={"runtime_mode": "local_config"},
                    created_at=timestamp,
                    updated_at=timestamp,
                )
                thread = self._host._thread_repository.add(thread)
            else:
                thread = existing_thread
                thread.state = RuntimeThreadState.processing
                thread.updated_at = timestamp
                if str(title or "").strip():
                    thread.title = str(title).strip()
                await self._host._thread_repository.save(thread)

            user_message = RuntimeThreadMessage(
                id=uuid.uuid4(),
                thread_id=thread_id,
                role=RuntimeMessageRole.user,
                content={"text": str(prompt or "").strip()},
                created_at=timestamp,
            )
            user_message = self._host._thread_message_repository.add(user_message)
            thread.last_message_id = user_message.id
            await self._host._thread_repository.save(thread)
            if uow is not None:
                await uow.flush()

            job_id = uuid.uuid4()
            execution = await self._host._runtime_host.create_agent(
                job_id=job_id,
                request=CreateAgentJobRequest(
                    job_type=JobType.AGENT,
                    agent_definition_id=agent.id,
                    workspace_id=self._host.context.workspace_id,
                    actor_id=actor_id,
                    thread_id=thread_id,
                ),
                event_emitter=None,
            )
            if uow is not None:
                await uow.commit()
            response = getattr(execution, "response", {}) or {}
            return {
                "thread_id": thread_id,
                "job_id": job_id,
                "summary": response.get("summary"),
                "result": response.get("result"),
                "visualization": response.get("visualization"),
                "error": response.get("error"),
            }

    async def list_agents(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for name, record in self._host._agents.items():
            definition = dict(record.agent_definition.definition or {})
            tools = definition.get("tools") if isinstance(definition.get("tools"), list) else []
            items.append(
                {
                    "id": record.id,
                    "name": name,
                    "description": record.config.description or record.agent_definition.description,
                    "default": self._host._default_agent is not None and self._host._default_agent.id == record.id,
                    "llm_connection": record.config.llm_connection,
                    "tool_count": len(tools),
                    "tools": [
                        {
                            "name": item.get("name"),
                            "tool_type": item.get("tool_type"),
                            "description": item.get("description"),
                        }
                        for item in tools
                        if isinstance(item, dict)
                    ],
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
            "semantic_model": record.config.semantic_model,
            "dataset": record.config.dataset,
            "instructions": record.config.instructions,
        }
