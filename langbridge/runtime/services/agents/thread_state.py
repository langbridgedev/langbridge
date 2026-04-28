import uuid
from datetime import datetime, timezone
from typing import Any

from langbridge.ai import MetaControllerRun
from langbridge.runtime.models import RuntimeMessageRole, RuntimeThread, RuntimeThreadMessage, RuntimeThreadState
from langbridge.runtime.ports import ThreadMessageStore, ThreadStore
from langbridge.runtime.services.agents.context import AgentConversationContextBuilder
from langbridge.runtime.services.agents.memory import AgentConversationMemoryWriter
from langbridge.runtime.services.errors import ExecutionValidationError


class AgentThreadStateManager:
    def __init__(
        self,
        *,
        thread_repository: ThreadStore,
        thread_message_repository: ThreadMessageStore,
        memory_writer: AgentConversationMemoryWriter,
    ) -> None:
        self._thread_repository = thread_repository
        self._thread_message_repository = thread_message_repository
        self._memory_writer = memory_writer

    async def get_thread_and_last_user_message(
        self,
        thread_id: uuid.UUID,
    ) -> tuple[RuntimeThread, RuntimeThreadMessage, list[RuntimeThreadMessage]]:
        thread = await self._thread_repository.get_by_id(thread_id)
        if thread is None:
            raise ExecutionValidationError(f"Thread with ID {thread_id} does not exist.")

        messages = await self._thread_message_repository.list_for_thread(thread.id)
        if not messages:
            raise ExecutionValidationError(f"Thread {thread.id} has no messages to process.")

        last_message: RuntimeThreadMessage | None = None
        if thread.last_message_id is not None:
            last_message = next((msg for msg in messages if msg.id == thread.last_message_id), None)
        if last_message is None:
            last_message = messages[-1]
        if AgentConversationContextBuilder.role_value(last_message.role) != RuntimeMessageRole.user.value:
            user_messages = [
                msg
                for msg in messages
                if AgentConversationContextBuilder.role_value(msg.role) == RuntimeMessageRole.user.value
            ]
            if not user_messages:
                raise ExecutionValidationError(f"Thread {thread.id} does not contain a user message.")
            last_message = user_messages[-1]
        return thread, last_message, messages

    async def save_thread(self, thread: RuntimeThread) -> RuntimeThread:
        return await self._thread_repository.save(thread)

    async def reset_after_failure(self, *, thread_id: uuid.UUID) -> RuntimeThread | None:
        thread = await self._thread_repository.get_by_id(thread_id)
        if thread is not None:
            self.clear_active_run_metadata(thread)
            self.set_awaiting_user_input(thread)
            thread.updated_at = datetime.now(timezone.utc)
            await self._thread_repository.save(thread)
        return thread

    def persist_ai_state(
        self,
        thread: RuntimeThread,
        response: dict[str, Any],
        *,
        user_query: str,
        ai_run: MetaControllerRun | None = None,
    ) -> dict[str, Any] | None:
        diagnostics = response.get("diagnostics")
        metadata = dict(thread.metadata or {})
        if isinstance(diagnostics, dict):
            diagnostic_ai_run = diagnostics.get("ai_run")
            if isinstance(diagnostic_ai_run, dict):
                metadata["last_ai_run"] = {
                    "execution_mode": diagnostic_ai_run.get("execution_mode"),
                    "status": diagnostic_ai_run.get("status"),
                    "route": diagnostic_ai_run.get("route"),
                    "diagnostics": diagnostic_ai_run.get("diagnostics"),
                }
        continuation_state = self._memory_writer.build_continuation_state(
            response=response,
            user_query=user_query,
            ai_run=ai_run,
        )
        if continuation_state:
            metadata["continuation_state"] = continuation_state
        thread.metadata = metadata
        return continuation_state

    def record_assistant_message(
        self,
        *,
        thread: RuntimeThread,
        user_message: RuntimeThreadMessage,
        response: dict[str, Any],
        agent_id: uuid.UUID,
        ai_run: MetaControllerRun,
        continuation_state: dict[str, Any] | None,
    ) -> RuntimeThreadMessage:
        assistant_message_id = uuid.uuid4()
        content = {
            "summary": response.get("summary"),
            "answer": response.get("answer"),
            "answer_markdown": response.get("answer_markdown"),
            "artifacts": response.get("artifacts"),
            "result": response.get("result"),
            "visualization": response.get("visualization"),
            "research": response.get("research"),
            "diagnostics": response.get("diagnostics"),
        }
        if isinstance(continuation_state, dict) and continuation_state:
            content["continuation_state"] = continuation_state
        assistant_message = RuntimeThreadMessage(
            id=assistant_message_id,
            thread_id=thread.id,
            parent_message_id=user_message.id,
            role=RuntimeMessageRole.assistant,
            content=content,
            model_snapshot={
                "agent_id": str(agent_id),
                "runtime": "langbridge.ai",
                "meta_controller_execution_mode": ai_run.execution_mode,
                "meta_controller_status": ai_run.status,
                "route": ai_run.plan.route,
            },
            error=response.get("error") if isinstance(response.get("error"), dict) else None,
        )
        self._thread_message_repository.add(assistant_message)
        thread.last_message_id = assistant_message_id
        self.set_awaiting_user_input(thread)
        thread.updated_at = datetime.now(timezone.utc)
        return assistant_message

    def clear_active_run_metadata(self, thread: RuntimeThread) -> None:
        metadata = dict(thread.metadata or {})
        metadata.pop("active_run_id", None)
        metadata.pop("active_run_type", None)
        thread.metadata = metadata

    def set_awaiting_user_input(self, thread: RuntimeThread) -> None:
        thread.state = RuntimeThreadState.awaiting_user_input
