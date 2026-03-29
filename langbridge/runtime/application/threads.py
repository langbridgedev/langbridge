
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from langbridge.runtime.models import RuntimeThread, RuntimeThreadState

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


class ThreadApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    async def create_thread(
        self,
        *,
        title: str | None = None,
    ) -> dict[str, Any]:
        actor_id = self._host._resolve_actor_id()
        timestamp = datetime.now(timezone.utc)
        thread = RuntimeThread(
            id=uuid.uuid4(),
            workspace_id=self._host.context.workspace_id,
            title=str(title or "").strip() or "New thread",
            created_by=actor_id,
            state=RuntimeThreadState.awaiting_user_input,
            metadata={"runtime_mode": "local_config"},
            created_at=timestamp,
            updated_at=timestamp,
        )
        async with self._host._runtime_operation_scope() as uow:
            thread = self._host._thread_repository.add(thread)
            if uow is not None:
                await uow.commit()
            return self._host._serialize_thread(thread)

    async def update_thread(
        self,
        *,
        thread_id: uuid.UUID,
        title: str | None = None,
    ) -> dict[str, Any]:
        async with self._host._runtime_operation_scope() as uow:
            thread = await self._host._thread_repository.get_by_id(thread_id)
            if thread is None or thread.workspace_id != self._host.context.workspace_id:
                raise ValueError(f"Thread '{thread_id}' was not found.")
            normalized_title = str(title or "").strip()
            thread.title = normalized_title or None
            thread.updated_at = datetime.now(timezone.utc)
            await self._host._thread_repository.save(thread)
            if uow is not None:
                await uow.commit()
            return self._host._serialize_thread(thread)

    async def delete_thread(
        self,
        *,
        thread_id: uuid.UUID,
    ) -> dict[str, Any]:
        async with self._host._runtime_operation_scope() as uow:
            thread = await self._host._thread_repository.get_by_id(thread_id)
            if thread is None or thread.workspace_id != self._host.context.workspace_id:
                raise ValueError(f"Thread '{thread_id}' was not found.")
            delete_messages = getattr(self._host._thread_message_repository, "delete_for_thread", None)
            if delete_messages is not None:
                await delete_messages(thread_id)
            await self._host._thread_repository.delete(thread_id)
            if uow is not None:
                await uow.commit()
            return {
                "status": "deleted",
                "thread_id": thread_id,
            }

    async def list_threads(self) -> list[dict[str, Any]]:
        async with self._host._runtime_operation_scope():
            threads = await self._host._thread_repository.list_for_workspace(
                self._host.context.workspace_id
            )
            return [self._host._serialize_thread(thread) for thread in threads]

    async def get_thread(
        self,
        *,
        thread_id: uuid.UUID,
    ) -> dict[str, Any]:
        async with self._host._runtime_operation_scope():
            thread = await self._host._thread_repository.get_by_id(thread_id)
            if thread is None or thread.workspace_id != self._host.context.workspace_id:
                raise ValueError(f"Thread '{thread_id}' was not found.")
            return self._host._serialize_thread(thread)

    async def list_thread_messages(
        self,
        *,
        thread_id: uuid.UUID,
    ) -> list[dict[str, Any]]:
        async with self._host._runtime_operation_scope():
            thread = await self._host._thread_repository.get_by_id(thread_id)
            if thread is None or thread.workspace_id != self._host.context.workspace_id:
                raise ValueError(f"Thread '{thread_id}' was not found.")
            messages = await self._host._thread_message_repository.list_for_thread(thread_id)
            return [self._host._serialize_thread_message(message) for message in messages]
