
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.persistence.db.threads import Thread
from .base import AsyncBaseRepository


class ThreadRepository(AsyncBaseRepository[Thread]):
    """Data access helper for conversation threads."""

    def __init__(self, session: AsyncSession):
        super().__init__(session, Thread)

    def _select_for_actor(self, actor_id: uuid.UUID):
        return (
            select(Thread)
            .filter(Thread.created_by_actor_id == actor_id)
            .order_by(Thread.created_at.desc())
        )

    def _select_for_workspace(self, workspace_id: uuid.UUID):
        return (
            select(Thread)
            .filter(Thread.workspace_id == workspace_id)
            .order_by(Thread.created_at.desc())
        )

    async def list_for_actor(self, actor_id: uuid.UUID) -> list[Thread]:
        result = await self._session.scalars(self._select_for_actor(actor_id))
        return list(result.all())

    async def list_for_workspace(self, workspace_id: uuid.UUID) -> list[Thread]:
        result = await self._session.scalars(self._select_for_workspace(workspace_id))
        return list(result.all())

    async def get_for_actor(self, thread_id: uuid.UUID, actor_id: uuid.UUID) -> Thread | None:
        stmt = (
            select(Thread)
            .filter(Thread.id == thread_id)
            .filter(Thread.created_by_actor_id == actor_id)
        )
        return await self._session.scalar(stmt)
