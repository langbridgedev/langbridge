import uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.persistence.db.agent import LLMConnection
from .base import AsyncBaseRepository


class LLMConnectionRepository(AsyncBaseRepository[LLMConnection]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, LLMConnection)

    def _select_with_relationships(self):
        return select(LLMConnection)

    async def get_all(
        self,
        *,
        workspace_id: uuid.UUID | None = None,
    ) -> list[LLMConnection]:
        stmt = self._select_with_relationships()
        if workspace_id is not None:
            stmt = stmt.filter(LLMConnection.workspace_id == workspace_id)
        result = await self._session.scalars(stmt)
        return list(result.all())

    async def get_by_id(self, id_: object) -> LLMConnection | None:
        stmt = self._select_with_relationships().filter(LLMConnection.id == id_)
        result = await self._session.scalars(stmt)
        return result.one_or_none()
