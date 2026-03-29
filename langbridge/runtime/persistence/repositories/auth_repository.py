
import uuid

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from langbridge.runtime.persistence.db.auth import (
    RuntimeLocalAuthCredential,
    RuntimeLocalAuthState,
)
from langbridge.runtime.persistence.db.workspace import RuntimeActor

from .base import AsyncBaseRepository


class RuntimeLocalAuthStateRepository(AsyncBaseRepository[RuntimeLocalAuthState]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, RuntimeLocalAuthState)

    async def get_for_workspace(self, *, workspace_id: uuid.UUID) -> RuntimeLocalAuthState | None:
        result = await self._session.scalars(
            select(RuntimeLocalAuthState).where(RuntimeLocalAuthState.workspace_id == workspace_id)
        )
        return result.one_or_none()


class RuntimeLocalAuthCredentialRepository(AsyncBaseRepository[RuntimeLocalAuthCredential]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, RuntimeLocalAuthCredential)

    def _query(self):
        return select(RuntimeLocalAuthCredential).options(
            selectinload(RuntimeLocalAuthCredential.actor),
        )

    async def get_by_actor_id(self, *, actor_id: uuid.UUID) -> RuntimeLocalAuthCredential | None:
        result = await self._session.scalars(
            self._query().where(RuntimeLocalAuthCredential.actor_id == actor_id)
        )
        return result.one_or_none()

    async def list_for_workspace(self, *, workspace_id: uuid.UUID) -> list[RuntimeLocalAuthCredential]:
        result = await self._session.scalars(
            self._query()
            .join(RuntimeActor, RuntimeLocalAuthCredential.actor_id == RuntimeActor.id)
            .where(RuntimeLocalAuthCredential.workspace_id == workspace_id)
            .order_by(RuntimeActor.created_at.asc())
        )
        return list(result.all())

    async def get_by_identifier(
        self,
        *,
        workspace_id: uuid.UUID,
        identifier: str,
    ) -> RuntimeLocalAuthCredential | None:
        normalized_identifier = str(identifier or "").strip().casefold()
        if not normalized_identifier:
            return None
        result = await self._session.scalars(
            self._query()
            .join(RuntimeActor, RuntimeLocalAuthCredential.actor_id == RuntimeActor.id)
            .where(
                RuntimeLocalAuthCredential.workspace_id == workspace_id,
                or_(
                    func.lower(RuntimeActor.subject) == normalized_identifier,
                    func.lower(RuntimeActor.email) == normalized_identifier,
                ),
            )
            .limit(1)
        )
        return result.first()


__all__ = [
    "RuntimeLocalAuthCredentialRepository",
    "RuntimeLocalAuthStateRepository",
]
