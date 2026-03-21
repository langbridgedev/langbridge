from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from langbridge.runtime.persistence.db.workspace import (
    RuntimeActor,
    Workspace,
)

from .base import AsyncBaseRepository


class WorkspaceRepository(AsyncBaseRepository[Workspace]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Workspace)

    def _query(self):
        return select(Workspace).options(
            selectinload(Workspace.actors),
        )

    async def get_by_id(self, id_: uuid.UUID) -> Workspace | None:
        result = await self._session.scalars(self._query().where(Workspace.id == id_))
        return result.one_or_none()

    async def get_by_name(self, name: str) -> Workspace | None:
        result = await self._session.scalars(self._query().where(Workspace.name == name))
        return result.one_or_none()

    async def get_configured(self) -> Workspace | None:
        result = await self._session.scalars(self._query().order_by(Workspace.created_at.asc()).limit(1))
        return result.first()

    async def ensure_configured(
        self,
        *,
        workspace_id: uuid.UUID,
        name: str,
    ) -> Workspace:
        workspace = await self.get_by_id(workspace_id)
        if workspace is not None:
            if workspace.name != name:
                workspace.name = name
            return workspace

        workspace = Workspace(id=workspace_id, name=name)
        self.add(workspace)
        await self.flush()
        return workspace


class RuntimeActorRepository(AsyncBaseRepository[RuntimeActor]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, RuntimeActor)

    async def get_by_id(self, id_: uuid.UUID) -> RuntimeActor | None:
        result = await self._session.scalars(select(RuntimeActor).where(RuntimeActor.id == id_))
        return result.one_or_none()

    async def get_by_subject(
        self,
        *,
        workspace_id: uuid.UUID,
        subject: str,
    ) -> RuntimeActor | None:
        result = await self._session.scalars(
            select(RuntimeActor).where(
                RuntimeActor.workspace_id == workspace_id,
                RuntimeActor.subject == subject,
            )
        )
        return result.one_or_none()


__all__ = [
    "RuntimeActorRepository",
    "WorkspaceRepository",
]
