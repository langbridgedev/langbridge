from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.persistence.db.runtime import (
    RuntimeInstanceRecord,
    RuntimeInstanceStatus,
    RuntimeRegistrationTokenRecord,
)
from .base import AsyncBaseRepository


class RuntimeInstanceRepository(AsyncBaseRepository[RuntimeInstanceRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, RuntimeInstanceRecord)

    async def get_by_id(self, id_: object) -> RuntimeInstanceRecord | None:
        stmt = select(RuntimeInstanceRecord).where(RuntimeInstanceRecord.id == id_)
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def get_active_for_workspace(self, workspace_id: uuid.UUID) -> list[RuntimeInstanceRecord]:
        stmt = (
            select(RuntimeInstanceRecord)
            .where(
                RuntimeInstanceRecord.workspace_id == workspace_id,
                RuntimeInstanceRecord.status == RuntimeInstanceStatus.active,
            )
            .order_by(RuntimeInstanceRecord.last_seen_at.desc().nullslast())
        )
        result = await self._session.scalars(stmt)
        return list(result.all())

    async def list_for_workspace(self, workspace_id: uuid.UUID) -> list[RuntimeInstanceRecord]:
        stmt = (
            select(RuntimeInstanceRecord)
            .where(RuntimeInstanceRecord.workspace_id == workspace_id)
            .order_by(RuntimeInstanceRecord.last_seen_at.desc().nullslast())
        )
        result = await self._session.scalars(stmt)
        return list(result.all())


class RuntimeRegistrationTokenRepository(AsyncBaseRepository[RuntimeRegistrationTokenRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, RuntimeRegistrationTokenRecord)

    async def get_by_token_hash(self, token_hash: str) -> RuntimeRegistrationTokenRecord | None:
        stmt: Select[tuple[RuntimeRegistrationTokenRecord]] = (
            select(RuntimeRegistrationTokenRecord)
            .where(RuntimeRegistrationTokenRecord.token_hash == token_hash)
            .limit(1)
        )
        result = await self._session.scalars(stmt)
        return result.first()

    async def create_token(
        self,
        *,
        workspace_id: uuid.UUID,
        token_hash: str,
        expires_at: datetime,
        created_by_actor_id: uuid.UUID | None,
    ) -> RuntimeRegistrationTokenRecord:
        now = datetime.now(timezone.utc)
        record = RuntimeRegistrationTokenRecord(
            workspace_id=workspace_id,
            token_hash=token_hash,
            expires_at=expires_at,
            created_by_actor_id=created_by_actor_id,
            created_at=now,
        )
        self.add(record)
        await self.flush()
        return record
