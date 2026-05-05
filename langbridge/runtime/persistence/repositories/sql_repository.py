
import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.persistence.db.sql import (
    SqlJobRecord,
    SqlJobResultArtifactRecord,
)

from .base import AsyncBaseRepository


class SqlJobRepository(AsyncBaseRepository[SqlJobRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, SqlJobRecord)

    async def get_by_id_for_workspace(
        self,
        *,
        sql_job_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> SqlJobRecord | None:
        result = await self._session.scalars(
            select(SqlJobRecord).where(
                SqlJobRecord.id == sql_job_id,
                SqlJobRecord.workspace_id == workspace_id,
            )
        )
        return result.one_or_none()

    async def list_history(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        limit: int = 100,
    ) -> list[SqlJobRecord]:
        query = select(SqlJobRecord).where(SqlJobRecord.workspace_id == workspace_id)
        if actor_id is not None:
            query = query.where(SqlJobRecord.actor_id == actor_id)
        result = await self._session.scalars(
            query.order_by(SqlJobRecord.created_at.desc()).limit(max(1, limit))
        )
        return list(result.all())

    async def count_active_for_workspace(self, *, workspace_id: uuid.UUID) -> int:
        result = await self._session.scalar(
            select(func.count(SqlJobRecord.id)).where(
                SqlJobRecord.workspace_id == workspace_id,
                SqlJobRecord.status.in_(["queued", "running"]),
            )
        )
        return int(result or 0)

    async def count_bytes_scanned_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
    ) -> int:
        result = await self._session.scalar(
            select(func.coalesce(func.sum(SqlJobRecord.bytes_scanned), 0)).where(
                SqlJobRecord.workspace_id == workspace_id,
                SqlJobRecord.status == "succeeded",
            )
        )
        return int(result or 0)


class SqlJobResultArtifactRepository(AsyncBaseRepository[SqlJobResultArtifactRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, SqlJobResultArtifactRecord)

    async def list_for_job(self, *, sql_job_id: uuid.UUID) -> list[SqlJobResultArtifactRecord]:
        result = await self._session.scalars(
            select(SqlJobResultArtifactRecord)
            .where(SqlJobResultArtifactRecord.sql_job_id == sql_job_id)
            .order_by(SqlJobResultArtifactRecord.created_at.desc())
        )
        return list(result.all())

    async def get_by_id_for_workspace(
        self,
        *,
        artifact_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> SqlJobResultArtifactRecord | None:
        result = await self._session.scalars(
            select(SqlJobResultArtifactRecord).where(
                SqlJobResultArtifactRecord.id == artifact_id,
                SqlJobResultArtifactRecord.workspace_id == workspace_id,
            )
        )
        return result.one_or_none()

