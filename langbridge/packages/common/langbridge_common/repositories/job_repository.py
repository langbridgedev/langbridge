from uuid import UUID
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..db.job import JobEventRecord, JobRecord
from .base import AsyncBaseRepository


class JobRepository(AsyncBaseRepository[JobRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, JobRecord)

    async def get_by_id(self, id_: object) -> JobRecord | None:
        stmt = (
            select(JobRecord)
            .options(
                selectinload(JobRecord.job_events),
                selectinload(JobRecord.job_tasks),
            )
            .where(JobRecord.id == id_)
        )
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def get_existing_ids(self, ids: set[UUID]) -> set[UUID]:
        if not ids:
            return set()
        stmt = select(JobRecord.id).where(JobRecord.id.in_(ids))
        result = await self._session.scalars(stmt)
        return set(result.all())

    def add_job_event(self, event: JobEventRecord) -> JobEventRecord:
        self._session.add(event)
        return event

    async def list_for_organisation_and_type(
        self,
        *,
        organisation_id: str,
        job_type: str | None = None,
        limit: int = 50,
    ) -> list[JobRecord]:
        stmt = (
            select(JobRecord)
            .options(
                selectinload(JobRecord.job_events),
                selectinload(JobRecord.job_tasks),
            )
            .where(JobRecord.organisation_id == organisation_id)
            .order_by(desc(JobRecord.created_at))
            .limit(max(1, limit))
        )
        if job_type:
            stmt = stmt.where(JobRecord.job_type == job_type)
        result = await self._session.scalars(stmt)
        return list(result.all())
