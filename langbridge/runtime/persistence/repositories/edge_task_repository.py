
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.persistence.db.runtime import (
    EdgeResultReceiptRecord,
    EdgeTaskRecord,
    EdgeTaskStatus,
)
from .base import AsyncBaseRepository


class EdgeTaskRepository(AsyncBaseRepository[EdgeTaskRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, EdgeTaskRecord)

    async def get_by_id(self, id_: object) -> EdgeTaskRecord | None:
        stmt = select(EdgeTaskRecord).where(EdgeTaskRecord.id == id_)
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def list_runnable_for_runtime(
        self,
        *,
        workspace_id: uuid.UUID,
        runtime_id: uuid.UUID,
        limit: int = 50,
    ) -> list[EdgeTaskRecord]:
        stmt = (
            select(EdgeTaskRecord)
            .where(
                EdgeTaskRecord.workspace_id == workspace_id,
                EdgeTaskRecord.target_runtime_id == runtime_id,
                EdgeTaskRecord.status == EdgeTaskStatus.queued,
            )
            .order_by(EdgeTaskRecord.enqueued_at.asc())
            .limit(limit)
        )
        result = await self._session.scalars(stmt)
        return list(result.all())


class EdgeResultReceiptRepository(AsyncBaseRepository[EdgeResultReceiptRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, EdgeResultReceiptRecord)

    async def get_by_request_id(
        self,
        *,
        workspace_id: uuid.UUID,
        runtime_id: uuid.UUID,
        request_id: str,
    ) -> EdgeResultReceiptRecord | None:
        stmt = (
            select(EdgeResultReceiptRecord)
            .where(
                EdgeResultReceiptRecord.workspace_id == workspace_id,
                EdgeResultReceiptRecord.runtime_id == runtime_id,
                EdgeResultReceiptRecord.request_id == request_id,
            )
            .limit(1)
        )
        result = await self._session.scalars(stmt)
        return result.first()

    async def create_receipt(
        self,
        *,
        workspace_id: uuid.UUID,
        runtime_id: uuid.UUID,
        request_id: str,
        task_id: uuid.UUID | None,
        payload_hash: str | None = None,
    ) -> EdgeResultReceiptRecord:
        record = EdgeResultReceiptRecord(
            workspace_id=workspace_id,
            runtime_id=runtime_id,
            request_id=request_id,
            task_id=task_id,
            payload_hash=payload_hash,
            created_at=datetime.now(timezone.utc),
        )
        self.add(record)
        await self.flush()
        return record
