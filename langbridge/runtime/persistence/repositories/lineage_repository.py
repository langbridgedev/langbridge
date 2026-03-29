
import uuid

from sqlalchemy import and_, delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.persistence.db.lineage import LineageEdgeRecord

from .base import AsyncBaseRepository


class LineageEdgeRepository(AsyncBaseRepository[LineageEdgeRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, LineageEdgeRecord)

    async def delete_for_target(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> None:
        await self._session.execute(
            delete(LineageEdgeRecord).where(
                LineageEdgeRecord.workspace_id == workspace_id,
                LineageEdgeRecord.target_type == target_type,
                LineageEdgeRecord.target_id == target_id,
            )
        )

    async def delete_for_node(
        self,
        *,
        workspace_id: uuid.UUID,
        node_type: str,
        node_id: str,
    ) -> None:
        await self._session.execute(
            delete(LineageEdgeRecord).where(
                LineageEdgeRecord.workspace_id == workspace_id,
                or_(
                    and_(
                        LineageEdgeRecord.source_type == node_type,
                        LineageEdgeRecord.source_id == node_id,
                    ),
                    and_(
                        LineageEdgeRecord.target_type == node_type,
                        LineageEdgeRecord.target_id == node_id,
                    ),
                ),
            )
        )

    async def list_inbound(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> list[LineageEdgeRecord]:
        result = await self._session.scalars(
            select(LineageEdgeRecord).where(
                LineageEdgeRecord.workspace_id == workspace_id,
                LineageEdgeRecord.target_type == target_type,
                LineageEdgeRecord.target_id == target_id,
            )
        )
        return list(result.all())

    async def list_outbound(
        self,
        *,
        workspace_id: uuid.UUID,
        source_type: str,
        source_id: str,
    ) -> list[LineageEdgeRecord]:
        result = await self._session.scalars(
            select(LineageEdgeRecord).where(
                LineageEdgeRecord.workspace_id == workspace_id,
                LineageEdgeRecord.source_type == source_type,
                LineageEdgeRecord.source_id == source_id,
            )
        )
        return list(result.all())
