from __future__ import annotations

import uuid

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.packages.common.langbridge_common.db.connector_sync import ConnectorSyncStateRecord

from .base import AsyncBaseRepository


class ConnectorSyncStateRepository(AsyncBaseRepository[ConnectorSyncStateRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, ConnectorSyncStateRecord)

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
    ) -> list[ConnectorSyncStateRecord]:
        result = await self._session.scalars(
            select(ConnectorSyncStateRecord)
            .where(
                ConnectorSyncStateRecord.workspace_id == workspace_id,
                ConnectorSyncStateRecord.connection_id == connection_id,
            )
            .order_by(ConnectorSyncStateRecord.resource_name.asc(), desc(ConnectorSyncStateRecord.updated_at))
        )
        return list(result.all())

    async def get_for_resource(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        resource_name: str,
    ) -> ConnectorSyncStateRecord | None:
        result = await self._session.scalars(
            select(ConnectorSyncStateRecord).where(
                ConnectorSyncStateRecord.workspace_id == workspace_id,
                ConnectorSyncStateRecord.connection_id == connection_id,
                ConnectorSyncStateRecord.resource_name == resource_name,
            )
        )
        return result.one_or_none()
