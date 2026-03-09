from __future__ import annotations

import uuid
from typing import Iterable

from sqlalchemy import String, and_, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.packages.common.langbridge_common.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)

from .base import AsyncBaseRepository


class DatasetRepository(AsyncBaseRepository[DatasetRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, DatasetRecord)

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None = None,
        search: str | None = None,
        tags: Iterable[str] | None = None,
        dataset_types: Iterable[str] | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[DatasetRecord]:
        query = select(DatasetRecord).where(DatasetRecord.workspace_id == workspace_id)
        if project_id is not None:
            query = query.where(
                or_(DatasetRecord.project_id == project_id, DatasetRecord.project_id.is_(None))
            )
        if search:
            pattern = f"%{search.strip().lower()}%"
            query = query.where(
                or_(
                    func.lower(DatasetRecord.name).like(pattern),
                    func.lower(func.coalesce(DatasetRecord.description, "")).like(pattern),
                )
            )
        if tags:
            normalized_tags = [tag.strip().lower() for tag in tags if tag and tag.strip()]
            if normalized_tags:
                # JSON containment is dialect-specific; fallback to LIKE against JSON text.
                tag_filters = [
                    func.lower(func.cast(DatasetRecord.tags_json, String)).like(f'%"{tag}"%')
                    for tag in normalized_tags
                ]
                query = query.where(and_(*tag_filters))
        if dataset_types:
            normalized_types = [item.strip().upper() for item in dataset_types if item and item.strip()]
            if normalized_types:
                query = query.where(DatasetRecord.dataset_type.in_(normalized_types))

        result = await self._session.scalars(
            query.order_by(desc(DatasetRecord.updated_at)).offset(max(0, offset)).limit(max(1, limit))
        )
        return list(result.all())

    async def get_for_workspace(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> DatasetRecord | None:
        result = await self._session.scalars(
            select(DatasetRecord).where(
                DatasetRecord.id == dataset_id,
                DatasetRecord.workspace_id == workspace_id,
            )
        )
        return result.one_or_none()

    async def count_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
    ) -> int:
        count = await self._session.scalar(
            select(func.count(DatasetRecord.id)).where(
                DatasetRecord.workspace_id == workspace_id,
            )
        )
        return int(count or 0)

    async def get_by_ids_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids: Iterable[uuid.UUID],
    ) -> list[DatasetRecord]:
        normalized_ids = [dataset_id for dataset_id in dataset_ids if dataset_id is not None]
        if not normalized_ids:
            return []
        result = await self._session.scalars(
            select(DatasetRecord).where(
                DatasetRecord.workspace_id == workspace_id,
                DatasetRecord.id.in_(normalized_ids),
            )
        )
        return list(result.all())

    async def find_file_dataset_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        table_name: str,
    ) -> DatasetRecord | None:
        result = await self._session.scalars(
            select(DatasetRecord).where(
                DatasetRecord.workspace_id == workspace_id,
                DatasetRecord.connection_id == connection_id,
                DatasetRecord.dataset_type == "FILE",
                DatasetRecord.table_name == table_name,
            )
        )
        return result.one_or_none()

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        dataset_types: Iterable[str] | None = None,
        limit: int = 500,
    ) -> list[DatasetRecord]:
        query = select(DatasetRecord).where(
            DatasetRecord.workspace_id == workspace_id,
            DatasetRecord.connection_id == connection_id,
        )
        if dataset_types:
            normalized_types = [item.strip().upper() for item in dataset_types if item and item.strip()]
            if normalized_types:
                query = query.where(DatasetRecord.dataset_type.in_(normalized_types))
        result = await self._session.scalars(
            query.order_by(desc(DatasetRecord.updated_at)).limit(max(1, limit))
        )
        return list(result.all())


class DatasetColumnRepository(AsyncBaseRepository[DatasetColumnRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, DatasetColumnRecord)

    async def list_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
    ) -> list[DatasetColumnRecord]:
        result = await self._session.scalars(
            select(DatasetColumnRecord)
            .where(DatasetColumnRecord.dataset_id == dataset_id)
            .order_by(DatasetColumnRecord.ordinal_position.asc(), DatasetColumnRecord.name.asc())
        )
        return list(result.all())

    async def delete_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
    ) -> None:
        rows = await self.list_for_dataset(dataset_id=dataset_id)
        for row in rows:
            await self.delete(row)


class DatasetPolicyRepository(AsyncBaseRepository[DatasetPolicyRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, DatasetPolicyRecord)

    async def get_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
    ) -> DatasetPolicyRecord | None:
        result = await self._session.scalars(
            select(DatasetPolicyRecord).where(DatasetPolicyRecord.dataset_id == dataset_id)
        )
        return result.one_or_none()


class DatasetRevisionRepository(AsyncBaseRepository[DatasetRevisionRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, DatasetRevisionRecord)

    async def list_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        limit: int = 50,
    ) -> list[DatasetRevisionRecord]:
        result = await self._session.scalars(
            select(DatasetRevisionRecord)
            .where(DatasetRevisionRecord.dataset_id == dataset_id)
            .order_by(DatasetRevisionRecord.revision_number.desc())
            .limit(max(1, limit))
        )
        return list(result.all())

    async def get_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        revision_id: uuid.UUID,
    ) -> DatasetRevisionRecord | None:
        result = await self._session.scalars(
            select(DatasetRevisionRecord).where(
                DatasetRevisionRecord.dataset_id == dataset_id,
                DatasetRevisionRecord.id == revision_id,
            )
        )
        return result.one_or_none()

    async def next_revision_number(self, *, dataset_id: uuid.UUID) -> int:
        max_number = await self._session.scalar(
            select(func.max(DatasetRevisionRecord.revision_number)).where(
                DatasetRevisionRecord.dataset_id == dataset_id
            )
        )
        return int(max_number or 0) + 1
