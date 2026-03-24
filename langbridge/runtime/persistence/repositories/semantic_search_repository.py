from typing import List
from uuid import UUID

from sqlalchemy import select

from langbridge.runtime.persistence.db.semantic import SemanticVectorIndexEntry

from .base import AsyncBaseRepository


class SemanticVectorIndexRepository(AsyncBaseRepository[SemanticVectorIndexEntry]):
    def __init__(self, session):
        super().__init__(session, SemanticVectorIndexEntry)

    async def list_for_workspace(
        self,
        *,
        workspace_id: UUID,
        semantic_model_id: UUID | None = None,
    ) -> List[SemanticVectorIndexEntry]:
        stmt = select(SemanticVectorIndexEntry).where(
            SemanticVectorIndexEntry.workspace_id == workspace_id
        )
        if semantic_model_id is not None:
            stmt = stmt.where(SemanticVectorIndexEntry.semantic_model_id == semantic_model_id)
        stmt = stmt.order_by(
            SemanticVectorIndexEntry.dataset_key.asc(),
            SemanticVectorIndexEntry.dimension_name.asc(),
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_for_workspace(
        self,
        *,
        vector_index_id: UUID,
        workspace_id: UUID,
    ) -> SemanticVectorIndexEntry | None:
        stmt = select(SemanticVectorIndexEntry).where(
            SemanticVectorIndexEntry.id == vector_index_id,
            SemanticVectorIndexEntry.workspace_id == workspace_id,
        )
        result = await self._session.execute(stmt)
        return result.scalars().one_or_none()

    async def get_for_dimension(
        self,
        *,
        workspace_id: UUID,
        semantic_model_id: UUID,
        dataset_key: str,
        dimension_name: str,
    ) -> SemanticVectorIndexEntry | None:
        stmt = select(SemanticVectorIndexEntry).where(
            SemanticVectorIndexEntry.workspace_id == workspace_id,
            SemanticVectorIndexEntry.semantic_model_id == semantic_model_id,
            SemanticVectorIndexEntry.dataset_key == dataset_key,
            SemanticVectorIndexEntry.dimension_name == dimension_name,
        )
        result = await self._session.execute(stmt)
        return result.scalars().one_or_none()

    async def delete_for_workspace(
        self,
        *,
        workspace_id: UUID,
        vector_index_id: UUID,
    ) -> None:
        record = await self.get_for_workspace(
            vector_index_id=vector_index_id,
            workspace_id=workspace_id,
        )
        if record is not None:
            await self.delete(record)
