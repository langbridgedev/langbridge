from typing import List, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.contracts.semantic import (
    SemanticModelRecordResponse,
)
from langbridge.packages.common.langbridge_common.db.semantic import SemanticModelEntry, SemanticVectorStoreEntry
from langbridge.packages.common.langbridge_common.interfaces.semantic_models import (
    ISemanticModelStore,
)
from .base import AsyncBaseRepository


class SemanticModelRepository(AsyncBaseRepository[SemanticModelEntry]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, SemanticModelEntry)

    async def list_for_scope(self, organization_id: UUID, project_id: Optional[UUID] = None) -> List[SemanticModelEntry]:
        query = select(SemanticModelEntry).filter(SemanticModelEntry.organization_id == organization_id)
        if project_id:
            query = query.filter(SemanticModelEntry.project_id == project_id)
        result = await self._session.scalars(query.order_by(SemanticModelEntry.created_at.desc()))
        return list(result.all())

    async def get_for_scope(self, model_id: UUID, organization_id: UUID) -> Optional[SemanticModelEntry]:
        return (
            await (
                self._session.scalars(select(SemanticModelEntry).filter(
                    SemanticModelEntry.id == model_id,
                    SemanticModelEntry.organization_id == organization_id,
                ))
            )
        ).one_or_none()

    async def get_by_ids(self, model_ids: List[UUID]) -> List[SemanticModelEntry]:
        return list(
            (await self._session.scalars(select(SemanticModelEntry).filter(SemanticModelEntry.id.in_(model_ids)))).all()
        )

class SemanticVectorStoreRepository(AsyncBaseRepository[SemanticVectorStoreEntry]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, SemanticVectorStoreEntry)

    async def list_for_scope(self, organization_id: UUID, project_id: Optional[UUID] = None) -> List[SemanticVectorStoreEntry]:
        query = select(SemanticVectorStoreEntry).filter(SemanticVectorStoreEntry.organization_id == organization_id)
        if project_id:
            query = query.filter(SemanticVectorStoreEntry.project_id == project_id)
        result = await self._session.scalars(query.order_by(SemanticVectorStoreEntry.created_at.desc()))
        return list(result.all())

    async def get_for_scope(self, store_id: UUID, organization_id: UUID) -> Optional[SemanticVectorStoreEntry]:
        return (
            await (
                self._session.scalars(select(SemanticVectorStoreEntry).filter(
                    SemanticVectorStoreEntry.id == store_id,
                    SemanticVectorStoreEntry.organization_id == organization_id,
                ))
            )
        ).one_or_none()


class SemanticModelStore(ISemanticModelStore):
    def __init__(self, repository: SemanticModelRepository):
        self._repository = repository

    async def get_by_id(self, model_id: UUID) -> SemanticModelRecordResponse | None:
        entry = await self._repository.get_by_id(model_id)
        if entry is None:
            return None
        return SemanticModelRecordResponse.model_validate(entry)

    async def get_by_ids(self, model_ids: list[UUID]) -> list[SemanticModelRecordResponse]:
        entries = await self._repository.get_by_ids(model_ids)
        return [SemanticModelRecordResponse.model_validate(entry) for entry in entries]
