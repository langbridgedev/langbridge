from typing import List, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.models import SemanticModelMetadata
from langbridge.runtime.persistence.db.semantic import SemanticModelEntry
from langbridge.runtime.persistence.mappers import from_semantic_model_record
from langbridge.runtime.ports import (
    ISemanticModelStore
)
from .base import AsyncBaseRepository


class SemanticModelRepository(AsyncBaseRepository[SemanticModelEntry]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, SemanticModelEntry)

    async def list_for_workspace(self, workspace_id: UUID) -> List[SemanticModelEntry]:
        query = select(SemanticModelEntry).filter(SemanticModelEntry.workspace_id == workspace_id)
        result = await self._session.scalars(query.order_by(SemanticModelEntry.created_at.desc()))
        return list(result.all())

    async def get_for_workspace(self, model_id: UUID, workspace_id: UUID) -> Optional[SemanticModelEntry]:
        return (
            await (
                self._session.scalars(select(SemanticModelEntry).filter(
                    SemanticModelEntry.id == model_id,
                    SemanticModelEntry.workspace_id == workspace_id,
                ))
            )
        ).one_or_none()

    async def get_by_ids(self, model_ids: List[UUID]) -> List[SemanticModelEntry]:
        return list(
            (await self._session.scalars(select(SemanticModelEntry).filter(SemanticModelEntry.id.in_(model_ids)))).all()
        )

    async def get_by_ids_for_workspace(
        self,
        *,
        workspace_id: UUID,
        model_ids: list[UUID],
    ) -> list[SemanticModelEntry]:
        return list(
            (
                await self._session.scalars(
                    select(SemanticModelEntry).filter(
                        SemanticModelEntry.workspace_id == workspace_id,
                        SemanticModelEntry.id.in_(model_ids),
                    )
                )
            ).all()
        )

class SemanticModelStore(ISemanticModelStore):
    def __init__(self, repository: SemanticModelRepository):
        self._repository = repository

    async def get_by_id(self, model_id: UUID) -> SemanticModelMetadata | None:
        entry = await self._repository.get_by_id(model_id)
        if entry is None:
            return None
        return from_semantic_model_record(entry)

    async def get_by_ids(self, model_ids: list[UUID]) -> list[SemanticModelMetadata]:
        entries = await self._repository.get_by_ids(model_ids)
        return [
            runtime_model
            for entry in entries
            if (runtime_model := from_semantic_model_record(entry)) is not None
        ]
