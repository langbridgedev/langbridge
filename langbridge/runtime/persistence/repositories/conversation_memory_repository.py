from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.persistence.db.threads import (
    ConversationMemoryItem,
    MemoryCategory,
)
from .base import AsyncBaseRepository


class ConversationMemoryRepository(AsyncBaseRepository[ConversationMemoryItem]):
    """Persistence helper for distilled long-term conversation memory."""

    def __init__(self, session: AsyncSession):
        super().__init__(session, ConversationMemoryItem)

    async def list_for_thread(self, thread_id: uuid.UUID, *, limit: int = 200) -> list[ConversationMemoryItem]:
        stmt = (
            select(ConversationMemoryItem)
            .where(ConversationMemoryItem.thread_id == thread_id)
            .order_by(ConversationMemoryItem.created_at.desc())
            .limit(max(1, int(limit)))
        )
        result = await self._session.scalars(stmt)
        return list(result.all())

    def create_item(
        self,
        *,
        thread_id: uuid.UUID,
        category: str,
        content: str,
        metadata_json: Optional[dict[str, Any]] = None,
        actor_id: Optional[uuid.UUID] = None,
    ) -> Optional[ConversationMemoryItem]:
        clean_content = str(content or "").strip()
        if not clean_content:
            return None

        try:
            category_enum = MemoryCategory(str(category))
        except ValueError:
            category_enum = MemoryCategory.fact

        record = ConversationMemoryItem(
            id=uuid.uuid4(),
            thread_id=thread_id,
            actor_id=actor_id,
            category=category_enum,
            content=clean_content,
            metadata_json=metadata_json or {},
        )
        self.add(record)
        return record

    async def touch_items(self, item_ids: Iterable[uuid.UUID]) -> None:
        ids = [item_id for item_id in item_ids if isinstance(item_id, uuid.UUID)]
        if not ids:
            return
        stmt = (
            update(ConversationMemoryItem)
            .where(ConversationMemoryItem.id.in_(ids))
            .values(last_accessed_at=datetime.now(timezone.utc))
        )
        await self._session.execute(stmt)


__all__ = ["ConversationMemoryRepository"]
