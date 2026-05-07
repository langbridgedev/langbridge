from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.persistence.db.lease import RuntimeLeaseRecord

from .base import AsyncBaseRepository


class RuntimeLeaseRepository(AsyncBaseRepository[RuntimeLeaseRecord]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, RuntimeLeaseRecord)

    async def acquire_lease(
        self,
        *,
        name: str,
        owner_id: str,
        lease_seconds: int,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        now = datetime.now(timezone.utc)
        leased_until = now + timedelta(seconds=max(1, int(lease_seconds)))
        normalized_name = self._normalize_name(name)
        normalized_owner = self._normalize_owner(owner_id)
        update_result = await self._session.execute(
            update(RuntimeLeaseRecord)
            .where(
                RuntimeLeaseRecord.name == normalized_name,
                or_(
                    RuntimeLeaseRecord.owner_id == normalized_owner,
                    RuntimeLeaseRecord.leased_until.is_(None),
                    RuntimeLeaseRecord.leased_until <= now,
                ),
            )
            .values(
                owner_id=normalized_owner,
                leased_until=leased_until,
                heartbeat_at=now,
                metadata_json=dict(metadata or {}),
                updated_at=now,
            )
            .returning(RuntimeLeaseRecord.name)
        )
        if update_result.scalar_one_or_none() is not None:
            await self.flush()
            return True

        existing = await self.get_by_name(normalized_name)
        if existing is not None:
            return False

        self.add(
            RuntimeLeaseRecord(
                name=normalized_name,
                owner_id=normalized_owner,
                leased_until=leased_until,
                heartbeat_at=now,
                metadata_json=dict(metadata or {}),
                created_at=now,
                updated_at=now,
            )
        )
        try:
            await self.flush()
        except IntegrityError:
            await self._session.rollback()
            return False
        return True

    async def heartbeat_lease(
        self,
        *,
        name: str,
        owner_id: str,
        lease_seconds: int,
    ) -> bool:
        now = datetime.now(timezone.utc)
        result = await self._session.execute(
            update(RuntimeLeaseRecord)
            .where(
                RuntimeLeaseRecord.name == self._normalize_name(name),
                RuntimeLeaseRecord.owner_id == self._normalize_owner(owner_id),
            )
            .values(
                leased_until=now + timedelta(seconds=max(1, int(lease_seconds))),
                heartbeat_at=now,
                updated_at=now,
            )
            .returning(RuntimeLeaseRecord.name)
        )
        if result.scalar_one_or_none() is None:
            return False
        await self.flush()
        return True

    async def release_lease(self, *, name: str, owner_id: str) -> bool:
        now = datetime.now(timezone.utc)
        result = await self._session.execute(
            update(RuntimeLeaseRecord)
            .where(
                RuntimeLeaseRecord.name == self._normalize_name(name),
                RuntimeLeaseRecord.owner_id == self._normalize_owner(owner_id),
            )
            .values(
                owner_id=None,
                leased_until=now,
                heartbeat_at=now,
                updated_at=now,
            )
            .returning(RuntimeLeaseRecord.name)
        )
        if result.scalar_one_or_none() is None:
            return False
        await self.flush()
        return True

    async def cleanup_expired(self, *, older_than_seconds: int) -> int:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=max(1, int(older_than_seconds)))
        result = await self._session.execute(
            delete(RuntimeLeaseRecord).where(
                RuntimeLeaseRecord.leased_until.is_not(None),
                RuntimeLeaseRecord.leased_until < cutoff,
            )
        )
        await self.flush()
        return int(result.rowcount or 0)

    async def get_by_name(self, name: str) -> RuntimeLeaseRecord | None:
        result = await self._session.scalars(
            select(RuntimeLeaseRecord).where(RuntimeLeaseRecord.name == self._normalize_name(name))
        )
        return result.one_or_none()

    def _normalize_name(self, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("Runtime lease name must not be empty.")
        return normalized

    def _normalize_owner(self, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("Runtime lease owner_id must not be empty.")
        return normalized
