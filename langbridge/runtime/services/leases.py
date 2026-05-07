from __future__ import annotations

from typing import Any


class RuntimeLeaseService:
    def __init__(self, *, repository: Any) -> None:
        self._repository = repository

    async def acquire(
        self,
        *,
        name: str,
        owner_id: str,
        lease_seconds: int,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        return bool(
            await self._repository.acquire_lease(
                name=self._normalize_name(name),
                owner_id=self._normalize_owner(owner_id),
                lease_seconds=self._normalize_lease_seconds(lease_seconds),
                metadata=dict(metadata or {}),
            )
        )

    async def heartbeat(
        self,
        *,
        name: str,
        owner_id: str,
        lease_seconds: int,
    ) -> bool:
        return bool(
            await self._repository.heartbeat_lease(
                name=self._normalize_name(name),
                owner_id=self._normalize_owner(owner_id),
                lease_seconds=self._normalize_lease_seconds(lease_seconds),
            )
        )

    async def release(self, *, name: str, owner_id: str) -> bool:
        return bool(
            await self._repository.release_lease(
                name=self._normalize_name(name),
                owner_id=self._normalize_owner(owner_id),
            )
        )

    async def cleanup_expired(self, *, older_than_seconds: int) -> int:
        cleanup = getattr(self._repository, "cleanup_expired", None)
        if cleanup is None:
            return 0
        return int(await cleanup(older_than_seconds=max(1, int(older_than_seconds))))

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

    def _normalize_lease_seconds(self, value: int) -> int:
        return max(1, int(value))
