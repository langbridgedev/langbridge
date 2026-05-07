from __future__ import annotations

import asyncio

import pytest

from langbridge.runtime.persistence.db import (
    create_async_engine_for_url,
    create_async_session_factory,
    create_engine_for_url,
    initialize_database,
)
from langbridge.runtime.persistence.repositories.lease_repository import RuntimeLeaseRepository
from langbridge.runtime.services.leases import RuntimeLeaseService


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


async def _create_sqlite_lease_service(tmp_path):
    metadata_path = tmp_path / "metadata.db"
    sync_engine = create_engine_for_url(f"sqlite:///{metadata_path}")
    try:
        initialize_database(sync_engine)
    finally:
        sync_engine.dispose()

    async_engine = create_async_engine_for_url(f"sqlite+aiosqlite:///{metadata_path}")
    session_factory = create_async_session_factory(async_engine)
    return async_engine, session_factory


@pytest.mark.anyio
async def test_runtime_lease_service_acquires_renews_and_releases(tmp_path) -> None:
    async_engine, session_factory = await _create_sqlite_lease_service(tmp_path)
    try:
        async with session_factory() as session:
            service = RuntimeLeaseService(repository=RuntimeLeaseRepository(session))
            assert await service.acquire(
                name="background-task:sync",
                owner_id="worker-1",
                lease_seconds=60,
            ) is True
            await session.commit()

        async with session_factory() as session:
            service = RuntimeLeaseService(repository=RuntimeLeaseRepository(session))
            assert await service.acquire(
                name="background-task:sync",
                owner_id="worker-2",
                lease_seconds=60,
            ) is False
            assert await service.heartbeat(
                name="background-task:sync",
                owner_id="worker-1",
                lease_seconds=60,
            ) is True
            assert await service.release(
                name="background-task:sync",
                owner_id="worker-1",
            ) is True
            await session.commit()

        async with session_factory() as session:
            service = RuntimeLeaseService(repository=RuntimeLeaseRepository(session))
            assert await service.acquire(
                name="background-task:sync",
                owner_id="worker-2",
                lease_seconds=60,
            ) is True
            await session.commit()
    finally:
        await async_engine.dispose()


@pytest.mark.anyio
async def test_runtime_lease_service_only_one_concurrent_owner_wins(tmp_path) -> None:
    async_engine, session_factory = await _create_sqlite_lease_service(tmp_path)
    try:
        async def acquire(owner_id: str) -> bool:
            async with session_factory() as session:
                service = RuntimeLeaseService(repository=RuntimeLeaseRepository(session))
                acquired = await service.acquire(
                    name="background-task:sync",
                    owner_id=owner_id,
                    lease_seconds=60,
                )
                await session.commit()
                return acquired

        outcomes = await asyncio.gather(*(acquire(f"worker-{index}") for index in range(8)))

        assert outcomes.count(True) == 1
    finally:
        await async_engine.dispose()
