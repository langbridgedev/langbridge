from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager
from typing import Any
import json

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from .base import Base
from .model_registry import register_runtime_metadata_models


def _build_connect_args(database_url: str) -> dict[str, Any]:
    """Return driver-specific connect arguments."""
    connect_args: dict[str, Any] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    if database_url.startswith("sqlite+aiosqlite"):
        connect_args["check_same_thread"] = False
    return connect_args


def _engine_kwargs(
    database_url: str,
    echo: bool,
    pool_size: int | None,
    max_overflow: int | None,
    pool_timeout: int | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "echo": echo,
        "future": True,
        "pool_pre_ping": True,
        "connect_args": _build_connect_args(database_url),
        "json_serializer": lambda obj: json.dumps(obj, default=str),
        "json_deserializer": json.loads,
    }
    if database_url.startswith("sqlite"):
        kwargs["poolclass"] = NullPool
    else:
        if pool_size is not None:
            kwargs["pool_size"] = pool_size
        if max_overflow is not None:
            kwargs["max_overflow"] = max_overflow
        if pool_timeout is not None:
            kwargs["pool_timeout"] = pool_timeout
    return kwargs


def create_engine_for_url(
    database_url: str,
    echo: bool = False,
    *,
    pool_size: int | None = None,
    max_overflow: int | None = None,
    pool_timeout: int | None = None,
) -> Engine:
    """Create SQLAlchemy engine configured for SQLite or PostgreSQL."""
    kwargs = _engine_kwargs(database_url, echo, pool_size, max_overflow, pool_timeout)
    return create_engine(database_url, **kwargs)


def create_async_engine_for_url(
    database_url: str,
    echo: bool = False,
    *,
    pool_size: int | None = None,
    max_overflow: int | None = None,
    pool_timeout: int | None = None,
) -> AsyncEngine:
    """Create an async SQLAlchemy engine configured for SQLite or PostgreSQL."""
    kwargs = _engine_kwargs(database_url, echo, pool_size, max_overflow, pool_timeout)
    return create_async_engine(database_url, **kwargs)


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Build a session factory bound to the provided engine."""
    return sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


def create_async_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build an async session factory bound to the provided async engine."""
    return async_sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
    )


def session_scope(session_factory: sessionmaker[Session]) -> Generator[Session, None, None]:
    """Provide a transactional scope around a series of operations."""
    session = session_factory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@asynccontextmanager
async def async_session_scope(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Provide a transactional async scope around a series of operations."""
    session = session_factory()
    try:
        yield session
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


def initialize_database(engine: Engine) -> None:
    """Legacy/test helper for creating the current metadata schema without Alembic."""
    register_runtime_metadata_models()
    Base.metadata.create_all(bind=engine)
