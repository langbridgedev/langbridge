
from pathlib import Path

from .models import (
    LocalRuntimeConfig,
    LocalRuntimeMetadataStoreConfig,
    ResolvedLocalRuntimeMetadataStoreConfig,
)


def _resolve_relative_path(base_dir: Path, value: str | None) -> str | None:
    normalized = str(value or "").strip()
    if not normalized or normalized == ":memory:":
        return normalized or None
    candidate = Path(normalized)
    if candidate.is_absolute():
        return str(candidate)
    return str((base_dir / candidate).resolve())


def _resolve_storage_uri(base_dir: Path, value: str | None) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if "://" in normalized:
        return normalized
    resolved_path = _resolve_relative_path(base_dir, normalized)
    if not resolved_path:
        return None
    return Path(resolved_path).resolve().as_uri()


def _sqlite_database_urls(path: Path) -> tuple[str, str]:
    resolved_path = path.resolve()
    database_path = resolved_path.as_posix()
    return (
        f"sqlite:///{database_path}",
        f"sqlite+aiosqlite:///{database_path}",
    )


def _normalize_postgres_metadata_store_url(url: str) -> tuple[str, str, str]:
    normalized = str(url or "").strip()
    if normalized.startswith("postgres://"):
        canonical = f"postgresql://{normalized[len('postgres://'):]}"
    elif normalized.startswith("postgresql+asyncpg://"):
        canonical = f"postgresql://{normalized[len('postgresql+asyncpg://'):]}"
    elif normalized.startswith("postgresql+psycopg://"):
        canonical = f"postgresql://{normalized[len('postgresql+psycopg://'):]}"
    elif normalized.startswith("postgresql://"):
        canonical = normalized
    else:
        raise ValueError(
            "runtime.metadata_store postgres url must start with postgres://, postgresql://, "
            "postgresql+asyncpg://, or postgresql+psycopg://."
        )
    return (
        canonical,
        canonical.replace("postgresql://", "postgresql+psycopg://", 1),
        canonical.replace("postgresql://", "postgresql+asyncpg://", 1),
    )


def normalize_runtime_config(
    *,
    config: LocalRuntimeConfig,
    config_path: Path,
) -> LocalRuntimeConfig:
    base_dir = config_path.parent
    metadata_store = config.runtime.metadata_store
    if metadata_store is not None and metadata_store.type == "sqlite":
        metadata_store.path = _resolve_relative_path(base_dir, metadata_store.path or ".langbridge/metadata.db")

    execution = config.runtime.execution
    if str(execution.engine or "").strip().lower() == "duckdb":
        execution.duckdb.path = _resolve_relative_path(base_dir, execution.duckdb.path)
        execution.duckdb.temp_directory = _resolve_relative_path(base_dir, execution.duckdb.temp_directory)

    for connector in config.connectors:
        for path_key in ("path", "location"):
            path_value = connector.connection.get(path_key)
            if isinstance(path_value, str):
                connector.connection[path_key] = _resolve_relative_path(base_dir, path_value)

    for dataset in config.datasets:
        if dataset.source.path:
            dataset.source.path = _resolve_relative_path(base_dir, dataset.source.path)
        if dataset.source.storage_uri and "://" not in str(dataset.source.storage_uri):
            dataset.source.storage_uri = _resolve_storage_uri(base_dir, dataset.source.storage_uri)

    return config


def resolve_metadata_store_config(
    *,
    config_path: Path,
    metadata_store: LocalRuntimeMetadataStoreConfig | None,
) -> ResolvedLocalRuntimeMetadataStoreConfig:
    if metadata_store is None:
        sqlite_path = (config_path.parent / ".langbridge" / "metadata.db").resolve()
        sync_url, async_url = _sqlite_database_urls(sqlite_path)
        return ResolvedLocalRuntimeMetadataStoreConfig(
            type="sqlite",
            path=sqlite_path,
            sync_url=sync_url,
            async_url=async_url,
        )

    if metadata_store.type == "in_memory":
        return ResolvedLocalRuntimeMetadataStoreConfig(type="in_memory")

    if metadata_store.type == "sqlite":
        raw_path = _resolve_relative_path(config_path.parent, metadata_store.path or ".langbridge/metadata.db")
        sqlite_path = Path(str(raw_path)).resolve()
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        sqlite_path.touch(exist_ok=True)
        sync_url, async_url = _sqlite_database_urls(sqlite_path)
        return ResolvedLocalRuntimeMetadataStoreConfig(
            type="sqlite",
            path=sqlite_path,
            sync_url=sync_url,
            async_url=async_url,
            echo=metadata_store.echo,
        )

    canonical_url, sync_url, async_url = _normalize_postgres_metadata_store_url(str(metadata_store.url or ""))
    return ResolvedLocalRuntimeMetadataStoreConfig(
        type="postgres",
        url=canonical_url,
        sync_url=sync_url,
        async_url=async_url,
        echo=metadata_store.echo,
        pool_size=metadata_store.pool_size,
        max_overflow=metadata_store.max_overflow,
        pool_timeout=metadata_store.pool_timeout,
    )


__all__ = [
    "_normalize_postgres_metadata_store_url",
    "_resolve_relative_path",
    "_resolve_storage_uri",
    "normalize_runtime_config",
    "resolve_metadata_store_config",
]
