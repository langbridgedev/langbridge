
from dataclasses import dataclass
from pathlib import Path
import importlib
import sys
from typing import Any

from sqlalchemy import inspect

from langbridge.runtime.config import load_runtime_config, resolve_metadata_store_config
from langbridge.runtime.config.models import ResolvedLocalRuntimeMetadataStoreConfig
from langbridge.runtime.persistence.db import (
    create_engine_for_url,
    get_runtime_metadata,
)


class RuntimeMetadataMigrationError(RuntimeError):
    """Raised when the runtime metadata store cannot be migrated safely."""


class RuntimeMetadataMigrationRequiredError(RuntimeMetadataMigrationError):
    """Raised when the runtime metadata schema is behind and auto-apply is disabled."""


@dataclass(slots=True, frozen=True)
class RuntimeMetadataSchemaStatus:
    current_revision: str | None
    head_revision: str
    is_current: bool
    has_version_table: bool
    has_runtime_tables: bool


@dataclass(slots=True, frozen=True)
class RuntimeMetadataMigrationResult:
    current_revision: str | None
    head_revision: str
    upgraded: bool
    stamped_legacy_schema: bool


def migrate_runtime_metadata_store(
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
) -> RuntimeMetadataMigrationResult:
    if metadata_store.type == "in_memory":
        return RuntimeMetadataMigrationResult(
            current_revision=None,
            head_revision="in_memory",
            upgraded=False,
            stamped_legacy_schema=False,
        )

    _prepare_metadata_store_filesystem(metadata_store)
    config = build_runtime_metadata_alembic_config(metadata_store)
    _, _, _, script_directory_cls = _load_alembic_modules()
    script = script_directory_cls.from_config(config)
    head_revision = script.get_current_head()
    if head_revision is None:
        raise RuntimeMetadataMigrationError("Alembic has no runtime metadata head revision configured.")

    stamped_legacy_schema = _stamp_unversioned_current_schema_if_possible(
        metadata_store=metadata_store,
        config=config,
        head_revision=head_revision,
    )
    status = get_runtime_metadata_schema_status(metadata_store)
    if status.is_current:
        return RuntimeMetadataMigrationResult(
            current_revision=status.current_revision,
            head_revision=head_revision,
            upgraded=False,
            stamped_legacy_schema=stamped_legacy_schema,
        )

    command_module, _, _, _ = _load_alembic_modules()
    command_module.upgrade(config, "head")
    status = get_runtime_metadata_schema_status(metadata_store)
    return RuntimeMetadataMigrationResult(
        current_revision=status.current_revision,
        head_revision=head_revision,
        upgraded=True,
        stamped_legacy_schema=stamped_legacy_schema,
    )


def migrate_runtime_metadata_for_config(config_path: str | Path) -> RuntimeMetadataMigrationResult:
    resolved_config_path = Path(config_path).resolve()
    config = load_runtime_config(resolved_config_path)
    metadata_store = resolve_metadata_store_config(
        config_path=resolved_config_path,
        metadata_store=config.runtime.metadata_store,
    )
    return migrate_runtime_metadata_store(metadata_store)


def ensure_runtime_metadata_schema_current(
    *,
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
    auto_apply: bool,
    config_path: str | Path | None = None,
) -> RuntimeMetadataMigrationResult | None:
    if metadata_store.type == "in_memory":
        return None

    status = get_runtime_metadata_schema_status(metadata_store)
    if status.is_current:
        return RuntimeMetadataMigrationResult(
            current_revision=status.current_revision,
            head_revision=status.head_revision,
            upgraded=False,
            stamped_legacy_schema=False,
        )

    if auto_apply:
        return migrate_runtime_metadata_store(metadata_store)

    if config_path is not None:
        resolved_config_path = Path(config_path).resolve()
        raise RuntimeMetadataMigrationRequiredError(
            "Runtime metadata schema is behind"
            f" for {resolved_config_path}. Run `langbridge migrate --config {resolved_config_path}` "
            "or set `runtime.migrations.auto_apply: true`."
        )
    raise RuntimeMetadataMigrationRequiredError(
        "Runtime metadata schema is behind. Run `langbridge migrate --config <path>` "
        "or set `runtime.migrations.auto_apply: true`."
    )


def get_runtime_metadata_schema_status(
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
) -> RuntimeMetadataSchemaStatus:
    _prepare_metadata_store_filesystem(metadata_store)
    config = build_runtime_metadata_alembic_config(metadata_store)
    _, _, _, script_directory_cls = _load_alembic_modules()
    script = script_directory_cls.from_config(config)
    head_revision = script.get_current_head()
    if head_revision is None:
        raise RuntimeMetadataMigrationError("Alembic has no runtime metadata head revision configured.")

    engine = create_engine_for_url(
        metadata_store.sync_url or "",
        metadata_store.echo,
        pool_size=metadata_store.pool_size,
        max_overflow=metadata_store.max_overflow,
        pool_timeout=metadata_store.pool_timeout,
    )
    try:
        with engine.connect() as connection:
            _, _, migration_context_cls, _ = _load_alembic_modules()
            context = migration_context_cls.configure(connection)
            current_revision = context.get_current_revision()
            inspector = inspect(connection)
            table_names = set(inspector.get_table_names())
    finally:
        engine.dispose()

    has_version_table = "alembic_version" in table_names
    runtime_table_names = set(get_runtime_metadata().tables.keys())
    has_runtime_tables = bool(runtime_table_names & table_names)
    return RuntimeMetadataSchemaStatus(
        current_revision=current_revision,
        head_revision=head_revision,
        is_current=current_revision == head_revision,
        has_version_table=has_version_table,
        has_runtime_tables=has_runtime_tables,
    )


def resolve_runtime_repo_root() -> Path:
    current_file = Path(__file__).resolve()
    for candidate in (current_file.parent, *current_file.parents):
        if (candidate / "alembic.ini").exists() and (candidate / "alembic").is_dir():
            return candidate
    raise RuntimeMetadataMigrationError(
        f"Could not locate Alembic configuration from {current_file}."
    )


def build_runtime_metadata_alembic_config(
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
) -> Any:
    _, config_cls, _, _ = _load_alembic_modules()
    repo_root = resolve_runtime_repo_root()
    config = config_cls(str(repo_root / "alembic.ini"))
    config.set_main_option("script_location", str(repo_root / "alembic"))
    config.set_main_option("sqlalchemy.url", metadata_store.sync_url or "")
    config.attributes["database_url"] = metadata_store.sync_url or ""
    config.attributes["configure_logger"] = False
    return config


def _stamp_unversioned_current_schema_if_possible(
    *,
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
    config: Any,
    head_revision: str,
) -> bool:
    status = get_runtime_metadata_schema_status(metadata_store)
    if status.has_version_table or not status.has_runtime_tables:
        return False

    if not _database_matches_current_runtime_metadata_schema(metadata_store):
        raise RuntimeMetadataMigrationError(
            "Runtime metadata tables already exist but are not tracked by Alembic and do not match "
            "the current baseline schema. Back up the database and align it manually before running migrations."
        )

    command_module, _, _, _ = _load_alembic_modules()
    command_module.stamp(config, head_revision)
    return True


def _database_matches_current_runtime_metadata_schema(
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
) -> bool:
    runtime_metadata = get_runtime_metadata()
    engine = create_engine_for_url(
        metadata_store.sync_url or "",
        metadata_store.echo,
        pool_size=metadata_store.pool_size,
        max_overflow=metadata_store.max_overflow,
        pool_timeout=metadata_store.pool_timeout,
    )
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            table_names = set(inspector.get_table_names())
            for table_name, table in runtime_metadata.tables.items():
                if table_name not in table_names:
                    return False
                existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
                expected_columns = {column.name for column in table.columns}
                if not expected_columns.issubset(existing_columns):
                    return False
    finally:
        engine.dispose()
    return True


def _prepare_metadata_store_filesystem(
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
) -> None:
    if metadata_store.path is None:
        return
    metadata_store.path.parent.mkdir(parents=True, exist_ok=True)
    metadata_store.path.touch(exist_ok=True)


def _load_alembic_modules() -> tuple[Any, Any, Any, Any]:
    repo_root = resolve_runtime_repo_root()
    removed_entries: list[str] = []
    candidate_entries = {"", ".", str(repo_root), repo_root.as_posix()}
    for entry in list(sys.path):
        if entry in candidate_entries:
            removed_entries.append(entry)
            sys.path.remove(entry)
    try:
        command_module = importlib.import_module("alembic.command")
        config_module = importlib.import_module("alembic.config")
        migration_module = importlib.import_module("alembic.runtime.migration")
        script_module = importlib.import_module("alembic.script")
    finally:
        for entry in reversed(removed_entries):
            sys.path.insert(0, entry)
    return (
        command_module,
        getattr(config_module, "Config"),
        getattr(migration_module, "MigrationContext"),
        getattr(script_module, "ScriptDirectory"),
    )


__all__ = [
    "RuntimeMetadataMigrationError",
    "RuntimeMetadataMigrationRequiredError",
    "RuntimeMetadataMigrationResult",
    "RuntimeMetadataSchemaStatus",
    "build_runtime_metadata_alembic_config",
    "ensure_runtime_metadata_schema_current",
    "get_runtime_metadata_schema_status",
    "migrate_runtime_metadata_for_config",
    "migrate_runtime_metadata_store",
    "resolve_runtime_repo_root",
]
