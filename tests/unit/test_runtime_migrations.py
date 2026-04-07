
import json
import sqlite3
from pathlib import Path

import pytest

from langbridge.cli.main import main
from langbridge.runtime import build_configured_local_runtime
from langbridge.runtime.config import load_runtime_config, resolve_metadata_store_config
from langbridge.runtime.config.models import LocalRuntimeConfig
from langbridge.runtime.hosting import create_runtime_api_app
from langbridge.runtime.persistence.db import create_engine_for_url, initialize_database
from langbridge.runtime.persistence.migrations import (
    RuntimeMetadataMigrationRequiredError,
    build_runtime_metadata_alembic_config,
    migrate_runtime_metadata_store,
)


def _write_runtime_config(
    tmp_path: Path,
    *,
    metadata_store_type: str = "sqlite",
    metadata_store_path: str = "runtime-metadata.db",
    auto_apply: bool | None = None,
) -> Path:
    lines = [
        "version: 1",
        "runtime:",
    ]
    if auto_apply is not None:
        lines.extend(
            [
                "  migrations:",
                f"    auto_apply: {'true' if auto_apply else 'false'}",
            ]
        )
    lines.extend(
        [
            "  metadata_store:",
            f"    type: {metadata_store_type}",
            f"    path: {metadata_store_path}",
        ]
    )
    config_path = tmp_path / "langbridge_config.yml"
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config_path


def _sqlite_metadata_path(config_path: Path) -> Path:
    return (config_path.parent / "runtime-metadata.db").resolve()


def _sqlite_table_names(database_path: Path) -> set[str]:
    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    finally:
        connection.close()
    return {str(row[0]) for row in rows}


def _sqlite_alembic_revision(database_path: Path) -> str | None:
    connection = sqlite3.connect(database_path)
    try:
        row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    finally:
        connection.close()
    return None if row is None else str(row[0])


def test_cli_migrate_runs_runtime_metadata_migrations(tmp_path: Path, capsys) -> None:
    config_path = _write_runtime_config(tmp_path)

    exit_code = main(["migrate", "--config", str(config_path)])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["head_revision"]
    assert payload["current_revision"] == payload["head_revision"]
    assert payload["upgraded"] is True
    assert payload["stamped_legacy_schema"] is False
    metadata_db = _sqlite_metadata_path(config_path)
    assert "alembic_version" in _sqlite_table_names(metadata_db)
    assert _sqlite_alembic_revision(metadata_db) == payload["head_revision"]


def test_configured_runtime_migrates_sqlite_metadata_store_on_bootstrap(tmp_path: Path) -> None:
    config_path = _write_runtime_config(tmp_path)

    runtime = build_configured_local_runtime(config_path=config_path)
    try:
        metadata_db = _sqlite_metadata_path(config_path)
        assert "datasets" in _sqlite_table_names(metadata_db)
        assert _sqlite_alembic_revision(metadata_db) is not None
    finally:
        runtime.close()


def test_runtime_metadata_migrations_use_normalized_postgres_sync_url() -> None:
    config = LocalRuntimeConfig.model_validate(
        {
            "version": 1,
            "runtime": {
                "metadata_store": {
                    "type": "postgres",
                    "url": "postgres://langbridge:secret@db.example.com:5432/langbridge",
                }
            },
        }
    )

    metadata_store = resolve_metadata_store_config(
        config_path=Path("/tmp/langbridge_config.yml"),
        metadata_store=config.runtime.metadata_store,
    )
    alembic_config = build_runtime_metadata_alembic_config(metadata_store)

    assert metadata_store.sync_url == "postgresql+psycopg://langbridge:secret@db.example.com:5432/langbridge"
    assert alembic_config.get_main_option("sqlalchemy.url") == metadata_store.sync_url


def test_runtime_host_auto_migrates_sqlite_metadata_store_by_default(tmp_path: Path) -> None:
    config_path = _write_runtime_config(tmp_path, auto_apply=True)

    app = create_runtime_api_app(config_path=config_path)
    try:
        metadata_db = _sqlite_metadata_path(config_path)
        assert "alembic_version" in _sqlite_table_names(metadata_db)
        assert _sqlite_alembic_revision(metadata_db) is not None
        assert app.state.runtime_host.metadata_store.type == "sqlite"
    finally:
        app.state.runtime_host.close()


def test_runtime_host_fails_fast_when_schema_is_behind_and_auto_apply_disabled(tmp_path: Path) -> None:
    config_path = _write_runtime_config(tmp_path, auto_apply=False)

    with pytest.raises(RuntimeMetadataMigrationRequiredError) as exc_info:
        create_runtime_api_app(config_path=config_path)

    assert f"langbridge migrate --config {config_path.resolve()}" in str(exc_info.value)


def test_runtime_migrate_stamps_existing_unversioned_current_schema(tmp_path: Path) -> None:
    config_path = _write_runtime_config(tmp_path)
    config = load_runtime_config(config_path)
    metadata_store = resolve_metadata_store_config(
        config_path=config_path,
        metadata_store=config.runtime.metadata_store,
    )
    engine = create_engine_for_url(metadata_store.sync_url or "")
    try:
        initialize_database(engine)
    finally:
        engine.dispose()

    result = migrate_runtime_metadata_store(metadata_store)

    assert result.stamped_legacy_schema is True
    assert result.current_revision == result.head_revision
    assert _sqlite_alembic_revision(_sqlite_metadata_path(config_path)) == result.head_revision


def test_runtime_migrate_restamps_current_schema_from_superseded_revision(tmp_path: Path) -> None:
    config_path = _write_runtime_config(tmp_path)
    config = load_runtime_config(config_path)
    metadata_store = resolve_metadata_store_config(
        config_path=config_path,
        metadata_store=config.runtime.metadata_store,
    )
    engine = create_engine_for_url(metadata_store.sync_url or "")
    try:
        initialize_database(engine)
    finally:
        engine.dispose()

    metadata_db = _sqlite_metadata_path(config_path)
    connection = sqlite3.connect(metadata_db)
    try:
        connection.execute("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        connection.execute(
            "INSERT INTO alembic_version (version_num) VALUES (?)",
            ("67a2742aa6ff",),
        )
        connection.commit()
    finally:
        connection.close()

    result = migrate_runtime_metadata_store(metadata_store)

    assert result.stamped_legacy_schema is False
    assert result.upgraded is False
    assert result.current_revision == result.head_revision
    assert _sqlite_alembic_revision(metadata_db) == result.head_revision
