
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from langbridge.runtime.persistence.db import get_runtime_metadata

config = context.config

if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name)

target_metadata = get_runtime_metadata()


def _get_database_url() -> str:
    database_url = config.attributes.get("database_url")
    if database_url:
        return str(database_url)
    configured_url = config.get_main_option("sqlalchemy.url")
    if configured_url:
        return configured_url
    raise RuntimeError("Alembic runtime metadata database URL is not configured.")


def run_migrations_offline() -> None:
    context.configure(
        url=_get_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = _get_database_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
