from __future__ import annotations

from logging.config import fileConfig
from pathlib import Path
import sys

from alembic import context
from sqlalchemy import engine_from_config, pool


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.db import Base
from langbridge.packages.common.langbridge_common.db import agent as _agent  # noqa: F401
from langbridge.packages.common.langbridge_common.db import associations as _associations  # noqa: F401
from langbridge.packages.common.langbridge_common.db import auth as _auth  # noqa: F401
from langbridge.packages.common.langbridge_common.db import bi as _bi  # noqa: F401
from langbridge.packages.common.langbridge_common.db import connector as _connector  # noqa: F401
from langbridge.packages.common.langbridge_common.db import environment as _environment  # noqa: F401
from langbridge.packages.common.langbridge_common.db import semantic as _semantic  # noqa: F401
from langbridge.packages.common.langbridge_common.db import threads as _threads  # noqa: F401
from langbridge.packages.common.langbridge_common.db import job as _job  # noqa: F401
from langbridge.packages.common.langbridge_common.db import messages as _messages  # noqa: F401
from langbridge.packages.common.langbridge_common.db import runtime as _runtime  # noqa: F401
from langbridge.packages.common.langbridge_common.db import sql as _sql  # noqa: F401


config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _get_url() -> str:
    url = settings.SQLALCHEMY_DATABASE_URI
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def run_migrations_offline() -> None:
    url = _get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = _get_url()
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
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
