"""add dataset sql alias

Revision ID: e6f9a1b2c3d4
Revises: d4b8e2f1c6a7
Create Date: 2026-03-11 16:30:00.000000

"""

from __future__ import annotations

import re

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e6f9a1b2c3d4"
down_revision = "d4b8e2f1c6a7"
branch_labels = None
depends_on = None


_ALIAS_SANITIZER = re.compile(r"[^a-z0-9_]+")


def _base_alias(value: str | None) -> str:
    candidate = _ALIAS_SANITIZER.sub("_", str(value or "").strip().lower())
    candidate = re.sub(r"_+", "_", candidate).strip("_")
    if not candidate:
        return "dataset"
    if candidate[0].isdigit():
        return f"dataset_{candidate}"
    return candidate


def upgrade() -> None:
    op.add_column("datasets", sa.Column("sql_alias", sa.String(length=128), nullable=True))

    bind = op.get_bind()
    dataset_table = sa.table(
        "datasets",
        sa.column("id", sa.String()),
        sa.column("workspace_id", sa.String()),
        sa.column("name", sa.String()),
        sa.column("sql_alias", sa.String()),
    )
    rows = bind.execute(
        sa.select(
            dataset_table.c.id,
            dataset_table.c.workspace_id,
            dataset_table.c.name,
        )
    ).fetchall()

    seen_by_workspace: dict[str, set[str]] = {}
    for row in rows:
        workspace_id = str(row.workspace_id)
        used = seen_by_workspace.setdefault(workspace_id, set())
        base = _base_alias(row.name)
        candidate = base
        counter = 2
        while candidate in used:
            candidate = f"{base}_{counter}"
            counter += 1
        used.add(candidate)
        bind.execute(
            dataset_table.update()
            .where(dataset_table.c.id == row.id)
            .values(sql_alias=candidate)
        )

    op.alter_column("datasets", "sql_alias", nullable=False)
    op.create_unique_constraint("uq_datasets_workspace_sql_alias", "datasets", ["workspace_id", "sql_alias"])
    op.create_index("ix_datasets_workspace_sql_alias", "datasets", ["workspace_id", "sql_alias"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_datasets_workspace_sql_alias", table_name="datasets")
    op.drop_constraint("uq_datasets_workspace_sql_alias", "datasets", type_="unique")
    op.drop_column("datasets", "sql_alias")
