"""add connector is_managed column

Revision ID: b7e1c2a4d9f0
Revises: c1a7d9e4f2b3
Create Date: 2026-03-11 12:00:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b7e1c2a4d9f0"
down_revision = "c1a7d9e4f2b3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "connectors",
        sa.Column("is_managed", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.alter_column("connectors", "is_managed", server_default=None)


def downgrade() -> None:
    op.drop_column("connectors", "is_managed")
