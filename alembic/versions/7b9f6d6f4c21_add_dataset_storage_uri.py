"""add dataset storage uri

Revision ID: 7b9f6d6f4c21
Revises: f2c0d3b1a8e4
Create Date: 2026-03-05 12:00:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "7b9f6d6f4c21"
down_revision = "f2c0d3b1a8e4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("datasets", sa.Column("storage_uri", sa.String(length=2048), nullable=True))


def downgrade() -> None:
    op.drop_column("datasets", "storage_uri")
