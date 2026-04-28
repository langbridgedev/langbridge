"""add dataset label column

Revision ID: c2f4a8b19d11
Revises: 9c4e2a1d7f0b
Create Date: 2026-04-21 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "c2f4a8b19d11"
down_revision = "9c4e2a1d7f0b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("datasets", sa.Column("label", sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column("datasets", "label")
