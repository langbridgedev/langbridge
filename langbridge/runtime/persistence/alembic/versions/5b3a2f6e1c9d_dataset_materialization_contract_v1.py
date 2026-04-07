"""dataset materialization contract v1

Revision ID: 5b3a2f6e1c9d
Revises: 67a2742aa6ff
Create Date: 2026-04-01 00:00:00.000000
"""


from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5b3a2f6e1c9d"
down_revision = "67a2742aa6ff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("datasets", schema=None) as batch_op:
        batch_op.add_column(sa.Column("source_json", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("sync_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("datasets", schema=None) as batch_op:
        batch_op.drop_column("sync_json")
        batch_op.drop_column("source_json")
