"""runtime governance v1

Revision ID: 67a2742aa6ff
Revises: 8230e54e4fec
Create Date: 2026-03-30 00:00:00.000000
"""


from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "67a2742aa6ff"
down_revision = "8230e54e4fec"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("runtime_actors", schema=None) as batch_op:
        batch_op.add_column(sa.Column("username", sa.String(length=64), nullable=True))
        batch_op.add_column(
            sa.Column("status", sa.String(length=32), nullable=False, server_default="active")
        )
        batch_op.create_index(batch_op.f("ix_runtime_actors_username"), ["username"], unique=False)
        batch_op.create_unique_constraint(
            "uq_runtime_actors_workspace_username",
            ["workspace_id", "username"],
        )

    op.execute("UPDATE runtime_actors SET username = subject WHERE username IS NULL")
    op.execute(
        "UPDATE runtime_actors SET status = CASE WHEN is_active THEN 'active' ELSE 'disabled' END"
    )

    with op.batch_alter_table("runtime_local_auth_credentials", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "password_algorithm",
                sa.String(length=64),
                nullable=False,
                server_default="pbkdf2_sha256",
            )
        )
        batch_op.add_column(
            sa.Column(
                "password_updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            )
        )
        batch_op.add_column(
            sa.Column(
                "must_rotate_password",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("0"),
            )
        )

    op.execute(
        "UPDATE runtime_local_auth_credentials "
        "SET password_algorithm = 'pbkdf2_sha256', "
        "password_updated_at = COALESCE(updated_at, created_at, CURRENT_TIMESTAMP), "
        "must_rotate_password = 0"
    )

    with op.batch_alter_table("connectors", schema=None) as batch_op:
        batch_op.add_column(sa.Column("created_by_actor_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("updated_by_actor_id", sa.Uuid(), nullable=True))
        batch_op.create_index(batch_op.f("ix_connectors_created_by_actor_id"), ["created_by_actor_id"], unique=False)
        batch_op.create_index(batch_op.f("ix_connectors_updated_by_actor_id"), ["updated_by_actor_id"], unique=False)

    with op.batch_alter_table("semantic_models", schema=None) as batch_op:
        batch_op.add_column(sa.Column("created_by_actor_id", sa.Uuid(), nullable=True))
        batch_op.add_column(sa.Column("updated_by_actor_id", sa.Uuid(), nullable=True))
        batch_op.create_index(
            batch_op.f("ix_semantic_models_created_by_actor_id"),
            ["created_by_actor_id"],
            unique=False,
        )
        batch_op.create_index(
            batch_op.f("ix_semantic_models_updated_by_actor_id"),
            ["updated_by_actor_id"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("semantic_models", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_semantic_models_updated_by_actor_id"))
        batch_op.drop_index(batch_op.f("ix_semantic_models_created_by_actor_id"))
        batch_op.drop_column("updated_by_actor_id")
        batch_op.drop_column("created_by_actor_id")

    with op.batch_alter_table("connectors", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_connectors_updated_by_actor_id"))
        batch_op.drop_index(batch_op.f("ix_connectors_created_by_actor_id"))
        batch_op.drop_column("updated_by_actor_id")
        batch_op.drop_column("created_by_actor_id")

    with op.batch_alter_table("runtime_local_auth_credentials", schema=None) as batch_op:
        batch_op.drop_column("must_rotate_password")
        batch_op.drop_column("password_updated_at")
        batch_op.drop_column("password_algorithm")

    with op.batch_alter_table("runtime_actors", schema=None) as batch_op:
        batch_op.drop_constraint("uq_runtime_actors_workspace_username", type_="unique")
        batch_op.drop_index(batch_op.f("ix_runtime_actors_username"))
        batch_op.drop_column("status")
        batch_op.drop_column("username")
