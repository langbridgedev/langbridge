"""add dataset execution descriptor fields

Revision ID: c1a7d9e4f2b3
Revises: 8f3c1b0e4c2d
Create Date: 2026-03-08 14:30:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c1a7d9e4f2b3"
down_revision = "8f3c1b0e4c2d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("datasets", sa.Column("source_kind", sa.String(length=32), nullable=True))
    op.add_column("datasets", sa.Column("connector_kind", sa.String(length=64), nullable=True))
    op.add_column("datasets", sa.Column("storage_kind", sa.String(length=32), nullable=True))
    op.add_column("datasets", sa.Column("relation_identity_json", sa.JSON(), nullable=True))
    op.add_column("datasets", sa.Column("execution_capabilities_json", sa.JSON(), nullable=True))

    op.create_index(op.f("ix_datasets_source_kind"), "datasets", ["source_kind"], unique=False)
    op.create_index(op.f("ix_datasets_connector_kind"), "datasets", ["connector_kind"], unique=False)
    op.create_index(op.f("ix_datasets_storage_kind"), "datasets", ["storage_kind"], unique=False)

    op.execute(
        """
        UPDATE datasets
        SET source_kind = CASE
            WHEN dataset_type IN ('TABLE', 'SQL') THEN 'database'
            WHEN dataset_type = 'FEDERATED' THEN 'virtual'
            WHEN dataset_type = 'FILE' AND CAST(COALESCE(file_config_json, '{}'::json) AS TEXT) LIKE '%"connector_sync"%' THEN
                CASE
                    WHEN LOWER(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT)) LIKE '%"connector_type": "shopify"%'
                      OR LOWER(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT)) LIKE '%"connector_type": "hubspot"%'
                      OR LOWER(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT)) LIKE '%"connector_type": "salesforce"%'
                      OR LOWER(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT)) LIKE '%"connector_type": "stripe"%'
                      OR LOWER(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT)) LIKE '%"connector_type": "google_analytics"%'
                    THEN 'saas'
                    ELSE 'api'
                END
            WHEN dataset_type = 'FILE' THEN 'file'
            ELSE 'file'
        END
        WHERE source_kind IS NULL
        """
    )

    op.execute(
        """
        UPDATE datasets
        SET storage_kind = CASE
            WHEN dataset_type = 'TABLE' THEN 'table'
            WHEN dataset_type = 'SQL' THEN 'view'
            WHEN dataset_type = 'FEDERATED' THEN 'virtual'
            WHEN dataset_type = 'FILE' AND (
                LOWER(COALESCE(storage_uri, '')) LIKE '%.parquet'
                OR LOWER(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT)) LIKE '%"format": "parquet"%'
                OR LOWER(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT)) LIKE '%"file_format": "parquet"%'
            ) THEN 'parquet'
            WHEN dataset_type = 'FILE' AND (
                LOWER(COALESCE(storage_uri, '')) LIKE '%.json'
                OR LOWER(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT)) LIKE '%"format": "json"%'
                OR LOWER(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT)) LIKE '%"file_format": "json"%'
            ) THEN 'json'
            WHEN dataset_type = 'FILE' THEN 'csv'
            ELSE 'virtual'
        END
        WHERE storage_kind IS NULL
        """
    )

    op.execute(
        """
        UPDATE datasets
        SET connector_kind = LOWER(connectors.connector_type)
        FROM connectors
        WHERE datasets.connection_id = connectors.id
          AND datasets.connector_kind IS NULL
        """
    )

    op.execute(
        """
        UPDATE datasets
        SET connector_kind = CASE
            WHEN dataset_type = 'FEDERATED' THEN 'virtual'
            WHEN source_kind IN ('saas', 'api') THEN
                LOWER(SUBSTRING(CAST(COALESCE(file_config_json, '{}'::json) AS TEXT) FROM '"connector_type": "([^"]+)"'))
            WHEN storage_kind = 'parquet' THEN 'parquet_lake'
            WHEN storage_kind = 'json' THEN 'json_upload'
            WHEN storage_kind = 'csv' THEN 'csv_upload'
            ELSE 'file_upload'
        END
        WHERE connector_kind IS NULL
        """
    )

    op.execute(
        """
        UPDATE datasets
        SET relation_identity_json = '{}'::json,
            execution_capabilities_json = '{}'::json
        WHERE relation_identity_json IS NULL OR execution_capabilities_json IS NULL
        """
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_datasets_storage_kind"), table_name="datasets")
    op.drop_index(op.f("ix_datasets_connector_kind"), table_name="datasets")
    op.drop_index(op.f("ix_datasets_source_kind"), table_name="datasets")
    op.drop_column("datasets", "execution_capabilities_json")
    op.drop_column("datasets", "relation_identity_json")
    op.drop_column("datasets", "storage_kind")
    op.drop_column("datasets", "connector_kind")
    op.drop_column("datasets", "source_kind")
