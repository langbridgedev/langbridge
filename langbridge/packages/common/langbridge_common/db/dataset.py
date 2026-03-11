from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UUID,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class DatasetRecord(Base):
    __tablename__ = "datasets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    project_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("projects.id"),
        nullable=True,
        index=True,
    )
    connection_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("connectors.id"),
        nullable=True,
        index=True,
    )
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id"),
        nullable=True,
    )
    updated_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id"),
        nullable=True,
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    sql_alias: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        default=lambda: f"dataset_{uuid.uuid4().hex[:8]}",
    )
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    tags_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    dataset_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source_kind: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    connector_kind: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    storage_kind: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    dialect: Mapped[str | None] = mapped_column(String(64), nullable=True)

    catalog_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    storage_uri: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    sql_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    relation_identity_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    execution_capabilities_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    referenced_dataset_ids_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    federated_plan_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    file_config_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="published")
    revision_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    row_count_estimate: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    bytes_estimate: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_profiled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )

    columns: Mapped[list["DatasetColumnRecord"]] = relationship(
        "DatasetColumnRecord",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    policy: Mapped["DatasetPolicyRecord | None"] = relationship(
        "DatasetPolicyRecord",
        back_populates="dataset",
        cascade="all, delete-orphan",
        uselist=False,
    )
    revisions: Mapped[list["DatasetRevisionRecord"]] = relationship(
        "DatasetRevisionRecord",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("workspace_id", "name", name="uq_datasets_workspace_name"),
        UniqueConstraint("workspace_id", "sql_alias", name="uq_datasets_workspace_sql_alias"),
        Index("ix_datasets_workspace_name", "workspace_id", "name"),
        Index("ix_datasets_workspace_sql_alias", "workspace_id", "sql_alias"),
        Index("ix_datasets_workspace_updated_at", "workspace_id", "updated_at"),
    )


class DatasetColumnRecord(Base):
    __tablename__ = "dataset_columns"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("datasets.id", ondelete="cascade"),
        nullable=False,
        index=True,
    )
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    data_type: Mapped[str] = mapped_column(String(128), nullable=False)
    nullable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ordinal_position: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    is_allowed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_computed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    expression: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    dataset: Mapped[DatasetRecord] = relationship("DatasetRecord", back_populates="columns")

    __table_args__ = (
        UniqueConstraint("dataset_id", "name", name="uq_dataset_columns_dataset_name"),
        Index("ix_dataset_columns_dataset_ordinal", "dataset_id", "ordinal_position"),
    )


class DatasetPolicyRecord(Base):
    __tablename__ = "dataset_policies"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("datasets.id", ondelete="cascade"),
        nullable=False,
        unique=True,
        index=True,
    )
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    max_rows_preview: Mapped[int] = mapped_column(Integer, nullable=False, default=1000)
    max_export_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=10000)
    redaction_rules_json: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False, default=dict)
    row_filters_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    allow_dml: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    dataset: Mapped[DatasetRecord] = relationship("DatasetRecord", back_populates="policy")


class DatasetRevisionRecord(Base):
    __tablename__ = "dataset_revisions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("datasets.id", ondelete="cascade"),
        nullable=False,
        index=True,
    )
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    revision_number: Mapped[int] = mapped_column(Integer, nullable=False)
    revision_hash: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    change_summary: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    definition_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    schema_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    policy_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    source_bindings_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    execution_characteristics_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    note: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    dataset: Mapped[DatasetRecord] = relationship("DatasetRecord", back_populates="revisions")

    __table_args__ = (
        UniqueConstraint("dataset_id", "revision_number", name="uq_dataset_revisions_number"),
        Index("ix_dataset_revisions_dataset_created_at", "dataset_id", "created_at"),
        Index("ix_dataset_revisions_workspace_created_at", "workspace_id", "created_at"),
    )
