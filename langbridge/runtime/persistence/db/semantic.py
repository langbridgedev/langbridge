import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, String, Text, UUID, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class SemanticModelEntry(Base):
    __tablename__ = "semantic_models"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    connector_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("connectors.id"), nullable=True)
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspaces.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    content_yaml: Mapped[str] = mapped_column(Text, nullable=False)
    content_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    workspace = relationship("Workspace", back_populates="semantic_models")
    vector_indexes: Mapped[list["SemanticVectorIndexEntry"]] = relationship(
        "SemanticVectorIndexEntry",
        back_populates="semantic_model",
        cascade="all, delete-orphan",
    )


class SemanticVectorIndexEntry(Base):
    __tablename__ = "semantic_vector_indexes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspaces.id"), nullable=False, index=True)
    semantic_model_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("semantic_models.id"),
        nullable=False,
        index=True,
    )
    dataset_key: Mapped[str] = mapped_column(String(255), nullable=False)
    dimension_name: Mapped[str] = mapped_column(String(255), nullable=False)
    vector_store_target: Mapped[str] = mapped_column(String(64), nullable=False)
    vector_connector_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    vector_connector_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("connectors.id"), nullable=True)
    vector_index_name: Mapped[str] = mapped_column(String(255), nullable=False)
    refresh_interval_seconds: Mapped[int | None] = mapped_column(nullable=True)
    refresh_status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    indexed_value_count: Mapped[int | None] = mapped_column(nullable=True)
    embedding_dimension: Mapped[int | None] = mapped_column(nullable=True)
    last_refresh_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_refresh_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    workspace = relationship("Workspace", back_populates="semantic_vector_indexes")
    semantic_model = relationship("SemanticModelEntry", back_populates="vector_indexes")

    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "semantic_model_id",
            "dataset_key",
            "dimension_name",
            name="uq_semantic_vector_index_dimension",
        ),
    )
