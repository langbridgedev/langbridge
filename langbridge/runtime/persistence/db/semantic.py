import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, DateTime, ForeignKey, String, Table, Text, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .workspace import Workspace


vector_entry_semantic = Table(
    "vector_entry_semantic",
    Base.metadata,
    Column("vector_entry_id", UUID(as_uuid=True), ForeignKey("semantic_vector_stores.id"), primary_key=True),
    Column("semantic_model_id", UUID(as_uuid=True), ForeignKey("semantic_models.id"), primary_key=True),
)

class SemanticModelEntry(Base):
    __tablename__ = "semantic_models"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    connector_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("connectors.id"), nullable=True)
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspaces.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    content_yaml: Mapped[str] = mapped_column(Text, nullable=False)
    content_json: Mapped[str] = mapped_column(Text, nullable=False)
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
    vector_stores: Mapped[list["SemanticVectorStoreEntry"]] = relationship(
        "SemanticVectorStoreEntry",
        secondary=vector_entry_semantic,
        back_populates="semantic_models",
    )

class SemanticVectorStoreEntry(Base):
    __tablename__ = "semantic_vector_stores"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    connector_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("connectors.id"), nullable=False)
    workspace_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("workspaces.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    vector_store_type: Mapped[str] = mapped_column(String(100), nullable=False)
    metadata_filters: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    workspace = relationship("Workspace", back_populates="semantic_vector_stores")
    semantic_models: Mapped[list[SemanticModelEntry]] = relationship(
        "SemanticModelEntry",
        secondary=vector_entry_semantic,
        back_populates="vector_stores",
    )
