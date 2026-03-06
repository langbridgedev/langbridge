from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, JSON, String, UUID, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class LineageEdgeRecord(Base):
    __tablename__ = "lineage_edges"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id"),
        nullable=False,
        index=True,
    )
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    target_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[str] = mapped_column(String(255), nullable=False)
    edge_type: Mapped[str] = mapped_column(String(64), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "source_type",
            "source_id",
            "target_type",
            "target_id",
            "edge_type",
            name="uq_lineage_edges_workspace_source_target_edge",
        ),
        Index("ix_lineage_edges_workspace_source", "workspace_id", "source_type", "source_id"),
        Index("ix_lineage_edges_workspace_target", "workspace_id", "target_type", "target_id"),
        Index("ix_lineage_edges_workspace_created_at", "workspace_id", "created_at"),
    )
