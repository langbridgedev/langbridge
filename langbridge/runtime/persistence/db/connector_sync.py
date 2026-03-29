
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, JSON, String, Uuid as UUID, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ConnectorSyncStateRecord(Base):
    __tablename__ = "connector_sync_states"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workspaces.id"),
        nullable=False,
        index=True,
    )
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("connectors.id", ondelete="cascade"),
        nullable=False,
        index=True,
    )
    connector_type: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_name: Mapped[str] = mapped_column(String(255), nullable=False)
    sync_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="INCREMENTAL")
    last_cursor: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    state_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    dataset_ids_json: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="never_synced")
    error_message: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    records_synced: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_synced: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
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

    __table_args__ = (
        UniqueConstraint(
            "workspace_id",
            "connection_id",
            "resource_name",
            name="uq_connector_sync_states_workspace_connection_resource",
        ),
        Index(
            "ix_connector_sync_states_workspace_connection_updated",
            "workspace_id",
            "connection_id",
            "updated_at",
        ),
        Index(
            "ix_connector_sync_states_workspace_resource",
            "workspace_id",
            "resource_name",
        ),
    )
