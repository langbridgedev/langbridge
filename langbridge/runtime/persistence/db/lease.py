from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, Index, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RuntimeLeaseRecord(Base):
    __tablename__ = "runtime_leases"

    name: Mapped[str] = mapped_column(String(255), primary_key=True)
    owner_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    leased_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )
    heartbeat_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utc_now,
        onupdate=_utc_now,
        nullable=False,
    )

    __table_args__ = (
        Index("ix_runtime_leases_name_until", "name", "leased_until"),
    )
