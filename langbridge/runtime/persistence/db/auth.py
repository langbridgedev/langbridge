
import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, String, Uuid as UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from .base import Base

if TYPE_CHECKING:
    from .workspace import RuntimeActor, Workspace


class RuntimeLocalAuthState(Base):
    __tablename__ = "runtime_local_auth_state"

    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workspaces.id"),
        primary_key=True,
        nullable=False,
    )
    session_secret: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    workspace: Mapped["Workspace"] = relationship("Workspace")


class RuntimeLocalAuthCredential(Base):
    __tablename__ = "runtime_local_auth_credentials"

    actor_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runtime_actors.id"),
        primary_key=True,
        nullable=False,
    )
    workspace_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workspaces.id"),
        nullable=False,
        index=True,
    )
    password_hash: Mapped[str] = mapped_column(String(512), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    actor: Mapped["RuntimeActor"] = relationship("RuntimeActor")
    workspace: Mapped["Workspace"] = relationship("Workspace")


__all__ = [
    "RuntimeLocalAuthCredential",
    "RuntimeLocalAuthState",
]
