import enum
from datetime import datetime
import uuid
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, JSON, String, Uuid as UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

class LLMProvider(enum.Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AZURE = "azure"
    OLLAMA = "ollama"
    openai = "openai"  # for backward compatibility

class LLMConnection(Base):
    """SQLAlchemy model for LLM connection configurations."""

    __tablename__ = "llm_connections"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(1024))
    provider = Column(String(50), nullable=False)
    api_key = Column(String(255), nullable=False)
    model = Column(String(50))
    configuration = Column(JSON)
    is_active = Column(Boolean, default=True)
    default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    workspace_id = Column(UUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)

    def __repr__(self):
        return f"<LLMConnection(name='{self.name}', provider='{self.provider}')>"


class AgentDefinition(Base):
    """SQLAlchemy model for AI agents."""

    __tablename__ = "agents"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(String(500))
    llm_connection_id = Column(UUID(as_uuid=True), ForeignKey("llm_connections.id"), nullable=False)
    definition = Column(JSON)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    management_mode: Mapped[str] = mapped_column(String(50), nullable=False)
    lifecycle_state: Mapped[str] = mapped_column(String(50), nullable=False)
