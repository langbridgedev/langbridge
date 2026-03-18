from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class LLMProvider(str, Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AZURE = "azure"


class LLMConnectionBase(BaseModel):
    name: str = Field(..., description="Name of the LLM connection")
    provider: LLMProvider = Field(
        ..., description="LLM provider (openai, anthropic, etc.)"
    )
    model: str = Field(..., description="Model name (e.g., gpt-4, claude-3)")
    configuration: dict[str, Any] | None = Field(
        default_factory=dict,
        description="Additional provider-specific configuration",
    )
    description: str | None = Field(
        default=None,
        description="Description of the LLM connection",
    )
    organization_id: UUID | None = None
    project_id: UUID | None = None

    model_config = ConfigDict(from_attributes=True)


class LLMConnectionCreate(LLMConnectionBase):
    api_key: str = Field(..., description="API key for the LLM provider")


class LLMConnectionUpdate(BaseModel):
    name: str
    api_key: str
    model: str
    configuration: dict[str, Any]
    is_active: bool
    organization_id: UUID | None = None
    project_id: UUID | None = None


class LLMConnectionResponse(LLMConnectionBase):
    id: UUID
    is_active: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None
    organization_id: UUID | None = None
    project_id: UUID | None = None


class LLMConnectionSecretResponse(LLMConnectionResponse):
    api_key: str


class LLMConnectionTest(BaseModel):
    provider: LLMProvider
    api_key: str
    model: str
    configuration: dict[str, Any] | None = Field(default_factory=dict)


__all__ = [
    "LLMProvider",
    "LLMConnectionBase",
    "LLMConnectionCreate",
    "LLMConnectionUpdate",
    "LLMConnectionResponse",
    "LLMConnectionSecretResponse",
    "LLMConnectionTest",
]
