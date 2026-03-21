from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import Field

from langbridge.runtime.models.base import RuntimeModel


class LLMProvider(str, Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    AZURE = "azure"


class LLMConnectionSecret(RuntimeModel):
    id: uuid.UUID
    name: str
    provider: LLMProvider
    model: str
    configuration: dict[str, Any] = Field(default_factory=dict)
    api_key: str
    description: str | None = None
    is_active: bool = True
    workspace_id: uuid.UUID
    created_at: datetime | None = None
    updated_at: datetime | None = None
