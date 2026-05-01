from datetime import datetime
import uuid
from typing import Any

from pydantic import Field

from langbridge.runtime.models.base import RuntimeModel


class RuntimeJobStreamEvent(RuntimeModel):
    sequence: int
    event: str
    status: str
    stage: str
    message: str
    timestamp: datetime
    job_type: str = "runtime"
    thread_id: uuid.UUID | None = None
    job_id: uuid.UUID | None = None
    message_id: uuid.UUID | None = None
    visibility: str = "internal"
    terminal: bool = False
    source: str | None = None
    raw_event_type: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)
