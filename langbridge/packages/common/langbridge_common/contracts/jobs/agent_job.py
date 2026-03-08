from datetime import datetime
from enum import Enum
from typing import Any, Optional
import uuid
from pydantic import Field
from .type import JobType
from ..base import _Base


class CreateAgentJobRequest(_Base):
    job_type: JobType
    agent_definition_id: uuid.UUID
    organisation_id: uuid.UUID
    project_id: uuid.UUID
    user_id: uuid.UUID
    thread_id: uuid.UUID


class JobEventVisibility(str, Enum):
    public = "public"
    internal = "internal"


class JobEventResponse(_Base):
    id: uuid.UUID
    event_type: str
    visibility: JobEventVisibility
    message: str
    source: Optional[str] = None
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None


class JobFinalResponse(_Base):
    result: Any | None = None
    visualization: Any | None = None
    summary: str | None = None


class AgentJobStateResponse(_Base):
    id: uuid.UUID
    job_type: str
    status: str
    progress: int
    error: dict[str, Any] | None = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    events: list[JobEventResponse] = Field(default_factory=list)
    final_response: JobFinalResponse | None = None
    thinking_breakdown: dict[str, Any] | None = None
    has_internal_events: bool = False


class AgentJobCancelResponse(_Base):
    accepted: bool
    status: str
