from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import Field

from langbridge.contracts.base import _Base
from langbridge.contracts.jobs.type import JobType


class CreateAgentJobRequest(_Base):
    job_type: JobType
    agent_definition_id: uuid.UUID
    workspace_id: uuid.UUID
    actor_id: uuid.UUID
    thread_id: uuid.UUID


class JobEventVisibility(str, Enum):
    public = "public"
    internal = "internal"


class JobEventResponse(_Base):
    id: uuid.UUID
    event_type: str
    visibility: JobEventVisibility
    message: str
    source: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


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
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    events: list[JobEventResponse] = Field(default_factory=list)
    final_response: JobFinalResponse | None = None
    thinking_breakdown: dict[str, Any] | None = None
    has_internal_events: bool = False


class AgentJobCancelResponse(_Base):
    accepted: bool
    status: str


__all__ = [
    "CreateAgentJobRequest",
    "JobEventVisibility",
    "JobEventResponse",
    "JobFinalResponse",
    "AgentJobStateResponse",
    "AgentJobCancelResponse",
]
