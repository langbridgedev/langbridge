from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict
from uuid import UUID

from pydantic import Field

from .base import _Base


class ExecutionMode(str, Enum):
    hosted = "hosted"
    customer_runtime = "customer_runtime"


class RuntimeRegistrationRequest(_Base):
    registration_token: str
    display_name: str | None = None
    tags: list[str] = Field(default_factory=list)
    capabilities: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RuntimeRegistrationResponse(_Base):
    ep_id: UUID
    workspace_id: UUID
    access_token: str
    token_type: str = "bearer"
    expires_at: datetime


class RuntimeHeartbeatRequest(_Base):
    status: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RuntimeHeartbeatResponse(_Base):
    accepted: bool = True
    server_time: datetime
    access_token: str
    expires_at: datetime


class RuntimeCapabilitiesUpdateRequest(_Base):
    tags: list[str] = Field(default_factory=list)
    capabilities: dict[str, Any] = Field(default_factory=dict)


class RuntimeCapabilitiesUpdateResponse(_Base):
    accepted: bool = True
    updated_at: datetime


class RuntimeInstanceResponse(_Base):
    ep_id: UUID
    workspace_id: UUID
    display_name: str | None = None
    status: str
    tags: list[str] = Field(default_factory=list)
    capabilities: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    registered_at: datetime
    last_seen_at: datetime | None = None
    updated_at: datetime | None = None


class EdgeTaskPullRequest(_Base):
    max_tasks: int = Field(default=1, ge=1, le=10)
    long_poll_seconds: int = Field(default=20, ge=1, le=60)
    visibility_timeout_seconds: int = Field(default=90, ge=10, le=600)


class EdgeTaskLease(_Base):
    task_id: UUID
    lease_id: str
    delivery_attempt: int
    envelope: Dict[str, Any] = Field(default_factory=dict)


class EdgeTaskPullResponse(_Base):
    tasks: list[EdgeTaskLease] = Field(default_factory=list)


class EdgeTaskAckRequest(_Base):
    task_id: UUID
    lease_id: str


class EdgeTaskAckResponse(_Base):
    accepted: bool = True
    status: str


class EdgeTaskFailRequest(_Base):
    task_id: UUID
    lease_id: str
    error: str
    retry_delay_seconds: int = Field(default=5, ge=0, le=600)


class EdgeTaskFailResponse(_Base):
    accepted: bool = True
    status: str


class EdgeTaskResultRequest(_Base):
    request_id: str
    task_id: UUID | None = None
    lease_id: str | None = None
    envelopes: list[Dict[str, Any]] = Field(default_factory=list)


class EdgeTaskResultResponse(_Base):
    accepted: bool = True
    duplicate: bool = False
