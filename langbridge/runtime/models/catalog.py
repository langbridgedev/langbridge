
import enum
import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from langbridge.runtime.models.base import RuntimeModel


class DatasetRevision(RuntimeModel):
    id: uuid.UUID
    dataset_id: uuid.UUID
    workspace_id: uuid.UUID
    revision_number: int
    revision_hash: str | None = None
    change_summary: str | None = None
    definition: dict[str, Any] | None = None
    schema_snapshot: list[dict[str, Any]] | None = None
    policy: dict[str, Any] | None = None
    source_bindings: list[dict[str, Any]] | None = None
    execution_characteristics: dict[str, Any] | None = None
    status: str | None = None
    snapshot: dict[str, Any] = Field(default_factory=dict)
    note: str | None = None
    created_by: uuid.UUID | None = None
    created_at: datetime | None = None

    @property
    def definition_json(self) -> dict[str, Any] | None:
        return None if self.definition is None else dict(self.definition)

    @property
    def schema_json(self) -> list[dict[str, Any]] | None:
        return (
            None
            if self.schema_snapshot is None
            else [dict(item) for item in self.schema_snapshot]
        )

    @property
    def policy_json(self) -> dict[str, Any] | None:
        return None if self.policy is None else dict(self.policy)

    @property
    def source_bindings_json(self) -> list[dict[str, Any]] | None:
        return None if self.source_bindings is None else [dict(item) for item in self.source_bindings]

    @property
    def execution_characteristics_json(self) -> dict[str, Any] | None:
        return (
            None
            if self.execution_characteristics is None
            else dict(self.execution_characteristics)
        )

    @property
    def snapshot_json(self) -> dict[str, Any]:
        return dict(self.snapshot)


class LineageEdge(RuntimeModel):
    id: uuid.UUID | None = None
    workspace_id: uuid.UUID
    source_type: str
    source_id: str
    target_type: str
    target_id: str
    edge_type: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None

    @property
    def metadata_json(self) -> dict[str, Any]:
        return dict(self.metadata)


class RuntimeJobStatus(str, enum.Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class RuntimeJob(RuntimeModel):
    id: uuid.UUID
    workspace_id: str
    job_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, Any] = Field(default_factory=dict)
    status: RuntimeJobStatus | str = RuntimeJobStatus.queued
    progress: int = 0
    status_message: str | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    created_at: datetime | None = None
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime | None = None
