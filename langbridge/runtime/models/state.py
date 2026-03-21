from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from langbridge.runtime.models.base import RuntimeModel


class ConnectorSyncState(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    connection_id: uuid.UUID
    connector_type: str
    resource_name: str
    sync_mode: str = "INCREMENTAL"
    last_cursor: str | None = None
    last_sync_at: datetime | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    status: str = "never_synced"
    error_message: str | None = None
    records_synced: int = 0
    bytes_synced: int | None = None
    dataset_ids: list[uuid.UUID] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def state_json(self) -> dict[str, Any]:
        return dict(self.state)


class SqlJob(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    actor_id: uuid.UUID
    connection_id: uuid.UUID | None = None
    workbench_mode: str
    selected_datasets_json: list[dict[str, Any]] = Field(default_factory=list)
    execution_mode: str = "single"
    status: str = "queued"
    query_text: str
    query_hash: str
    query_params_json: dict[str, Any] = Field(default_factory=dict)
    requested_limit: int | None = None
    enforced_limit: int = 1000
    requested_timeout_seconds: int | None = None
    enforced_timeout_seconds: int = 30
    is_explain: bool = False
    is_federated: bool = False
    correlation_id: str | None = None
    policy_snapshot_json: dict[str, Any] = Field(default_factory=dict)
    result_columns_json: list[dict[str, Any]] | None = None
    result_rows_json: list[dict[str, Any]] | None = None
    row_count_preview: int = 0
    total_rows_estimate: int | None = None
    bytes_scanned: int | None = None
    duration_ms: int | None = None
    result_cursor: str | None = None
    redaction_applied: bool = False
    error_json: dict[str, Any] | None = None
    warning_json: dict[str, Any] | None = None
    stats_json: dict[str, Any] | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    updated_at: datetime | None = None


class SqlJobResultArtifact(RuntimeModel):
    id: uuid.UUID
    sql_job_id: uuid.UUID
    workspace_id: uuid.UUID
    created_by: uuid.UUID
    format: str
    mime_type: str
    row_count: int = 0
    byte_size: int | None = None
    storage_backend: str
    storage_reference: str
    payload: dict[str, Any] | None = None
    created_at: datetime | None = None

    @property
    def payload_json(self) -> dict[str, Any] | None:
        return None if self.payload is None else dict(self.payload)
