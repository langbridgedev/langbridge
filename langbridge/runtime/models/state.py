
import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import Field, field_validator

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.runtime.models.base import RuntimeModel


class ConnectorSyncMode(str, Enum):
    INCREMENTAL = "INCREMENTAL"
    FULL_REFRESH = "FULL_REFRESH"
    WEBHOOK_ASSISTED = "WEBHOOK_ASSISTED"


class ConnectorSyncStatus(str, Enum):
    NEVER_SYNCED = "never_synced"
    RUNNING = "running"
    FAILED = "failed"
    SUCCEEDED = "succeeded"


def _normalize_enum_value(
    enum_cls: type[Enum],
    value: Any,
    *,
    case: str | None = None,
) -> Enum | None:
    if value is None:
        return None
    if isinstance(value, enum_cls):
        return value
    raw_value = str(getattr(value, "value", value) or "").strip()
    if not raw_value:
        return None
    if case == "lower":
        raw_value = raw_value.lower()
    elif case == "upper":
        raw_value = raw_value.upper()
    return enum_cls(raw_value)


class ConnectorSyncState(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    connection_id: uuid.UUID
    connector_type: ConnectorRuntimeType
    resource_name: str
    sync_mode: ConnectorSyncMode = ConnectorSyncMode.INCREMENTAL
    last_cursor: str | None = None
    last_sync_at: datetime | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    status: ConnectorSyncStatus = ConnectorSyncStatus.NEVER_SYNCED
    error_message: str | None = None
    records_synced: int = 0
    bytes_synced: int | None = None
    dataset_ids: list[uuid.UUID] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_validator("connector_type", mode="before")
    @classmethod
    def _validate_connector_type(cls, value: Any) -> ConnectorRuntimeType:
        normalized = _normalize_enum_value(ConnectorRuntimeType, value, case="upper")
        if normalized is None:
            raise ValueError("connector_type is required.")
        return normalized

    @field_validator("sync_mode", mode="before")
    @classmethod
    def _validate_sync_mode(cls, value: Any) -> ConnectorSyncMode:
        normalized = _normalize_enum_value(ConnectorSyncMode, value, case="upper")
        if normalized is None:
            return ConnectorSyncMode.INCREMENTAL
        return normalized

    @field_validator("status", mode="before")
    @classmethod
    def _validate_status(cls, value: Any) -> ConnectorSyncStatus:
        normalized = _normalize_enum_value(ConnectorSyncStatus, value, case="lower")
        if normalized is None:
            return ConnectorSyncStatus.NEVER_SYNCED
        return normalized

    @property
    def connector_type_value(self) -> str:
        return self.connector_type.value

    @property
    def sync_mode_value(self) -> str:
        return self.sync_mode.value

    @property
    def status_value(self) -> str:
        return self.status.value

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
