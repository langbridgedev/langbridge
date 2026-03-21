from __future__ import annotations

from datetime import datetime
import uuid
from typing import Any

from pydantic import Field, model_validator

from langbridge.runtime.models.base import RuntimeModel, RuntimeRequestModel
from langbridge.runtime.models.jobs import SqlSelectedDataset


class RuntimeInfoResponse(RuntimeModel):
    api_version: str = "v1"
    runtime_mode: str
    config_path: str
    workspace_id: uuid.UUID
    actor_id: uuid.UUID | None = None
    roles: list[str] = Field(default_factory=list)
    default_semantic_model: str | None = None
    default_agent: str | None = None
    capabilities: list[str] = Field(default_factory=list)


class RuntimeDatasetSummary(RuntimeModel):
    id: uuid.UUID | None = None
    name: str
    label: str | None = None
    description: str | None = None
    connector: str | None = None
    semantic_model: str | None = None
    managed: bool = False


class RuntimeDatasetListResponse(RuntimeModel):
    items: list[RuntimeDatasetSummary] = Field(default_factory=list)
    total: int = 0


class RuntimeConnectorSummary(RuntimeModel):
    id: uuid.UUID | None = None
    name: str
    description: str | None = None
    connector_type: str | None = None
    supports_sync: bool = False
    supported_resources: list[str] = Field(default_factory=list)
    sync_strategy: str | None = None
    managed: bool = False


class RuntimeConnectorListResponse(RuntimeModel):
    items: list[RuntimeConnectorSummary] = Field(default_factory=list)
    total: int = 0


class RuntimeDatasetPreviewRequest(RuntimeRequestModel):
    limit: int | None = Field(default=None, ge=1)
    filters: dict[str, Any] = Field(default_factory=dict)
    sort: list[dict[str, Any]] = Field(default_factory=list)
    user_context: dict[str, Any] = Field(default_factory=dict)


class RuntimeDatasetPreviewResponse(RuntimeModel):
    dataset_id: uuid.UUID | None = None
    dataset_name: str | None = None
    status: str
    columns: list[dict[str, Any]] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count_preview: int = 0
    effective_limit: int | None = None
    redaction_applied: bool = False
    duration_ms: int | None = None
    bytes_scanned: int | None = None
    generated_sql: str | None = None
    error: str | None = None
    job_id: uuid.UUID | None = None


class RuntimeSemanticQueryRequest(RuntimeRequestModel):
    semantic_models: list[str] = Field(default_factory=list)
    semantic_model: str | None = None
    measures: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    filters: list[dict[str, Any]] = Field(default_factory=list)
    time_dimensions: list[dict[str, Any]] = Field(default_factory=list)
    limit: int | None = Field(default=None, ge=1)
    order: dict[str, str] | list[dict[str, str]] | None = None

    @model_validator(mode="after")
    def _hydrate_semantic_models(self) -> "RuntimeSemanticQueryRequest":
        normalized = [item for item in self.semantic_models if str(item).strip()]
        if self.semantic_model and str(self.semantic_model).strip():
            normalized.insert(0, str(self.semantic_model).strip())
        deduplicated: list[str] = []
        for item in normalized:
            if item not in deduplicated:
                deduplicated.append(item)
        self.semantic_models = deduplicated
        return self


class RuntimeSemanticQueryResponse(RuntimeModel):
    status: str
    semantic_model_id: uuid.UUID | None = None
    semantic_model_ids: list[uuid.UUID] = Field(default_factory=list)
    connector_id: uuid.UUID | None = None
    data: list[dict[str, Any]] = Field(default_factory=list)
    annotations: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] | None = None
    generated_sql: str | None = None
    error: str | None = None


class RuntimeSqlQueryRequest(RuntimeRequestModel):
    query: str = Field(..., min_length=1)
    connection_id: uuid.UUID | None = None
    connection_name: str | None = None
    selected_datasets: list[SqlSelectedDataset] = Field(default_factory=list)
    query_dialect: str = "tsql"
    params: dict[str, Any] = Field(default_factory=dict)
    requested_limit: int | None = Field(default=None, ge=1)
    requested_timeout_seconds: int | None = Field(default=None, ge=1)
    explain: bool = False


class RuntimeSqlQueryResponse(RuntimeModel):
    sql_job_id: uuid.UUID | None = None
    status: str
    columns: list[dict[str, Any]] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count_preview: int = 0
    total_rows_estimate: int | None = None
    bytes_scanned: int | None = None
    duration_ms: int | None = None
    redaction_applied: bool = False
    error: dict[str, Any] | None = None
    query: str | None = None
    generated_sql: str | None = None


class RuntimeAgentAskRequest(RuntimeRequestModel):
    message: str = Field(..., min_length=1)
    agent_id: uuid.UUID | None = None
    agent_name: str | None = None
    thread_id: uuid.UUID | None = None
    title: str | None = None
    metadata_json: dict[str, Any] | None = None


class RuntimeAgentAskResponse(RuntimeModel):
    thread_id: uuid.UUID | None = None
    status: str
    job_id: uuid.UUID | None = None
    summary: str | None = None
    result: Any | None = None
    visualization: Any | None = None
    error: dict[str, Any] | None = None
    events: list[dict[str, Any]] = Field(default_factory=list)


class RuntimeSyncResourceSummary(RuntimeModel):
    name: str
    label: str | None = None
    primary_key: str | None = None
    parent_resource: str | None = None
    cursor_field: str | None = None
    incremental_cursor_field: str | None = None
    supports_incremental: bool = False
    default_sync_mode: str | None = None
    status: str | None = None
    last_cursor: str | None = None
    last_sync_at: datetime | None = None
    dataset_ids: list[uuid.UUID] = Field(default_factory=list)
    dataset_names: list[str] = Field(default_factory=list)
    records_synced: int = 0
    bytes_synced: int | None = None


class RuntimeSyncResourceListResponse(RuntimeModel):
    items: list[RuntimeSyncResourceSummary] = Field(default_factory=list)
    total: int = 0


class RuntimeSyncStateSummary(RuntimeModel):
    id: uuid.UUID | None = None
    workspace_id: uuid.UUID | None = None
    connection_id: uuid.UUID | None = None
    connector_name: str | None = None
    connector_type: str | None = None
    resource_name: str
    sync_mode: str | None = None
    last_cursor: str | None = None
    last_sync_at: datetime | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    status: str | None = None
    error_message: str | None = None
    records_synced: int = 0
    bytes_synced: int | None = None
    dataset_ids: list[uuid.UUID] = Field(default_factory=list)
    dataset_names: list[str] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class RuntimeSyncStateListResponse(RuntimeModel):
    items: list[RuntimeSyncStateSummary] = Field(default_factory=list)
    total: int = 0


class RuntimeSyncRequest(RuntimeRequestModel):
    resource_names: list[str] = Field(default_factory=list)
    sync_mode: str = "INCREMENTAL"
    force_full_refresh: bool = False

    @model_validator(mode="after")
    def _validate_resources(self) -> "RuntimeSyncRequest":
        normalized: list[str] = []
        for item in self.resource_names:
            value = str(item or "").strip()
            if value and value not in normalized:
                normalized.append(value)
        if not normalized:
            raise ValueError("resource_names must contain at least one resource.")
        self.resource_names = normalized
        self.sync_mode = str(self.sync_mode or "INCREMENTAL").strip().upper() or "INCREMENTAL"
        return self


class RuntimeSyncExecutionResult(RuntimeModel):
    resource_name: str
    sync_mode: str | None = None
    records_synced: int = 0
    bytes_synced: int | None = None
    last_cursor: str | None = None
    dataset_ids: list[uuid.UUID] = Field(default_factory=list)
    dataset_names: list[str] = Field(default_factory=list)


class RuntimeSyncResponse(RuntimeModel):
    status: str
    connector_id: uuid.UUID | None = None
    connector_name: str | None = None
    sync_mode: str | None = None
    resources: list[RuntimeSyncExecutionResult] = Field(default_factory=list)
    summary: str | None = None
    error: str | None = None
