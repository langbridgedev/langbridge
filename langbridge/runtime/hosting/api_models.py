from datetime import datetime
import uuid
from typing import Any

from pydantic import Field, model_validator

from langbridge.runtime.models.base import RuntimeModel, RuntimeRequestModel
from langbridge.runtime.models.metadata import (
    ConnectorCapabilities,
    DatasetMaterializationMode,
    ManagementMode,
)


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
    materialization_mode: str | None = None
    status: str | None = None
    sync_resource: str | None = None
    sync_status: str | None = None
    last_sync_at: datetime | None = None
    management_mode: ManagementMode
    managed: bool = False


class RuntimeDatasetListResponse(RuntimeModel):
    items: list[RuntimeDatasetSummary] = Field(default_factory=list)
    total: int = 0


class RuntimeConnectorSummary(RuntimeModel):
    id: uuid.UUID | None = None
    name: str
    description: str | None = None
    connector_type: str | None = None
    connector_family: str | None = None
    supports_sync: bool = False
    supported_resources: list[str] = Field(default_factory=list)
    sync_strategy: str | None = None
    capabilities: dict[str, Any] = Field(default_factory=dict)
    management_mode: ManagementMode
    managed: bool = False


class RuntimeConnectorListResponse(RuntimeModel):
    items: list[RuntimeConnectorSummary] = Field(default_factory=list)
    total: int = 0


class RuntimeSemanticModelSummary(RuntimeModel):
    id: uuid.UUID | None = None
    name: str
    description: str | None = None
    default: bool = False
    dataset_count: int = 0
    dataset_names: list[str] = Field(default_factory=list)
    dimension_count: int = 0
    measure_count: int = 0
    management_mode: ManagementMode
    managed: bool = False


class RuntimeSemanticModelListResponse(RuntimeModel):
    items: list[RuntimeSemanticModelSummary] = Field(default_factory=list)
    total: int = 0


class RuntimeConnectorCreateRequest(RuntimeRequestModel):
    name: str = Field(..., min_length=1, max_length=255)
    type: str = Field(..., min_length=1, max_length=64)
    description: str | None = Field(default=None, max_length=1024)
    connection: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    secrets: dict[str, Any] = Field(default_factory=dict)
    policy: dict[str, Any] | None = None
    capabilities: ConnectorCapabilities | dict[str, Any] | None = None


class RuntimeDatasetSourceRequest(RuntimeRequestModel):
    table: str | None = None
    resource: str | None = None
    sql: str | None = None
    path: str | None = None
    storage_uri: str | None = None
    format: str | None = None
    file_format: str | None = None
    header: bool | None = None
    delimiter: str | None = None
    quote: str | None = None

    @model_validator(mode="after")
    def _validate_source(self) -> "RuntimeDatasetSourceRequest":
        has_table = bool(str(self.table or "").strip())
        has_resource = bool(str(self.resource or "").strip())
        has_sql = bool(str(self.sql or "").strip())
        has_file = bool(str(self.path or "").strip() or str(self.storage_uri or "").strip())
        configured_modes = sum((has_table, has_resource, has_sql, has_file))
        if configured_modes != 1:
            raise ValueError(
                "Dataset source must define exactly one of table, resource, sql, or path/storage_uri."
            )
        return self


class RuntimeDatasetPolicyRequest(RuntimeRequestModel):
    max_rows_preview: int | None = Field(default=None, ge=1)
    max_export_rows: int | None = Field(default=None, ge=1)
    redaction_rules: dict[str, str] = Field(default_factory=dict)
    row_filters: list[str] = Field(default_factory=list)
    allow_dml: bool = False


class RuntimeDatasetCreateRequest(RuntimeRequestModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=1024)
    connector: str = Field(..., min_length=1, max_length=255)
    materialization_mode: DatasetMaterializationMode = DatasetMaterializationMode.LIVE
    source: RuntimeDatasetSourceRequest
    tags: list[str] = Field(default_factory=list)
    policy: RuntimeDatasetPolicyRequest | None = None

    @model_validator(mode="after")
    def _validate_materialization_mode_source(self) -> "RuntimeDatasetCreateRequest":
        resource_name = str(self.source.resource or "").strip()
        table_name = str(self.source.table or "").strip()
        sql = str(self.source.sql or "").strip()
        storage_uri = str(self.source.storage_uri or "").strip()
        path = str(self.source.path or "").strip()

        if self.materialization_mode == DatasetMaterializationMode.SYNCED:
            if sql or storage_uri or path:
                raise ValueError(
                    "Synced datasets must declare source.resource with the connector resource name."
                )
            if resource_name:
                return self
            if table_name:
                self.source.resource = table_name
                self.source.table = None
                return self
            raise ValueError(
                "Synced datasets must declare source.resource with the connector resource name."
            )

        if resource_name:
            raise ValueError(
                "Live datasets cannot use source.resource; use source.table, source.sql, or source.path/source.storage_uri."
            )
        return self


class RuntimeSemanticModelCreateRequest(RuntimeRequestModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=1024)
    model: dict[str, Any] | None = None
    datasets: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_payload(self) -> "RuntimeSemanticModelCreateRequest":
        if not self.model and not self.datasets:
            raise ValueError("Semantic model creation requires model or datasets.")
        return self


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
    selected_datasets: list[uuid.UUID] = Field(default_factory=list)
    query_dialect: str = "tsql"
    params: dict[str, Any] = Field(default_factory=dict)
    requested_limit: int | None = Field(default=None, ge=1)
    requested_timeout_seconds: int | None = Field(default=None, ge=1)
    explain: bool = False

    @model_validator(mode="after")
    def _validate_sql_mode(self) -> "RuntimeSqlQueryRequest":
        normalized: list[uuid.UUID] = []
        for dataset_id in self.selected_datasets:
            if dataset_id not in normalized:
                normalized.append(dataset_id)
        self.selected_datasets = normalized

        if self.connection_id is not None and self.connection_name:
            raise ValueError("Specify only one of connection_id or connection_name for direct SQL requests.")
        if (self.connection_id is not None or self.connection_name) and self.selected_datasets:
            raise ValueError("selected_datasets cannot be combined with explicit direct SQL requests.")
        return self


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


class RuntimeThreadCreateRequest(RuntimeRequestModel):
    title: str | None = None


class RuntimeThreadUpdateRequest(RuntimeRequestModel):
    title: str | None = None


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


class RuntimeAuthBootstrapRequest(RuntimeRequestModel):
    username: str = Field(..., min_length=3, max_length=64)
    email: str = Field(..., min_length=3, max_length=320)
    password: str = Field(..., min_length=8)


class RuntimeAuthLoginRequest(RuntimeRequestModel):
    identifier: str | None = Field(default=None, min_length=1, max_length=320)
    username: str | None = Field(default=None, min_length=1, max_length=64)
    email: str | None = Field(default=None, min_length=1, max_length=320)
    password: str = Field(..., min_length=1)

    @model_validator(mode="after")
    def _normalize_identifier(self) -> "RuntimeAuthLoginRequest":
        if not str(self.identifier or "").strip():
            if str(self.username or "").strip():
                self.identifier = str(self.username).strip()
            elif str(self.email or "").strip():
                self.identifier = str(self.email).strip()
        if not str(self.identifier or "").strip():
            raise ValueError("identifier, username, or email is required.")
        return self
