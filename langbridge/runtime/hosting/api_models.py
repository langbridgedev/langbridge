from datetime import datetime
import uuid
from typing import Any

from pydantic import Field, field_serializer, field_validator, model_validator

from langbridge.connectors.base.config import (
    ConnectorFamily,
    ConnectorConfigEntrySchema,
    ConnectorPluginMetadata,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)
from langbridge.runtime.datasets.contracts import (
    DatasetMaterializationConfig,
    DatasetMaterializationMode,
    DatasetRequestConfig,
    DatasetSchemaHint,
    DatasetSourceConfig,
    DatasetSyncPolicy,
)
from langbridge.runtime.models.base import RuntimeModel, RuntimeRequestModel
from langbridge.runtime.models.federation_diagnostics import RuntimeFederationDiagnostics
from langbridge.runtime.models.metadata import (
    ConnectorCapabilities,
    DatasetStatus,
    DatasetType,
    ManagementMode,
)
from langbridge.runtime.models.jobs import SqlQueryRequest, SqlQueryScope
from langbridge.runtime.models.state import ConnectorSyncMode, ConnectorSyncStatus


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
    semantic_models: list[str] = Field(default_factory=list)
    semantic_model: str | None = None
    materialization: dict[str, Any] | None = None
    materialization_mode: DatasetMaterializationMode | None = None
    source: dict[str, Any] | None = None
    schema_hint: dict[str, Any] | None = None
    status: DatasetStatus | None = None
    sync_status: ConnectorSyncStatus | None = None
    last_sync_at: datetime | None = None
    management_mode: ManagementMode
    managed: bool = False


class RuntimeDatasetListResponse(RuntimeModel):
    items: list[RuntimeDatasetSummary] = Field(default_factory=list)
    total: int = 0

class RuntimeConnectorTypeSummary(RuntimeModel):
    name: ConnectorRuntimeType
    label: str | None = None
    description: str | None = None
    family: ConnectorFamily | None = None
    supports_sync: bool = False
    supported_resources: list[str] = Field(default_factory=list)
    default_sync_strategy: ConnectorSyncStrategy | None = None
    capabilities_schema: dict[str, Any] = Field(default_factory=dict)

class RuntimeConnectorSummary(RuntimeModel):
    id: uuid.UUID | None = None
    name: str
    description: str | None = None
    connector_type: ConnectorRuntimeType | None = None
    connector_family: ConnectorFamily | None = None
    supports_sync: bool = False
    supported_resources: list[str] = Field(default_factory=list)
    default_sync_strategy: ConnectorSyncStrategy | None = None
    capabilities: dict[str, Any] = Field(default_factory=dict)
    management_mode: ManagementMode
    managed: bool = False

    @field_validator("connector_type", mode="before")
    @classmethod
    def _validate_connector_type(cls, value: Any) -> ConnectorRuntimeType | None:
        if value in {None, ""}:
            return None
        return ConnectorRuntimeType(str(getattr(value, "value", value)).strip().upper())

    @field_validator("connector_family", mode="before")
    @classmethod
    def _validate_connector_family(cls, value: Any) -> ConnectorFamily | None:
        if value in {None, ""}:
            return None
        return ConnectorFamily(str(getattr(value, "value", value)).strip().upper())

    @field_validator("default_sync_strategy", mode="before")
    @classmethod
    def _validate_default_sync_strategy(cls, value: Any) -> ConnectorSyncStrategy | None:
        if value in {None, ""}:
            return None
        return ConnectorSyncStrategy(str(getattr(value, "value", value)).strip().upper())

    @field_serializer("connector_family", when_used="json")
    def _serialize_connector_family(self, value: ConnectorFamily | None) -> str | None:
        if value is None:
            return None
        return value.value.lower()


class RuntimeConnectorListResponse(RuntimeModel):
    items: list[RuntimeConnectorSummary] = Field(default_factory=list)
    total: int = 0

class RuntimeConnectorTypesListResponse(RuntimeModel):
    items: list[RuntimeConnectorTypeSummary] = Field(default_factory=list)
    total: int = 0


class RuntimeConnectorConfigSchemaResponse(RuntimeModel):
    connector_type: ConnectorRuntimeType
    name: str
    description: str
    version: str
    config: list[ConnectorConfigEntrySchema] = Field(default_factory=list)
    plugin_metadata: ConnectorPluginMetadata | None = None

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
    type: ConnectorRuntimeType
    description: str | None = Field(default=None, max_length=1024)
    connection: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    secrets: dict[str, Any] = Field(default_factory=dict)
    policy: dict[str, Any] | None = None
    capabilities: ConnectorCapabilities | dict[str, Any] | None = None

    @field_validator("type", mode="before")
    @classmethod
    def _validate_type(cls, value: Any) -> ConnectorRuntimeType:
        return ConnectorRuntimeType(str(getattr(value, "value", value) or "").strip().upper())


class RuntimeConnectorUpdateRequest(RuntimeRequestModel):
    description: str | None = Field(default=None, max_length=1024)
    connection: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    secrets: dict[str, Any] | None = None
    policy: dict[str, Any] | None = None
    capabilities: ConnectorCapabilities | dict[str, Any] | None = None


class RuntimeDatasetSourceRequest(DatasetSourceConfig):
    pass


class RuntimeDatasetSyncSourceRequest(DatasetSourceConfig):
    pass


class RuntimeDatasetPolicyRequest(RuntimeRequestModel):
    max_rows_preview: int | None = Field(default=None, ge=1)
    max_export_rows: int | None = Field(default=None, ge=1)
    redaction_rules: dict[str, str] = Field(default_factory=dict)
    row_filters: list[str] = Field(default_factory=list)
    allow_dml: bool = False


class RuntimeDatasetSyncConfigRequest(DatasetSyncPolicy):
    source: RuntimeDatasetSyncSourceRequest

    @model_validator(mode="before")
    @classmethod
    def _normalize_sync_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        strategy = normalized.get("strategy")
        if isinstance(strategy, DatasetSyncPolicy):
            normalized.update(strategy.model_dump(mode="json", exclude_none=True))
            normalized["strategy"] = strategy.strategy
        return normalized


class RuntimeDatasetMaterializationRequest(DatasetMaterializationConfig):
    pass


class RuntimeDatasetCreateRequest(RuntimeRequestModel):
    name: str = Field(..., min_length=1, max_length=255)
    label: str | None = Field(default=None, max_length=255)
    description: str | None = Field(default=None, max_length=1024)
    connector: str | None = Field(default=None, min_length=1, max_length=255)
    source: RuntimeDatasetSourceRequest
    materialization: RuntimeDatasetMaterializationRequest
    schema_hint: DatasetSchemaHint | None = None
    tags: list[str] = Field(default_factory=list)
    policy: RuntimeDatasetPolicyRequest | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        legacy_mode = normalized.pop("materialization_mode", None)
        legacy_sync = normalized.pop("sync", None)
        if normalized.get("materialization") is None:
            normalized["materialization"] = {
                "mode": legacy_mode,
                "sync": legacy_sync,
            }
        if normalized.get("source") is None and isinstance(legacy_sync, dict) and legacy_sync.get("source") is not None:
            normalized["source"] = legacy_sync.get("source")
        return normalized

    @model_validator(mode="after")
    def _validate_materialization_mode_source(self) -> "RuntimeDatasetCreateRequest":
        connector_name = str(self.connector or "").strip()
        if self.materialization.mode == DatasetMaterializationMode.SYNCED:
            if not connector_name:
                raise ValueError("Dataset connector is required for synced datasets.")
        if not connector_name and self.source.kind.value in {"table", "sql", "resource", "request"}:
            raise ValueError("Dataset connector is required for table-backed, sql-backed, and API dataset sources.")
        return self

    @property
    def materialization_mode(self) -> DatasetMaterializationMode:
        return self.materialization.mode

    @property
    def sync(self) -> RuntimeDatasetSyncConfigRequest | None:
        sync_policy = self.materialization.sync
        if sync_policy is None:
            return None
        return RuntimeDatasetSyncConfigRequest.model_validate(
            {
                "source": self.source.model_dump(mode="json"),
                **sync_policy.model_dump(mode="json", exclude_none=True),
            }
        )


class RuntimeDatasetUpdateRequest(RuntimeRequestModel):
    label: str | None = Field(default=None, max_length=255)
    description: str | None = Field(default=None, max_length=1024)
    materialization: RuntimeDatasetMaterializationRequest | None = None
    source: RuntimeDatasetSourceRequest | None = None
    schema_hint: DatasetSchemaHint | None = None
    tags: list[str] | None = None
    policy: RuntimeDatasetPolicyRequest | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        legacy_mode = normalized.pop("materialization_mode", None)
        legacy_sync = normalized.pop("sync", None)
        if normalized.get("materialization") is None and (legacy_mode is not None or legacy_sync is not None):
            normalized["materialization"] = {
                "mode": legacy_mode,
                "sync": legacy_sync,
            }
        if normalized.get("source") is None and isinstance(legacy_sync, dict) and legacy_sync.get("source") is not None:
            normalized["source"] = legacy_sync.get("source")
        return normalized

    @model_validator(mode="after")
    def _validate_materialization_mode_source(self) -> "RuntimeDatasetUpdateRequest":
        return self

    @property
    def materialization_mode(self) -> DatasetMaterializationMode | None:
        if self.materialization is None:
            return None
        return self.materialization.mode

    @property
    def sync(self) -> RuntimeDatasetSyncConfigRequest | None:
        if self.materialization is None or self.materialization.sync is None or self.source is None:
            return None
        sync_policy = self.materialization.sync
        return RuntimeDatasetSyncConfigRequest.model_validate(
            {
                "source": self.source.model_dump(mode="json"),
                **sync_policy.model_dump(mode="json", exclude_none=True),
            }
        )


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


class RuntimeSemanticModelUpdateRequest(RuntimeRequestModel):
    description: str | None = Field(default=None, max_length=1024)
    model: dict[str, Any] | None = None
    datasets: list[str] | None = None


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


class RuntimeDatasetSyncRequest(RuntimeRequestModel):
    sync_mode: ConnectorSyncMode = ConnectorSyncMode.INCREMENTAL
    force_full_refresh: bool = False

    @field_validator("sync_mode", mode="before")
    @classmethod
    def _normalize_sync_mode(cls, value: Any) -> ConnectorSyncMode:
        normalized = str(getattr(value, "value", value) or ConnectorSyncMode.INCREMENTAL.value).strip().upper()
        return ConnectorSyncMode(normalized)


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
    federation_diagnostics: RuntimeFederationDiagnostics | None = None
    error: str | None = None


class RuntimeSqlQueryRequest(SqlQueryRequest):
    pass


class RuntimeSqlQueryResponse(RuntimeModel):
    sql_job_id: uuid.UUID | None = None
    query_scope: SqlQueryScope
    status: str
    semantic_model_id: uuid.UUID | None = None
    semantic_model_ids: list[uuid.UUID] = Field(default_factory=list)
    connector_id: uuid.UUID | None = None
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
    federation_diagnostics: RuntimeFederationDiagnostics | None = None


class RuntimeAgentAskRequest(RuntimeRequestModel):
    message: str = Field(..., min_length=1)
    agent_id: uuid.UUID | None = None
    agent_name: str | None = None
    thread_id: uuid.UUID | None = None
    title: str | None = None
    agent_mode: str | None = None
    metadata_json: dict[str, Any] | None = None


class RuntimeAgentAskResponse(RuntimeModel):
    thread_id: uuid.UUID | None = None
    status: str
    run_id: uuid.UUID | None = None
    job_id: uuid.UUID | None = None
    message_id: uuid.UUID | None = None
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
    default_sync_mode: ConnectorSyncMode | None = None
    status: ConnectorSyncStatus | None = None
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
    connector_type: ConnectorRuntimeType | None = None
    resource_name: str | None = None
    source_key: str | None = None
    source: dict[str, Any] = Field(default_factory=dict)
    sync_mode: ConnectorSyncMode | None = None
    last_cursor: str | None = None
    last_sync_at: datetime | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    status: ConnectorSyncStatus | None = None
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


class RuntimeDatasetSyncStateResponse(RuntimeModel):
    dataset_id: uuid.UUID | None = None
    dataset_name: str | None = None
    connector_id: uuid.UUID | None = None
    connector_name: str | None = None
    connector_type: ConnectorRuntimeType | None = None
    materialization_mode: DatasetMaterializationMode | None = None
    source_key: str
    source: dict[str, Any] = Field(default_factory=dict)
    sync: dict[str, Any] = Field(default_factory=dict)
    sync_state: RuntimeSyncStateSummary | None = None


class RuntimeSyncRequest(RuntimeRequestModel):
    resource_names: list[str] = Field(default_factory=list)
    sync_mode: ConnectorSyncMode = ConnectorSyncMode.INCREMENTAL
    force_full_refresh: bool = False

    @field_validator("sync_mode", mode="before")
    @classmethod
    def _normalize_sync_mode(cls, value: Any) -> ConnectorSyncMode:
        normalized = str(getattr(value, "value", value) or ConnectorSyncMode.INCREMENTAL.value).strip().upper()
        return ConnectorSyncMode(normalized)

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
        self.sync_mode = ConnectorSyncMode(
            str(getattr(self.sync_mode, "value", self.sync_mode) or ConnectorSyncMode.INCREMENTAL.value)
            .strip()
            .upper()
        )
        return self


class RuntimeSyncExecutionResult(RuntimeModel):
    resource_name: str | None = None
    source_key: str | None = None
    source: dict[str, Any] = Field(default_factory=dict)
    sync_mode: ConnectorSyncMode | None = None
    records_synced: int = 0
    bytes_synced: int | None = None
    last_cursor: str | None = None
    dataset_ids: list[uuid.UUID] = Field(default_factory=list)
    dataset_names: list[str] = Field(default_factory=list)


class RuntimeSyncResponse(RuntimeModel):
    status: str
    dataset_id: uuid.UUID | None = None
    dataset_name: str | None = None
    connector_id: uuid.UUID | None = None
    connector_name: str | None = None
    sync_mode: ConnectorSyncMode | None = None
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


class RuntimeActorSummary(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    subject: str
    username: str
    email: str
    display_name: str
    actor_type: str
    status: str
    roles: list[str] = Field(default_factory=list)
    auth_provider: str = "local_password"
    password_algorithm: str
    password_updated_at: datetime
    must_rotate_password: bool = False
    created_at: datetime
    updated_at: datetime


class RuntimeActorListResponse(RuntimeModel):
    items: list[RuntimeActorSummary] = Field(default_factory=list)
    total: int = 0


class RuntimeActorCreateRequest(RuntimeRequestModel):
    username: str = Field(..., min_length=3, max_length=64)
    email: str = Field(..., min_length=3, max_length=320)
    display_name: str | None = Field(default=None, max_length=255)
    actor_type: str = Field(default="human", min_length=1, max_length=64)
    password: str = Field(..., min_length=8)
    roles: list[str] = Field(default_factory=list)


class RuntimeActorUpdateRequest(RuntimeRequestModel):
    email: str | None = Field(default=None, min_length=3, max_length=320)
    display_name: str | None = Field(default=None, max_length=255)
    actor_type: str | None = Field(default=None, min_length=1, max_length=64)
    status: str | None = Field(default=None, min_length=1, max_length=32)
    roles: list[str] | None = None


class RuntimeActorResetPasswordRequest(RuntimeRequestModel):
    password: str = Field(..., min_length=8)
    must_rotate_password: bool = False
