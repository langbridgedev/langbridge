
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from langbridge.connectors.base.config import (
    ConnectorFamily,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)
from langbridge.runtime.models.base import RuntimeModel

class ManagementMode(str, Enum):
      CONFIG_MANAGED = "config_managed"
      RUNTIME_MANAGED = "runtime_managed"

class LifecycleState(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"

class SecretReference(RuntimeModel):
    provider_type: Literal[
        "env",
        "kubernetes",
        "vault",
        "azure_key_vault",
        "aws_secrets_manager",
    ]
    identifier: str
    key: str | None = None
    version: str | None = None


class ConnectionPolicy(RuntimeModel):
    allowed_schemas: list[str] = Field(default_factory=list)
    allowed_tables: list[str] = Field(default_factory=list)
    max_row_limit: int | None = None
    redaction_rules: dict[str, str] = Field(default_factory=dict)


class ConnectionMetadata(RuntimeModel, extra="allow"):
    extra: dict[str, Any] = Field(default_factory=dict)


class ConnectorCapabilities(RuntimeModel):
    supports_live_datasets: bool = False
    supports_synced_datasets: bool = False
    supports_incremental_sync: bool = False
    supports_query_pushdown: bool = False
    supports_preview: bool = False
    supports_federated_execution: bool = False


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
    enum_name_prefix = f"{enum_cls.__name__}.".lower()
    if "." in raw_value and raw_value.lower().startswith(enum_name_prefix):
        raw_value = raw_value.rsplit(".", 1)[-1]
    if case == "lower":
        raw_value = raw_value.lower()
    elif case == "upper":
        raw_value = raw_value.upper()
    return enum_cls(raw_value)


class ConnectorMetadata(RuntimeModel):
    id: uuid.UUID
    name: str
    description: str | None = None
    version: str | None = None
    label: str | None = None
    icon: str | None = None
    connector_type: ConnectorRuntimeType | None = None
    connector_family: ConnectorFamily | None = None
    workspace_id: uuid.UUID | None = None
    config: dict[str, Any] | None = None
    connection_metadata: ConnectionMetadata | None = None
    secret_references: dict[str, SecretReference] = Field(default_factory=dict)
    connection_policy: ConnectionPolicy | None = None
    supported_resources: list[str] = Field(default_factory=list)
    default_sync_strategy: ConnectorSyncStrategy | None = None
    capabilities: ConnectorCapabilities | None = None
    is_managed: bool = False
    created_by: uuid.UUID | None = None
    updated_by: uuid.UUID | None = None
    management_mode: ManagementMode
    lifecycle_state: LifecycleState

    @field_validator("connector_type", mode="before")
    @classmethod
    def _validate_connector_type(cls, value: Any) -> ConnectorRuntimeType | None:
        return _normalize_enum_value(ConnectorRuntimeType, value, case="upper")

    @field_validator("connector_family", mode="before")
    @classmethod
    def _validate_connector_family(cls, value: Any) -> ConnectorFamily | None:
        return _normalize_enum_value(ConnectorFamily, value, case="upper")

    @field_validator("default_sync_strategy", mode="before")
    @classmethod
    def _validate_default_sync_strategy(cls, value: Any) -> ConnectorSyncStrategy | None:
        return _normalize_enum_value(ConnectorSyncStrategy, value, case="upper")

    @property
    def connector_type_value(self) -> str | None:
        return None if self.connector_type is None else self.connector_type.value

    @property
    def connector_family_value(self) -> str | None:
        if self.connector_family is None:
            return None
        return self.connector_family.value.lower()

    @property
    def default_sync_strategy_value(self) -> str | None:
        return (
            None
            if self.default_sync_strategy is None
            else self.default_sync_strategy.value
        )

    @property
    def capabilities_json(self) -> dict[str, Any] | None:
        if self.capabilities is None:
            return None
        return self.capabilities.model_dump(mode="json")


class DatasetColumnMetadata(RuntimeModel):
    id: uuid.UUID
    dataset_id: uuid.UUID
    workspace_id: uuid.UUID | None = None
    name: str
    data_type: str
    nullable: bool = True
    description: str | None = None
    is_allowed: bool = True
    is_computed: bool = False
    expression: str | None = None
    ordinal_position: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DatasetPolicyMetadata(RuntimeModel):
    id: uuid.UUID | None = None
    dataset_id: uuid.UUID | None = None
    workspace_id: uuid.UUID | None = None
    max_rows_preview: int = 1000
    max_export_rows: int = 10000
    redaction_rules: dict[str, str] = Field(default_factory=dict)
    row_filters: list[str] = Field(default_factory=list)
    allow_dml: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def redaction_rules_json(self) -> dict[str, str]:
        return dict(self.redaction_rules)

    @property
    def row_filters_json(self) -> list[str]:
        return list(self.row_filters)


class DatasetSourceKind(str, Enum):
    DATABASE = "database"
    SAAS = "saas"
    API = "api"
    FILE = "file"
    VIRTUAL = "virtual"


class DatasetStorageKind(str, Enum):
    TABLE = "table"
    MEMORY = "memory"
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    VIEW = "view"
    VIRTUAL = "virtual"


class DatasetMaterializationMode(str, Enum):
    LIVE = "live"
    SYNCED = "synced"


class DatasetSource(RuntimeModel):
    table: str | None = None
    resource: str | None = None
    sql: str | None = None
    storage_uri: str | None = None
    format: str | None = None
    header: bool | None = None
    delimiter: str | None = None
    quote: str | None = None

    @model_validator(mode="after")
    def _validate_source(self) -> "DatasetSource":
        has_table = bool(str(self.table or "").strip())
        has_resource = bool(str(self.resource or "").strip())
        has_sql = bool(str(self.sql or "").strip())
        has_file = bool(str(self.storage_uri or "").strip())
        configured_modes = sum((has_table, has_resource, has_sql, has_file))
        if configured_modes != 1:
            raise ValueError(
                "Dataset source must define exactly one of table, resource, sql, or storage_uri."
            )
        return self


class DatasetSyncConfig(RuntimeModel):
    resource: str
    strategy: ConnectorSyncStrategy
    cadence: str | None = None
    cursor_field: str | None = None
    initial_cursor: str | None = None
    lookback_window: str | None = None
    backfill_start: str | None = None
    backfill_end: str | None = None
    sync_on_start: bool = False
    flattern_into_datasets: bool = False

    @field_validator("strategy", mode="before")
    @classmethod
    def _validate_strategy(cls, value: Any) -> ConnectorSyncStrategy:
        normalized = _normalize_enum_value(ConnectorSyncStrategy, value, case="upper")
        if normalized is None:
            raise ValueError("Dataset sync strategy is required.")
        return normalized

    @model_validator(mode="after")
    def _validate_sync(self) -> "DatasetSyncConfig":
        resource = str(self.resource or "").strip()
        if not resource:
            raise ValueError("Dataset sync config requires resource.")
        self.resource = resource
        return self


class DatasetType(str, Enum):
    TABLE = "TABLE"
    SQL = "SQL"
    API = "API"
    FILE = "FILE"
    FEDERATED = "FEDERATED"


class DatasetStatus(str, Enum):
    PUBLISHED = "published"
    PENDING_SYNC = "pending_sync"


class DatasetExecutionCapabilities(RuntimeModel):
    supports_structured_scan: bool = False
    supports_sql_federation: bool = False
    supports_filter_pushdown: bool = False
    supports_projection_pushdown: bool = False
    supports_aggregation_pushdown: bool = False
    supports_join_pushdown: bool = False
    supports_materialization: bool = False
    supports_semantic_modeling: bool = False


class DatasetRelationIdentity(RuntimeModel):
    canonical_reference: str
    relation_name: str
    qualified_name: str | None = None
    catalog_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    storage_uri: str | None = None
    dataset_id: uuid.UUID | None = None
    connector_id: uuid.UUID | None = None
    source_kind: DatasetSourceKind
    storage_kind: DatasetStorageKind


class DatasetMetadata(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    connection_id: uuid.UUID | None = None
    owner_id: uuid.UUID | None = None
    created_by: uuid.UUID | None = None
    updated_by: uuid.UUID | None = None
    name: str
    sql_alias: str
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    dataset_type: DatasetType
    materialization_mode: DatasetMaterializationMode
    source: DatasetSource | None = None
    sync: DatasetSyncConfig | None = None
    source_kind: DatasetSourceKind | None = None
    connector_kind: str | None = None
    storage_kind: DatasetStorageKind | None = None
    dialect: str | None = None
    catalog_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    storage_uri: str | None = None
    sql_text: str | None = None
    relation_identity: dict[str, Any] | None = None
    execution_capabilities: dict[str, Any] | None = None
    referenced_dataset_ids: list[Any] = Field(default_factory=list)
    federated_plan: dict[str, Any] | None = None
    file_config: dict[str, Any] | None = None
    status: DatasetStatus = DatasetStatus.PUBLISHED
    revision_id: uuid.UUID | None = None
    row_count_estimate: int | None = None
    bytes_estimate: int | None = None
    last_profiled_at: datetime | None = None
    columns: list[DatasetColumnMetadata] = Field(default_factory=list)
    policy: DatasetPolicyMetadata | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    management_mode: ManagementMode
    lifecycle_state: LifecycleState

    @field_validator("dataset_type", mode="before")
    @classmethod
    def _validate_dataset_type(cls, value: Any) -> DatasetType:
        normalized = _normalize_enum_value(DatasetType, value, case="upper")
        if normalized is None:
            raise ValueError("dataset_type is required.")
        return normalized

    @field_validator("materialization_mode", mode="before")
    @classmethod
    def _validate_materialization_mode(
        cls,
        value: Any,
    ) -> DatasetMaterializationMode:
        normalized = _normalize_enum_value(DatasetMaterializationMode, value, case="lower")
        if normalized is None:
            raise ValueError("materialization_mode is required.")
        return normalized

    @field_validator("source", mode="before")
    @classmethod
    def _validate_source(cls, value: Any) -> DatasetSource | None:
        if value is None or value == "":
            return None
        if isinstance(value, DatasetSource):
            return value
        return DatasetSource.model_validate(value)

    @field_validator("sync", mode="before")
    @classmethod
    def _validate_sync(cls, value: Any) -> DatasetSyncConfig | None:
        if value is None or value == "":
            return None
        if isinstance(value, DatasetSyncConfig):
            return value
        return DatasetSyncConfig.model_validate(value)

    @field_validator("source_kind", mode="before")
    @classmethod
    def _validate_source_kind(cls, value: Any) -> DatasetSourceKind | None:
        return _normalize_enum_value(DatasetSourceKind, value, case="lower")

    @field_validator("storage_kind", mode="before")
    @classmethod
    def _validate_storage_kind(cls, value: Any) -> DatasetStorageKind | None:
        return _normalize_enum_value(DatasetStorageKind, value, case="lower")

    @field_validator("status", mode="before")
    @classmethod
    def _validate_status(cls, value: Any) -> DatasetStatus:
        normalized = _normalize_enum_value(DatasetStatus, value, case="lower")
        if normalized is None:
            return DatasetStatus.PUBLISHED
        return normalized

    @model_validator(mode="after")
    def _validate_materialization_contract(self) -> "DatasetMetadata":
        if self.materialization_mode == DatasetMaterializationMode.LIVE:
            if self.source is None:
                raise ValueError("Live datasets must define source.")
            if self.sync is not None:
                raise ValueError("Live datasets must not define sync config.")
            return self
        if self.sync is None:
            raise ValueError("Synced datasets must define sync config.")
        if self.source is not None:
            raise ValueError("Synced datasets must not define live source config.")
        return self

    @property
    def tags_json(self) -> list[str]:
        return list(self.tags)

    @property
    def dataset_type_value(self) -> str:
        return self.dataset_type.value

    @property
    def materialization_mode_value(self) -> str | None:
        return None if self.materialization_mode is None else self.materialization_mode.value

    @property
    def source_json(self) -> dict[str, Any] | None:
        return None if self.source is None else self.source.model_dump(mode="json", exclude_none=True)

    @property
    def sync_json(self) -> dict[str, Any] | None:
        return None if self.sync is None else self.sync.model_dump(mode="json", exclude_none=True)

    @property
    def source_kind_value(self) -> str | None:
        return None if self.source_kind is None else self.source_kind.value

    @property
    def storage_kind_value(self) -> str | None:
        return None if self.storage_kind is None else self.storage_kind.value

    @property
    def status_value(self) -> str:
        return self.status.value

    @property
    def relation_identity_json(self) -> dict[str, Any] | None:
        return None if self.relation_identity is None else dict(self.relation_identity)

    @property
    def execution_capabilities_json(self) -> dict[str, Any] | None:
        return (
            None
            if self.execution_capabilities is None
            else dict(self.execution_capabilities)
        )

    @property
    def referenced_dataset_ids_json(self) -> list[Any]:
        return list(self.referenced_dataset_ids)

    @property
    def federated_plan_json(self) -> dict[str, Any] | None:
        return None if self.federated_plan is None else dict(self.federated_plan)

    @property
    def file_config_json(self) -> dict[str, Any] | None:
        return None if self.file_config is None else dict(self.file_config)


class SemanticModelMetadata(RuntimeModel):
    id: uuid.UUID
    connector_id: uuid.UUID | None = None
    workspace_id: uuid.UUID
    created_by: uuid.UUID | None = None
    updated_by: uuid.UUID | None = None
    name: str
    description: str | None = None
    content_yaml: str
    content_json: dict[str, Any] | str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    management_mode: ManagementMode
    lifecycle_state: LifecycleState

class SemanticVectorIndexStatus(str, Enum):
    PENDING = "pending"
    REFRESHING = "refreshing"
    READY = "ready"
    FAILED = "failed"


class SemanticVectorStoreTarget(str, Enum):
    MANAGED_FAISS = "managed_faiss"
    CONNECTOR = "connector"


class SemanticVectorIndexMetadata(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    semantic_model_id: uuid.UUID
    dataset_key: str
    dimension_name: str
    vector_store_target: SemanticVectorStoreTarget
    vector_connector_name: str | None = None
    vector_connector_id: uuid.UUID | None = None
    vector_index_name: str
    refresh_interval_seconds: int | None = None
    refresh_status: SemanticVectorIndexStatus = SemanticVectorIndexStatus.PENDING
    indexed_value_count: int | None = None
    embedding_dimension: int | None = None
    last_refresh_started_at: datetime | None = None
    last_refreshed_at: datetime | None = None
    last_refresh_error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
