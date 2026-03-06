from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import Field, model_validator

from .base import _Base


class DatasetType(str, Enum):
    TABLE = "TABLE"
    SQL = "SQL"
    FEDERATED = "FEDERATED"
    FILE = "FILE"


class DatasetStatus(str, Enum):
    DRAFT = "draft"
    PUBLISHED = "published"


class DatasetLineageNodeType(str, Enum):
    CONNECTION = "connection"
    SOURCE_TABLE = "source_table"
    API_RESOURCE = "api_resource"
    FILE_RESOURCE = "file_resource"
    DATASET = "dataset"
    SEMANTIC_MODEL = "semantic_model"
    UNIFIED_SEMANTIC_MODEL = "unified_semantic_model"
    SAVED_QUERY = "saved_query"
    DASHBOARD = "dashboard"


class DatasetLineageEdgeType(str, Enum):
    DERIVES_FROM = "DERIVES_FROM"
    REFERENCES = "REFERENCES"
    GENERATED_BY = "GENERATED_BY"
    FEEDS = "FEEDS"
    MATERIALIZES_FROM = "MATERIALIZES_FROM"


class DatasetSortDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


class DatasetColumnRequest(_Base):
    name: str = Field(..., min_length=1, max_length=255)
    data_type: str = Field(..., min_length=1, max_length=128)
    nullable: bool = True
    description: str | None = Field(default=None, max_length=1024)
    is_allowed: bool = True
    is_computed: bool = False
    expression: str | None = None
    ordinal_position: int | None = Field(default=None, ge=0)


class DatasetColumnResponse(_Base):
    id: UUID
    dataset_id: UUID
    name: str
    data_type: str
    nullable: bool
    description: str | None = None
    is_allowed: bool
    is_computed: bool
    expression: str | None = None
    ordinal_position: int


class DatasetPolicyRequest(_Base):
    max_rows_preview: int | None = Field(default=None, ge=1)
    max_export_rows: int | None = Field(default=None, ge=1)
    redaction_rules: dict[str, str] | None = None
    row_filters: list[str] | None = None
    allow_dml: bool | None = None


class DatasetPolicyDefaultsRequest(_Base):
    max_preview_rows: int | None = Field(default=None, ge=1)
    max_export_rows: int | None = Field(default=None, ge=1)
    allow_dml: bool | None = None
    redaction_rules: dict[str, str] = Field(default_factory=dict)


class DatasetPolicyResponse(_Base):
    max_rows_preview: int
    max_export_rows: int
    redaction_rules: dict[str, str] = Field(default_factory=dict)
    row_filters: list[str] = Field(default_factory=list)
    allow_dml: bool = False


class DatasetStatsResponse(_Base):
    row_count_estimate: int | None = None
    bytes_estimate: int | None = None
    last_profiled_at: datetime | None = None


class DatasetCreateRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=1024)
    change_summary: str | None = Field(default=None, max_length=1024)
    tags: list[str] = Field(default_factory=list)
    dataset_type: DatasetType
    connection_id: UUID | None = None
    dialect: str | None = Field(default=None, max_length=64)
    catalog_name: str | None = Field(default=None, max_length=255)
    schema_name: str | None = Field(default=None, max_length=255)
    table_name: str | None = Field(default=None, max_length=255)
    storage_uri: str | None = Field(default=None, max_length=2048)
    sql_text: str | None = None
    referenced_dataset_ids: list[UUID] = Field(default_factory=list)
    federated_plan: dict[str, Any] | None = None
    file_config: dict[str, Any] | None = None
    columns: list[DatasetColumnRequest] = Field(default_factory=list)
    policy: DatasetPolicyRequest | None = None
    status: DatasetStatus = DatasetStatus.PUBLISHED

    @model_validator(mode="after")
    def _validate_shape(self) -> "DatasetCreateRequest":
        if self.dataset_type in {DatasetType.TABLE, DatasetType.SQL} and self.connection_id is None:
            raise ValueError("connection_id is required for TABLE and SQL datasets.")
        if self.dataset_type == DatasetType.TABLE:
            if not self.table_name:
                raise ValueError("table_name is required for TABLE datasets.")
        if self.dataset_type == DatasetType.SQL:
            if not (self.sql_text or "").strip():
                raise ValueError("sql_text is required for SQL datasets.")
        if self.dataset_type == DatasetType.FEDERATED:
            if not self.referenced_dataset_ids and not self.federated_plan:
                raise ValueError("FEDERATED datasets require referenced_dataset_ids or federated_plan.")
        if self.dataset_type == DatasetType.FILE:
            if not ((self.storage_uri or "").strip() or self.file_config):
                raise ValueError("FILE datasets require storage_uri or file_config.")
        return self


class DatasetSelectionColumnRequest(_Base):
    name: str = Field(..., min_length=1, max_length=255)
    data_type: str | None = Field(default=None, max_length=128)
    nullable: bool | None = None


class DatasetSelectionRequest(_Base):
    schema: str = Field(..., min_length=1, max_length=255)
    table: str = Field(..., min_length=1, max_length=255)
    columns: list[DatasetSelectionColumnRequest] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_columns(self) -> "DatasetSelectionRequest":
        names = [column.name.strip().lower() for column in self.columns if column.name.strip()]
        if len(set(names)) != len(names):
            raise ValueError("columns must contain unique names per table selection.")
        return self


class DatasetEnsureRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    connection_id: UUID
    schema: str = Field(..., min_length=1, max_length=255)
    table: str = Field(..., min_length=1, max_length=255)
    columns: list[DatasetSelectionColumnRequest] = Field(default_factory=list)
    name: str | None = Field(default=None, max_length=255)
    naming_template: str | None = Field(default=None, max_length=128)
    policy_defaults: DatasetPolicyDefaultsRequest | None = None
    tags: list[str] = Field(default_factory=list)


class DatasetEnsureResponse(_Base):
    dataset_id: UUID
    created: bool
    name: str


class DatasetBulkCreateRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    connection_id: UUID
    selections: list[DatasetSelectionRequest] = Field(default_factory=list)
    naming_template: str = "{schema}.{table}"
    policy_defaults: DatasetPolicyDefaultsRequest | None = None
    tags: list[str] = Field(default_factory=list)
    profile_after_create: bool = False

    @model_validator(mode="after")
    def _validate_bulk_shape(self) -> "DatasetBulkCreateRequest":
        if len(self.selections) == 0:
            raise ValueError("At least one table selection is required.")
        if len(self.selections) > 500:
            raise ValueError("At most 500 table selections are allowed per bulk request.")
        return self


class DatasetBulkCreateStartResponse(_Base):
    job_id: UUID
    job_status: str


class DatasetBulkCreateResult(_Base):
    created_count: int = 0
    reused_count: int = 0
    dataset_ids: list[UUID] = Field(default_factory=list)
    errors: list[dict[str, Any]] = Field(default_factory=list)


class DatasetUpdateRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=1024)
    change_summary: str | None = Field(default=None, max_length=1024)
    tags: list[str] | None = None
    connection_id: UUID | None = None
    dialect: str | None = Field(default=None, max_length=64)
    catalog_name: str | None = Field(default=None, max_length=255)
    schema_name: str | None = Field(default=None, max_length=255)
    table_name: str | None = Field(default=None, max_length=255)
    storage_uri: str | None = Field(default=None, max_length=2048)
    sql_text: str | None = None
    referenced_dataset_ids: list[UUID] | None = None
    federated_plan: dict[str, Any] | None = None
    file_config: dict[str, Any] | None = None
    columns: list[DatasetColumnRequest] | None = None
    policy: DatasetPolicyRequest | None = None
    status: DatasetStatus | None = None


class DatasetResponse(_Base):
    id: UUID
    workspace_id: UUID
    project_id: UUID | None = None
    connection_id: UUID | None = None
    owner_id: UUID | None = None
    name: str
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    dataset_type: DatasetType
    dialect: str | None = None
    catalog_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    storage_uri: str | None = None
    sql_text: str | None = None
    referenced_dataset_ids: list[UUID] = Field(default_factory=list)
    federated_plan: dict[str, Any] | None = None
    file_config: dict[str, Any] | None = None
    status: DatasetStatus
    revision_id: UUID | None = None
    columns: list[DatasetColumnResponse] = Field(default_factory=list)
    policy: DatasetPolicyResponse
    stats: DatasetStatsResponse
    created_at: datetime
    updated_at: datetime


class DatasetListResponse(_Base):
    items: list[DatasetResponse] = Field(default_factory=list)
    total: int = 0


class DatasetPreviewSortItem(_Base):
    column: str
    direction: DatasetSortDirection = DatasetSortDirection.ASC


class DatasetPreviewRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    limit: int | None = Field(default=None, ge=1)
    filters: dict[str, Any] = Field(default_factory=dict)
    sort: list[DatasetPreviewSortItem] = Field(default_factory=list)
    user_context: dict[str, Any] = Field(default_factory=dict)


class DatasetPreviewColumn(_Base):
    name: str
    data_type: str | None = None


class DatasetPreviewResponse(_Base):
    job_id: UUID
    status: str
    dataset_id: UUID
    columns: list[DatasetPreviewColumn] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count_preview: int = 0
    effective_limit: int
    redaction_applied: bool = False
    duration_ms: int | None = None
    bytes_scanned: int | None = None
    error: str | None = None


class DatasetProfileRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    user_context: dict[str, Any] = Field(default_factory=dict)


class DatasetProfileResponse(_Base):
    job_id: UUID
    status: str
    dataset_id: UUID
    row_count_estimate: int | None = None
    bytes_estimate: int | None = None
    distinct_counts: dict[str, int] = Field(default_factory=dict)
    null_rates: dict[str, float] = Field(default_factory=dict)
    profiled_at: datetime | None = None
    error: str | None = None


class DatasetCsvIngestResponse(_Base):
    dataset_id: UUID
    job_id: UUID
    job_status: str
    storage_uri: str


class DatasetCatalogItem(_Base):
    id: UUID
    name: str
    dataset_type: DatasetType
    tags: list[str] = Field(default_factory=list)
    columns: list[DatasetColumnResponse] = Field(default_factory=list)
    updated_at: datetime


class DatasetCatalogResponse(_Base):
    workspace_id: UUID
    items: list[DatasetCatalogItem] = Field(default_factory=list)


class DatasetVersionSummaryResponse(_Base):
    id: UUID
    dataset_id: UUID
    revision_number: int
    revision_hash: str | None = None
    created_at: datetime
    created_by: UUID | None = None
    change_summary: str | None = None
    status: DatasetStatus | None = None
    is_current: bool = False


class DatasetVersionResponse(DatasetVersionSummaryResponse):
    definition_snapshot: dict[str, Any] = Field(default_factory=dict)
    schema_snapshot: list[dict[str, Any]] = Field(default_factory=list)
    policy_snapshot: dict[str, Any] = Field(default_factory=dict)
    source_bindings_snapshot: list[dict[str, Any]] = Field(default_factory=list)
    execution_characteristics_snapshot: dict[str, Any] | None = None
    legacy_snapshot: dict[str, Any] | None = None


class DatasetVersionListResponse(_Base):
    items: list[DatasetVersionSummaryResponse] = Field(default_factory=list)


class DatasetVersionFieldDiff(_Base):
    field: str
    change_type: str
    before: Any | None = None
    after: Any | None = None


class DatasetSchemaColumnDiff(_Base):
    column_name: str
    change_type: str
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None


class DatasetVersionDiffResponse(_Base):
    dataset_id: UUID
    from_revision_id: UUID
    to_revision_id: UUID
    from_revision_number: int
    to_revision_number: int
    summary: list[str] = Field(default_factory=list)
    definition_changes: list[DatasetVersionFieldDiff] = Field(default_factory=list)
    policy_changes: list[DatasetVersionFieldDiff] = Field(default_factory=list)
    source_binding_changes: list[DatasetVersionFieldDiff] = Field(default_factory=list)
    execution_changes: list[DatasetVersionFieldDiff] = Field(default_factory=list)
    schema_changes: list[DatasetSchemaColumnDiff] = Field(default_factory=list)


class DatasetRestoreRequest(_Base):
    workspace_id: UUID
    revision_id: UUID
    project_id: UUID | None = None
    change_summary: str | None = Field(default=None, max_length=1024)


class DatasetLineageNodeResponse(_Base):
    node_type: DatasetLineageNodeType
    node_id: str
    label: str
    direction: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class DatasetLineageEdgeResponse(_Base):
    source_type: DatasetLineageNodeType
    source_id: str
    target_type: DatasetLineageNodeType
    target_id: str
    edge_type: DatasetLineageEdgeType
    metadata: dict[str, Any] = Field(default_factory=dict)


class DatasetLineageResponse(_Base):
    dataset_id: UUID
    nodes: list[DatasetLineageNodeResponse] = Field(default_factory=list)
    edges: list[DatasetLineageEdgeResponse] = Field(default_factory=list)
    upstream_count: int = 0
    downstream_count: int = 0


class DatasetImpactItemResponse(_Base):
    node_type: DatasetLineageNodeType
    node_id: str
    label: str
    direct: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class DatasetImpactResponse(_Base):
    dataset_id: UUID
    total_downstream_assets: int = 0
    direct_dependents: list[DatasetImpactItemResponse] = Field(default_factory=list)
    dependent_datasets: list[DatasetImpactItemResponse] = Field(default_factory=list)
    semantic_models: list[DatasetImpactItemResponse] = Field(default_factory=list)
    unified_semantic_models: list[DatasetImpactItemResponse] = Field(default_factory=list)
    saved_queries: list[DatasetImpactItemResponse] = Field(default_factory=list)
    dashboards: list[DatasetImpactItemResponse] = Field(default_factory=list)


class DatasetUsageResponse(_Base):
    semantic_models: list[dict[str, Any]] = Field(default_factory=list)
    unified_semantic_models: list[dict[str, Any]] = Field(default_factory=list)
    dependent_datasets: list[dict[str, Any]] = Field(default_factory=list)
    dashboards: list[dict[str, Any]] = Field(default_factory=list)
    saved_queries: list[dict[str, Any]] = Field(default_factory=list)
