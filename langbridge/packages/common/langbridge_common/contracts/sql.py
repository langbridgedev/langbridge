from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import Field, model_validator

from .base import _Base


class SqlExecutionMode(str, Enum):
    single = "single"
    federated = "federated"


class SqlJobStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"
    awaiting_approval = "awaiting_approval"


class SqlAssistMode(str, Enum):
    generate = "generate"
    fix = "fix"
    explain = "explain"
    lint = "lint"


class SqlDialect(str, Enum):
    tsql = "tsql"
    postgres = "postgres"
    mysql = "mysql"
    snowflake = "snowflake"
    redshift = "redshift"
    bigquery = "bigquery"
    oracle = "oracle"
    sqlite = "sqlite"


class SqlColumnMetadata(_Base):
    name: str
    type: str | None = None


class SqlFederatedDatasetReference(_Base):
    alias: str = Field(..., min_length=1, max_length=128)
    dataset_id: UUID


class SqlExecuteRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    connection_id: UUID | None = None
    federated: bool = False
    query: str = Field(..., min_length=1)
    query_dialect: SqlDialect = SqlDialect.tsql
    params: dict[str, Any] = Field(default_factory=dict)
    requested_limit: int | None = Field(default=None, ge=1)
    requested_timeout_seconds: int | None = Field(default=None, ge=1)
    explain: bool = False
    federated_datasets: list[SqlFederatedDatasetReference] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_target(self) -> "SqlExecuteRequest":
        if not self.federated and self.connection_id is None:
            raise ValueError("connection_id is required for single datasource execution.")
        if self.federated and self.connection_id is not None:
            raise ValueError("connection_id must be omitted for federated execution mode.")
        if self.federated and not self.federated_datasets:
            raise ValueError("federated execution requires at least one federated dataset.")
        if not self.query.strip():
            raise ValueError("query must not be empty.")
        return self


class SqlExecuteResponse(_Base):
    sql_job_id: UUID
    expensive_query: bool = False
    warnings: list[str] = Field(default_factory=list)


class SqlCancelRequest(_Base):
    sql_job_id: UUID
    workspace_id: UUID


class SqlCancelResponse(_Base):
    accepted: bool
    status: SqlJobStatus


class SqlJobResultArtifactResponse(_Base):
    id: UUID
    format: str
    mime_type: str
    row_count: int = 0
    byte_size: int | None = None
    storage_reference: str
    created_at: datetime


class SqlJobResponse(_Base):
    id: UUID
    workspace_id: UUID
    project_id: UUID | None = None
    user_id: UUID
    connection_id: UUID | None = None
    execution_mode: SqlExecutionMode
    status: SqlJobStatus
    query_hash: str
    is_explain: bool = False
    is_federated: bool = False
    requested_limit: int | None = None
    enforced_limit: int
    requested_timeout_seconds: int | None = None
    enforced_timeout_seconds: int
    row_count_preview: int = 0
    total_rows_estimate: int | None = None
    bytes_scanned: int | None = None
    duration_ms: int | None = None
    redaction_applied: bool = False
    warning: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    correlation_id: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    artifacts: list[SqlJobResultArtifactResponse] = Field(default_factory=list)


class SqlJobResultsResponse(_Base):
    sql_job_id: UUID
    status: SqlJobStatus
    columns: list[SqlColumnMetadata] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count_preview: int = 0
    total_rows_estimate: int | None = None
    next_cursor: str | None = None
    artifacts: list[SqlJobResultArtifactResponse] = Field(default_factory=list)


class SqlHistoryResponse(_Base):
    items: list[SqlJobResponse] = Field(default_factory=list)


class SqlSavedQueryCreateRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    connection_id: UUID | None = None
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=1024)
    query: str = Field(..., min_length=1)
    tags: list[str] = Field(default_factory=list)
    default_params: dict[str, Any] = Field(default_factory=dict)
    is_shared: bool = False
    last_sql_job_id: UUID | None = None


class SqlSavedQueryUpdateRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    connection_id: UUID | None = None
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=1024)
    query: str | None = Field(default=None, min_length=1)
    tags: list[str] | None = None
    default_params: dict[str, Any] | None = None
    is_shared: bool | None = None
    last_sql_job_id: UUID | None = None


class SqlSavedQueryResponse(_Base):
    id: UUID
    workspace_id: UUID
    project_id: UUID | None = None
    created_by: UUID
    updated_by: UUID
    connection_id: UUID | None = None
    name: str
    description: str | None = None
    query: str
    query_hash: str
    tags: list[str] = Field(default_factory=list)
    default_params: dict[str, Any] = Field(default_factory=dict)
    is_shared: bool = False
    last_sql_job_id: UUID | None = None
    last_result_artifact_id: UUID | None = None
    created_at: datetime
    updated_at: datetime


class SqlSavedQueryListResponse(_Base):
    items: list[SqlSavedQueryResponse] = Field(default_factory=list)


class SqlWorkspacePolicyBounds(_Base):
    max_preview_rows_upper_bound: int
    max_export_rows_upper_bound: int
    max_runtime_seconds_upper_bound: int
    max_concurrency_upper_bound: int


class SqlWorkspacePolicyResponse(_Base):
    workspace_id: UUID
    max_preview_rows: int
    max_export_rows: int
    max_runtime_seconds: int
    max_concurrency: int
    allow_dml: bool
    allow_federation: bool
    allowed_schemas: list[str] = Field(default_factory=list)
    allowed_tables: list[str] = Field(default_factory=list)
    default_datasource: UUID | None = None
    budget_limit_bytes: int | None = None
    bounds: SqlWorkspacePolicyBounds
    updated_at: datetime | None = None


class SqlWorkspacePolicyUpdateRequest(_Base):
    workspace_id: UUID
    max_preview_rows: int | None = Field(default=None, ge=1)
    max_export_rows: int | None = Field(default=None, ge=1)
    max_runtime_seconds: int | None = Field(default=None, ge=1)
    max_concurrency: int | None = Field(default=None, ge=1)
    allow_dml: bool | None = None
    allow_federation: bool | None = None
    allowed_schemas: list[str] | None = None
    allowed_tables: list[str] | None = None
    default_datasource: UUID | None = None
    budget_limit_bytes: int | None = Field(default=None, ge=1)


class SqlAssistRequest(_Base):
    workspace_id: UUID
    connection_id: UUID | None = None
    mode: SqlAssistMode
    prompt: str = Field(..., min_length=1, max_length=8000)
    query: str | None = None


class SqlAssistResponse(_Base):
    mode: SqlAssistMode
    suggestion: str
    warnings: list[str] = Field(default_factory=list)
