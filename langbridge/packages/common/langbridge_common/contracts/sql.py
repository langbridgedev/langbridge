from __future__ import annotations

import re
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from .base import _Base


class SqlExecutionMode(str, Enum):
    single = "single"
    federated = "federated"


class SqlWorkbenchMode(str, Enum):
    dataset = "dataset"
    direct_sql = "direct_sql"


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


class SqlSelectedDataset(_Base):
    alias: str | None = Field(default=None, min_length=1, max_length=128)
    sql_alias: str | None = Field(default=None, min_length=1, max_length=128)
    dataset_id: UUID
    dataset_name: str | None = Field(default=None, max_length=255)
    canonical_reference: str | None = Field(default=None, max_length=512)
    connector_id: UUID | None = None
    source_kind: str | None = Field(default=None, max_length=32)
    storage_kind: str | None = Field(default=None, max_length=32)

    @field_validator("alias")
    @classmethod
    def _validate_alias(cls, value: str | None) -> str | None:
        if value is None:
            return None
        alias = value.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias):
            raise ValueError(
                "alias must start with a letter or underscore and contain only letters, numbers, or underscores."
            )
        return alias

    @field_validator("sql_alias")
    @classmethod
    def _validate_sql_alias(cls, value: str | None) -> str | None:
        if value is None:
            return None
        alias = value.strip().lower()
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", alias):
            raise ValueError(
                "sql_alias must start with a letter or underscore and contain only lowercase letters, numbers, or underscores."
            )
        return alias

    @model_validator(mode="after")
    def _hydrate_alias_fields(self) -> "SqlSelectedDataset":
        if not self.sql_alias and self.alias:
            self.sql_alias = self.alias.strip().lower()
        if not self.alias and self.sql_alias:
            self.alias = self.sql_alias
        return self


class SqlFederatedDatasetReference(SqlSelectedDataset):
    pass


class SqlExecuteRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    workbench_mode: SqlWorkbenchMode = SqlWorkbenchMode.dataset
    connection_id: UUID | None = None
    federated: bool | None = None
    query: str = Field(..., min_length=1)
    query_dialect: SqlDialect = SqlDialect.tsql
    params: dict[str, Any] = Field(default_factory=dict)
    requested_limit: int | None = Field(default=None, ge=1)
    requested_timeout_seconds: int | None = Field(default=None, ge=1)
    explain: bool = False
    selected_datasets: list[SqlSelectedDataset] = Field(default_factory=list)
    federated_datasets: list[SqlSelectedDataset] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_target(self) -> "SqlExecuteRequest":
        if not self.selected_datasets and self.federated_datasets:
            self.selected_datasets = list(self.federated_datasets)

        if "workbench_mode" not in self.model_fields_set:
            if self.federated is not None:
                self.workbench_mode = (
                    SqlWorkbenchMode.dataset if self.federated else SqlWorkbenchMode.direct_sql
                )
            elif self.connection_id is not None:
                self.workbench_mode = SqlWorkbenchMode.direct_sql
            elif self.selected_datasets:
                self.workbench_mode = SqlWorkbenchMode.dataset

        if self.workbench_mode == SqlWorkbenchMode.direct_sql:
            if self.connection_id is None:
                raise ValueError("connection_id is required for direct SQL execution.")
            if self.selected_datasets:
                raise ValueError("selected_datasets must be omitted for direct SQL execution.")
        else:
            if self.connection_id is not None:
                raise ValueError("connection_id must be omitted for dataset execution mode.")
            if not self.selected_datasets:
                raise ValueError("dataset execution requires at least one selected dataset.")

        if not self.query.strip():
            raise ValueError("query must not be empty.")

        self.federated = self.workbench_mode == SqlWorkbenchMode.dataset
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
    workbench_mode: SqlWorkbenchMode = SqlWorkbenchMode.dataset
    connection_id: UUID | None = None
    selected_datasets: list[SqlSelectedDataset] = Field(default_factory=list)
    execution_mode: SqlExecutionMode
    status: SqlJobStatus
    query: str
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
    workbench_mode: SqlWorkbenchMode = SqlWorkbenchMode.dataset
    connection_id: UUID | None = None
    selected_datasets: list[SqlSelectedDataset] = Field(default_factory=list)
    name: str = Field(..., min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=1024)
    query: str = Field(..., min_length=1)
    tags: list[str] = Field(default_factory=list)
    default_params: dict[str, Any] = Field(default_factory=dict)
    is_shared: bool = False
    last_sql_job_id: UUID | None = None

    @model_validator(mode="after")
    def _validate_mode(self) -> "SqlSavedQueryCreateRequest":
        if self.workbench_mode == SqlWorkbenchMode.direct_sql:
            if self.connection_id is None:
                raise ValueError("connection_id is required for direct SQL saved queries.")
            if self.selected_datasets:
                raise ValueError("selected_datasets must be omitted for direct SQL saved queries.")
        else:
            if self.connection_id is not None:
                raise ValueError("connection_id must be omitted for dataset saved queries.")
            if not self.selected_datasets:
                raise ValueError("dataset saved queries require at least one selected dataset.")
        return self


class SqlSavedQueryUpdateRequest(_Base):
    workspace_id: UUID
    project_id: UUID | None = None
    workbench_mode: SqlWorkbenchMode | None = None
    connection_id: UUID | None = None
    selected_datasets: list[SqlSelectedDataset] | None = None
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
    workbench_mode: SqlWorkbenchMode = SqlWorkbenchMode.dataset
    connection_id: UUID | None = None
    selected_datasets: list[SqlSelectedDataset] = Field(default_factory=list)
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
