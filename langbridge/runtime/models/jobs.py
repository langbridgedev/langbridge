from __future__ import annotations

import re
import uuid
from enum import Enum
from typing import Any, Literal

from pydantic import Field, model_validator

from langbridge.runtime.models.base import RuntimeRequestModel
from langbridge.runtime.models.semantic import (
    UnifiedSemanticMetricRequest,
    UnifiedSemanticRelationshipRequest,
    UnifiedSemanticSourceModelRequest,
)


class JobType(str, Enum):
    AGENT = "agent"
    SEMANTIC_QUERY = "semantic_query"
    AGENTIC_SEMANTIC_MODEL = "agentic_semantic_model"
    SQL = "sql"
    DATASET_PREVIEW = "dataset_preview"
    DATASET_PROFILE = "dataset_profile"
    DATASET_BULK_CREATE = "dataset_bulk_create"
    DATASET_CSV_INGEST = "dataset_csv_ingest"
    CONNECTOR_SYNC = "connector_sync"


class SqlWorkbenchMode(str, Enum):
    dataset = "dataset"
    direct_sql = "direct_sql"


_CONNECTOR_SYNC_MODES = {"INCREMENTAL", "FULL_REFRESH", "WEBHOOK_ASSISTED"}


def _actor_id_field() -> Any:
    return Field()


class RuntimeJobRequestModel(RuntimeRequestModel):
    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_job_payload(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        normalized = dict(data)

        if (
            "actor_id" not in normalized
            and "actorId" not in normalized
            and "userId" in normalized
        ):
            normalized["actorId"] = normalized["userId"]

        if "workspace_id" not in normalized and "workspaceId" not in normalized:
            for legacy_key in ("organisationId", "organizationId", "projectId"):
                if legacy_key in normalized:
                    normalized["workspaceId"] = normalized[legacy_key]
                    break

        return normalized


class SqlSelectedDataset(RuntimeRequestModel):
    alias: str | None = Field(default=None, min_length=1, max_length=128)
    sql_alias: str | None = Field(default=None, min_length=1, max_length=128)
    dataset_id: uuid.UUID
    dataset_name: str | None = Field(default=None, max_length=255)
    canonical_reference: str | None = Field(default=None, max_length=512)
    connector_id: uuid.UUID | None = None
    source_kind: str | None = Field(default=None, max_length=32)
    storage_kind: str | None = Field(default=None, max_length=32)

    @model_validator(mode="after")
    def _hydrate_alias_fields(self) -> "SqlSelectedDataset":
        alias = str(self.alias or "").strip() or None
        sql_alias = str(self.sql_alias or "").strip().lower() or None

        if alias and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias):
            raise ValueError(
                "alias must start with a letter or underscore and contain only letters, numbers, or underscores."
            )
        if sql_alias and not re.fullmatch(r"[a-z_][a-z0-9_]*", sql_alias):
            raise ValueError(
                "sql_alias must start with a letter or underscore and contain only lowercase letters, numbers, or underscores."
            )

        if not sql_alias and alias:
            sql_alias = alias.lower()
        if not alias and sql_alias:
            alias = sql_alias

        self.alias = alias
        self.sql_alias = sql_alias
        return self


class CreateSqlJobRequest(RuntimeJobRequestModel):
    job_type: JobType = JobType.SQL
    sql_job_id: uuid.UUID
    workspace_id: uuid.UUID
    actor_id: uuid.UUID = _actor_id_field()
    workbench_mode: SqlWorkbenchMode = SqlWorkbenchMode.dataset
    connection_id: uuid.UUID | None = None
    execution_mode: Literal["single", "federated"] = "single"
    query: str = Field(..., min_length=1)
    query_dialect: str = Field(default="tsql", min_length=1, max_length=64)
    params: dict[str, Any] = Field(default_factory=dict)
    requested_limit: int | None = Field(default=None, ge=1)
    requested_timeout_seconds: int | None = Field(default=None, ge=1)
    enforced_limit: int = Field(..., ge=1)
    enforced_timeout_seconds: int = Field(..., ge=1)
    allow_dml: bool = False
    allow_federation: bool = False
    allowed_schemas: list[str] = Field(default_factory=list)
    allowed_tables: list[str] = Field(default_factory=list)
    redaction_rules: dict[str, str] = Field(default_factory=dict)
    selected_datasets: list[uuid.UUID] = Field(default_factory=list)
    federated_datasets: list[SqlSelectedDataset] = Field(default_factory=list)
    explain: bool = False
    correlation_id: str | None = None

    @model_validator(mode="after")
    def _validate_request(self) -> "CreateSqlJobRequest":
        normalized_selected: list[uuid.UUID] = []
        for dataset_id in self.selected_datasets:
            if dataset_id not in normalized_selected:
                normalized_selected.append(dataset_id)
        self.selected_datasets = normalized_selected

        normalized_federated: list[SqlSelectedDataset] = []
        seen_federated_ids: set[uuid.UUID] = set()
        for dataset in self.federated_datasets:
            if dataset.dataset_id in seen_federated_ids:
                continue
            normalized_federated.append(dataset)
            seen_federated_ids.add(dataset.dataset_id)
        self.federated_datasets = normalized_federated

        if not self.selected_datasets and self.federated_datasets:
            self.selected_datasets = [dataset.dataset_id for dataset in self.federated_datasets]

        if "workbench_mode" not in self.model_fields_set:
            self.workbench_mode = (
                SqlWorkbenchMode.dataset
                if self.execution_mode == "federated"
                else SqlWorkbenchMode.direct_sql
            )

        if self.workbench_mode == SqlWorkbenchMode.direct_sql:
            if self.execution_mode != "single":
                raise ValueError("Direct SQL jobs must use single execution mode.")
            if self.connection_id is None:
                raise ValueError("connection_id is required for direct SQL jobs.")
            if self.selected_datasets:
                raise ValueError("selected_datasets must be omitted for direct SQL jobs.")
            if self.federated_datasets:
                raise ValueError("federated_datasets must be omitted for direct SQL jobs.")
        else:
            if self.execution_mode != "federated":
                raise ValueError("Dataset SQL jobs must use federated execution mode.")
            if self.connection_id is not None:
                raise ValueError("connection_id must be omitted for dataset SQL jobs.")
            if not self.allow_federation:
                raise ValueError("Dataset SQL execution is not enabled for this workspace.")

        if not self.query.strip():
            raise ValueError("query is required.")
        self.query_dialect = self.query_dialect.strip().lower() or "tsql"
        return self


class CreateSemanticQueryJobRequest(RuntimeJobRequestModel):
    job_type: JobType = JobType.SEMANTIC_QUERY
    workspace_id: uuid.UUID
    actor_id: uuid.UUID = _actor_id_field()
    query_scope: Literal["semantic_model", "unified"] = "semantic_model"
    semantic_model_id: uuid.UUID | None = None
    connector_id: uuid.UUID | None = None
    semantic_model_ids: list[uuid.UUID] | None = None
    source_models: list[UnifiedSemanticSourceModelRequest] | None = None
    relationships: list[UnifiedSemanticRelationshipRequest] | None = None
    metrics: dict[str, UnifiedSemanticMetricRequest] | None = None
    query: dict[str, Any]

    @model_validator(mode="after")
    def _validate_scope_payload(self) -> "CreateSemanticQueryJobRequest":
        if self.query_scope == "semantic_model":
            if self.semantic_model_id is None:
                raise ValueError("semantic_model_id is required for semantic_model query scope.")
            return self

        if self.query_scope == "unified":
            if not self.semantic_model_ids:
                raise ValueError(
                    "semantic_model_ids must include at least one model id for unified query scope."
                )
            return self

        raise ValueError(f"Unsupported semantic query scope '{self.query_scope}'.")


class DatasetPolicyDefaultsRequest(RuntimeRequestModel):
    max_preview_rows: int | None = Field(default=None, ge=1)
    max_export_rows: int | None = Field(default=None, ge=1)
    allow_dml: bool | None = None
    redaction_rules: dict[str, str] = Field(default_factory=dict)


class DatasetSelectionColumnRequest(RuntimeRequestModel):
    name: str = Field(..., min_length=1, max_length=255)
    data_type: str | None = Field(default=None, max_length=128)
    nullable: bool | None = None


class DatasetSelectionRequest(RuntimeRequestModel):
    schema_name: str = Field(..., alias="schema", min_length=1, max_length=255)
    table: str = Field(..., min_length=1, max_length=255)
    columns: list[DatasetSelectionColumnRequest] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_columns(self) -> "DatasetSelectionRequest":
        names = [column.name.strip().lower() for column in self.columns if column.name.strip()]
        if len(set(names)) != len(names):
            raise ValueError("columns must contain unique names per table selection.")
        return self


class CreateDatasetPreviewJobRequest(RuntimeJobRequestModel):
    job_type: JobType = JobType.DATASET_PREVIEW
    dataset_id: uuid.UUID
    workspace_id: uuid.UUID
    actor_id: uuid.UUID = _actor_id_field()
    requested_limit: int | None = Field(default=None, ge=1)
    enforced_limit: int = Field(..., ge=1)
    filters: dict[str, Any] = Field(default_factory=dict)
    sort: list[dict[str, Any]] = Field(default_factory=list)
    user_context: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None
    operation: Literal["preview"] = "preview"

    @model_validator(mode="after")
    def _validate_request(self) -> "CreateDatasetPreviewJobRequest":
        if self.requested_limit is not None and self.requested_limit < 1:
            raise ValueError("requested_limit must be greater than zero when supplied.")
        return self


class CreateDatasetProfileJobRequest(RuntimeJobRequestModel):
    job_type: JobType = JobType.DATASET_PROFILE
    dataset_id: uuid.UUID
    workspace_id: uuid.UUID
    actor_id: uuid.UUID = _actor_id_field()
    user_context: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None
    operation: Literal["profile"] = "profile"


class CreateDatasetCsvIngestJobRequest(RuntimeJobRequestModel):
    job_type: JobType = JobType.DATASET_CSV_INGEST
    dataset_id: uuid.UUID
    workspace_id: uuid.UUID
    actor_id: uuid.UUID = _actor_id_field()
    storage_uri: str | None = None
    correlation_id: str | None = None
    operation: Literal["csv_ingest"] = "csv_ingest"


class CreateDatasetBulkCreateJobRequest(RuntimeJobRequestModel):
    job_type: JobType = JobType.DATASET_BULK_CREATE
    workspace_id: uuid.UUID
    actor_id: uuid.UUID = _actor_id_field()
    connection_id: uuid.UUID
    selections: list[DatasetSelectionRequest] = Field(default_factory=list)
    naming_template: str = "{schema}.{table}"
    policy_defaults: DatasetPolicyDefaultsRequest | None = None
    tags: list[str] = Field(default_factory=list)
    profile_after_create: bool = False
    correlation_id: str | None = None
    operation: Literal["bulk_create"] = "bulk_create"

    @model_validator(mode="after")
    def _validate_bulk(self) -> "CreateDatasetBulkCreateJobRequest":
        if len(self.selections) == 0:
            raise ValueError("Bulk create job requires at least one selection.")
        if len(self.selections) > 500:
            raise ValueError("Bulk create job supports at most 500 selections.")
        return self


class CreateConnectorSyncJobRequest(RuntimeJobRequestModel):
    job_type: JobType = JobType.CONNECTOR_SYNC
    workspace_id: uuid.UUID
    actor_id: uuid.UUID = _actor_id_field()
    connection_id: uuid.UUID
    resource_names: list[str] = Field(default_factory=list)
    sync_mode: str = "INCREMENTAL"
    force_full_refresh: bool = False
    correlation_id: str | None = None
    operation: Literal["connector_sync"] = "connector_sync"

    @model_validator(mode="after")
    def _validate_resource_names(self) -> "CreateConnectorSyncJobRequest":
        normalized = [str(value or "").strip() for value in self.resource_names if str(value or "").strip()]
        if not normalized:
            raise ValueError("Connector sync requires at least one resource.")
        sync_mode = str(getattr(self.sync_mode, "value", self.sync_mode) or "INCREMENTAL").strip().upper()
        if sync_mode not in _CONNECTOR_SYNC_MODES:
            raise ValueError(f"Unsupported connector sync mode '{sync_mode}'.")
        self.resource_names = normalized
        self.sync_mode = sync_mode
        return self


class CreateAgentJobRequest(RuntimeJobRequestModel):
    job_type: JobType
    agent_definition_id: uuid.UUID
    workspace_id: uuid.UUID
    actor_id: uuid.UUID = _actor_id_field()
    thread_id: uuid.UUID
