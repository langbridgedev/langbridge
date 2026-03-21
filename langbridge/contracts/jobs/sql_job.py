from __future__ import annotations

import uuid
from typing import Any, Literal

from pydantic import Field, model_validator

from langbridge.contracts.base import _Base
from langbridge.contracts.jobs.type import JobType
from langbridge.contracts.sql import SqlSelectedDataset, SqlWorkbenchMode


class CreateSqlJobRequest(_Base):
    job_type: JobType = JobType.SQL
    sql_job_id: uuid.UUID
    workspace_id: uuid.UUID
    actor_id: uuid.UUID
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
    selected_datasets: list[SqlSelectedDataset] = Field(default_factory=list)
    federated_datasets: list[SqlSelectedDataset] = Field(default_factory=list)
    explain: bool = False
    correlation_id: str | None = None

    @model_validator(mode="after")
    def _validate_request(self) -> "CreateSqlJobRequest":
        if not self.selected_datasets and self.federated_datasets:
            self.selected_datasets = list(self.federated_datasets)

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
        else:
            if self.execution_mode != "federated":
                raise ValueError("Dataset SQL jobs must use federated execution mode.")
            if self.connection_id is not None:
                raise ValueError("connection_id must be omitted for dataset SQL jobs.")
            if not self.allow_federation:
                raise ValueError("Dataset SQL execution is not enabled for this workspace.")
            if not self.selected_datasets:
                raise ValueError("Dataset SQL jobs require at least one selected dataset.")

        if not self.query.strip():
            raise ValueError("query is required.")
        self.query_dialect = self.query_dialect.strip().lower() or "tsql"
        return self


__all__ = ["CreateSqlJobRequest"]
