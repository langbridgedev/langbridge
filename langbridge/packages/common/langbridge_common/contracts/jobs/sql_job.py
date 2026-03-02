from __future__ import annotations

from typing import Any, Literal
import uuid

from pydantic import Field, model_validator

from langbridge.packages.common.langbridge_common.contracts.base import _Base

from .type import JobType


class CreateSqlJobRequest(_Base):
    job_type: JobType = JobType.SQL
    sql_job_id: uuid.UUID
    workspace_id: uuid.UUID
    project_id: uuid.UUID | None = None
    user_id: uuid.UUID
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
    explain: bool = False
    correlation_id: str | None = None

    @model_validator(mode="after")
    def _validate_request(self) -> "CreateSqlJobRequest":
        if self.execution_mode == "single" and self.connection_id is None:
            raise ValueError("connection_id is required for single datasource SQL jobs.")
        if self.execution_mode == "federated" and self.connection_id is not None:
            raise ValueError("connection_id must be omitted for federated SQL jobs.")
        if self.execution_mode == "federated" and not self.allow_federation:
            raise ValueError("Federated SQL execution is not enabled for this workspace.")
        if not self.query.strip():
            raise ValueError("query is required.")
        self.query_dialect = self.query_dialect.strip().lower() or "tsql"
        return self
