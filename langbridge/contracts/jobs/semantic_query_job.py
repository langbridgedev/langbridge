from __future__ import annotations

import uuid
from typing import Any, Literal

from pydantic import Field, model_validator

from langbridge.contracts.base import _Base
from langbridge.contracts.jobs.type import JobType
from langbridge.contracts.semantic.semantic_query import (
    UnifiedSemanticMetricRequest,
    UnifiedSemanticRelationshipRequest,
    UnifiedSemanticSourceModelRequest,
)


class CreateSemanticQueryJobRequest(_Base):
    job_type: JobType = JobType.SEMANTIC_QUERY
    workspace_id: uuid.UUID
    actor_id: uuid.UUID
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
                raise ValueError("semantic_model_ids must include at least one model id for unified query scope.")
            return self

        raise ValueError(f"Unsupported semantic query scope '{self.query_scope}'.")


__all__ = ["CreateSemanticQueryJobRequest"]
