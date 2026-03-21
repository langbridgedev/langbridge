from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import Field, model_validator

from langbridge.contracts.base import _Base


class SemanticQueryMetaResponse(_Base):
    id: UUID
    name: str
    description: str | None = None
    connector_id: UUID | None = None
    workspace_id: UUID
    semantic_model: dict[str, Any]


class SemanticQueryRequest(_Base):
    workspace_id: UUID
    semantic_model_id: UUID
    query: dict[str, Any]


class SemanticQueryResponse(_Base):
    id: UUID
    workspace_id: UUID
    semantic_model_id: UUID
    data: list[dict[str, Any]]
    annotations: list[dict[str, Any]]
    metadata: list[dict[str, Any]] | None = None


class UnifiedSemanticRelationshipRequest(_Base):
    name: str | None = None
    source_semantic_model_id: UUID
    source_field: str
    target_semantic_model_id: UUID
    target_field: str
    relationship_type: str = "inner"
    operator: str = "="


class UnifiedSemanticSourceModelRequest(_Base):
    id: UUID
    alias: str
    name: str | None = None
    description: str | None = None


class UnifiedSemanticMetricRequest(_Base):
    expression: str
    description: str | None = None


class UnifiedSemanticQueryRequest(_Base):
    workspace_id: UUID
    connector_id: UUID | None = None
    semantic_model_ids: list[UUID]
    source_models: list[UnifiedSemanticSourceModelRequest] = Field(default_factory=list)
    relationships: list[UnifiedSemanticRelationshipRequest] = Field(default_factory=list)
    metrics: dict[str, UnifiedSemanticMetricRequest] | None = None
    query: dict[str, Any]

    @model_validator(mode="after")
    def _validate_semantic_model_ids(self) -> "UnifiedSemanticQueryRequest":
        if not self.semantic_model_ids:
            raise ValueError("semantic_model_ids must include at least one model id.")
        return self


class UnifiedSemanticQueryMetaRequest(_Base):
    workspace_id: UUID
    connector_id: UUID | None = None
    semantic_model_ids: list[UUID]
    source_models: list[UnifiedSemanticSourceModelRequest] = Field(default_factory=list)
    relationships: list[UnifiedSemanticRelationshipRequest] = Field(default_factory=list)
    metrics: dict[str, UnifiedSemanticMetricRequest] | None = None

    @model_validator(mode="after")
    def _validate_semantic_model_ids(self) -> "UnifiedSemanticQueryMetaRequest":
        if not self.semantic_model_ids:
            raise ValueError("semantic_model_ids must include at least one model id.")
        return self


class UnifiedSemanticQueryMetaResponse(_Base):
    connector_id: UUID
    workspace_id: UUID
    semantic_model_ids: list[UUID]
    semantic_model: dict[str, Any]


class UnifiedSemanticQueryResponse(_Base):
    id: UUID
    workspace_id: UUID
    connector_id: UUID
    semantic_model_ids: list[UUID]
    data: list[dict[str, Any]]
    annotations: list[dict[str, Any]]
    metadata: list[dict[str, Any]] | None = None


class SemanticQueryJobResponse(_Base):
    job_id: UUID
    job_status: str


__all__ = [
    "SemanticQueryMetaResponse",
    "SemanticQueryRequest",
    "SemanticQueryResponse",
    "UnifiedSemanticRelationshipRequest",
    "UnifiedSemanticSourceModelRequest",
    "UnifiedSemanticMetricRequest",
    "UnifiedSemanticQueryRequest",
    "UnifiedSemanticQueryMetaRequest",
    "UnifiedSemanticQueryMetaResponse",
    "UnifiedSemanticQueryResponse",
    "SemanticQueryJobResponse",
]
