from __future__ import annotations

import uuid
from typing import Any

from pydantic import Field

from langbridge.packages.runtime.models.base import RuntimeModel, RuntimeRequestModel


class SemanticQueryMetaResponse(RuntimeModel):
    id: uuid.UUID
    name: str
    description: str | None = None
    connector_id: uuid.UUID | None = None
    organization_id: uuid.UUID
    project_id: uuid.UUID | None = None
    semantic_model: dict[str, Any] = Field(default_factory=dict)


class SemanticQueryRequest(RuntimeRequestModel):
    organization_id: uuid.UUID
    project_id: uuid.UUID | None = None
    semantic_model_id: uuid.UUID
    query: dict[str, Any] = Field(default_factory=dict)


class SemanticQueryResponse(RuntimeModel):
    id: uuid.UUID
    organization_id: uuid.UUID
    project_id: uuid.UUID | None = None
    semantic_model_id: uuid.UUID
    data: list[dict[str, Any]] = Field(default_factory=list)
    annotations: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] | None = None


class UnifiedSemanticSourceModelRequest(RuntimeModel):
    id: uuid.UUID
    alias: str
    name: str | None = None
    description: str | None = None


class UnifiedSemanticRelationshipRequest(RuntimeRequestModel):
    name: str | None = None
    source_semantic_model_id: uuid.UUID
    source_field: str
    target_semantic_model_id: uuid.UUID
    target_field: str
    relationship_type: str = "inner"
    operator: str = "="


class UnifiedSemanticMetricRequest(RuntimeRequestModel):
    expression: str
    description: str | None = None


class UnifiedSemanticQueryResponse(RuntimeModel):
    id: uuid.UUID
    organization_id: uuid.UUID
    project_id: uuid.UUID | None = None
    connector_id: uuid.UUID
    semantic_model_ids: list[uuid.UUID] = Field(default_factory=list)
    data: list[dict[str, Any]] = Field(default_factory=list)
    annotations: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] | None = None
