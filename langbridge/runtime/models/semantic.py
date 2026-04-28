
import uuid
from typing import Any

from pydantic import Field

from langbridge.runtime.models.base import RuntimeModel, RuntimeRequestModel
from langbridge.runtime.models.federation_diagnostics import RuntimeFederationDiagnostics


class SemanticQueryMetaResponse(RuntimeModel):
    id: uuid.UUID
    name: str
    description: str | None = None
    connector_id: uuid.UUID | None = None
    workspace_id: uuid.UUID
    semantic_model: dict[str, Any] = Field(default_factory=dict)


class SemanticQueryRequest(RuntimeRequestModel):
    workspace_id: uuid.UUID
    semantic_model_id: uuid.UUID
    query: dict[str, Any] = Field(default_factory=dict)

class SemanticQueryResponse(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    semantic_model_id: uuid.UUID
    data: list[dict[str, Any]] = Field(default_factory=list)
    annotations: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] | None = None
    federation_diagnostics: RuntimeFederationDiagnostics | None = None


class SemanticGraphSourceModelRequest(RuntimeModel):
    id: uuid.UUID
    alias: str
    name: str | None = None
    description: str | None = None


class SemanticGraphRelationshipRequest(RuntimeRequestModel):
    name: str | None = None
    source_semantic_model_id: uuid.UUID
    source_field: str
    target_semantic_model_id: uuid.UUID
    target_field: str
    relationship_type: str = "inner"
    operator: str = "="


class SemanticGraphMetricRequest(RuntimeRequestModel):
    expression: str
    description: str | None = None


class SemanticGraphQueryResponse(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    connector_id: uuid.UUID
    semantic_model_ids: list[uuid.UUID] = Field(default_factory=list)
    data: list[dict[str, Any]] = Field(default_factory=list)
    annotations: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] | None = None
    federation_diagnostics: RuntimeFederationDiagnostics | None = None


UnifiedSemanticSourceModelRequest = SemanticGraphSourceModelRequest
UnifiedSemanticRelationshipRequest = SemanticGraphRelationshipRequest
UnifiedSemanticMetricRequest = SemanticGraphMetricRequest
UnifiedSemanticQueryResponse = SemanticGraphQueryResponse
