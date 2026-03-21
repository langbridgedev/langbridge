from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import ConfigDict, Field, model_validator

from langbridge.contracts.base import _Base


class SemanticModelCreateRequest(_Base):
    connector_id: UUID | None = None
    workspace_id: UUID
    name: str
    description: str | None = None
    model_yaml: str | None = None
    auto_generate: bool = False
    source_dataset_ids: list[UUID] | None = None

    @model_validator(mode="after")
    def _validate_dataset_generation(self) -> "SemanticModelCreateRequest":
        if self.auto_generate and not self.source_dataset_ids:
            raise ValueError("source_dataset_ids must include at least one dataset when auto_generate is true.")
        return self


class SemanticModelUpdateRequest(_Base):
    connector_id: UUID | None = None
    name: str | None = None
    description: str | None = None
    model_yaml: str | None = None
    auto_generate: bool = False
    source_dataset_ids: list[UUID] | None = None


class SemanticModelRecordResponse(_Base):
    id: UUID
    workspace_id: UUID
    name: str
    description: str | None = None
    content_yaml: str
    created_at: datetime
    updated_at: datetime
    connector_id: UUID | None = None
    source_dataset_ids: list[UUID] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class SemanticModelCatalogFieldResponse(_Base):
    name: str
    type: str
    nullable: bool | None = None
    primary_key: bool = False


class SemanticModelCatalogDatasetResponse(_Base):
    id: UUID
    name: str
    sql_alias: str
    description: str | None = None
    connection_id: UUID | None = None
    source_kind: str
    storage_kind: str
    fields: list[SemanticModelCatalogFieldResponse] = Field(default_factory=list)


class SemanticModelCatalogResponse(_Base):
    workspace_id: UUID
    items: list[SemanticModelCatalogDatasetResponse] = Field(default_factory=list)


class SemanticModelSelectionGenerateRequest(_Base):
    dataset_ids: list[UUID]
    selected_fields: dict[str, list[str]] = Field(default_factory=dict)
    description: str | None = None

    @model_validator(mode="after")
    def _validate_dataset_ids(self) -> "SemanticModelSelectionGenerateRequest":
        if not self.dataset_ids:
            raise ValueError("dataset_ids must include at least one dataset.")
        return self


class SemanticModelSelectionGenerateResponse(_Base):
    yaml_text: str
    warnings: list[str]


class SemanticModelAgenticJobCreateRequest(_Base):
    workspace_id: UUID
    name: str
    description: str | None = None
    filename: str | None = None
    dataset_ids: list[UUID]
    question_prompts: list[str]
    include_sample_values: bool = False

    @model_validator(mode="after")
    def _validate_dataset_ids(self) -> "SemanticModelAgenticJobCreateRequest":
        if not self.dataset_ids:
            raise ValueError("dataset_ids must include at least one dataset.")
        return self


class SemanticModelAgenticJobCreateResponse(_Base):
    job_id: UUID
    job_status: str
    semantic_model_id: UUID
