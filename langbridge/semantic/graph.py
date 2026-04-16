from typing import Dict, List, Optional
from uuid import UUID

import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from langbridge.semantic.model import Metric


class SemanticGraphSourceModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    alias: str
    name: str | None = None
    description: str | None = None

    @field_validator("alias")
    @classmethod
    def _validate_alias(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("Semantic graph source alias is required.")
        return normalized


class SemanticGraphRelationship(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    source_semantic_model_id: UUID
    source_field: str
    target_semantic_model_id: UUID
    target_field: str
    relationship_type: str = "inner"
    operator: str = "="

    @field_validator("source_field", "target_field", "relationship_type", "operator")
    @classmethod
    def _validate_required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("Semantic graph relationship fields must be non-empty.")
        return normalized

    @field_validator("name")
    @classmethod
    def _validate_optional_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class SemanticGraph(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: str
    source_models: List[SemanticGraphSourceModel]
    name: Optional[str] = None
    description: Optional[str] = None
    relationships: Optional[List[SemanticGraphRelationship]] = None
    metrics: Optional[Dict[str, Metric]] = None

    @model_validator(mode="after")
    def _validate_source_models(self) -> "SemanticGraph":
        if not self.source_models:
            raise ValueError("Semantic graph must define at least one source model.")

        seen_ids: set[UUID] = set()
        seen_aliases: set[str] = set()
        for source_model in self.source_models:
            if source_model.id in seen_ids:
                raise ValueError("Semantic graph source model ids must be unique.")
            if source_model.alias in seen_aliases:
                raise ValueError("Semantic graph source model aliases must be unique.")
            seen_ids.add(source_model.id)
            seen_aliases.add(source_model.alias)
        return self

    def yml_dump(self) -> str:
        return yaml.safe_dump(
            self.model_dump(by_alias=True, exclude_none=True),
            sort_keys=False,
        )
