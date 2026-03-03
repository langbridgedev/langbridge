

from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator

from langbridge.packages.common.langbridge_common.contracts.base import _Base


class SemanticModelCreateRequest(_Base):
    connector_id: UUID
    organization_id: UUID
    project_id: Optional[UUID] = None
    name: str
    description: Optional[str] = None
    model_yaml: Optional[str] = None
    auto_generate: bool = False

    @field_validator("project_id", mode="before")
    @classmethod
    def normalize_project_id(cls, value: object) -> object:
        if isinstance(value, str) and value.strip() == "":
            return None
        return value


class SemanticModelUpdateRequest(_Base):
    connector_id: Optional[UUID] = None
    project_id: Optional[UUID] = None
    name: Optional[str] = None
    description: Optional[str] = None
    model_yaml: Optional[str] = None
    auto_generate: bool = False

    @field_validator("project_id", mode="before")
    @classmethod
    def normalize_project_id(cls, value: object) -> object:
        if isinstance(value, str) and value.strip() == "":
            return None
        return value


class SemanticModelRecordResponse(_Base):
    id: UUID
    organization_id: UUID
    project_id: Optional[UUID] = None
    name: str
    description: Optional[str] = None
    content_yaml: str
    created_at: datetime
    updated_at: datetime
    connector_id: UUID

    model_config = ConfigDict(from_attributes=True)


class SemanticModelCatalogColumnResponse(_Base):
    name: str
    type: str
    nullable: Optional[bool] = None
    primary_key: bool = False


class SemanticModelCatalogTableResponse(_Base):
    schema: str
    name: str
    fully_qualified_name: str
    columns: List[SemanticModelCatalogColumnResponse]


class SemanticModelCatalogSchemaResponse(_Base):
    name: str
    tables: List[SemanticModelCatalogTableResponse]


class SemanticModelCatalogResponse(_Base):
    connector_id: UUID
    schemas: List[SemanticModelCatalogSchemaResponse]
    table_count: int
    column_count: int


class SemanticModelSelectionGenerateRequest(_Base):
    connector_id: UUID
    selected_tables: List[str]
    selected_columns: Dict[str, List[str]]
    include_sample_values: bool = False
    description: Optional[str] = None


class SemanticModelSelectionGenerateResponse(_Base):
    yaml_text: str
    warnings: List[str] = Field(default_factory=list)


class SemanticModelAgenticJobCreateRequest(_Base):
    connector_id: UUID
    project_id: Optional[UUID] = None
    name: str
    description: Optional[str] = None
    filename: Optional[str] = None
    selected_tables: List[str]
    selected_columns: Dict[str, List[str]]
    question_prompts: List[str]
    include_sample_values: bool = False

    @field_validator("project_id", mode="before")
    @classmethod
    def normalize_project_id(cls, value: object) -> object:
        if isinstance(value, str) and value.strip() == "":
            return None
        return value


class SemanticModelAgenticJobCreateResponse(_Base):
    job_id: UUID
    job_status: str
    semantic_model_id: UUID
