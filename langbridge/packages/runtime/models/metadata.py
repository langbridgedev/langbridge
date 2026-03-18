from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import Field

from langbridge.packages.runtime.models.base import RuntimeModel


class SecretReference(RuntimeModel):
    provider_type: Literal[
        "env",
        "kubernetes",
        "vault",
        "azure_key_vault",
        "aws_secrets_manager",
    ]
    identifier: str
    key: str | None = None
    version: str | None = None


class ConnectionPolicy(RuntimeModel):
    allowed_schemas: list[str] = Field(default_factory=list)
    allowed_tables: list[str] = Field(default_factory=list)
    max_row_limit: int | None = None
    redaction_rules: dict[str, str] = Field(default_factory=dict)


class ConnectionMetadata(RuntimeModel):
    host: str | None = None
    port: int | None = None
    database: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    role: str | None = None
    account: str | None = None
    user: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class ConnectorMetadata(RuntimeModel):
    id: uuid.UUID
    name: str
    description: str | None = None
    version: str | None = None
    label: str | None = None
    icon: str | None = None
    connector_type: str | None = None
    organization_id: uuid.UUID | None = None
    project_id: uuid.UUID | None = None
    config: dict[str, Any] | None = None
    connection_metadata: ConnectionMetadata | None = None
    secret_references: dict[str, SecretReference] = Field(default_factory=dict)
    connection_policy: ConnectionPolicy | None = None
    is_managed: bool = False


class DatasetColumnMetadata(RuntimeModel):
    id: uuid.UUID
    dataset_id: uuid.UUID
    workspace_id: uuid.UUID | None = None
    name: str
    data_type: str
    nullable: bool = True
    description: str | None = None
    is_allowed: bool = True
    is_computed: bool = False
    expression: str | None = None
    ordinal_position: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None


class DatasetPolicyMetadata(RuntimeModel):
    id: uuid.UUID | None = None
    dataset_id: uuid.UUID | None = None
    workspace_id: uuid.UUID | None = None
    max_rows_preview: int = 1000
    max_export_rows: int = 10000
    redaction_rules: dict[str, str] = Field(default_factory=dict)
    row_filters: list[str] = Field(default_factory=list)
    allow_dml: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def redaction_rules_json(self) -> dict[str, str]:
        return dict(self.redaction_rules)

    @property
    def row_filters_json(self) -> list[str]:
        return list(self.row_filters)


class DatasetSourceKind(str, Enum):
    DATABASE = "database"
    SAAS = "saas"
    API = "api"
    FILE = "file"
    VIRTUAL = "virtual"


class DatasetStorageKind(str, Enum):
    TABLE = "table"
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    VIEW = "view"
    VIRTUAL = "virtual"


class DatasetExecutionCapabilities(RuntimeModel):
    supports_structured_scan: bool = False
    supports_sql_federation: bool = False
    supports_filter_pushdown: bool = False
    supports_projection_pushdown: bool = False
    supports_aggregation_pushdown: bool = False
    supports_join_pushdown: bool = False
    supports_materialization: bool = False
    supports_semantic_modeling: bool = False


class DatasetRelationIdentity(RuntimeModel):
    canonical_reference: str
    relation_name: str
    qualified_name: str | None = None
    catalog_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    storage_uri: str | None = None
    dataset_id: uuid.UUID | None = None
    connector_id: uuid.UUID | None = None
    source_kind: DatasetSourceKind
    storage_kind: DatasetStorageKind


class DatasetMetadata(RuntimeModel):
    id: uuid.UUID
    workspace_id: uuid.UUID
    project_id: uuid.UUID | None = None
    connection_id: uuid.UUID | None = None
    owner_id: uuid.UUID | None = None
    created_by: uuid.UUID | None = None
    updated_by: uuid.UUID | None = None
    name: str
    sql_alias: str
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    dataset_type: str
    source_kind: str | None = None
    connector_kind: str | None = None
    storage_kind: str | None = None
    dialect: str | None = None
    catalog_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    storage_uri: str | None = None
    sql_text: str | None = None
    relation_identity: dict[str, Any] | None = None
    execution_capabilities: dict[str, Any] | None = None
    referenced_dataset_ids: list[Any] = Field(default_factory=list)
    federated_plan: dict[str, Any] | None = None
    file_config: dict[str, Any] | None = None
    status: str = "published"
    revision_id: uuid.UUID | None = None
    row_count_estimate: int | None = None
    bytes_estimate: int | None = None
    last_profiled_at: datetime | None = None
    columns: list[DatasetColumnMetadata] = Field(default_factory=list)
    policy: DatasetPolicyMetadata | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @property
    def tags_json(self) -> list[str]:
        return list(self.tags)

    @property
    def relation_identity_json(self) -> dict[str, Any] | None:
        return None if self.relation_identity is None else dict(self.relation_identity)

    @property
    def execution_capabilities_json(self) -> dict[str, Any] | None:
        return (
            None
            if self.execution_capabilities is None
            else dict(self.execution_capabilities)
        )

    @property
    def referenced_dataset_ids_json(self) -> list[Any]:
        return list(self.referenced_dataset_ids)

    @property
    def federated_plan_json(self) -> dict[str, Any] | None:
        return None if self.federated_plan is None else dict(self.federated_plan)

    @property
    def file_config_json(self) -> dict[str, Any] | None:
        return None if self.file_config is None else dict(self.file_config)


class SemanticModelMetadata(RuntimeModel):
    id: uuid.UUID
    connector_id: uuid.UUID | None = None
    organization_id: uuid.UUID
    project_id: uuid.UUID | None = None
    name: str
    description: str | None = None
    content_yaml: str
    content_json: dict[str, Any] | str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
