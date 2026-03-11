from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Literal, Optional, cast
from uuid import UUID

from pydantic import Field, model_validator

from langbridge.packages.common.langbridge_common.db.connector import Connector

from .base import _Base


class SecretReference(_Base):
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


class ConnectionPolicy(_Base):
    allowed_schemas: list[str] = Field(default_factory=list)
    allowed_tables: list[str] = Field(default_factory=list)
    max_row_limit: int | None = None
    redaction_rules: dict[str, str] = Field(default_factory=dict)


class ConnectionMetadata(_Base):
    host: str | None = None
    port: int | None = None
    database: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    role: str | None = None
    account: str | None = None
    user: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class ConnectorFamily(str, Enum):
    DATABASE = "DATABASE"
    API = "API"
    VECTOR_DB = "VECTOR_DB"


class ConnectorSyncStrategy(str, Enum):
    FULL_REFRESH = "FULL_REFRESH"
    INCREMENTAL = "INCREMENTAL"
    WINDOWED_INCREMENTAL = "WINDOWED_INCREMENTAL"
    MANUAL = "MANUAL"


class ConnectorSyncMode(str, Enum):
    FULL_REFRESH = "FULL_REFRESH"
    INCREMENTAL = "INCREMENTAL"
    WEBHOOK_ASSISTED = "WEBHOOK_ASSISTED"


class ConnectorSyncStatus(str, Enum):
    NEVER_SYNCED = "never_synced"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ConnectorAuthSchemaField(_Base):
    field: str
    label: str | None = None
    required: bool = True
    description: str
    type: str
    secret: bool = False
    default: str | None = None
    value_list: list[str] = Field(default_factory=list)


class ConnectorPluginMetadata(_Base):
    connector_type: str
    connector_family: ConnectorFamily
    supported_resources: list[str] = Field(default_factory=list)
    auth_schema: list[ConnectorAuthSchemaField] = Field(default_factory=list)
    sync_strategy: ConnectorSyncStrategy | None = None


class ConnectorDTO(_Base):
    id: Optional[UUID] = None
    name: str
    description: Optional[str] = None
    version: Optional[str] = None
    label: Optional[str] = None
    icon: Optional[str] = None
    connector_type: Optional[str] = None
    organization_id: UUID
    project_id: Optional[UUID] = None
    config: Optional[Dict[str, Any]] = None
    connection_metadata: ConnectionMetadata | None = None
    secret_references: dict[str, SecretReference] = Field(default_factory=dict)
    connection_policy: ConnectionPolicy | None = None
    is_managed: bool = False


def _parse_connector_config(raw_config: Any) -> Optional[Dict[str, Any]]:
    config: Optional[Dict[str, Any]] = None
    if isinstance(raw_config, (str, bytes)):
        try:
            parsed = json.loads(raw_config)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            config = parsed
    elif isinstance(raw_config, dict):
        config = raw_config
    return config


class ConnectorResponse(_Base):
    id: UUID
    name: str
    description: Optional[str] = None
    version: Optional[str] = None
    label: Optional[str] = None
    icon: Optional[str] = None
    connector_type: Optional[str] = None
    organization_id: UUID
    project_id: Optional[UUID] = None
    config: Optional[Dict[str, Any]] = None
    connection_metadata: ConnectionMetadata | None = None
    secret_references: dict[str, SecretReference] = Field(default_factory=dict)
    connection_policy: ConnectionPolicy | None = None
    catalog_summary: "ConnectorCatalogSummary | None" = None
    plugin_metadata: "ConnectorPluginMetadata | None" = None
    is_managed: bool = False

    @staticmethod
    def from_connector(
        connector: Connector,
        organization_id: Optional[UUID] = None,
        project_id: Optional[UUID] = None,
        plugin_metadata: "ConnectorPluginMetadata | None" = None,
    ) -> "ConnectorResponse":
        config = _parse_connector_config(connector.config_json)

        resolved_org_id = organization_id
        if resolved_org_id is None and getattr(connector, "organizations", None):
            resolved_org_id = cast(UUID, connector.organizations[0].id)

        if resolved_org_id is None:
            raise ValueError("ConnectorResponse requires organization_id")

        raw_metadata = getattr(connector, "connection_metadata_json", None)
        raw_secret_refs = getattr(connector, "secret_references_json", None)
        raw_policy = getattr(connector, "access_policy_json", None)

        return ConnectorResponse(
            id=cast(Optional[UUID], connector.id),
            name=cast(str, connector.name),
            description=cast(Optional[str], connector.description),
            version="",
            label=cast(Optional[str], connector.name),
            icon="",
            connector_type=cast(Optional[str], connector.connector_type),
            organization_id=resolved_org_id,
            project_id=project_id,
            config=config,
            is_managed=connector.is_managed,
            connection_metadata=(
                ConnectionMetadata.model_validate(raw_metadata)
                if isinstance(raw_metadata, dict)
                else None
            ),
            secret_references={
                key: SecretReference.model_validate(value)
                for key, value in (raw_secret_refs or {}).items()
                if isinstance(value, dict)
            }
            if isinstance(raw_secret_refs, dict)
            else {},
            connection_policy=(
                ConnectionPolicy.model_validate(raw_policy)
                if isinstance(raw_policy, dict)
                else None
            ),
            plugin_metadata=plugin_metadata,
        )


class CreateConnectorRequest(_Base):
    name: str
    description: Optional[str] = None
    version: Optional[str] = None
    label: Optional[str] = None
    connector_type: str
    organization_id: UUID
    project_id: Optional[UUID] = None
    config: Optional[Dict[str, Any]] = None
    connection_metadata: ConnectionMetadata | None = None
    secret_references: dict[str, SecretReference] = Field(default_factory=dict)
    connection_policy: ConnectionPolicy | None = None
    is_managed: bool = False


class UpdateConnectorRequest(_Base):
    name: Optional[str] = None
    description: Optional[str] = None
    version: Optional[str] = None
    label: Optional[str] = None
    icon: Optional[str] = None
    connector_type: Optional[str] = None
    organization_id: UUID
    project_id: Optional[UUID] = None
    config: Optional[Dict[str, Any]] = None
    connection_metadata: ConnectionMetadata | None = None
    secret_references: dict[str, SecretReference] | None = None
    connection_policy: ConnectionPolicy | None = None
    is_managed: Optional[bool] = None


class ConnectorListResponse(_Base):
    connectors: list[ConnectorResponse] = []


class ConnectorSourceSchemasResponse(_Base):
    schemas: list[str] = []


class ConnectorSourceSchemaResponse(_Base):
    schema: str
    tables: list[str]


class ConnectorSourceSchemaColumnResponse(_Base):
    name: str
    type: str
    nullable: Optional[bool] = None
    primary_key: Optional[bool] = False


class ConnectorSourceSchemaTableResponse(_Base):
    name: str
    columns: Dict[str, ConnectorSourceSchemaColumnResponse] = {}


class ConnectorSourceSchemaViewResponse(_Base):
    name: str
    columns: Dict[str, ConnectorSourceSchemaColumnResponse] = {}
    definition: str


class ConnectorCatalogSummary(_Base):
    schema_count: int = 0
    table_count: int = 0
    column_count: int = 0


class ConnectorCatalogColumnResponse(_Base):
    name: str
    type: str
    nullable: Optional[bool] = None
    primary_key: Optional[bool] = False


class ConnectorCatalogTableResponse(_Base):
    schema: str
    name: str
    fully_qualified_name: str
    columns: list[ConnectorCatalogColumnResponse] = Field(default_factory=list)


class ConnectorCatalogSchemaResponse(_Base):
    name: str
    tables: list[ConnectorCatalogTableResponse] = Field(default_factory=list)


class ConnectorCatalogResponse(_Base):
    connector_id: UUID
    schemas: list[ConnectorCatalogSchemaResponse] = Field(default_factory=list)
    schema_count: int = 0
    table_count: int = 0
    column_count: int = 0
    offset: int = 0
    limit: int = 200
    has_more: bool = False


class ConnectorSyncRequest(_Base):
    resources: list[str] = Field(default_factory=list)
    sync_mode: ConnectorSyncMode = ConnectorSyncMode.INCREMENTAL
    force_full_refresh: bool = False

    @model_validator(mode="after")
    def _validate_resources(self) -> "ConnectorSyncRequest":
        normalized = [str(resource or "").strip() for resource in self.resources if str(resource or "").strip()]
        if not normalized:
            raise ValueError("At least one resource must be selected for sync.")
        self.resources = normalized
        return self


class ConnectorSyncStartResponse(_Base):
    job_id: UUID
    job_status: str


class ConnectorTestResponse(_Base):
    status: str
    message: str


class ConnectorResourceResponse(_Base):
    name: str
    label: str | None = None
    primary_key: str | None = None
    parent_resource: str | None = None
    cursor_field: str | None = None
    incremental_cursor_field: str | None = None
    supports_incremental: bool = False
    default_sync_mode: ConnectorSyncMode = ConnectorSyncMode.FULL_REFRESH
    status: ConnectorSyncStatus = ConnectorSyncStatus.NEVER_SYNCED
    last_cursor: str | None = None
    last_sync_at: datetime | None = None
    dataset_ids: list[UUID] = Field(default_factory=list)
    dataset_names: list[str] = Field(default_factory=list)
    records_synced: int | None = None


class ConnectorResourceListResponse(_Base):
    connector_id: UUID
    items: list[ConnectorResourceResponse] = Field(default_factory=list)


class ConnectorSyncStateResponse(_Base):
    id: UUID
    workspace_id: UUID
    connection_id: UUID
    connector_type: str
    resource_name: str
    sync_mode: ConnectorSyncMode
    last_cursor: str | None = None
    last_sync_at: datetime | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    status: ConnectorSyncStatus = ConnectorSyncStatus.NEVER_SYNCED
    error_message: str | None = None
    records_synced: int = 0
    bytes_synced: int | None = None
    created_at: datetime
    updated_at: datetime
    dataset_ids: list[UUID] = Field(default_factory=list)


class ConnectorSyncStateListResponse(_Base):
    connection_id: UUID
    items: list[ConnectorSyncStateResponse] = Field(default_factory=list)


class ConnectorSyncHistoryItemResponse(_Base):
    job_id: UUID
    status: str
    progress: int = 0
    status_message: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: dict[str, Any] | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class ConnectorSyncHistoryResponse(_Base):
    connection_id: UUID
    items: list[ConnectorSyncHistoryItemResponse] = Field(default_factory=list)
