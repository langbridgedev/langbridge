from __future__ import annotations

import json
from typing import Any, Dict, Literal, Optional, cast
from uuid import UUID

from pydantic import Field

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
    catalog_summary: "ConnectorCatalogSummary | None" = None

    @staticmethod
    def from_connector(
        connector: Connector,
        organization_id: Optional[UUID] = None,
        project_id: Optional[UUID] = None,
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
