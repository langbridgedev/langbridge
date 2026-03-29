
from typing import Mapping, Sequence

from langbridge.connectors.base.config import (
    ConnectorAuthFieldSchema,
    ConnectorCapabilities,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorFamily,
    ConnectorPluginMetadata,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)

from .manifest import DeclarativeConnectorManifest


def build_declarative_auth_schema(
    manifest: DeclarativeConnectorManifest,
    *,
    token_description: str | None = None,
) -> tuple[ConnectorAuthFieldSchema, ...]:
    fields = [
        ConnectorAuthFieldSchema(
            field=manifest.auth.token_field,
            label=manifest.auth.token_label,
            description=token_description
            or f"{manifest.display_name} API token used for {manifest.auth.strategy} authentication.",
            type="password",
            required=True,
            secret=True,
        )
    ]
    for header in manifest.auth.optional_headers:
        fields.append(
            ConnectorAuthFieldSchema(
                field=header.field,
                label=header.header_name,
                description=header.description
                or f"Optional value for the {header.header_name} header.",
                type="string",
                required=header.required,
                secret=False,
            )
        )
    return tuple(fields)


def build_declarative_config_entries(
    manifest: DeclarativeConnectorManifest,
    *,
    token_description: str | None = None,
    field_labels: Mapping[str, str] | None = None,
    field_descriptions: Mapping[str, str] | None = None,
    include_base_url: bool = True,
    base_url_field: str = "api_base_url",
    base_url_label: str = "API Base URL",
    base_url_description: str = "Optional API base URL override.",
) -> tuple[ConnectorConfigEntrySchema, ...]:
    resolved_labels = dict(field_labels or {})
    resolved_descriptions = dict(field_descriptions or {})
    entries = [
        ConnectorConfigEntrySchema(
            field=manifest.auth.token_field,
            label=resolved_labels.get(manifest.auth.token_field, manifest.auth.token_label),
            description=resolved_descriptions.get(
                manifest.auth.token_field,
                token_description
                or f"{manifest.display_name} API token used for {manifest.auth.strategy} authentication.",
            ),
            type="password",
            required=True,
        )
    ]
    for header in manifest.auth.optional_headers:
        entries.append(
            ConnectorConfigEntrySchema(
                field=header.field,
                label=resolved_labels.get(header.field, header.header_name),
                description=resolved_descriptions.get(
                    header.field,
                    header.description or f"Optional value for the {header.header_name} header.",
                ),
                type="string",
                required=header.required,
            )
        )
    if include_base_url:
        entries.append(
            ConnectorConfigEntrySchema(
                field=base_url_field,
                label=base_url_label,
                description=base_url_description,
                type="string",
                required=False,
                default=manifest.base_url,
            )
        )
    return tuple(entries)


def build_declarative_plugin_metadata(
    manifest: DeclarativeConnectorManifest,
    *,
    connector_type: ConnectorRuntimeType | None = None,
    connector_family: ConnectorFamily | None = None,
    auth_schema: Sequence[ConnectorAuthFieldSchema] | None = None,
    sync_strategy: ConnectorSyncStrategy | None = None,
) -> ConnectorPluginMetadata:
    resolved_auth_schema = tuple(auth_schema or build_declarative_auth_schema(manifest))
    resolved_connector_type = connector_type or ConnectorRuntimeType(manifest.connector_type)
    resolved_connector_family = connector_family or ConnectorFamily(manifest.connector_family)
    return ConnectorPluginMetadata(
        connector_type=resolved_connector_type.value,
        connector_family=resolved_connector_family,
        supported_resources=list(manifest.resource_keys),
        auth_schema=list(resolved_auth_schema),
        sync_strategy=sync_strategy,
        capabilities=ConnectorCapabilities(
            supports_live_datasets=False,
            supports_synced_datasets=True,
            supports_incremental_sync=sync_strategy in {
                ConnectorSyncStrategy.INCREMENTAL,
                ConnectorSyncStrategy.WINDOWED_INCREMENTAL,
            },
        ),
    )


def build_declarative_connector_config_schema(
    manifest: DeclarativeConnectorManifest,
    *,
    connector_type: ConnectorRuntimeType | None = None,
    connector_family: ConnectorFamily | None = None,
    auth_schema: Sequence[ConnectorAuthFieldSchema] | None = None,
    sync_strategy: ConnectorSyncStrategy | None = None,
    description_suffix: str | None = None,
    token_description: str | None = None,
    field_labels: Mapping[str, str] | None = None,
    field_descriptions: Mapping[str, str] | None = None,
    include_base_url: bool = True,
    base_url_field: str = "api_base_url",
    base_url_label: str = "API Base URL",
    base_url_description: str = "Optional API base URL override.",
) -> ConnectorConfigSchema:
    resolved_auth_schema = tuple(auth_schema or build_declarative_auth_schema(manifest))
    description = manifest.description
    if description_suffix:
        description = f"{description} {description_suffix}"
    return ConnectorConfigSchema(
        name=manifest.display_name,
        description=description,
        version=manifest.schema_version,
        config=list(
            build_declarative_config_entries(
                manifest,
                token_description=token_description,
                field_labels=field_labels,
                field_descriptions=field_descriptions,
                include_base_url=include_base_url,
                base_url_field=base_url_field,
                base_url_label=base_url_label,
                base_url_description=base_url_description,
            )
        ),
        plugin_metadata=build_declarative_plugin_metadata(
            manifest,
            connector_type=connector_type,
            connector_family=connector_family,
            auth_schema=resolved_auth_schema,
            sync_strategy=sync_strategy,
        ),
    )
