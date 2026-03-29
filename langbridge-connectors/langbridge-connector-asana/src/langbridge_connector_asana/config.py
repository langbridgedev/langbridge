
from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorAuthFieldSchema,
    ConnectorConfigEntrySchema,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)
from langbridge.connectors.saas.declarative import (
    build_declarative_config_entries,
    build_declarative_plugin_metadata,
    load_declarative_connector_manifest,
)

_MANIFEST = load_declarative_connector_manifest(
    "langbridge_connector_asana.manifests",
    "asana.yaml",
)


class AsanaDeclarativeConnectorConfig(BaseConnectorConfig):
    access_token: str
    workspace_gid: str
    api_base_url: str | None = None


class AsanaDeclarativeConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.ASANA

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return AsanaDeclarativeConnectorConfig(**config)


class AsanaDeclarativeConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.ASANA

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name=_MANIFEST.display_name,
            description=(
                f"{_MANIFEST.description} The package derives the workspace-scoped base URL "
                "from `workspace_gid` unless `api_base_url` is explicitly overridden."
            ),
            version=_MANIFEST.schema_version,
            config=[
                ConnectorConfigEntrySchema(
                    field="workspace_gid",
                    label="Workspace GID",
                    description="Asana workspace GID used to scope teams, projects, and users.",
                    type="string",
                    required=True,
                ),
                *build_declarative_config_entries(
                    _MANIFEST,
                    token_description="Asana personal access token or OAuth access token.",
                    include_base_url=False,
                ),
                ConnectorConfigEntrySchema(
                    field="api_base_url",
                    label="API Base URL",
                    description=(
                        "Optional Asana API base URL override. Defaults to "
                        "`https://app.asana.com/api/1.0/workspaces/{workspace_gid}`."
                    ),
                    type="string",
                    required=False,
                ),
            ],
            plugin_metadata=build_declarative_plugin_metadata(
                _MANIFEST,
                connector_type=ConnectorRuntimeType.ASANA,
                auth_schema=ASANA_AUTH_SCHEMA,
                sync_strategy=ConnectorSyncStrategy.FULL_REFRESH,
            ),
        )


ASANA_MANIFEST = _MANIFEST
ASANA_SUPPORTED_RESOURCES = _MANIFEST.resource_keys
ASANA_SYNC_STRATEGY = ConnectorSyncStrategy.FULL_REFRESH
ASANA_AUTH_SCHEMA = (
    ConnectorAuthFieldSchema(
        field="workspace_gid",
        label="Workspace GID",
        description="Asana workspace GID used to scope teams, projects, and users.",
        type="string",
        required=True,
        secret=False,
    ),
    ConnectorAuthFieldSchema(
        field="access_token",
        label="Access Token",
        description="Asana personal access token or OAuth access token.",
        type="password",
        required=True,
        secret=True,
    ),
)
