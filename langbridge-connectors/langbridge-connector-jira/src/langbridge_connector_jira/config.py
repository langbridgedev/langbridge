
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
    "langbridge_connector_jira.manifests",
    "jira.yaml",
)


class JiraDeclarativeConnectorConfig(BaseConnectorConfig):
    access_token: str
    cloud_id: str
    api_base_url: str | None = None


class JiraDeclarativeConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.JIRA

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return JiraDeclarativeConnectorConfig(**config)


class JiraDeclarativeConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.JIRA

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name=_MANIFEST.display_name,
            description=(
                f"{_MANIFEST.description} The package derives the Jira Cloud REST base URL "
                "from `cloud_id` unless `api_base_url` is explicitly overridden."
            ),
            version=_MANIFEST.schema_version,
            config=[
                ConnectorConfigEntrySchema(
                    field="cloud_id",
                    label="Cloud ID",
                    description="Atlassian Cloud ID used to scope the Jira REST proxy base URL.",
                    type="string",
                    required=True,
                ),
                *build_declarative_config_entries(
                    _MANIFEST,
                    token_description="Atlassian OAuth access token for Jira Cloud.",
                    include_base_url=False,
                ),
                ConnectorConfigEntrySchema(
                    field="api_base_url",
                    label="API Base URL",
                    description=(
                        "Optional Jira REST API base URL override. Defaults to "
                        "`https://api.atlassian.com/ex/jira/{cloud_id}/rest/api/3`."
                    ),
                    type="string",
                    required=False,
                ),
            ],
            plugin_metadata=build_declarative_plugin_metadata(
                _MANIFEST,
                connector_type=ConnectorRuntimeType.JIRA,
                auth_schema=JIRA_AUTH_SCHEMA,
                sync_strategy=ConnectorSyncStrategy.FULL_REFRESH,
            ),
        )


JIRA_MANIFEST = _MANIFEST
JIRA_SUPPORTED_RESOURCES = _MANIFEST.resource_keys
JIRA_SYNC_STRATEGY = ConnectorSyncStrategy.FULL_REFRESH
JIRA_AUTH_SCHEMA = (
    ConnectorAuthFieldSchema(
        field="cloud_id",
        label="Cloud ID",
        description="Atlassian Cloud ID used to scope the Jira REST proxy base URL.",
        type="string",
        required=True,
        secret=False,
    ),
    ConnectorAuthFieldSchema(
        field="access_token",
        label="Access Token",
        description="Atlassian OAuth access token for Jira Cloud.",
        type="password",
        required=True,
        secret=True,
    ),
)
