
from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigSchema,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)
from langbridge.connectors.saas.declarative import (
    build_declarative_auth_schema,
    build_declarative_connector_config_schema,
    load_declarative_connector_manifest,
)

_MANIFEST = load_declarative_connector_manifest(
    "langbridge_connector_github.manifests",
    "github.yaml",
)


class GitHubDeclarativeConnectorConfig(BaseConnectorConfig):
    access_token: str
    api_base_url: str = "https://api.github.com"


class GitHubDeclarativeConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.GITHUB

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return GitHubDeclarativeConnectorConfig(**config)


class GitHubDeclarativeConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.GITHUB

    @classmethod
    def create(cls, _: dict) -> ConnectorConfigSchema:
        return build_declarative_connector_config_schema(
            _MANIFEST,
            connector_type=ConnectorRuntimeType.GITHUB,
            sync_strategy=ConnectorSyncStrategy.INCREMENTAL,
            auth_schema=GITHUB_AUTH_SCHEMA,
            description_suffix=(
                "Manifest-defined GitHub REST resources execute through the core declarative "
                "HTTP SaaS runtime."
            ),
            token_description="GitHub personal access token or GitHub App user token.",
            base_url_description="Optional GitHub REST API base URL override.",
        )


GITHUB_MANIFEST = _MANIFEST
GITHUB_SUPPORTED_RESOURCES = _MANIFEST.resource_keys
GITHUB_SYNC_STRATEGY = ConnectorSyncStrategy.INCREMENTAL
GITHUB_AUTH_SCHEMA = build_declarative_auth_schema(
    _MANIFEST,
    token_description="GitHub access token.",
)
