
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.saas.declarative import DeclarativeHttpApiConnector

from .config import GITHUB_MANIFEST, GitHubDeclarativeConnectorConfig


class GitHubDeclarativeApiConnector(DeclarativeHttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.GITHUB
    MANIFEST = GITHUB_MANIFEST
    config: GitHubDeclarativeConnectorConfig
