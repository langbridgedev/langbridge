
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.saas.declarative import DeclarativeHttpApiConnector

from .config import JIRA_MANIFEST, JiraDeclarativeConnectorConfig


class JiraDeclarativeApiConnector(DeclarativeHttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.JIRA
    MANIFEST = JIRA_MANIFEST
    config: JiraDeclarativeConnectorConfig

    def _base_url(self) -> str:
        if self.config.api_base_url:
            return self.config.api_base_url.rstrip("/")
        cloud_id = self.config.cloud_id.strip().strip("/")
        return f"https://api.atlassian.com/ex/jira/{cloud_id}/rest/api/3"
