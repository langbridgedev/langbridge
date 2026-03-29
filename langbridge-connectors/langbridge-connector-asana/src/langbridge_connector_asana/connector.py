
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.saas.declarative import DeclarativeHttpApiConnector

from .config import ASANA_MANIFEST, AsanaDeclarativeConnectorConfig


class AsanaDeclarativeApiConnector(DeclarativeHttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.ASANA
    MANIFEST = ASANA_MANIFEST
    config: AsanaDeclarativeConnectorConfig

    def _base_url(self) -> str:
        if self.config.api_base_url:
            return self.config.api_base_url.rstrip("/")
        workspace_gid = self.config.workspace_gid.strip().strip("/")
        return f"https://app.asana.com/api/1.0/workspaces/{workspace_gid}"
