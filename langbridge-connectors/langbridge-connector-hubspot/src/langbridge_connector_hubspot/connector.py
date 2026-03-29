
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.saas.declarative import DeclarativeHttpApiConnector

from .config import HUBSPOT_MANIFEST, HubSpotDeclarativeConnectorConfig


class HubSpotDeclarativeApiConnector(DeclarativeHttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.HUBSPOT
    MANIFEST = HUBSPOT_MANIFEST
    config: HubSpotDeclarativeConnectorConfig
