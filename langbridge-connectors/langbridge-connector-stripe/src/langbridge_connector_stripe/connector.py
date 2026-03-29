
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.saas.declarative import DeclarativeHttpApiConnector

from .config import STRIPE_MANIFEST, StripeDeclarativeConnectorConfig


class StripeDeclarativeApiConnector(DeclarativeHttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.STRIPE
    MANIFEST = STRIPE_MANIFEST
    config: StripeDeclarativeConnectorConfig
