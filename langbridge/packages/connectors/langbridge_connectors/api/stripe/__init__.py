from langbridge.plugins import (
    ConnectorPlugin,
    ConnectorFamily,
    ConnectorRuntimeType,
    register_connector_plugin,
)

from .config import (
    STRIPE_AUTH_SCHEMA,
    STRIPE_SUPPORTED_RESOURCES,
    STRIPE_SYNC_STRATEGY,
    StripeConnectorConfig,
    StripeConnectorConfigFactory,
    StripeConnectorConfigSchemaFactory,
)
from .connector import StripeApiConnector

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.STRIPE,
        connector_family=ConnectorFamily.API,
        supported_resources=STRIPE_SUPPORTED_RESOURCES,
        auth_schema=STRIPE_AUTH_SCHEMA,
        sync_strategy=STRIPE_SYNC_STRATEGY,
        config_factory=StripeConnectorConfigFactory,
        config_schema_factory=StripeConnectorConfigSchemaFactory,
        api_connector_class=StripeApiConnector,
    )
)

__all__ = [
    "StripeApiConnector",
    "StripeConnectorConfig",
    "StripeConnectorConfigFactory",
    "StripeConnectorConfigSchemaFactory",
]
