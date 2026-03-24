from __future__ import annotations

from langbridge.connectors.base.config import (
    ConnectorFamily,
    ConnectorRuntimeType,
)
from langbridge.plugins import ConnectorPlugin, register_connector_plugin

from .config import (
    STRIPE_AUTH_SCHEMA,
    STRIPE_SUPPORTED_RESOURCES,
    STRIPE_SYNC_STRATEGY,
    StripeDeclarativeConnectorConfigFactory,
    StripeDeclarativeConnectorConfigSchemaFactory,
)
from .connector import StripeDeclarativeApiConnector

PLUGIN = register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.STRIPE,
        connector_family=ConnectorFamily.API,
        supported_resources=STRIPE_SUPPORTED_RESOURCES,
        auth_schema=STRIPE_AUTH_SCHEMA,
        sync_strategy=STRIPE_SYNC_STRATEGY,
        config_factory=StripeDeclarativeConnectorConfigFactory,
        config_schema_factory=StripeDeclarativeConnectorConfigSchemaFactory,
        api_connector_class=StripeDeclarativeApiConnector,
    )
)


def get_connector_plugin() -> ConnectorPlugin:
    return PLUGIN


def register_plugin() -> ConnectorPlugin:
    return PLUGIN
