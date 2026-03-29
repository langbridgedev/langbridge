from importlib import import_module
from typing import Any

from langbridge.plugins import (
    ConnectorCapabilities,
    ConnectorFamily,
    ConnectorPlugin,
    ConnectorRuntimeType,
    register_connector_plugin,
)

from .config import (
    BigQueryConnectorConfig,
    BigQueryConnectorConfigFactory,
    BigQueryConnectorConfigSchemaFactory,
)

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.BIGQUERY,
        connector_family=ConnectorFamily.DATABASE,
        capabilities=ConnectorCapabilities(
            supports_live_datasets=True,
            supports_query_pushdown=True,
            supports_preview=True,
            supports_federated_execution=True,
        ),
        config_factory=BigQueryConnectorConfigFactory,
        config_schema_factory=BigQueryConnectorConfigSchemaFactory,
    )
)

_LAZY_EXPORTS = {
    "BigQueryConnector": ".connector",
}

__all__ = [
    "BigQueryConnectorConfig",
    "BigQueryConnectorConfigFactory",
    "BigQueryConnectorConfigSchemaFactory",
    "BigQueryConnector",
]


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
