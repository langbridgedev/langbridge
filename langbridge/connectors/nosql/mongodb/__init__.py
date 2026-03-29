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
    MongoDBConnectorConfig,
    MongoDBConnectorConfigFactory,
    MongoDBConnectorConfigSchemaFactory,
)

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.MONGODB,
        connector_family=ConnectorFamily.DATABASE,
        capabilities=ConnectorCapabilities(),
        config_factory=MongoDBConnectorConfigFactory,
        config_schema_factory=MongoDBConnectorConfigSchemaFactory,
    )
)

_LAZY_EXPORTS = {
    "MongoDBConnector": ".connector",
}

__all__ = [
    "MongoDBConnector",
    "MongoDBConnectorConfig",
    "MongoDBConnectorConfigFactory",
    "MongoDBConnectorConfigSchemaFactory",
]


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
