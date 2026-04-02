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
    S3StorageConnectorConfig,
    S3StorageConnectorConfigFactory,
    S3StorageConnectorConfigSchemaFactory,
)

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.S3,
        connector_family=ConnectorFamily.STORAGE,
        capabilities=ConnectorCapabilities(),
        config_factory=S3StorageConnectorConfigFactory,
        config_schema_factory=S3StorageConnectorConfigSchemaFactory,
    )
)

_LAZY_EXPORTS = {
    "S3StorageConnector": ".connector",
}

__all__ = [
    "S3StorageConnector",
    "S3StorageConnectorConfig",
    "S3StorageConnectorConfigFactory",
    "S3StorageConnectorConfigSchemaFactory",
]


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
