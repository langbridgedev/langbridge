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
    LocalStorageConnectorConfig,
    LocalStorageConnectorConfigFactory,
    LocalStorageConnectorConfigSchemaFactory
)

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.LOCAL_FILESYSTEM,
        connector_family=ConnectorFamily.STORAGE,
        capabilities=ConnectorCapabilities(),
        config_factory=LocalStorageConnectorConfigFactory,
        config_schema_factory=LocalStorageConnectorConfigSchemaFactory,
    )
)

_LAZY_EXPORTS = {
    "LocalStorageConnector": ".connector",
}

__all__ = [
    "LocalStorageConnector",
    "LocalStorageConnectorConfig",
    "LocalStorageConnectorConfigFactory",
    "LocalStorageConnectorConfigSchemaFactory"
]


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
