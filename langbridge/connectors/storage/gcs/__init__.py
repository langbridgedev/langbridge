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
    GcsStorageConnectorConfig,
    GcsStorageConnectorConfigFactory,
    GcsStorageConnectorConfigSchemaFactory,
)

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.GCS,
        connector_family=ConnectorFamily.STORAGE,
        capabilities=ConnectorCapabilities(),
        config_factory=GcsStorageConnectorConfigFactory,
        config_schema_factory=GcsStorageConnectorConfigSchemaFactory,
    )
)

_LAZY_EXPORTS = {
    "GcsStorageConnector": ".connector",
}

__all__ = [
    "GcsStorageConnector",
    "GcsStorageConnectorConfig",
    "GcsStorageConnectorConfigFactory",
    "GcsStorageConnectorConfigSchemaFactory",
]


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
