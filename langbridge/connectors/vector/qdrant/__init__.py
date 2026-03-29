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
    QdrantConnectorConfig,
    QdrantConnectorConfigFactory,
    QdrantConnectorConfigSchemaFactory,
)

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.QDRANT,
        connector_family=ConnectorFamily.VECTOR_DB,
        capabilities=ConnectorCapabilities(),
        config_factory=QdrantConnectorConfigFactory,
        config_schema_factory=QdrantConnectorConfigSchemaFactory,
    )
)

_LAZY_EXPORTS = {
    "QdrantConnector": ".connector",
}

__all__ = [
    "QdrantConnector",
    "QdrantConnectorConfig",
    "QdrantConnectorConfigFactory",
    "QdrantConnectorConfigSchemaFactory",
]


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
