"""Core plugin interfaces for the Langbridge runtime monolith.

Official connector implementations stay in the separate ``langbridge-connectors``
package and register themselves through this core surface.
"""

from importlib import import_module
from typing import Any

_EXPORTS = {
    "langbridge.connectors.base.config": (
        "BaseConnectorConfig",
        "BaseConnectorConfigFactory",
        "BaseConnectorConfigSchemaFactory",
        "ConnectorAuthFieldSchema",
        "ConnectorConfigEntrySchema",
        "ConnectorConfigSchema",
        "ConnectorFamily",
        "ConnectorPluginMetadata",
        "ConnectorRuntimeType",
        "ConnectorSyncStrategy",
    ),
    "langbridge.connectors.base.connector": (
        "ApiConnector",
        "ApiExtractResult",
        "ApiResource",
        "ApiSyncResult",
        "AuthError",
        "Connector",
        "ConnectorError",
        "ManagedVectorDB",
        "NoSqlConnector",
        "NoSqlQueryResult",
        "QueryResult",
        "SqlConnector",
        "VecotorDBConnector",
        "run_sync",
    ),
    "langbridge.plugins.connectors": (
        "ApiConnectorFactory",
        "ConnectorInstanceRegistry",
        "ConnectorPlugin",
        "ConnectorPluginRegistry",
        "NoSqlConnectorFactory",
        "SqlConnectorFactory",
        "VectorDBConnectorFactory",
        "ensure_builtin_connectors_loaded",
        "ensure_builtin_plugins_loaded",
        "get_connector_config_factory",
        "get_connector_config_schema_factory",
        "get_connector_plugin",
        "list_connector_plugins",
        "register_connector_plugin",
    ),
}

__all__ = [name for names in _EXPORTS.values() for name in names]


def __getattr__(name: str) -> Any:
    for module_name, exports in _EXPORTS.items():
        if name not in exports:
            continue
        module = import_module(module_name)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
