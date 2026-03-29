
from importlib import import_module
from typing import Any

_EXPORTS = {
    "langbridge.connectors.base.config": (
        "BaseConnectorConfig",
        "BaseConnectorConfigFactory",
        "BaseConnectorConfigSchemaFactory",
        "ConnectorAuthFieldSchema",
        "ConnectorCapabilities",
        "ConnectorConfigEntrySchema",
        "ConnectorConfigSchema",
        "ConnectorFamily",
        "ConnectorPluginMetadata",
        "ConnectorRuntimeType",
        "ConnectorSyncStrategy",
    ),
    "langbridge.connectors.base.metadata": (
        "BaseMetadataExtractor",
        "ColumnMetadata",
        "ForeignKeyMetadata",
        "TableMetadata",
        "build_connector_config",
        "get_metadata_extractor",
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
        "ConnectorError",
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
    "langbridge.connectors.base.http": (
        "ApiResourceDefinition",
        "HttpApiConnector",
        "flatten_api_records",
        "parse_link_header_cursor",
    ),
}

_MODULE_EXPORTS = {
    "http": "langbridge.connectors.base.http",
}

__all__ = [name for names in _EXPORTS.values() for name in names] + list(
    _MODULE_EXPORTS
)


def __getattr__(name: str) -> Any:
    module_name = _MODULE_EXPORTS.get(name)
    if module_name is not None:
        module = import_module(module_name)
        globals()[name] = module
        return module

    for module_name, exports in _EXPORTS.items():
        if name not in exports:
            continue
        module = import_module(module_name)
        value = getattr(module, name)
        globals()[name] = value
        return value

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
