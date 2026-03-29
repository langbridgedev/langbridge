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
    OracleConnectorConfig,
    OracleConnectorConfigFactory,
    OracleConnectorConfigSchemaFactory,
)

register_connector_plugin(
    ConnectorPlugin(
        connector_type=ConnectorRuntimeType.ORACLE,
        connector_family=ConnectorFamily.DATABASE,
        capabilities=ConnectorCapabilities(
            supports_live_datasets=True,
            supports_query_pushdown=True,
            supports_preview=True,
            supports_federated_execution=True,
        ),
        config_factory=OracleConnectorConfigFactory,
        config_schema_factory=OracleConnectorConfigSchemaFactory,
    )
)

_LAZY_EXPORTS = {
    "OracleConnector": ".connector",
}

__all__ = [
    "OracleConnectorConfig",
    "OracleConnectorConfigFactory",
    "OracleConnectorConfigSchemaFactory",
    "OracleConnector",
]


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
