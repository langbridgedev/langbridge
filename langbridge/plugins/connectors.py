"""
Connector registry responsible for managing available connectors.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from importlib.metadata import entry_points
from logging import Logger, getLogger
from pathlib import Path
import sys
from typing import TYPE_CHECKING, Type

from langbridge.connectors.base.config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorAuthFieldSchema,
    ConnectorFamily,
    ConnectorRuntimeType,
    ConnectorSyncStrategy,
)

if TYPE_CHECKING:
    from langbridge.connectors.base.connector import (
        ApiConnector,
        Connector,
        ManagedVectorDB,
        NoSqlConnector,
        SqlConnector,
        VecotorDBConnector,
    )

_BUILTIN_PLUGIN_MODULES = (
    "langbridge.connectors.builtin.postgres",
    "langbridge.connectors.builtin.mysql",
    "langbridge.connectors.builtin.sqlite",
    "langbridge.connectors.nosql.mongodb",
    "langbridge.connectors.sql.bigquery",
    "langbridge.connectors.sql.mariadb",
    "langbridge.connectors.sql.oracle",
    "langbridge.connectors.sql.redshift",
    "langbridge.connectors.sql.sqlserver",
    "langbridge.connectors.vector.faiss",
    "langbridge.connectors.vector.qdrant",
    "langbridge.connectors.saas.shopify",
    "langbridge.connectors.saas.hubspot",
    "langbridge.connectors.saas.google_analytics",
    "langbridge.connectors.saas.salesforce",
    "langbridge_connector_stripe.plugin",
)
_BUILTIN_CONNECTOR_MODULES = (
    "langbridge.connectors.builtin.postgres.connector",
    "langbridge.connectors.builtin.mysql.connector",
    "langbridge.connectors.builtin.sqlite.connector",
    "langbridge.connectors.nosql.mongodb.connector",
    "langbridge.connectors.sql.bigquery.connector",
    "langbridge.connectors.sql.mariadb.connector",
    "langbridge.connectors.sql.oracle.connector",
    "langbridge.connectors.sql.redshift.connector",
    "langbridge.connectors.sql.sqlserver.connector",
    "langbridge.connectors.vector.faiss.connector",
    "langbridge.connectors.vector.qdrant.connector",
    *_BUILTIN_PLUGIN_MODULES,
)

_builtin_plugins_loaded = False
_builtin_connectors_loaded = False
_entrypoint_plugins_loaded = False
logger = getLogger(__name__)


def _ensure_repo_connector_src_paths() -> None:
    repo_connectors_dir = Path(__file__).resolve().parents[2] / "langbridge-connectors"
    if not repo_connectors_dir.exists():
        return
    for src_dir in sorted(repo_connectors_dir.glob("*/src")):
        src_path = str(src_dir.resolve())
        if src_path not in sys.path:
            sys.path.insert(0, src_path)


def ensure_builtin_plugins_loaded() -> None:
    global _builtin_plugins_loaded

    if _builtin_plugins_loaded:
        return

    _ensure_repo_connector_src_paths()

    for module_path in _BUILTIN_PLUGIN_MODULES:
        try:
            import_module(module_path)
        except Exception as exc:
            logger.warning("Skipping connector plugin module %s: %s", module_path, exc)

    _builtin_plugins_loaded = True


def ensure_builtin_connectors_loaded() -> None:
    global _builtin_connectors_loaded

    if _builtin_connectors_loaded:
        return

    _ensure_repo_connector_src_paths()

    for module_path in _BUILTIN_CONNECTOR_MODULES:
        try:
            import_module(module_path)
        except Exception as exc:
            logger.warning("Skipping connector module %s: %s", module_path, exc)

    _builtin_connectors_loaded = True


def ensure_entrypoint_plugins_loaded() -> None:
    global _entrypoint_plugins_loaded

    if _entrypoint_plugins_loaded:
        return

    _plugin_registry.load_entrypoints()
    _entrypoint_plugins_loaded = True


@dataclass(frozen=True, slots=True)
class ConnectorPlugin:
    connector_type: ConnectorRuntimeType
    connector_family: ConnectorFamily
    supported_resources: tuple[str, ...] = ()
    auth_schema: tuple[ConnectorAuthFieldSchema, ...] = ()
    sync_strategy: ConnectorSyncStrategy | None = None
    config_factory: Type[BaseConnectorConfigFactory] | None = None
    config_schema_factory: Type[BaseConnectorConfigSchemaFactory] | None = None
    api_connector_class: Type[ApiConnector] | None = None


class ConnectorPluginRegistry:
    def __init__(self) -> None:
        self._plugins: dict[ConnectorRuntimeType, ConnectorPlugin] = {}

    def register(self, plugin: ConnectorPlugin) -> ConnectorPlugin:
        self._plugins[plugin.connector_type] = plugin
        return plugin

    def get(self, connector_type: ConnectorRuntimeType) -> ConnectorPlugin | None:
        return self._plugins.get(connector_type)

    def list(self) -> list[ConnectorPlugin]:
        return list(self._plugins.values())

    def load_entrypoints(self, group: str = "langbridge.connectors") -> None:
        for ep in entry_points(group=group):
            obj = ep.load()

            if isinstance(obj, ConnectorPlugin):
                self.register(obj)
                continue

            if callable(obj):
                plugin = obj()
                if not isinstance(plugin, ConnectorPlugin):
                    raise TypeError(
                        f"Entry point '{ep.name}' returned an invalid plugin: {plugin!r}"
                    )
                self.register(plugin)
                continue

            if not isinstance(obj, type):
                raise TypeError(
                    f"Entry point '{ep.name}' did not load a supported plugin object: {obj!r}"
                )

            if not issubclass(obj, ConnectorPlugin):
                raise TypeError(
                    f"Entry point '{ep.name}' must load a ConnectorPlugin subclass"
                )

            self.register(obj())  # type: ignore[call-arg]


_plugin_registry = ConnectorPluginRegistry()


def register_connector_plugin(plugin: ConnectorPlugin) -> ConnectorPlugin:
    return _plugin_registry.register(plugin)


def get_connector_plugin(connector_type: ConnectorRuntimeType) -> ConnectorPlugin | None:
    ensure_builtin_plugins_loaded()
    ensure_entrypoint_plugins_loaded()
    return _plugin_registry.get(connector_type)


def list_connector_plugins() -> list[ConnectorPlugin]:
    ensure_builtin_plugins_loaded()
    ensure_entrypoint_plugins_loaded()
    return _plugin_registry.list()

def get_connector_config_factory(type_s: ConnectorRuntimeType) -> Type[BaseConnectorConfigFactory]:
    ensure_builtin_connectors_loaded()
    plugin = get_connector_plugin(type_s)
    if plugin is not None and plugin.config_factory is not None:
        return plugin.config_factory

    subclasses = BaseConnectorConfigFactory.__subclasses__()
    for subclass in subclasses:
        if subclass.type.value == type_s.value:
            return subclass
    raise ValueError(f"No factory found for type: {type_s}")

def get_connector_config_schema_factory(type_s: ConnectorRuntimeType) -> Type[BaseConnectorConfigSchemaFactory]:
    ensure_builtin_connectors_loaded()
    plugin = get_connector_plugin(type_s)
    if plugin is not None and plugin.config_schema_factory is not None:
        return plugin.config_schema_factory

    subclasses = BaseConnectorConfigSchemaFactory.__subclasses__()
    for subclass in subclasses:
        if subclass.type.value == type_s.value:
            return subclass
    raise ValueError(f"No schema factory found for type: {type_s}")


class SqlConnectorFactory:
    """Factory for creating connectors."""

    def __init__(self) -> None:
        pass

    @staticmethod
    def get_sql_connector_class_reference(
        connector_type: ConnectorRuntimeType,
    ) -> Type[SqlConnector]:
        from langbridge.connectors.base.connector import SqlConnector

        ensure_builtin_connectors_loaded()
        subclasses = SqlConnector.__subclasses__()
        for subclass in subclasses:
            if getattr(subclass, "RUNTIME_TYPE", None) == connector_type:
                return subclass
        raise ValueError(f"No SQL connector found for runtime type: {connector_type}")

    @staticmethod
    def create_sql_connector(
        connector_type: ConnectorRuntimeType,
        config: BaseConnectorConfig,
        logger: Logger,
    ) -> SqlConnector:
        connector_class = SqlConnectorFactory.get_sql_connector_class_reference(
            connector_type
        )
        return connector_class(config=config, logger=logger)

    @staticmethod
    def get_sqlglot_dialect(connector_type: ConnectorRuntimeType) -> str:
        connector_class = SqlConnectorFactory.get_sql_connector_class_reference(
            connector_type
        )
        return str(getattr(connector_class, "SQLGLOT_DIALECT", "tsql"))


class ApiConnectorFactory:
    """Factory for creating API connectors from the plugin registry."""

    @staticmethod
    def get_api_connector_class_reference(
        connector_type: ConnectorRuntimeType,
    ) -> Type[ApiConnector]:
        from langbridge.connectors.base.connector import ApiConnector

        plugin = get_connector_plugin(connector_type)
        if plugin is not None and plugin.api_connector_class is not None:
            return plugin.api_connector_class

        for subclass in ApiConnector.__subclasses__():
            if getattr(subclass, "RUNTIME_TYPE", None) == connector_type:
                return subclass

        raise ValueError(f"No API connector found for runtime type: {connector_type}")

    @staticmethod
    def create_api_connector(
        connector_type: ConnectorRuntimeType,
        config: BaseConnectorConfig,
        logger: Logger,
    ) -> ApiConnector:
        connector_class = ApiConnectorFactory.get_api_connector_class_reference(
            connector_type
        )
        return connector_class(config=config, logger=logger)


class NoSqlConnectorFactory:
    """Factory for creating document database connectors."""

    @staticmethod
    def get_no_sql_connector_class_reference(
        connector_type: ConnectorRuntimeType,
    ) -> Type[NoSqlConnector]:
        from langbridge.connectors.base.connector import NoSqlConnector

        ensure_builtin_connectors_loaded()
        subclasses = NoSqlConnector.__subclasses__()
        for subclass in subclasses:
            if getattr(subclass, "RUNTIME_TYPE", None) == connector_type:
                return subclass
        raise ValueError(f"No no-sql connector found for runtime type: {connector_type}")

    @staticmethod
    def create_no_sql_connector(
        connector_type: ConnectorRuntimeType,
        config: BaseConnectorConfig,
        logger: Logger,
    ) -> NoSqlConnector:
        connector_class = NoSqlConnectorFactory.get_no_sql_connector_class_reference(
            connector_type
        )
        return connector_class(config=config, logger=logger)


class VectorDBConnectorFactory:
    """Factory for creating vector database connectors."""

    @staticmethod
    def get_vector_connector_class_reference(
        connector_type: ConnectorRuntimeType,
    ) -> Type[VecotorDBConnector]:
        from langbridge.connectors.base.connector import VecotorDBConnector

        ensure_builtin_connectors_loaded()
        subclasses = VecotorDBConnector.__subclasses__()
        for subclass in subclasses:
            if getattr(subclass, "RUNTIME_TYPE", None) == connector_type:
                return subclass
        raise ValueError(f"No vector connector found for runtime type: {connector_type}")

    @staticmethod
    def get_managed_vector_db_class_reference(
        connector_type: ConnectorRuntimeType,
    ) -> Type[ManagedVectorDB]:
        from langbridge.connectors.base.connector import ManagedVectorDB

        ensure_builtin_connectors_loaded()
        subclasses = ManagedVectorDB.__subclasses__()
        for subclass in subclasses:
            if getattr(subclass, "RUNTIME_TYPE", None) == connector_type:
                return subclass
        raise ValueError(
            f"No managed vector DB found for runtime type: {connector_type}"
        )

    @staticmethod
    def get_all_managed_vector_dbs() -> list[ConnectorRuntimeType]:
        from langbridge.connectors.base.connector import ManagedVectorDB

        ensure_builtin_connectors_loaded()
        managed_vector_dbs: list[ConnectorRuntimeType] = []
        subclasses = ManagedVectorDB.__subclasses__()
        for subclass in subclasses:
            runtime_type = getattr(subclass, "RUNTIME_TYPE", None)
            if runtime_type is not None:
                managed_vector_dbs.append(runtime_type)
        return managed_vector_dbs

    @staticmethod
    def create_vector_connector(
        connector_type: ConnectorRuntimeType,
        config: BaseConnectorConfig,
        logger: Logger,
    ) -> VecotorDBConnector:
        connector_class = VectorDBConnectorFactory.get_vector_connector_class_reference(
            connector_type
        )
        return connector_class(config=config, logger=logger)


class ConnectorInstanceRegistry:
    """Registry for managing connector instances."""

    def __init__(self) -> None:
        self._connectors: dict[str, Connector] = {}

    def add(self, connector: Connector, name: str) -> None:
        self._connectors[name] = connector

    def get(self, name: str) -> Connector:
        return self._connectors[name]

    def delete(self, name: str) -> None:
        del self._connectors[name]


__all__ = [
    "ApiConnectorFactory",
    "ConnectorInstanceRegistry",
    "ConnectorPlugin",
    "ConnectorPluginRegistry",
    "NoSqlConnectorFactory",
    "SqlConnectorFactory",
    "VectorDBConnectorFactory",
    "ensure_builtin_connectors_loaded",
    "ensure_entrypoint_plugins_loaded",
    "ensure_builtin_plugins_loaded",
    "get_connector_plugin",
    "list_connector_plugins",
    "register_connector_plugin",
]
