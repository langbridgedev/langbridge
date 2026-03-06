from abc import ABC
from enum import Enum
from typing import Any, List, Optional, Type

from pydantic import Field

from langbridge.packages.common.langbridge_common.contracts.base import _Base

class ConnectorRuntimeType(Enum):
    POSTGRES = "POSTGRES"
    MYSQL = "MYSQL"
    MARIADB = "MARIADB"
    MONGODB = "MONGODB"
    SNOWFLAKE = "SNOWFLAKE"
    REDSHIFT = "REDSHIFT"
    BIGQUERY = "BIGQUERY"
    SQLSERVER = "SQLSERVER"
    ORACLE = "ORACLE"
    SQLITE = "SQLITE"
    FAISS = "FAISS"
    QDRANT = "QDRANT"
    TRINO = "TRINO"
    SHOPIFY = "SHOPIFY"
    STRIPE = "STRIPE"
    HUBSPOT = "HUBSPOT"
    GOOGLE_ANALYTICS = "GOOGLE_ANALYTICS"
    SALESFORCE = "SALESFORCE"


class ConnectorFamily(str, Enum):
    DATABASE = "DATABASE"
    API = "API"
    VECTOR_DB = "VECTOR_DB"


class ConnectorSyncStrategy(str, Enum):
    FULL_REFRESH = "FULL_REFRESH"
    INCREMENTAL = "INCREMENTAL"
    WINDOWED_INCREMENTAL = "WINDOWED_INCREMENTAL"
    MANUAL = "MANUAL"

ConnectorType = ConnectorRuntimeType

class ConnectorConfigEntrySchema(_Base):
    field: str
    value: Optional[Any] = None
    label: Optional[str] = None
    required: bool
    default: Optional[str] = None
    description: str
    type: str
    value_list: Optional[List[str]] = None


class ConnectorAuthFieldSchema(_Base):
    field: str
    label: Optional[str] = None
    required: bool = True
    description: str
    type: str
    secret: bool = False
    default: Optional[str] = None
    value_list: Optional[List[str]] = None


class ConnectorPluginMetadata(_Base):
    connector_type: str
    connector_family: ConnectorFamily
    supported_resources: List[str] = Field(default_factory=list)
    auth_schema: List[ConnectorAuthFieldSchema] = Field(default_factory=list)
    sync_strategy: ConnectorSyncStrategy | None = None


class ConnectorConfigSchema(_Base):
    name: str
    description: str
    version: str
    label: str
    icon: str
    connector_type: str
    config: List[ConnectorConfigEntrySchema]
    plugin_metadata: ConnectorPluginMetadata | None = None

class BaseConnectorConfig(_Base):
    pass

class BaseConnectorConfigFactory(ABC):
    type: ConnectorRuntimeType

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return BaseConnectorConfig(**config)

class BaseConnectorConfigSchemaFactory(ABC):
    type: ConnectorRuntimeType

    @classmethod
    def create(cls, config: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(**config)

def get_connector_config_factory(type_s: ConnectorType) -> Type[BaseConnectorConfigFactory]:
    from .registry import ensure_builtin_plugins_loaded, get_connector_plugin

    ensure_builtin_plugins_loaded()
    plugin = get_connector_plugin(type_s)
    if plugin is not None and plugin.config_factory is not None:
        return plugin.config_factory

    subclasses = BaseConnectorConfigFactory.__subclasses__()
    for subclass in subclasses:
        if subclass.type.value == type_s.value:
            return subclass
    raise ValueError(f"No factory found for type: {type_s}")

def get_connector_config_schema_factory(type_s: ConnectorRuntimeType) -> Type[BaseConnectorConfigSchemaFactory]:
    from .registry import ensure_builtin_plugins_loaded, get_connector_plugin

    ensure_builtin_plugins_loaded()
    plugin = get_connector_plugin(type_s)
    if plugin is not None and plugin.config_schema_factory is not None:
        return plugin.config_schema_factory

    subclasses = BaseConnectorConfigSchemaFactory.__subclasses__()
    for subclass in subclasses:
        if subclass.type.value == type_s.value:
            return subclass
    raise ValueError(f"No schema factory found for type: {type_s}")
