from abc import ABC
from enum import Enum
from typing import Any, List, Optional, Type

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


class ConnectorConfigSchema(_Base):
    name: str
    description: str
    version: str
    label: str
    icon: str
    connector_type: str
    config: List[ConnectorConfigEntrySchema]

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
    subclasses = BaseConnectorConfigFactory.__subclasses__()
    for subclass in subclasses:
        if subclass.type.value == type_s.value:
            return subclass
    raise ValueError(f"No factory found for type: {type_s}")

def get_connector_config_schema_factory(type_s: ConnectorRuntimeType) -> Type[BaseConnectorConfigSchemaFactory]:
    subclasses = BaseConnectorConfigSchemaFactory.__subclasses__()
    for subclass in subclasses:
        if subclass.type.value == type_s.value:
            return subclass
    raise ValueError(f"No schema factory found for type: {type_s}")
