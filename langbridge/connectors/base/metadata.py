import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Type

from .errors import ConnectorTypeError

from .config import (
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    ConnectorRuntimeType,
)
from langbridge.plugins import get_connector_config_factory

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False

@dataclass
class ForeignKeyMetadata:
    name: str
    column: str
    foreign_key: str
    schema: str
    table: str

@dataclass
class TableMetadata:
    schema: str
    name: str
    columns: List[ColumnMetadata] | None = None

@dataclass
class SchemaMetadata:
    name: str

class BaseMetadataExtractor(ABC):
    type: ConnectorRuntimeType

    @abstractmethod
    def fetch_schemas(self, 
                      config: BaseConnectorConfig) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def fetch_tables(self, 
                     config: BaseConnectorConfig) -> List[str]:
        raise NotImplementedError
    
    @abstractmethod
    def fetch_columns(self, 
                      config: BaseConnectorConfig) -> List[ColumnMetadata]:
        raise NotImplementedError

    @abstractmethod
    def fetch_metadata(self, 
                       config: BaseConnectorConfig) -> List[TableMetadata]: #Full extraction
        raise NotImplementedError


def get_metadata_extractor(connector_type: ConnectorRuntimeType) -> BaseMetadataExtractor:
    for subclass in BaseMetadataExtractor.__subclasses__():
        if subclass.type == connector_type:
            return subclass()
    raise ConnectorTypeError(f"No metadata extractor found for connector type '{connector_type.value}'.")

def build_connector_config(connector_type: ConnectorRuntimeType, config_payload: dict) -> BaseConnectorConfig:
    factory: Type[BaseConnectorConfigFactory] = get_connector_config_factory(connector_type)
    return factory.create(config_payload)
