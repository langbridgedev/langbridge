from abc import ABC
from enum import Enum
from typing import Any, List, Optional, Type

from pydantic import BaseModel, ConfigDict, Field

class _Base(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        from_attributes=True,
    )

    def dict_json(self) -> str:
        return self.model_dump_json()


class ConnectorRuntimeType(str, Enum):
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
    SHOPIFY = "SHOPIFY"
    STRIPE = "STRIPE"
    HUBSPOT = "HUBSPOT"
    GITHUB = "GITHUB"
    JIRA = "JIRA"
    ASANA = "ASANA"
    GOOGLE_ANALYTICS = "GOOGLE_ANALYTICS"
    SALESFORCE = "SALESFORCE"
    
    LOCAL_FILESYSTEM = "LOCAL_FILESYSTEM"
    S3 = "S3"
    _S3 = "S3"
    GCS = "GCS"
    AZURE_BLOB = "AZURE_BLOB"


class ConnectorFamily(str, Enum):
    DATABASE = "DATABASE"
    NOSQL = "NOSQL"
    API = "API"
    VECTOR_DB = "VECTOR_DB"
    STORAGE = "STORAGE"

class ConnectorSyncStrategy(str, Enum):
    FULL_REFRESH = "FULL_REFRESH"
    INCREMENTAL = "INCREMENTAL"
    WINDOWED_INCREMENTAL = "WINDOWED_INCREMENTAL"
    MANUAL = "MANUAL"


class ConnectorCapabilities(_Base):
    supports_live_datasets: bool = False
    supports_synced_datasets: bool = False
    supports_incremental_sync: bool = False
    supports_query_pushdown: bool = False
    supports_preview: bool = False
    supports_federated_execution: bool = False

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
    connector_type: ConnectorRuntimeType
    connector_family: ConnectorFamily
    supported_resources: List[str] = Field(default_factory=list)
    auth_schema: List[ConnectorAuthFieldSchema] = Field(default_factory=list)
    default_sync_strategy: ConnectorSyncStrategy | None = None
    capabilities: ConnectorCapabilities = Field(default_factory=ConnectorCapabilities)


class ConnectorConfigSchema(_Base):
    name: str
    description: str
    version: str
    config: List[ConnectorConfigEntrySchema]
    plugin_metadata: ConnectorPluginMetadata | None = None

class BaseConnectorConfig(_Base):
    pass

class BaseConnectorConfigFactory(ABC):
    type: ConnectorRuntimeType

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return BaseConnectorConfig(**config)
    
    @classmethod
    def get_metadata_keys(cls) -> List[str]:
        return []

class BaseConnectorConfigSchemaFactory(ABC):
    type: ConnectorRuntimeType

    @classmethod
    def create(cls, config: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(**config)
