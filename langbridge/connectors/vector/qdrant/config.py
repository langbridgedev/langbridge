
from typing import Optional

from langbridge.connectors.base.config import (
    BaseConnectorConfigSchemaFactory,
    BaseConnectorConfigFactory,
    ConnectorConfigSchema,
    ConnectorConfigEntrySchema,
    BaseConnectorConfig,
    ConnectorRuntimeType,
)


def _parse_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


class QdrantConnectorConfig(BaseConnectorConfig):
    host: str
    port: int = 6333
    collection: str
    api_key: Optional[str] = None
    https: bool = False

    @classmethod
    def create_from_dict(cls, data: dict) -> "QdrantConnectorConfig":
        host = data.get("host")
        collection = data.get("collection")
        if not host or not collection:
            raise ValueError("'host' and 'collection' must be provided for Qdrant.")
        port = int(data.get("port", 6333))
        api_key = data.get("api_key")
        https = _parse_bool(data.get("https", False))
        return cls(
            host=host,
            port=port,
            collection=collection,
            api_key=api_key,
            https=https,
        )


class QdrantConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.QDRANT

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return QdrantConnectorConfig.create_from_dict(config)


class QdrantConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.QDRANT

    @classmethod
    def create(cls, config: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="Qdrant",
            description="Qdrant Connector (Qdrant)",
            version="1.0",
            config=[
                ConnectorConfigEntrySchema(
                    field="host",
                    label="Host",
                    description="Qdrant host",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="port",
                    label="Port",
                    description="Qdrant port (default 6333)",
                    type="number",
                    required=False,
                    default="6333",
                ),
                ConnectorConfigEntrySchema(
                    field="collection",
                    label="Collection",
                    description="Qdrant collection name",
                    type="string",
                    required=True,
                ),
                ConnectorConfigEntrySchema(
                    field="api_key",
                    label="API Key",
                    description="Qdrant API key (optional)",
                    type="string",
                    required=False,
                ),
                ConnectorConfigEntrySchema(
                    field="https",
                    label="HTTPS",
                    description="Use HTTPS when connecting to Qdrant",
                    type="boolean",
                    required=False,
                    default="false",
                ),
            ],
        )
