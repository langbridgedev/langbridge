from langbridge.connectors.base.config import (
    BaseConnectorConfigSchemaFactory, 
    BaseConnectorConfigFactory,
    ConnectorConfigSchema, 
    ConnectorConfigEntrySchema, 
    BaseConnectorConfig,
    ConnectorRuntimeType
)

class LocalStorageConnectorConfig(BaseConnectorConfig):
    location: str

    @classmethod
    def create_from_dict(cls, data: dict) -> "LocalStorageConnectorConfig":
        location = data.get("location")
        if location is None:
            raise ValueError("Both 'location' must be provided and non-None.")
        return cls(
            location=location
        )
        
class LocalStorageConnectorConfigFactory(BaseConnectorConfigFactory):
    type = ConnectorRuntimeType.LOCAL_FILESYSTEM

    @classmethod
    def create(cls, config: dict) -> BaseConnectorConfig:
        return LocalStorageConnectorConfig.create_from_dict(config)

class LocalStorageConnectorConfigSchemaFactory(BaseConnectorConfigSchemaFactory):
    type = ConnectorRuntimeType.LOCAL_FILESYSTEM

    @classmethod
    def create(cls, config: dict) -> ConnectorConfigSchema:
        return ConnectorConfigSchema(
            name="LocalStorage",
            description="Local Storage Connector (Local Filesystem)",
            version="1.0",
            config=[
                ConnectorConfigEntrySchema(field="location", label="Location", description="Faiss Location", type="string", required=True)
            ]
        )