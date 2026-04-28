import pytest

from langbridge.connectors.base import (
    StorageConnectorFactory,
    get_connector_config_factory,
)
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.storage.azure_blob import AzureBlobStorageConnector
from langbridge.connectors.storage.azure_blob.config import AzureBlobStorageConnectorConfig
from langbridge.connectors.storage.gcs import GcsStorageConnector
from langbridge.connectors.storage.gcs.config import GcsStorageConnectorConfig
from langbridge.connectors.storage.local import LocalStorageConnector
from langbridge.connectors.storage.s3 import S3StorageConnector
from langbridge.connectors.storage.s3.config import S3StorageConnectorConfig


class RecordingDuckDbConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str):
        self.statements.append(sql)
        return self


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_storage_connector_factory_resolves_runtime_types() -> None:
    assert (
        StorageConnectorFactory.get_storage_connector_class_reference(ConnectorRuntimeType.LOCAL_FILESYSTEM)
        is LocalStorageConnector
    )
    assert (
        StorageConnectorFactory.get_storage_connector_class_reference(ConnectorRuntimeType.S3)
        is S3StorageConnector
    )
    assert (
        StorageConnectorFactory.get_storage_connector_class_reference(ConnectorRuntimeType.GCS)
        is GcsStorageConnector
    )
    assert (
        StorageConnectorFactory.get_storage_connector_class_reference(ConnectorRuntimeType.AZURE_BLOB)
        is AzureBlobStorageConnector
    )


def test_storage_connector_factory_rejects_non_storage_runtime_types() -> None:
    with pytest.raises(ValueError, match="No storage connector found"):
        StorageConnectorFactory.get_storage_connector_class_reference(ConnectorRuntimeType.POSTGRES)


def test_storage_connector_config_factories_register_cloud_storage_types() -> None:
    assert get_connector_config_factory(ConnectorRuntimeType.S3).__name__ == "S3StorageConnectorConfigFactory"
    assert get_connector_config_factory(ConnectorRuntimeType.GCS).__name__ == "GcsStorageConnectorConfigFactory"
    assert (
        get_connector_config_factory(ConnectorRuntimeType.AZURE_BLOB).__name__
        == "AzureBlobStorageConnectorConfigFactory"
    )


@pytest.mark.anyio
async def test_s3_storage_connector_configures_duckdb_secret() -> None:
    connector = S3StorageConnector(
        config=S3StorageConnectorConfig(
            region_name="eu-west-2",
            endpoint_url="https://s3.example.internal",
            access_key_id="key-id",
            secret_access_key="secret-key",
            session_token="session-token",
            url_style="path",
        )
    )
    connection = RecordingDuckDbConnection()

    await connector.configure_duckdb_connection(
        connection,
        storage_uris=["s3://acme-bucket/orders.parquet"],
    )

    assert connection.statements[0] == "LOAD httpfs"
    assert "TYPE s3" in connection.statements[1]
    assert "KEY_ID 'key-id'" in connection.statements[1]
    assert "SECRET 'secret-key'" in connection.statements[1]
    assert "SESSION_TOKEN 'session-token'" in connection.statements[1]
    assert "REGION 'eu-west-2'" in connection.statements[1]
    assert "ENDPOINT 's3.example.internal'" in connection.statements[1]
    assert "URL_STYLE 'path'" in connection.statements[1]


@pytest.mark.anyio
async def test_gcs_storage_connector_configures_duckdb_secret() -> None:
    connector = GcsStorageConnector(
        config=GcsStorageConnectorConfig(
            hmac_key_id="gcs-key-id",
            hmac_secret="gcs-secret",
            endpoint_url="https://storage.googleapis.com",
        )
    )
    connection = RecordingDuckDbConnection()

    await connector.configure_duckdb_connection(
        connection,
        storage_uris=["gs://acme-bucket/orders.parquet"],
    )

    assert connection.statements[0] == "LOAD httpfs"
    assert "TYPE gcs" in connection.statements[1]
    assert "KEY_ID 'gcs-key-id'" in connection.statements[1]
    assert "SECRET 'gcs-secret'" in connection.statements[1]
    assert "ENDPOINT 'storage.googleapis.com'" in connection.statements[1]


@pytest.mark.anyio
async def test_azure_blob_storage_connector_configures_duckdb_secret() -> None:
    connector = AzureBlobStorageConnector(
        config=AzureBlobStorageConnectorConfig(
            connection_string="UseDevelopmentStorage=true",
        )
    )
    connection = RecordingDuckDbConnection()

    await connector.configure_duckdb_connection(
        connection,
        storage_uris=["az://container/orders.parquet"],
    )

    assert connection.statements[0] == "LOAD azure"
    assert "TYPE azure" in connection.statements[1]
    assert "CONNECTION_STRING 'UseDevelopmentStorage=true'" in connection.statements[1]
