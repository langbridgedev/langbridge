from collections.abc import Mapping, Sequence
import importlib
import logging
from typing import Any

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ManagedStorageConnector
from .config import AzureBlobStorageConnectorConfig
from .._duckdb import create_secret, load_extension


class AzureBlobStorageConnector(ManagedStorageConnector):
    config: AzureBlobStorageConnectorConfig
    RUNTIME_TYPE = ConnectorRuntimeType.AZURE_BLOB

    def __init__(
        self,
        config: AzureBlobStorageConnectorConfig,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(config=config, logger=logger)

    async def list_buckets(self) -> list[str]:
        service = self._service_client()
        return [container["name"] for container in service.list_containers()]

    async def list_objects(self, bucket: str) -> list[str]:
        container = self._service_client().get_container_client(bucket)
        return [blob["name"] for blob in container.list_blobs()]

    async def get_object(self, bucket: str, key: str) -> bytes:
        container = self._service_client().get_container_client(bucket)
        return container.download_blob(key).readall()

    async def configure_duckdb_connection(
        self,
        connection: Any,
        *,
        storage_uris: Sequence[str],
        options: Mapping[str, Any] | None = None,
    ) -> None:
        load_extension(connection, "azure")
        secret_clauses: dict[str, Any] = {"TYPE": "azure"}
        if self.config.connection_string:
            secret_clauses["CONNECTION_STRING"] = self.config.connection_string
        elif self.config.client_secret and self.config.account_name and self.config.client_id and self.config.tenant_id:
            secret_clauses["PROVIDER"] = "service_principal"
            secret_clauses["ACCOUNT_NAME"] = self.config.account_name
            secret_clauses["TENANT_ID"] = self.config.tenant_id
            secret_clauses["CLIENT_ID"] = self.config.client_id
            secret_clauses["CLIENT_SECRET"] = self.config.client_secret
        elif self.config.use_credential_chain and self.config.account_name:
            secret_clauses["PROVIDER"] = "credential_chain"
            secret_clauses["ACCOUNT_NAME"] = self.config.account_name
        elif self.config.account_name:
            secret_clauses["ACCOUNT_NAME"] = self.config.account_name
        else:
            raise ValueError(
                "Azure Blob parquet access requires connection_string, account_name, or service principal details."
            )
        create_secret(connection, secret_name="langbridge_azure_secret", clauses=secret_clauses)

    async def create_bucket(self, bucket_name: str) -> None:
        self._service_client().create_container(bucket_name)

    async def delete_bucket(self, bucket_name: str) -> None:
        self._service_client().delete_container(bucket_name)

    async def delete_object(self, bucket: str, key: str) -> None:
        self._service_client().get_container_client(bucket).delete_blob(key)

    async def upload_object(self, bucket: str, key: str, data: bytes) -> None:
        self._service_client().get_container_client(bucket).upload_blob(name=key, data=data, overwrite=True)

    async def update_object(self, bucket: str, key: str, data: bytes) -> None:
        await self.upload_object(bucket, key, data)

    @staticmethod
    async def create_managed_instance(
        config: AzureBlobStorageConnectorConfig,
        logger: logging.Logger | None = None,
    ) -> "AzureBlobStorageConnector":
        return AzureBlobStorageConnector(config=config, logger=logger)

    def _service_client(self):
        try:
            blob_module = importlib.import_module("azure.storage.blob")
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "azure-storage-blob is required to use the Azure Blob storage connector."
            ) from exc

        blob_service_client = getattr(blob_module, "BlobServiceClient")
        if self.config.connection_string:
            return blob_service_client.from_connection_string(self.config.connection_string)

        credential = self._sdk_credential()
        account_url = self.config.account_url or self._default_account_url(self.config.account_name)
        return blob_service_client(account_url=account_url, credential=credential)

    def _sdk_credential(self):
        if self.config.account_key:
            return self.config.account_key
        if self.config.sas_token:
            return self.config.sas_token
        if self.config.client_secret and self.config.client_id and self.config.tenant_id:
            try:
                identity_module = importlib.import_module("azure.identity")
            except ModuleNotFoundError as exc:
                raise ModuleNotFoundError(
                    "azure-identity is required for Azure service principal authentication."
                ) from exc
            credential_class = getattr(identity_module, "ClientSecretCredential")
            return credential_class(
                tenant_id=self.config.tenant_id,
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
            )
        if self.config.use_credential_chain:
            try:
                identity_module = importlib.import_module("azure.identity")
            except ModuleNotFoundError as exc:
                raise ModuleNotFoundError(
                    "azure-identity is required for Azure credential chain authentication."
                ) from exc
            credential_class = getattr(identity_module, "DefaultAzureCredential")
            return credential_class()
        return None

    @staticmethod
    def _default_account_url(account_name: str | None) -> str:
        normalized_name = str(account_name or "").strip()
        if not normalized_name:
            raise ValueError("Azure Blob storage connector requires account_name or account_url for SDK access.")
        return f"https://{normalized_name}.blob.core.windows.net"
