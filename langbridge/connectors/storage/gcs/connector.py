from collections.abc import Mapping, Sequence
import importlib
import logging
from typing import Any

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ManagedStorageConnector
from .config import GcsStorageConnectorConfig
from .._duckdb import create_secret, load_extension


class GcsStorageConnector(ManagedStorageConnector):
    config: GcsStorageConnectorConfig
    RUNTIME_TYPE = ConnectorRuntimeType.GCS

    def __init__(self, config: GcsStorageConnectorConfig, logger: logging.Logger | None = None) -> None:
        super().__init__(config=config, logger=logger)

    async def list_buckets(self) -> list[str]:
        client = self._client()
        return [bucket.name for bucket in client.list_buckets()]

    async def list_objects(self, bucket: str) -> list[str]:
        client = self._client()
        target_bucket = client.bucket(bucket)
        return [blob.name for blob in client.list_blobs(target_bucket)]

    async def get_object(self, bucket: str, key: str) -> bytes:
        client = self._client()
        blob = client.bucket(bucket).blob(key)
        return blob.download_as_bytes()

    async def configure_duckdb_connection(
        self,
        connection: Any,
        *,
        storage_uris: Sequence[str],
        options: Mapping[str, Any] | None = None,
    ) -> None:
        load_extension(connection, "httpfs")
        secret_clauses: dict[str, Any] = {"TYPE": "gcs"}
        if self.config.hmac_key_id and self.config.hmac_secret:
            secret_clauses["KEY_ID"] = self.config.hmac_key_id
            secret_clauses["SECRET"] = self.config.hmac_secret
        elif self.config.use_credential_chain:
            secret_clauses["PROVIDER"] = "credential_chain"
        else:
            raise ValueError(
                "GCS parquet access requires hmac_key_id/hmac_secret or use_credential_chain=True."
            )
        if self.config.endpoint_url:
            secret_clauses["ENDPOINT"] = self._endpoint_host(self.config.endpoint_url)
        if self.config.use_ssl is not None:
            secret_clauses["USE_SSL"] = self.config.use_ssl
        create_secret(connection, secret_name="langbridge_gcs_secret", clauses=secret_clauses)

    async def create_bucket(self, bucket_name: str) -> None:
        client = self._client()
        bucket = client.bucket(bucket_name)
        client.create_bucket(bucket)

    async def delete_bucket(self, bucket_name: str) -> None:
        self._client().bucket(bucket_name).delete()

    async def delete_object(self, bucket: str, key: str) -> None:
        self._client().bucket(bucket).blob(key).delete()

    async def upload_object(self, bucket: str, key: str, data: bytes) -> None:
        self._client().bucket(bucket).blob(key).upload_from_string(data)

    async def update_object(self, bucket: str, key: str, data: bytes) -> None:
        await self.upload_object(bucket, key, data)

    @staticmethod
    async def create_managed_instance(
        config: GcsStorageConnectorConfig,
        logger: logging.Logger | None = None,
    ) -> "GcsStorageConnector":
        return GcsStorageConnector(config=config, logger=logger)

    def _client(self):
        try:
            storage_module = importlib.import_module("google.cloud.storage")
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                "google-cloud-storage is required to use the GCS storage connector."
            ) from exc

        client_class = getattr(storage_module, "Client")
        if self.config.service_account_json_path:
            return client_class.from_service_account_json(
                self.config.service_account_json_path,
                project=self.config.project,
            )
        return client_class(project=self.config.project)

    @staticmethod
    def _endpoint_host(endpoint_url: str) -> str:
        normalized = str(endpoint_url or "").strip()
        return normalized.split("://", 1)[-1].rstrip("/")
