from collections.abc import Mapping, Sequence
import logging
from typing import Any

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ManagedStorageConnector
from .config import S3StorageConnectorConfig
from .._duckdb import create_secret, load_extension


class S3StorageConnector(ManagedStorageConnector):
    config: S3StorageConnectorConfig
    RUNTIME_TYPE = ConnectorRuntimeType.S3

    def __init__(self, config: S3StorageConnectorConfig, logger: logging.Logger | None = None) -> None:
        super().__init__(config=config, logger=logger)

    async def list_buckets(self) -> list[str]:
        client = self._client()
        payload = client.list_buckets()
        return [str(item.get("Name")) for item in payload.get("Buckets", []) if item.get("Name")]

    async def list_objects(self, bucket: str) -> list[str]:
        client = self._client()
        paginator = client.get_paginator("list_objects_v2")
        object_keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket):
            for item in page.get("Contents", []):
                key = str(item.get("Key") or "").strip()
                if key:
                    object_keys.append(key)
        return object_keys

    async def get_object(self, bucket: str, key: str) -> bytes:
        client = self._client()
        payload = client.get_object(Bucket=bucket, Key=key)
        return payload["Body"].read()

    async def configure_duckdb_connection(
        self,
        connection: Any,
        *,
        storage_uris: Sequence[str],
        options: Mapping[str, Any] | None = None,
    ) -> None:
        load_extension(connection, "httpfs")
        secret_clauses: dict[str, Any] = {"TYPE": "s3"}
        if self.config.access_key_id and self.config.secret_access_key:
            secret_clauses["KEY_ID"] = self.config.access_key_id
            secret_clauses["SECRET"] = self.config.secret_access_key
            if self.config.session_token:
                secret_clauses["SESSION_TOKEN"] = self.config.session_token
        else:
            secret_clauses["PROVIDER"] = "credential_chain"
        if self.config.region_name:
            secret_clauses["REGION"] = self.config.region_name
        if self.config.endpoint_url:
            secret_clauses["ENDPOINT"] = self._endpoint_host(self.config.endpoint_url)
        if self.config.url_style:
            secret_clauses["URL_STYLE"] = self.config.url_style
        if self.config.use_ssl is not None:
            secret_clauses["USE_SSL"] = self.config.use_ssl
        create_secret(connection, secret_name="langbridge_s3_secret", clauses=secret_clauses)

    async def create_bucket(self, bucket_name: str) -> None:
        client = self._client()
        params: dict[str, Any] = {"Bucket": bucket_name}
        if self.config.region_name and self.config.region_name != "us-east-1":
            params["CreateBucketConfiguration"] = {"LocationConstraint": self.config.region_name}
        client.create_bucket(**params)

    async def delete_bucket(self, bucket_name: str) -> None:
        self._client().delete_bucket(Bucket=bucket_name)

    async def delete_object(self, bucket: str, key: str) -> None:
        self._client().delete_object(Bucket=bucket, Key=key)

    async def upload_object(self, bucket: str, key: str, data: bytes) -> None:
        self._client().put_object(Bucket=bucket, Key=key, Body=data)

    async def update_object(self, bucket: str, key: str, data: bytes) -> None:
        await self.upload_object(bucket, key, data)

    @staticmethod
    async def create_managed_instance(
        config: S3StorageConnectorConfig,
        logger: logging.Logger | None = None,
    ) -> "S3StorageConnector":
        return S3StorageConnector(config=config, logger=logger)

    def _client(self):
        import boto3

        session = boto3.session.Session(
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            aws_session_token=self.config.session_token,
            profile_name=self.config.profile_name,
            region_name=self.config.region_name,
        )
        return session.client(
            "s3",
            endpoint_url=self.config.endpoint_url,
            use_ssl=bool(self.config.use_ssl),
        )

    @staticmethod
    def _endpoint_host(endpoint_url: str) -> str:
        normalized = str(endpoint_url or "").strip()
        return normalized.split("://", 1)[-1].rstrip("/")
