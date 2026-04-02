import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence
import uuid

import numpy as np

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ManagedStorageConnector
from langbridge.federation.utils import resolve_local_storage_path
from .config import LocalStorageConnectorConfig

class LocalStorageConnector(ManagedStorageConnector):
    config: LocalStorageConnectorConfig
    RUNTIME_TYPE = ConnectorRuntimeType.LOCAL_FILESYSTEM

    def __init__(self, config: LocalStorageConnectorConfig, logger: Optional[Any] = None):
        super().__init__(config=config, logger=logger)
        
    async def list_buckets(self) -> List[str]:
        # For local filesystem, we can treat the folders in the specified location as "buckets"
        location_path = Path(self.config.location)
        if not location_path.exists() or not location_path.is_dir():
            raise ValueError(f"Location {self.config.location} does not exist or is not a directory.")
        return [f.name for f in location_path.iterdir() if f.is_dir()]
    
    async def list_objects(self, bucket_name: str) -> List[str]:
        bucket_path = Path(self.config.location) / bucket_name
        if not bucket_path.exists() or not bucket_path.is_dir():
            raise ValueError(f"Bucket {bucket_name} does not exist or is not a directory.")
        return [f.name for f in bucket_path.iterdir() if f.is_file()]
    
    async def get_object(self, bucket_name: str, object_name: str) -> bytes:
        object_path = Path(self.config.location) / bucket_name / object_name
        if not object_path.exists() or not object_path.is_file():
            raise ValueError(f"Object {object_name} does not exist in bucket {bucket_name}.")
        return object_path.read_bytes()

    async def resolve_duckdb_scan_uris(
        self,
        storage_uris: Sequence[str],
        *,
        options: Mapping[str, Any] | None = None,
    ) -> List[str]:
        return [
            resolve_local_storage_path(str(storage_uri)).as_posix()
            for storage_uri in storage_uris
            if str(storage_uri or "").strip()
        ]
    
    async def create_bucket(self, bucket_name: str) -> None:
        bucket_path = Path(self.config.location) / bucket_name
        bucket_path.mkdir(parents=True, exist_ok=True)

    async def delete_bucket(self, bucket_name: str) -> None:
        bucket_path = Path(self.config.location) / bucket_name
        if bucket_path.exists():
            bucket_path.rmdir()

    async def upload_object(self, bucket_name: str, object_name: str, data: bytes) -> None:
        object_path = Path(self.config.location) / bucket_name / object_name
        object_path.write_bytes(data)

    async def delete_object(self, bucket_name: str, object_name: str) -> None:
        object_path = Path(self.config.location) / bucket_name / object_name
        if object_path.exists():
            object_path.unlink()

    async def update_object(self, bucket_name: str, object_name: str, data: bytes) -> None:
        await self.delete_object(bucket_name, object_name)
        await self.upload_object(bucket_name, object_name, data)
    
    @staticmethod
    async def create_managed_instance(config: LocalStorageConnectorConfig, logger: Optional[Any] = None) -> "LocalStorageConnector":
        # For local storage, we can simply return an instance of the connector with the provided config
        return LocalStorageConnector(config=config, logger=logger)
