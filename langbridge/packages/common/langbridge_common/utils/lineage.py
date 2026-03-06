from __future__ import annotations

import hashlib
import json
import uuid
from enum import Enum
from typing import Any


class LineageNodeType(str, Enum):
    CONNECTION = "connection"
    SOURCE_TABLE = "source_table"
    API_RESOURCE = "api_resource"
    FILE_RESOURCE = "file_resource"
    DATASET = "dataset"
    SEMANTIC_MODEL = "semantic_model"
    UNIFIED_SEMANTIC_MODEL = "unified_semantic_model"
    SAVED_QUERY = "saved_query"
    DASHBOARD = "dashboard"


class LineageEdgeType(str, Enum):
    DERIVES_FROM = "DERIVES_FROM"
    REFERENCES = "REFERENCES"
    GENERATED_BY = "GENERATED_BY"
    FEEDS = "FEEDS"
    MATERIALIZES_FROM = "MATERIALIZES_FROM"


def build_source_table_resource_id(
    *,
    connection_id: uuid.UUID,
    catalog_name: str | None = None,
    schema_name: str | None = None,
    table_name: str,
) -> str:
    parts = [part.strip() for part in (catalog_name, schema_name, table_name) if part and part.strip()]
    return f"{connection_id}:{'.'.join(parts)}"


def build_file_resource_id(storage_uri: str) -> str:
    normalized = (storage_uri or "").strip()
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return f"file:{digest}"


def build_api_resource_id(*, connection_id: uuid.UUID, resource_name: str) -> str:
    return f"{connection_id}:api:{str(resource_name or '').strip().lower()}"


def stable_payload_hash(payload: Any) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
