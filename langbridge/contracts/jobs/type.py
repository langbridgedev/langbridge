from __future__ import annotations

from enum import Enum


class JobType(str, Enum):
    AGENT = "agent"
    SEMANTIC_QUERY = "semantic_query"
    AGENTIC_SEMANTIC_MODEL = "agentic_semantic_model"
    SQL = "sql"
    DATASET_PREVIEW = "dataset_preview"
    DATASET_PROFILE = "dataset_profile"
    DATASET_BULK_CREATE = "dataset_bulk_create"
    DATASET_CSV_INGEST = "dataset_csv_ingest"
    CONNECTOR_SYNC = "connector_sync"


__all__ = ["JobType"]
