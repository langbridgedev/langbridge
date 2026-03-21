from __future__ import annotations

from typing import Any

from langbridge.runtime.models import SqlJob, SqlJobResultArtifact
from langbridge.runtime.persistence.db.sql import (
    SqlJobRecord,
    SqlJobResultArtifactRecord,
)


def from_sql_job_result_artifact_record(
    value: Any | None,
) -> SqlJobResultArtifact | None:
    if value is None:
        return None
    if isinstance(value, SqlJobResultArtifact):
        return value
    return SqlJobResultArtifact(
        id=getattr(value, "id"),
        sql_job_id=getattr(value, "sql_job_id"),
        workspace_id=getattr(value, "workspace_id"),
        created_by=(
            getattr(value, "created_by_actor_id", None)
            or getattr(value, "created_by")
        ),
        format=str(getattr(value, "format")),
        mime_type=str(getattr(value, "mime_type")),
        row_count=int(getattr(value, "row_count", 0) or 0),
        byte_size=getattr(value, "byte_size", None),
        storage_backend=str(getattr(value, "storage_backend")),
        storage_reference=str(getattr(value, "storage_reference")),
        payload=getattr(value, "payload", None) or getattr(value, "payload_json", None),
        created_at=getattr(value, "created_at", None),
    )


def to_sql_job_result_artifact_record(
    value: SqlJobResultArtifact | SqlJobResultArtifactRecord,
) -> SqlJobResultArtifactRecord:
    if isinstance(value, SqlJobResultArtifactRecord):
        return value
    return SqlJobResultArtifactRecord(
        id=value.id,
        sql_job_id=value.sql_job_id,
        workspace_id=value.workspace_id,
        created_by_actor_id=value.created_by,
        format=value.format,
        mime_type=value.mime_type,
        row_count=value.row_count,
        byte_size=value.byte_size,
        storage_backend=value.storage_backend,
        storage_reference=value.storage_reference,
        payload_json=value.payload_json,
        created_at=value.created_at,
    )


def from_sql_job_record(value: Any | None) -> SqlJob | None:
    if value is None:
        return None
    if isinstance(value, SqlJob):
        return value
    return SqlJob(
        id=getattr(value, "id"),
        workspace_id=getattr(value, "workspace_id"),
        actor_id=getattr(value, "actor_id"),
        connection_id=getattr(value, "connection_id", None),
        workbench_mode=str(getattr(value, "workbench_mode")),
        selected_datasets_json=list(getattr(value, "selected_datasets_json", None) or []),
        execution_mode=str(getattr(value, "execution_mode", "single")),
        status=str(getattr(value, "status", "queued")),
        query_text=str(getattr(value, "query_text")),
        query_hash=str(getattr(value, "query_hash")),
        query_params_json=dict(getattr(value, "query_params_json", None) or {}),
        requested_limit=getattr(value, "requested_limit", None),
        enforced_limit=int(getattr(value, "enforced_limit", 1000) or 1000),
        requested_timeout_seconds=getattr(value, "requested_timeout_seconds", None),
        enforced_timeout_seconds=int(getattr(value, "enforced_timeout_seconds", 30) or 30),
        is_explain=bool(getattr(value, "is_explain", False)),
        is_federated=bool(getattr(value, "is_federated", False)),
        correlation_id=getattr(value, "correlation_id", None),
        policy_snapshot_json=dict(getattr(value, "policy_snapshot_json", None) or {}),
        result_columns_json=(
            None
            if getattr(value, "result_columns_json", None) is None
            else list(getattr(value, "result_columns_json"))
        ),
        result_rows_json=(
            None
            if getattr(value, "result_rows_json", None) is None
            else list(getattr(value, "result_rows_json"))
        ),
        row_count_preview=int(getattr(value, "row_count_preview", 0) or 0),
        total_rows_estimate=getattr(value, "total_rows_estimate", None),
        bytes_scanned=getattr(value, "bytes_scanned", None),
        duration_ms=getattr(value, "duration_ms", None),
        result_cursor=getattr(value, "result_cursor", None),
        redaction_applied=bool(getattr(value, "redaction_applied", False)),
        error_json=getattr(value, "error_json", None),
        warning_json=getattr(value, "warning_json", None),
        stats_json=getattr(value, "stats_json", None),
        created_at=getattr(value, "created_at", None),
        started_at=getattr(value, "started_at", None),
        finished_at=getattr(value, "finished_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_sql_job_record(value: SqlJob | SqlJobRecord) -> SqlJobRecord:
    if isinstance(value, SqlJobRecord):
        return value
    return SqlJobRecord(
        id=value.id,
        workspace_id=value.workspace_id,
        actor_id=value.actor_id,
        connection_id=value.connection_id,
        workbench_mode=value.workbench_mode,
        selected_datasets_json=list(value.selected_datasets_json or []),
        execution_mode=value.execution_mode,
        status=value.status,
        query_text=value.query_text,
        query_hash=value.query_hash,
        query_params_json=dict(value.query_params_json or {}),
        requested_limit=value.requested_limit,
        enforced_limit=value.enforced_limit,
        requested_timeout_seconds=value.requested_timeout_seconds,
        enforced_timeout_seconds=value.enforced_timeout_seconds,
        is_explain=value.is_explain,
        is_federated=value.is_federated,
        correlation_id=value.correlation_id,
        policy_snapshot_json=dict(value.policy_snapshot_json or {}),
        result_columns_json=(
            None if value.result_columns_json is None else list(value.result_columns_json)
        ),
        result_rows_json=(
            None if value.result_rows_json is None else list(value.result_rows_json)
        ),
        row_count_preview=value.row_count_preview,
        total_rows_estimate=value.total_rows_estimate,
        bytes_scanned=value.bytes_scanned,
        duration_ms=value.duration_ms,
        result_cursor=value.result_cursor,
        redaction_applied=value.redaction_applied,
        error_json=value.error_json,
        warning_json=value.warning_json,
        stats_json=value.stats_json,
        created_at=value.created_at,
        started_at=value.started_at,
        finished_at=value.finished_at,
        updated_at=value.updated_at,
    )


__all__ = [
    "from_sql_job_record",
    "from_sql_job_result_artifact_record",
    "to_sql_job_record",
    "to_sql_job_result_artifact_record",
]
