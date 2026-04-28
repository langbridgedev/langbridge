import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from langbridge.runtime.models import CreateSqlJobRequest, SqlJob, SqlJobResultArtifact
from langbridge.runtime.ports import SqlJobArtifactStore


class SqlJobArtifactWriter:
    """Builds SQL job payloads and stores preview artifacts."""

    def __init__(self, *, artifact_store: SqlJobArtifactStore | None) -> None:
        self._artifact_store = artifact_store

    def result_payload(self, job: SqlJob) -> dict[str, Any]:
        stats = dict(job.stats_json or {})
        return {
            "columns": list(job.result_columns_json or []),
            "rows": list(job.result_rows_json or []),
            "row_count_preview": int(job.row_count_preview or 0),
            "total_rows_estimate": job.total_rows_estimate,
            "bytes_scanned": job.bytes_scanned,
            "duration_ms": job.duration_ms,
            "result_cursor": job.result_cursor,
            "redaction_applied": job.redaction_applied,
            "stats": stats,
            "federation_diagnostics": stats.get("federation_diagnostics"),
        }

    def selected_datasets_payload(self, request: CreateSqlJobRequest) -> list[dict[str, Any]]:
        if request.federated_datasets:
            return [
                dataset.model_dump(mode="json") if hasattr(dataset, "model_dump") else dict(dataset)
                for dataset in request.federated_datasets
            ]
        return [{"dataset_id": str(dataset_id)} for dataset_id in request.selected_datasets]

    def build_transient_job(self, request: CreateSqlJobRequest) -> SqlJob:
        now = datetime.now(timezone.utc)
        query_hash = hashlib.sha256(request.query.strip().encode("utf-8")).hexdigest()
        return SqlJob(
            id=request.sql_job_id,
            workspace_id=request.workspace_id,
            actor_id=request.actor_id,
            connection_id=request.connection_id,
            workbench_mode=(
                request.workbench_mode.value
                if hasattr(request.workbench_mode, "value")
                else str(request.workbench_mode)
            ),
            selected_datasets_json=self.selected_datasets_payload(request),
            execution_mode=request.execution_mode,
            status="queued",
            query_text=request.query,
            query_hash=query_hash,
            query_params_json=dict(request.params or {}),
            requested_limit=request.requested_limit,
            enforced_limit=request.enforced_limit,
            requested_timeout_seconds=request.requested_timeout_seconds,
            enforced_timeout_seconds=request.enforced_timeout_seconds,
            is_explain=request.explain,
            is_federated=request.execution_mode == "federated",
            correlation_id=request.correlation_id,
            policy_snapshot_json={
                "allow_dml": request.allow_dml,
                "allow_federation": request.allow_federation,
                "allowed_schemas": list(request.allowed_schemas or []),
                "allowed_tables": list(request.allowed_tables or []),
                "redaction_rules": dict(request.redaction_rules or {}),
            },
            created_at=now,
            updated_at=now,
        )

    def store_preview_artifact(
        self,
        *,
        job: SqlJob,
        columns_payload: list[dict[str, Any]],
        rows: list[dict[str, Any]],
        now: datetime,
    ) -> None:
        if self._artifact_store is None:
            return
        snapshot_artifact_id = uuid.uuid4()
        snapshot_artifact = SqlJobResultArtifact(
            id=snapshot_artifact_id,
            sql_job_id=job.id,
            workspace_id=job.workspace_id,
            created_by=job.actor_id,
            format="json_preview",
            mime_type="application/json",
            row_count=len(rows),
            byte_size=None,
            storage_backend="inline",
            storage_reference=f"inline://{snapshot_artifact_id}",
            payload_json={
                "columns": columns_payload,
                "rows": rows,
            },
            created_at=now,
        )
        self._artifact_store.add(snapshot_artifact)
