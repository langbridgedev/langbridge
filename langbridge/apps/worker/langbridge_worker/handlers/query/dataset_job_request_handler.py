from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import sqlglot
from pydantic import ValidationError
from sqlglot import exp

from langbridge.apps.worker.langbridge_worker.dataset_execution import (
    DatasetExecutionResolver,
    build_file_scan_sql,
)
from langbridge.apps.worker.langbridge_worker.tools import FederatedQueryTool
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.jobs.dataset_job import (
    CreateDatasetBulkCreateJobRequest,
    CreateDatasetCsvIngestJobRequest,
    CreateDatasetPreviewJobRequest,
    CreateDatasetProfileJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)
from langbridge.packages.common.langbridge_common.db.lineage import LineageEdgeRecord
from langbridge.packages.common.langbridge_common.db.job import JobRecord, JobStatus
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetColumnRepository,
    DatasetPolicyRepository,
    DatasetRepository,
    DatasetRevisionRepository,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.common.langbridge_common.repositories.lineage_repository import (
    LineageEdgeRepository,
)
from langbridge.packages.common.langbridge_common.utils.lineage import (
    LineageEdgeType,
    LineageNodeType,
    build_file_resource_id,
    build_source_table_resource_id,
    stable_payload_hash,
)
from langbridge.packages.common.langbridge_common.utils.sql import (
    apply_result_redaction,
    render_sql_with_params,
    sanitize_sql_error_message,
)
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.dataset_job import (
    DatasetJobRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler


_DEFAULT_PROFILE_COLUMN_LIMIT = 5


class DatasetJobRequestHandler(BaseMessageHandler):
    message_type: MessageType = MessageType.DATASET_JOB_REQUEST

    def __init__(
        self,
        job_repository: JobRepository,
        dataset_repository: DatasetRepository,
        dataset_column_repository: DatasetColumnRepository,
        dataset_policy_repository: DatasetPolicyRepository,
        dataset_revision_repository: DatasetRevisionRepository | None = None,
        lineage_edge_repository: LineageEdgeRepository | None = None,
        federated_query_tool: FederatedQueryTool | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._job_repository = job_repository
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._dataset_policy_repository = dataset_policy_repository
        self._dataset_revision_repository = dataset_revision_repository
        self._lineage_edge_repository = lineage_edge_repository
        self._federated_query_tool = federated_query_tool
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=dataset_repository,
        )

    async def handle(self, payload: DatasetJobRequestMessage) -> None:
        if self._federated_query_tool is None:
            raise BusinessValidationError("Federated query tool is not configured on this worker.")

        job_record = await self._job_repository.get_by_id(payload.job_id)
        if job_record is None:
            raise BusinessValidationError(f"Job with ID {payload.job_id} does not exist.")
        if job_record.status in {JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled}:
            self._logger.info("Dataset job %s already terminal (%s).", job_record.id, job_record.status)
            return None

        job_record.status = JobStatus.running
        job_record.progress = 5
        job_record.status_message = "Dataset execution started."
        if job_record.started_at is None:
            job_record.started_at = datetime.now(timezone.utc)

        try:
            if payload.job_type == JobType.DATASET_PREVIEW:
                request = self._parse_preview_request(payload)
                result = await self._run_preview(request)
                summary = f"Dataset preview completed with {int(result.get('row_count_preview') or 0)} rows."
            elif payload.job_type == JobType.DATASET_PROFILE:
                request = self._parse_profile_request(payload)
                result = await self._run_profile(request)
                summary = "Dataset profiling completed."
            elif payload.job_type == JobType.DATASET_CSV_INGEST:
                request = self._parse_csv_ingest_request(payload)
                result = await self._run_csv_ingest(request)
                summary = "CSV dataset ingestion completed."
            elif payload.job_type == JobType.DATASET_BULK_CREATE:
                request = self._parse_bulk_create_request(payload)
                result = await self._run_bulk_create(request, job_record)
                summary = (
                    f"Bulk dataset creation completed: {result.get('created_count', 0)} created, "
                    f"{result.get('reused_count', 0)} reused."
                )
            else:
                raise BusinessValidationError(f"Unsupported dataset job type '{payload.job_type.value}'.")

            job_record.result = {
                "result": result,
                "summary": summary,
            }
            job_record.status = JobStatus.succeeded
            job_record.progress = 100
            job_record.status_message = summary
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = None
        except Exception as exc:
            self._logger.exception("Dataset job %s failed: %s", job_record.id, exc)
            job_record.status = JobStatus.failed
            job_record.progress = 100
            job_record.status_message = "Dataset execution failed."
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = {"message": sanitize_sql_error_message(str(exc))}

        return None

    def _parse_preview_request(self, payload: DatasetJobRequestMessage) -> CreateDatasetPreviewJobRequest:
        try:
            return CreateDatasetPreviewJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid dataset preview request payload.") from exc

    def _parse_profile_request(self, payload: DatasetJobRequestMessage) -> CreateDatasetProfileJobRequest:
        try:
            return CreateDatasetProfileJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid dataset profile request payload.") from exc

    def _parse_csv_ingest_request(self, payload: DatasetJobRequestMessage) -> CreateDatasetCsvIngestJobRequest:
        try:
            return CreateDatasetCsvIngestJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid CSV ingest request payload.") from exc

    def _parse_bulk_create_request(self, payload: DatasetJobRequestMessage) -> CreateDatasetBulkCreateJobRequest:
        try:
            return CreateDatasetBulkCreateJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid dataset bulk-create request payload.") from exc

    async def _run_preview(self, request: CreateDatasetPreviewJobRequest) -> dict[str, Any]:
        dataset, columns, policy = await self._load_dataset_bundle(
            dataset_id=request.dataset_id,
            workspace_id=request.workspace_id,
        )
        effective_limit = min(max(1, request.enforced_limit), max(1, policy.max_rows_preview))

        workflow, table_key, dialect = await self._build_workflow(dataset=dataset)
        preview_sql = self._build_preview_sql(
            table_key=table_key,
            columns=columns,
            policy=policy,
            request=request,
            effective_limit=effective_limit,
            dialect=dialect,
        )
        execution = await self._federated_query_tool.execute_federated_query(
            {
                "workspace_id": str(request.workspace_id),
                "query": preview_sql,
                "dialect": dialect,
                "workflow": workflow.model_dump(mode="json"),
            }
        )

        rows_payload = execution.get("rows") or []
        rows = [row for row in rows_payload if isinstance(row, dict)]
        redacted_rows, redaction_applied = apply_result_redaction(
            rows=rows,
            redaction_rules=dict(policy.redaction_rules_json or {}),
        )

        execution_meta = self._extract_execution_meta(execution)
        selected_columns = [
            {"name": column.name, "type": column.data_type}
            for column in columns
            if column.is_allowed
        ]
        if not selected_columns:
            selected_columns = [
                {"name": str(name), "type": None}
                for name in (execution.get("columns") or [])
                if str(name).strip()
            ]

        return {
            "dataset_id": str(dataset.id),
            "columns": selected_columns,
            "rows": redacted_rows,
            "row_count_preview": len(redacted_rows),
            "effective_limit": effective_limit,
            "redaction_applied": redaction_applied,
            "duration_ms": execution_meta["duration_ms"],
            "bytes_scanned": execution_meta["bytes_scanned"],
            "query_sql": preview_sql,
        }

    async def _run_profile(self, request: CreateDatasetProfileJobRequest) -> dict[str, Any]:
        dataset, columns, policy = await self._load_dataset_bundle(
            dataset_id=request.dataset_id,
            workspace_id=request.workspace_id,
        )
        workflow, table_key, dialect = await self._build_workflow(dataset=dataset)

        base_filters = self._build_row_filter_expressions(
            policy=policy,
            request_context=request.user_context,
            workspace_id=request.workspace_id,
            project_id=request.project_id,
            user_id=request.user_id,
            dialect=dialect,
        )
        count_sql = self._build_count_sql(table_key=table_key, filters=base_filters, dialect=dialect)
        count_execution = await self._federated_query_tool.execute_federated_query(
            {
                "workspace_id": str(request.workspace_id),
                "query": count_sql,
                "dialect": dialect,
                "workflow": workflow.model_dump(mode="json"),
            }
        )
        row_count_estimate = self._extract_single_numeric(
            count_execution,
            preferred_keys=["row_count", "count", "rowcount"],
        )

        profiled_columns = [
            column
            for column in columns
            if column.is_allowed and not column.is_computed
        ][: _DEFAULT_PROFILE_COLUMN_LIMIT]
        distinct_counts: dict[str, int] = {}
        null_rates: dict[str, float] = {}
        for column in profiled_columns:
            stats_sql = self._build_column_profile_sql(
                table_key=table_key,
                column_name=column.name,
                filters=base_filters,
                dialect=dialect,
            )
            stats_execution = await self._federated_query_tool.execute_federated_query(
                {
                    "workspace_id": str(request.workspace_id),
                    "query": stats_sql,
                    "dialect": dialect,
                    "workflow": workflow.model_dump(mode="json"),
                }
            )
            distinct_count = self._extract_single_numeric(
                stats_execution,
                preferred_keys=["distinct_count", "distinct"],
            )
            null_count = self._extract_single_numeric(
                stats_execution,
                preferred_keys=["null_count", "nulls"],
            )
            if distinct_count is not None:
                distinct_counts[column.name] = distinct_count
            if row_count_estimate and row_count_estimate > 0 and null_count is not None:
                null_rates[column.name] = float(null_count) / float(row_count_estimate)

        execution_meta = self._extract_execution_meta(count_execution)
        now = datetime.now(timezone.utc)
        dataset.row_count_estimate = row_count_estimate
        dataset.bytes_estimate = execution_meta["bytes_scanned"]
        dataset.last_profiled_at = now
        dataset.updated_at = now

        return {
            "dataset_id": str(dataset.id),
            "row_count_estimate": row_count_estimate,
            "bytes_estimate": execution_meta["bytes_scanned"],
            "distinct_counts": distinct_counts,
            "null_rates": null_rates,
            "profiled_at": now.isoformat(),
        }

    async def _run_csv_ingest(self, request: CreateDatasetCsvIngestJobRequest) -> dict[str, Any]:
        dataset, _, _ = await self._load_dataset_bundle(
            dataset_id=request.dataset_id,
            workspace_id=request.workspace_id,
        )
        if str(dataset.dataset_type or "").upper() != "FILE":
            raise BusinessValidationError("CSV ingest requires a FILE dataset.")

        storage_uri = (request.storage_uri or dataset.storage_uri or "").strip()
        if not storage_uri:
            raise BusinessValidationError("CSV ingest dataset is missing storage_uri.")

        file_config = dict(dataset.file_config_json or {})
        file_format = str(
            file_config.get("format")
            or file_config.get("file_format")
            or "csv"
        ).strip().lower()
        if file_format != "csv":
            raise BusinessValidationError("CSV ingest only supports csv source files.")

        source_sql = build_file_scan_sql(storage_uri=storage_uri, file_config=file_config)
        parquet_file = (
            Path(settings.DATASET_FILE_LOCAL_DIR)
            / "parquet"
            / str(request.workspace_id)
            / f"{dataset.id}.parquet"
        )
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        escaped_parquet_file = str(parquet_file).replace("'", "''")

        connection = duckdb.connect(database=":memory:")
        try:
            describe_rows = connection.execute(
                f"DESCRIBE SELECT * FROM {source_sql}"
            ).fetchall()
            count_rows = connection.execute(
                f"SELECT COUNT(*) AS row_count FROM {source_sql}"
            ).fetchall()
            connection.execute(
                f"COPY (SELECT * FROM {source_sql}) TO '{escaped_parquet_file}' (FORMAT PARQUET)"
            )
        finally:
            connection.close()

        dataset.storage_uri = parquet_file.resolve().as_uri()
        dataset.dialect = "duckdb"
        dataset.file_config_json = {
            **file_config,
            "format": "parquet",
            "source_format": "csv",
            "source_storage_uri": storage_uri,
        }
        dataset.status = "published"
        dataset.table_name = dataset.table_name or dataset.name
        dataset.schema_name = dataset.schema_name or None
        dataset.row_count_estimate = int(count_rows[0][0]) if count_rows else None
        dataset.updated_at = datetime.now(timezone.utc)

        existing_columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        delete_for_dataset = getattr(self._dataset_column_repository, "delete_for_dataset", None)
        if callable(delete_for_dataset):
            await delete_for_dataset(dataset_id=dataset.id)
        elif existing_columns:
            # Repository test doubles may not expose delete support; clear by dataset when possible.
            self._logger.debug("Dataset column repository does not support bulk delete; appending inferred columns.")
        for index, row in enumerate(describe_rows):
            if len(row) < 2:
                continue
            column = DatasetColumnRecord(
                id=uuid.uuid4(),
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                name=str(row[0]),
                data_type=str(row[1]),
                nullable=True,
                ordinal_position=index,
                description=None,
                is_allowed=True,
                is_computed=False,
                expression=None,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            self._dataset_column_repository.add(column)

        await self._create_dataset_revision(
            dataset=dataset,
            policy=await self._get_or_create_policy(dataset),
            created_by=dataset.updated_by or request.user_id,
            change_summary="CSV ingest converted dataset to parquet.",
        )
        await self._replace_dataset_lineage(dataset)

        return {
            "dataset_id": str(dataset.id),
            "storage_uri": dataset.storage_uri,
            "row_count_estimate": dataset.row_count_estimate,
            "column_count": len(describe_rows),
            "format": "parquet",
        }

    async def _run_bulk_create(
        self,
        request: CreateDatasetBulkCreateJobRequest,
        job_record: JobRecord,
    ) -> dict[str, Any]:
        created_count = 0
        reused_count = 0
        dataset_ids: list[str] = []
        items: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        total = max(1, len(request.selections))

        for index, selection in enumerate(request.selections):
            job_record.progress = min(95, 10 + int((index / total) * 85))
            job_record.status_message = (
                f"Processing {selection.schema}.{selection.table} ({index + 1}/{total})"
            )
            try:
                existing = await self._find_existing_table_dataset(
                    workspace_id=request.workspace_id,
                    project_id=request.project_id,
                    connection_id=request.connection_id,
                    schema_name=selection.schema,
                    table_name=selection.table,
                    selected_columns=[column.name for column in selection.columns],
                )
                if existing is not None:
                    reused_count += 1
                    dataset_ids.append(str(existing.id))
                    items.append(
                        {
                            "schema": selection.schema,
                            "table": selection.table,
                            "dataset_id": str(existing.id),
                            "created": False,
                        }
                    )
                    continue

                dataset = await self._create_table_dataset_from_selection(
                    request=request,
                    schema_name=selection.schema,
                    table_name=selection.table,
                    columns=selection.columns,
                )
                created_count += 1
                dataset_ids.append(str(dataset.id))
                items.append(
                    {
                        "schema": selection.schema,
                        "table": selection.table,
                        "dataset_id": str(dataset.id),
                        "created": True,
                    }
                )
            except Exception as exc:  # noqa: BLE001 - continue processing the remaining selections
                errors.append(
                    {
                        "schema": selection.schema,
                        "table": selection.table,
                        "error": sanitize_sql_error_message(str(exc)),
                    }
                )

        return {
            "created_count": created_count,
            "reused_count": reused_count,
            "dataset_ids": dataset_ids[:200],
            "items": items[:500],
            "errors": errors[:200],
        }

    async def _find_existing_table_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        connection_id: uuid.UUID,
        schema_name: str,
        table_name: str,
        selected_columns: list[str],
    ) -> DatasetRecord | None:
        candidates = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            project_id=project_id,
            dataset_types=["TABLE"],
            limit=5000,
        )
        requested_signature = self._selection_signature(schema_name, table_name, selected_columns)
        for dataset in candidates:
            if dataset.connection_id != connection_id:
                continue
            if (dataset.schema_name or "").strip().lower() != schema_name.strip().lower():
                continue
            if (dataset.table_name or "").strip().lower() != table_name.strip().lower():
                continue
            columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
            existing_signature = self._selection_signature(
                schema_name,
                table_name,
                [column.name for column in columns if column.is_allowed],
            )
            if existing_signature == requested_signature:
                return dataset
        return None

    async def _create_table_dataset_from_selection(
        self,
        *,
        request: CreateDatasetBulkCreateJobRequest,
        schema_name: str,
        table_name: str,
        columns: list[Any],
    ) -> DatasetRecord:
        selected_columns: list[DatasetColumnRecord] = []
        for index, column in enumerate(columns):
            column_name = str(getattr(column, "name", "")).strip()
            if not column_name:
                continue
            selected_columns.append(
                DatasetColumnRecord(
                    id=uuid.uuid4(),
                    dataset_id=uuid.uuid4(),  # placeholder overwritten below
                    workspace_id=request.workspace_id,
                    name=column_name,
                    data_type=str(getattr(column, "data_type", None) or "unknown"),
                    nullable=bool(getattr(column, "nullable", True)),
                    ordinal_position=index,
                    description=None,
                    is_allowed=True,
                    is_computed=False,
                    expression=None,
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
            )

        base_name = self._render_name_template(
            request.naming_template,
            connection_id=request.connection_id,
            schema_name=schema_name,
            table_name=table_name,
        )
        final_name = await self._ensure_unique_dataset_name(
            workspace_id=request.workspace_id,
            project_id=request.project_id,
            base_name=base_name,
            suffix_seed=self._selection_signature(
                schema_name,
                table_name,
                [column.name for column in selected_columns],
            ),
        )
        now = datetime.now(timezone.utc)
        dataset = DatasetRecord(
            id=uuid.uuid4(),
            workspace_id=request.workspace_id,
            project_id=request.project_id,
            connection_id=request.connection_id,
            created_by=request.user_id,
            updated_by=request.user_id,
            name=final_name,
            description=None,
            tags_json=self._normalize_tags(list(request.tags or [])),
            dataset_type="TABLE",
            dialect=None,
            catalog_name=None,
            schema_name=schema_name,
            table_name=table_name,
            sql_text=None,
            referenced_dataset_ids_json=[],
            federated_plan_json=None,
            file_config_json=None,
            status="published",
            revision_id=None,
            row_count_estimate=None,
            bytes_estimate=None,
            last_profiled_at=None,
            created_at=now,
            updated_at=now,
        )
        self._dataset_repository.add(dataset)

        if not selected_columns:
            selected_columns.append(
                DatasetColumnRecord(
                    id=uuid.uuid4(),
                    dataset_id=dataset.id,
                    workspace_id=request.workspace_id,
                    name="*",
                    data_type="unknown",
                    nullable=True,
                    ordinal_position=0,
                    description=None,
                    is_allowed=True,
                    is_computed=False,
                    expression=None,
                    created_at=now,
                    updated_at=now,
                )
            )
        for index, column in enumerate(selected_columns):
            column.dataset_id = dataset.id
            column.ordinal_position = index
            self._dataset_column_repository.add(column)

        defaults = request.policy_defaults
        max_preview_rows = int(defaults.max_preview_rows) if defaults and defaults.max_preview_rows else settings.SQL_DEFAULT_MAX_PREVIEW_ROWS
        max_export_rows = int(defaults.max_export_rows) if defaults and defaults.max_export_rows else settings.SQL_DEFAULT_MAX_EXPORT_ROWS
        max_preview_rows = max(1, min(max_preview_rows, settings.SQL_POLICY_MAX_PREVIEW_ROWS_UPPER_BOUND))
        max_export_rows = max(1, min(max_export_rows, settings.SQL_POLICY_MAX_EXPORT_ROWS_UPPER_BOUND))
        allow_dml = bool(defaults.allow_dml) if defaults else False
        redaction_rules = dict(defaults.redaction_rules or {}) if defaults else {}
        self._dataset_policy_repository.add(
            DatasetPolicyRecord(
                id=uuid.uuid4(),
                dataset_id=dataset.id,
                workspace_id=request.workspace_id,
                max_rows_preview=max_preview_rows,
                max_export_rows=max_export_rows,
                redaction_rules_json=redaction_rules,
                row_filters_json=[],
                allow_dml=allow_dml,
                created_at=now,
                updated_at=now,
            )
        )
        policy = await self._get_or_create_policy(dataset)
        await self._create_dataset_revision(
            dataset=dataset,
            policy=policy,
            created_by=request.user_id,
            change_summary="Auto-generated dataset created.",
        )
        await self._replace_dataset_lineage(dataset)
        return dataset

    async def _ensure_unique_dataset_name(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        base_name: str,
        suffix_seed: str,
    ) -> str:
        rows = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            project_id=project_id,
            limit=5000,
        )
        taken = {row.name.strip().lower() for row in rows if row.name}
        if base_name.strip().lower() not in taken:
            return base_name.strip()

        candidate = f"{base_name.strip()}_{suffix_seed[:8]}"
        if candidate.lower() not in taken:
            return candidate

        counter = 2
        while True:
            numbered = f"{candidate}_{counter}"
            if numbered.lower() not in taken:
                return numbered
            counter += 1

    @staticmethod
    def _selection_signature(schema_name: str, table_name: str, selected_columns: list[str]) -> str:
        normalized_columns = ",".join(
            sorted({column.strip().lower() for column in selected_columns if column and column.strip()})
        ) or "*"
        payload = f"{schema_name.strip().lower()}|{table_name.strip().lower()}|{normalized_columns}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _render_name_template(
        naming_template: str,
        *,
        connection_id: uuid.UUID,
        schema_name: str,
        table_name: str,
    ) -> str:
        template = (naming_template or "{schema}.{table}").strip() or "{schema}.{table}"
        return (
            template.replace("{connection}", str(connection_id).replace("-", "_"))
            .replace("{schema}", schema_name.strip() or "schema")
            .replace("{table}", table_name.strip() or "table")
        )

    @staticmethod
    def _normalize_tags(tags: list[str]) -> list[str]:
        normalized = [tag.strip() for tag in tags if tag and tag.strip()]
        lowered = {tag.lower() for tag in normalized}
        if "auto-generated" not in lowered:
            normalized.append("auto-generated")
        return normalized

    async def _load_dataset_bundle(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> tuple[DatasetRecord, list[DatasetColumnRecord], DatasetPolicyRecord]:
        dataset = await self._dataset_repository.get_for_workspace(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
        )
        if dataset is None:
            raise BusinessValidationError("Dataset not found.")
        columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        policy = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
        if policy is None:
            policy = DatasetPolicyRecord(
                id=uuid.uuid4(),
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                max_rows_preview=settings.SQL_DEFAULT_MAX_PREVIEW_ROWS,
                max_export_rows=settings.SQL_DEFAULT_MAX_EXPORT_ROWS,
                redaction_rules_json={},
                row_filters_json=[],
                allow_dml=False,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            self._dataset_policy_repository.add(policy)
        return dataset, columns, policy

    async def _get_or_create_policy(self, dataset: DatasetRecord) -> DatasetPolicyRecord:
        policy = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
        if policy is not None:
            return policy

        policy = DatasetPolicyRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=dataset.workspace_id,
            max_rows_preview=settings.SQL_DEFAULT_MAX_PREVIEW_ROWS,
            max_export_rows=settings.SQL_DEFAULT_MAX_EXPORT_ROWS,
            redaction_rules_json={},
            row_filters_json=[],
            allow_dml=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self._dataset_policy_repository.add(policy)
        return policy

    async def _create_dataset_revision(
        self,
        *,
        dataset: DatasetRecord,
        policy: DatasetPolicyRecord,
        created_by: uuid.UUID | None,
        change_summary: str,
    ) -> None:
        if self._dataset_revision_repository is None:
            return

        columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        next_revision = await self._dataset_revision_repository.next_revision_number(dataset_id=dataset.id)
        definition = self._build_dataset_definition_snapshot(dataset)
        schema_snapshot = [
            {
                "name": column.name,
                "data_type": column.data_type,
                "nullable": column.nullable,
                "description": column.description,
                "is_allowed": column.is_allowed,
                "is_computed": column.is_computed,
                "expression": column.expression,
                "ordinal_position": column.ordinal_position,
            }
            for column in columns
        ]
        policy_snapshot = {
            "max_rows_preview": policy.max_rows_preview,
            "max_export_rows": policy.max_export_rows,
            "redaction_rules": dict(policy.redaction_rules_json or {}),
            "row_filters": list(policy.row_filters_json or []),
            "allow_dml": policy.allow_dml,
        }
        source_bindings = self._build_dataset_source_bindings(dataset)
        execution_characteristics = {
            "row_count_estimate": dataset.row_count_estimate,
            "bytes_estimate": dataset.bytes_estimate,
            "last_profiled_at": dataset.last_profiled_at.isoformat() if dataset.last_profiled_at else None,
        }
        snapshot = {
            "dataset": definition,
            "columns": schema_snapshot,
            "policy": policy_snapshot,
            "source_bindings": source_bindings,
            "execution_characteristics": execution_characteristics,
        }
        revision_id = uuid.uuid4()
        self._dataset_revision_repository.add(
            DatasetRevisionRecord(
                id=revision_id,
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                revision_number=next_revision,
                revision_hash=stable_payload_hash(snapshot),
                change_summary=change_summary,
                definition_json=definition,
                schema_json=schema_snapshot,
                policy_json=policy_snapshot,
                source_bindings_json=source_bindings,
                execution_characteristics_json=execution_characteristics,
                status=dataset.status,
                snapshot_json=snapshot,
                note=change_summary,
                created_by=created_by,
                created_at=datetime.now(timezone.utc),
            )
        )
        dataset.revision_id = revision_id

    @staticmethod
    def _build_dataset_definition_snapshot(dataset: DatasetRecord) -> dict[str, Any]:
        return {
            "id": str(dataset.id),
            "workspace_id": str(dataset.workspace_id),
            "project_id": str(dataset.project_id) if dataset.project_id else None,
            "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
            "name": dataset.name,
            "description": dataset.description,
            "tags": list(dataset.tags_json or []),
            "dataset_type": dataset.dataset_type,
            "dialect": dataset.dialect,
            "storage_uri": dataset.storage_uri,
            "catalog_name": dataset.catalog_name,
            "schema_name": dataset.schema_name,
            "table_name": dataset.table_name,
            "sql_text": dataset.sql_text,
            "referenced_dataset_ids": list(dataset.referenced_dataset_ids_json or []),
            "federated_plan": dataset.federated_plan_json,
            "file_config": dataset.file_config_json,
            "status": dataset.status,
        }

    def _build_dataset_source_bindings(self, dataset: DatasetRecord) -> list[dict[str, Any]]:
        dataset_type = str(dataset.dataset_type or "").upper()
        if dataset_type == "TABLE":
            return [
                {
                    "source_type": "connection",
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                },
                {
                    "source_type": "source_table",
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                    "catalog_name": dataset.catalog_name,
                    "schema_name": dataset.schema_name,
                    "table_name": dataset.table_name,
                },
            ]
        if dataset_type == "FILE":
            storage_uri = (
                str((dataset.file_config_json or {}).get("source_storage_uri") or "").strip()
                or str((dataset.file_config_json or {}).get("storage_uri") or "").strip()
                or dataset.storage_uri
            )
            return [
                {
                    "source_type": "file_resource",
                    "storage_uri": storage_uri,
                    "file_config": dict(dataset.file_config_json or {}),
                }
            ]
        if dataset_type == "FEDERATED":
            bindings: list[dict[str, Any]] = []
            seen: set[str] = set()
            for raw_value in dataset.referenced_dataset_ids_json or []:
                value = str(raw_value)
                if not value or value in seen:
                    continue
                seen.add(value)
                bindings.append({"source_type": "dataset", "dataset_id": value})
            plan = dataset.federated_plan_json if isinstance(dataset.federated_plan_json, dict) else {}
            tables_payload = plan.get("tables")
            iterable = tables_payload.values() if isinstance(tables_payload, dict) else tables_payload or []
            for item in iterable:
                if not isinstance(item, dict):
                    continue
                raw_id = item.get("dataset_id") or item.get("datasetId")
                if raw_id is None:
                    continue
                value = str(raw_id)
                if not value or value in seen:
                    continue
                seen.add(value)
                bindings.append({"source_type": "dataset", "dataset_id": value})
            return bindings
        return []

    async def _replace_dataset_lineage(self, dataset: DatasetRecord) -> None:
        if self._lineage_edge_repository is None:
            return

        await self._lineage_edge_repository.delete_for_target(
            workspace_id=dataset.workspace_id,
            target_type=LineageNodeType.DATASET.value,
            target_id=str(dataset.id),
        )

        edges: list[LineageEdgeRecord] = []
        if dataset.connection_id is not None:
            edges.append(
                LineageEdgeRecord(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.CONNECTION.value,
                    source_id=str(dataset.connection_id),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.FEEDS.value,
                    metadata_json={"connection_id": str(dataset.connection_id)},
                )
            )

        dataset_type = str(dataset.dataset_type or "").upper()
        if dataset_type == "TABLE" and dataset.connection_id is not None and dataset.table_name:
            edges.append(
                LineageEdgeRecord(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.SOURCE_TABLE.value,
                    source_id=build_source_table_resource_id(
                        connection_id=dataset.connection_id,
                        catalog_name=dataset.catalog_name,
                        schema_name=dataset.schema_name,
                        table_name=dataset.table_name,
                    ),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                    metadata_json={
                        "connection_id": str(dataset.connection_id),
                        "catalog_name": dataset.catalog_name,
                        "schema_name": dataset.schema_name,
                        "table_name": dataset.table_name,
                        "qualified_name": ".".join(
                            [
                                part
                                for part in (dataset.catalog_name, dataset.schema_name, dataset.table_name)
                                if part and str(part).strip()
                            ]
                        )
                        or str(dataset.table_name),
                    },
                )
            )
        elif dataset_type == "FILE":
            storage_uri = (
                str((dataset.file_config_json or {}).get("source_storage_uri") or "").strip()
                or str((dataset.file_config_json or {}).get("storage_uri") or "").strip()
                or dataset.storage_uri
            )
            if storage_uri:
                edges.append(
                    LineageEdgeRecord(
                        workspace_id=dataset.workspace_id,
                        source_type=LineageNodeType.FILE_RESOURCE.value,
                        source_id=build_file_resource_id(storage_uri),
                        target_type=LineageNodeType.DATASET.value,
                        target_id=str(dataset.id),
                        edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                        metadata_json={
                            "storage_uri": storage_uri,
                            "file_config": dict(dataset.file_config_json or {}),
                        },
                    )
                )
        elif dataset_type == "FEDERATED":
            seen_ids: set[str] = set()
            for item in self._build_dataset_source_bindings(dataset):
                raw_id = item.get("dataset_id")
                if raw_id is None:
                    continue
                source_id = str(raw_id)
                if not source_id or source_id == str(dataset.id) or source_id in seen_ids:
                    continue
                seen_ids.add(source_id)
                edges.append(
                    LineageEdgeRecord(
                        workspace_id=dataset.workspace_id,
                        source_type=LineageNodeType.DATASET.value,
                        source_id=source_id,
                        target_type=LineageNodeType.DATASET.value,
                        target_id=str(dataset.id),
                        edge_type=LineageEdgeType.DERIVES_FROM.value,
                        metadata_json={"match_type": "federated_child"},
                    )
                )

        for edge in edges:
            self._lineage_edge_repository.add(edge)

    async def _build_workflow(self, *, dataset: DatasetRecord):
        return await self._dataset_execution_resolver.build_workflow_for_dataset(dataset=dataset)

    def _build_preview_sql(
        self,
        *,
        table_key: str,
        columns: list[DatasetColumnRecord],
        policy: DatasetPolicyRecord,
        request: CreateDatasetPreviewJobRequest,
        effective_limit: int,
        dialect: str,
    ) -> str:
        select_expr = exp.select()
        allowed_columns = [column for column in columns if column.is_allowed]

        if not allowed_columns:
            select_expr = select_expr.select(exp.Star())
        else:
            projections: list[exp.Expression] = []
            for column in allowed_columns:
                if column.is_computed and column.expression:
                    try:
                        parsed_expression = sqlglot.parse_one(column.expression, read=dialect)
                        projections.append(exp.alias_(parsed_expression, column.name, quoted=True))
                    except sqlglot.ParseError:
                        continue
                    continue
                projections.append(
                    exp.Column(this=exp.Identifier(this=column.name, quoted=True))
                )
            if projections:
                select_expr = select_expr.select(*projections)
            else:
                select_expr = select_expr.select(exp.Star())

        select_expr = select_expr.from_(exp.table_(table_key, quoted=False))

        filter_expressions = self._build_filter_expressions(
            filters=request.filters,
            allowed_columns=allowed_columns,
            dialect=dialect,
        )
        filter_expressions.extend(
            self._build_row_filter_expressions(
                policy=policy,
                request_context=request.user_context,
                workspace_id=request.workspace_id,
                project_id=request.project_id,
                user_id=request.user_id,
                dialect=dialect,
            )
        )
        if filter_expressions:
            select_expr = select_expr.where(exp.and_(*filter_expressions))

        order_items: list[exp.Ordered] = []
        allowed_names = {column.name.lower() for column in allowed_columns}
        for item in request.sort:
            column = str(item.get("column") or "").strip()
            direction = str(item.get("direction") or "asc").strip().lower()
            if not column:
                continue
            if allowed_names and column.lower() not in allowed_names:
                continue
            order_items.append(
                exp.Ordered(
                    this=exp.Column(this=exp.Identifier(this=column, quoted=True)),
                    desc=direction == "desc",
                )
            )
        if order_items:
            select_expr = select_expr.order_by(*order_items)

        select_expr = select_expr.limit(effective_limit)
        return select_expr.sql(dialect=dialect)

    def _build_filter_expressions(
        self,
        *,
        filters: dict[str, Any],
        allowed_columns: list[DatasetColumnRecord],
        dialect: str,
    ) -> list[exp.Expression]:
        if not filters:
            return []

        allowed_names = {column.name.lower() for column in allowed_columns}
        expressions: list[exp.Expression] = []
        for raw_column, raw_value in filters.items():
            column = str(raw_column or "").strip()
            if not column:
                continue
            if allowed_names and column.lower() not in allowed_names:
                continue

            column_expr = exp.Column(this=exp.Identifier(this=column, quoted=True))
            if isinstance(raw_value, dict):
                operator = str(raw_value.get("operator") or "eq").strip().lower()
                value = raw_value.get("value")
                expressions.extend(
                    self._apply_operator_filter(
                        column_expr=column_expr,
                        operator=operator,
                        value=value,
                        dialect=dialect,
                    )
                )
                continue

            if isinstance(raw_value, list):
                literals = [self._literal_expression(item, dialect=dialect) for item in raw_value]
                expressions.append(exp.In(this=column_expr, expressions=literals))
                continue

            expressions.append(exp.EQ(this=column_expr, expression=self._literal_expression(raw_value, dialect=dialect)))

        return expressions

    def _apply_operator_filter(
        self,
        *,
        column_expr: exp.Column,
        operator: str,
        value: Any,
        dialect: str,
    ) -> list[exp.Expression]:
        if operator in {"eq", "equals"}:
            return [exp.EQ(this=column_expr, expression=self._literal_expression(value, dialect=dialect))]
        if operator in {"neq", "not_equals"}:
            return [exp.NEQ(this=column_expr, expression=self._literal_expression(value, dialect=dialect))]
        if operator in {"gt", "greater_than"}:
            return [exp.GT(this=column_expr, expression=self._literal_expression(value, dialect=dialect))]
        if operator in {"gte", "greater_than_or_equal"}:
            return [exp.GTE(this=column_expr, expression=self._literal_expression(value, dialect=dialect))]
        if operator in {"lt", "less_than"}:
            return [exp.LT(this=column_expr, expression=self._literal_expression(value, dialect=dialect))]
        if operator in {"lte", "less_than_or_equal"}:
            return [exp.LTE(this=column_expr, expression=self._literal_expression(value, dialect=dialect))]
        if operator in {"contains", "like"}:
            return [
                exp.Like(
                    this=column_expr,
                    expression=self._literal_expression(f"%{value}%", dialect=dialect),
                )
            ]
        if operator == "in" and isinstance(value, list):
            return [
                exp.In(
                    this=column_expr,
                    expressions=[self._literal_expression(item, dialect=dialect) for item in value],
                )
            ]
        return [exp.EQ(this=column_expr, expression=self._literal_expression(value, dialect=dialect))]

    def _build_row_filter_expressions(
        self,
        *,
        policy: DatasetPolicyRecord,
        request_context: dict[str, Any],
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        user_id: uuid.UUID,
        dialect: str,
    ) -> list[exp.Expression]:
        templates = list(policy.row_filters_json or [])
        if not templates:
            return []

        render_context: dict[str, Any] = {
            "workspace_id": str(workspace_id),
            "project_id": str(project_id) if project_id else None,
            "user_id": str(user_id),
        }
        render_context.update(request_context or {})

        expressions: list[exp.Expression] = []
        for template in templates:
            if not isinstance(template, str) or not template.strip():
                continue
            rendered = render_sql_with_params(template, render_context)
            try:
                expressions.append(sqlglot.parse_one(rendered, read=dialect))
            except sqlglot.ParseError as exc:
                raise BusinessValidationError(f"Invalid row filter policy expression: {exc}") from exc
        return expressions

    def _build_count_sql(
        self,
        *,
        table_key: str,
        filters: list[exp.Expression],
        dialect: str,
    ) -> str:
        query = (
            exp.select(exp.alias_(exp.Count(this=exp.Star()), "row_count", quoted=True))
            .from_(exp.table_(table_key, quoted=False))
        )
        if filters:
            query = query.where(exp.and_(*filters))
        return query.sql(dialect=dialect)

    def _build_column_profile_sql(
        self,
        *,
        table_key: str,
        column_name: str,
        filters: list[exp.Expression],
        dialect: str,
    ) -> str:
        column_expr = exp.Column(this=exp.Identifier(this=column_name, quoted=True))
        distinct_expr = exp.alias_(
            exp.Count(this=column_expr.copy(), distinct=True),
            "distinct_count",
            quoted=True,
        )
        null_expr = exp.alias_(
            exp.Sum(
                this=exp.Case(
                    ifs=[
                        (
                            exp.Is(this=column_expr.copy(), expression=exp.Null()),
                            exp.Literal.number(1),
                        )
                    ],
                    default=exp.Literal.number(0),
                )
            ),
            "null_count",
            quoted=True,
        )
        query = exp.select(distinct_expr, null_expr).from_(exp.table_(table_key, quoted=False))
        if filters:
            query = query.where(exp.and_(*filters))
        return query.sql(dialect=dialect)

    @staticmethod
    def _literal_expression(value: Any, *, dialect: str) -> exp.Expression:
        if value is None:
            return exp.Null()
        if isinstance(value, bool):
            return exp.true() if value else exp.false()
        if isinstance(value, (int, float)):
            return exp.Literal.number(value)
        if isinstance(value, (dict, list)):
            return exp.Literal.string(json.dumps(value))
        return exp.Literal.string(str(value))

    @staticmethod
    def _extract_execution_meta(execution: dict[str, Any]) -> dict[str, int | None]:
        execution_payload = execution.get("execution") if isinstance(execution, dict) else {}
        if not isinstance(execution_payload, dict):
            return {"duration_ms": None, "bytes_scanned": None}
        total_runtime = execution_payload.get("total_runtime_ms")
        duration_ms = int(total_runtime) if isinstance(total_runtime, (int, float)) else None
        bytes_scanned = 0
        has_bytes = False
        for metric in execution_payload.get("stage_metrics") or []:
            if not isinstance(metric, dict):
                continue
            value = metric.get("bytes_written")
            if isinstance(value, (int, float)):
                bytes_scanned += int(value)
                has_bytes = True
        return {
            "duration_ms": duration_ms,
            "bytes_scanned": bytes_scanned if has_bytes else None,
        }

    @staticmethod
    def _extract_single_numeric(
        execution: dict[str, Any],
        *,
        preferred_keys: list[str],
    ) -> int | None:
        rows_payload = execution.get("rows") or []
        if not isinstance(rows_payload, list) or not rows_payload:
            return None
        first_row = rows_payload[0]
        if not isinstance(first_row, dict):
            return None

        lowered = {str(key).lower(): value for key, value in first_row.items()}
        for key in preferred_keys:
            value = lowered.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str) and value.strip().isdigit():
                return int(value.strip())

        for value in first_row.values():
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str) and value.strip().isdigit():
                return int(value.strip())
        return None
