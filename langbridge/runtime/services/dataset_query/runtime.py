import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from langbridge.runtime.datasets.contracts import DatasetMaterializationConfig
from langbridge.runtime.execution import DuckDbExecutionEngine, ExecutionEngine, FederatedQueryTool
from langbridge.runtime.models import (
    CreateDatasetBulkCreateJobRequest,
    CreateDatasetCsvIngestJobRequest,
    CreateDatasetPreviewJobRequest,
    CreateDatasetProfileJobRequest,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    RuntimeJob,
    RuntimeJobStatus,
)
from langbridge.runtime.models.metadata import (
    DatasetMaterializationMode,
    DatasetSource,
    DatasetSourceKind,
    DatasetStatus,
    DatasetStorageKind,
    DatasetType,
)
from langbridge.runtime.ports import (
    DatasetCatalogStore,
    DatasetColumnStore,
    DatasetPolicyStore,
    DatasetRevisionStore,
    LineageEdgeStore,
    MutableJobHandle,
)
from langbridge.runtime.providers import ConnectorMetadataProvider, DatasetMetadataProvider
from langbridge.runtime.services.dataset_execution import (
    DatasetExecutionResolver,
    build_file_scan_sql,
)
from langbridge.runtime.services.dataset_query.dialects import DatasetConnectorDialectResolver
from langbridge.runtime.services.dataset_query.job_status import DatasetJobStatusWriter
from langbridge.runtime.services.dataset_query.lineage import DatasetQueryLineageWriter
from langbridge.runtime.services.dataset_query.metadata import DatasetQueryMetadataBuilder
from langbridge.runtime.services.dataset_query.naming import DatasetSelectionNamer
from langbridge.runtime.services.dataset_query.results import DatasetExecutionResultParser
from langbridge.runtime.services.dataset_query.sql_builder import DatasetQuerySqlBuilder
from langbridge.runtime.services.dataset_query.types import DatasetExecutionRequest
from langbridge.runtime.services.errors import ExecutionValidationError
from langbridge.runtime.settings import runtime_settings as settings
from langbridge.runtime.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
)
from langbridge.runtime.utils.lineage import stable_payload_hash
from langbridge.runtime.utils.sql import apply_result_redaction, sanitize_sql_error_message

_DEFAULT_PROFILE_COLUMN_LIMIT = 5


async def _flush_stores(*stores: Any) -> None:
    for store in stores:
        flush = getattr(store, "flush", None)
        if callable(flush):
            await flush()


class DatasetQueryService:
    """Runtime-facing facade for dataset preview, profiling, ingest, and creation jobs."""

    def __init__(
        self,
        dataset_repository: DatasetCatalogStore | None,
        dataset_column_repository: DatasetColumnStore | None,
        dataset_policy_repository: DatasetPolicyStore | None,
        dataset_revision_repository: DatasetRevisionStore | None = None,
        lineage_edge_repository: LineageEdgeStore | None = None,
        federated_query_tool: FederatedQueryTool | None = None,
        execution_engine: ExecutionEngine | None = None,
        dataset_provider: DatasetMetadataProvider | None = None,
        connector_provider: ConnectorMetadataProvider | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._dataset_policy_repository = dataset_policy_repository
        self._dataset_revision_repository = dataset_revision_repository
        self._federated_query_tool = federated_query_tool
        self._execution_engine = execution_engine or DuckDbExecutionEngine()
        self._dataset_provider = dataset_provider
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=dataset_repository,
            dataset_provider=dataset_provider,
        )

        self._dialects = DatasetConnectorDialectResolver(connector_provider=connector_provider)
        self._job_status = DatasetJobStatusWriter()
        self._metadata_builder = DatasetQueryMetadataBuilder()
        self._lineage_writer = DatasetQueryLineageWriter(
            lineage_edge_repository=lineage_edge_repository,
            metadata_builder=self._metadata_builder,
        )
        self._namer = DatasetSelectionNamer()
        self._result_parser = DatasetExecutionResultParser()
        self._sql_builder = DatasetQuerySqlBuilder()

    async def query_dataset(
        self,
        *,
        request: DatasetExecutionRequest,
        job_record: MutableJobHandle | None = None,
    ) -> dict[str, Any]:
        if job_record is not None:
            await self.execute_job(job_record=job_record, request=request)
            result_payload = job_record.result if isinstance(job_record.result, dict) else {}
            result = result_payload.get("result")
            if isinstance(result, dict):
                return result
            return {}

        if isinstance(request, CreateDatasetPreviewJobRequest):
            return await self._run_preview(request)
        if isinstance(request, CreateDatasetProfileJobRequest):
            return await self._run_profile(request)
        if isinstance(request, CreateDatasetCsvIngestJobRequest):
            if self._dataset_repository is None or self._dataset_column_repository is None:
                raise ExecutionValidationError("CSV ingest requires mutable dataset repositories.")
            return await self._run_csv_ingest(request)
        if isinstance(request, CreateDatasetBulkCreateJobRequest):
            if self._dataset_repository is None or self._dataset_column_repository is None:
                raise ExecutionValidationError("Bulk dataset creation requires mutable dataset repositories.")
            transient_job = RuntimeJob(
                id=uuid.uuid4(),
                workspace_id=str(request.workspace_id),
                job_type=request.job_type.value,
                payload=request.model_dump(mode="json"),
                headers={},
                status=RuntimeJobStatus.running,
                progress=0,
                status_message="Dataset execution started.",
                created_at=datetime.now(timezone.utc),
                queued_at=datetime.now(timezone.utc),
                started_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            return await self._run_bulk_create(request, transient_job)
        raise ExecutionValidationError(
            f"Unsupported dataset execution request '{type(request).__name__}'."
        )

    async def execute_job(
        self,
        *,
        job_record: MutableJobHandle,
        request: DatasetExecutionRequest,
    ) -> None:
        if self._federated_query_tool is None:
            raise ExecutionValidationError("Federated query tool is not configured on this runtime node.")

        try:
            if isinstance(request, CreateDatasetPreviewJobRequest):
                result = await self._run_preview(request)
                summary = f"Dataset preview completed with {int(result.get('row_count_preview') or 0)} rows."
            elif isinstance(request, CreateDatasetProfileJobRequest):
                result = await self._run_profile(request)
                summary = "Dataset profiling completed."
            elif isinstance(request, CreateDatasetCsvIngestJobRequest):
                result = await self._run_csv_ingest(request)
                summary = "CSV dataset ingestion completed."
            elif isinstance(request, CreateDatasetBulkCreateJobRequest):
                result = await self._run_bulk_create(request, job_record)
                summary = (
                    f"Bulk dataset creation completed: {result.get('created_count', 0)} created, "
                    f"{result.get('reused_count', 0)} reused."
                )
            else:
                raise ExecutionValidationError(
                    f"Unsupported dataset execution request '{type(request).__name__}'."
                )

            job_record.result = {
                "result": result,
                "summary": summary,
            }
            self._job_status.set_status(job_record, RuntimeJobStatus.succeeded)
            job_record.progress = 100
            job_record.status_message = summary
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = None
        except Exception as exc:
            self._logger.exception("Dataset job %s failed: %s", job_record.id, exc)
            self._job_status.set_status(job_record, RuntimeJobStatus.failed)
            job_record.progress = 100
            job_record.status_message = "Dataset execution failed."
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = {"message": sanitize_sql_error_message(str(exc))}

    async def _run_preview(self, request: CreateDatasetPreviewJobRequest) -> dict[str, Any]:
        dataset, columns, policy = await self._load_dataset_bundle(
            dataset_id=request.dataset_id,
            workspace_id=request.workspace_id,
        )
        effective_limit = min(max(1, request.enforced_limit), max(1, policy.max_rows_preview))
        workflow, table_key, dialect = await self._build_workflow(dataset=dataset)
        preview_sql = self._sql_builder.build_preview_sql(
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
        execution_meta = self._result_parser.execution_meta(execution)
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
        base_filters = self._sql_builder.build_row_filter_expressions(
            policy=policy,
            request_context=request.user_context,
            workspace_id=request.workspace_id,
            actor_id=request.actor_id,
            dialect=dialect,
        )
        count_sql = self._sql_builder.build_count_sql(
            table_key=table_key,
            filters=base_filters,
            dialect=dialect,
        )
        count_execution = await self._federated_query_tool.execute_federated_query(
            {
                "workspace_id": str(request.workspace_id),
                "query": count_sql,
                "dialect": dialect,
                "workflow": workflow.model_dump(mode="json"),
            }
        )
        row_count_estimate = self._result_parser.single_numeric(
            count_execution,
            preferred_keys=["row_count", "count", "rowcount"],
        )

        profiled_columns = [
            column for column in columns if column.is_allowed and not column.is_computed
        ][:_DEFAULT_PROFILE_COLUMN_LIMIT]
        distinct_counts: dict[str, int] = {}
        null_rates: dict[str, float] = {}
        for column in profiled_columns:
            stats_sql = self._sql_builder.build_column_profile_sql(
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
            distinct_count = self._result_parser.single_numeric(
                stats_execution,
                preferred_keys=["distinct_count", "distinct"],
            )
            null_count = self._result_parser.single_numeric(
                stats_execution,
                preferred_keys=["null_count", "nulls"],
            )
            if distinct_count is not None:
                distinct_counts[column.name] = distinct_count
            if row_count_estimate and row_count_estimate > 0 and null_count is not None:
                null_rates[column.name] = float(null_count) / float(row_count_estimate)

        execution_meta = self._result_parser.execution_meta(count_execution)
        now = datetime.now(timezone.utc)
        dataset.row_count_estimate = row_count_estimate
        dataset.bytes_estimate = execution_meta["bytes_scanned"]
        dataset.last_profiled_at = now
        dataset.updated_at = now
        if self._dataset_repository is not None:
            await self._dataset_repository.save(dataset)

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
        if dataset.dataset_type != DatasetType.FILE:
            raise ExecutionValidationError("CSV ingest requires a FILE dataset.")

        storage_uri = (request.storage_uri or dataset.storage_uri or "").strip()
        if not storage_uri:
            raise ExecutionValidationError("CSV ingest dataset is missing storage_uri.")

        file_config = dict(dataset.file_config_json or {})
        file_format = str(file_config.get("format") or file_config.get("file_format") or "csv").strip().lower()
        if file_format != "csv":
            raise ExecutionValidationError("CSV ingest only supports csv source files.")

        source_sql = build_file_scan_sql(storage_uri=storage_uri, file_config=file_config)
        parquet_file = (
            Path(settings.DATASET_FILE_LOCAL_DIR)
            / "parquet"
            / str(request.workspace_id)
            / f"{dataset.id}.parquet"
        )
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        escaped_parquet_file = str(parquet_file).replace("'", "''")

        connection = self._execution_engine.open_connection()
        try:
            describe_rows = connection.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
            count_rows = connection.execute(f"SELECT COUNT(*) AS row_count FROM {source_sql}").fetchall()
            connection.execute(
                f"COPY (SELECT * FROM {source_sql}) TO '{escaped_parquet_file}' (FORMAT PARQUET)"
            )
        finally:
            connection.close()

        dataset.storage_uri = parquet_file.resolve().as_uri()
        dataset.dialect = "duckdb"
        dataset.file_config = {
            **file_config,
            "format": "parquet",
            "source_format": "csv",
            "source_storage_uri": storage_uri,
        }
        dataset.source = DatasetSource(storage_uri=dataset.storage_uri, format="parquet")
        dataset.status = DatasetStatus.PUBLISHED
        dataset.materialization = DatasetMaterializationConfig(mode=DatasetMaterializationMode.LIVE)
        dataset.table_name = dataset.table_name or dataset.name
        dataset.schema_name = dataset.schema_name or None
        dataset.row_count_estimate = int(count_rows[0][0]) if count_rows else None
        dataset.updated_at = datetime.now(timezone.utc)

        existing_columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        delete_for_dataset = getattr(self._dataset_column_repository, "delete_for_dataset", None)
        if callable(delete_for_dataset):
            await delete_for_dataset(dataset_id=dataset.id)
        elif existing_columns:
            self._logger.debug("Dataset column repository does not support bulk delete; appending inferred columns.")
        for index, row in enumerate(describe_rows):
            if len(row) < 2:
                continue
            self._dataset_column_repository.add(
                DatasetColumnMetadata(
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
            )

        await self._create_dataset_revision(
            dataset=dataset,
            policy=await self._get_or_create_policy(dataset),
            created_by=dataset.updated_by or request.actor_id,
            change_summary="CSV ingest converted dataset to parquet.",
        )
        await self._lineage_writer.replace_dataset_lineage(dataset)
        if self._dataset_repository is not None:
            await self._dataset_repository.save(dataset)

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
        job_record: MutableJobHandle,
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
                f"Processing {selection.schema_name}.{selection.table} ({index + 1}/{total})"
            )
            try:
                existing = await self._find_existing_table_dataset(
                    workspace_id=request.workspace_id,
                    connection_id=request.connection_id,
                    schema_name=selection.schema_name,
                    table_name=selection.table,
                    selected_columns=[column.name for column in selection.columns],
                )
                if existing is not None:
                    reused_count += 1
                    dataset_ids.append(str(existing.id))
                    items.append(
                        {
                            "schema": selection.schema_name,
                            "table": selection.table,
                            "dataset_id": str(existing.id),
                            "created": False,
                        }
                    )
                    continue

                dataset = await self._create_table_dataset_from_selection(
                    request=request,
                    schema_name=selection.schema_name,
                    table_name=selection.table,
                    columns=selection.columns,
                )
                created_count += 1
                dataset_ids.append(str(dataset.id))
                items.append(
                    {
                        "schema": selection.schema_name,
                        "table": selection.table,
                        "dataset_id": str(dataset.id),
                        "created": True,
                    }
                )
            except Exception as exc:
                errors.append(
                    {
                        "schema": selection.schema_name,
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
        connection_id: uuid.UUID,
        schema_name: str,
        table_name: str,
        selected_columns: list[str],
    ) -> DatasetMetadata | None:
        candidates = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            dataset_types=["TABLE"],
            limit=5000,
        )
        requested_signature = self._namer.selection_signature(schema_name, table_name, selected_columns)
        for dataset in candidates:
            if dataset.connection_id != connection_id:
                continue
            if (dataset.schema_name or "").strip().lower() != schema_name.strip().lower():
                continue
            if (dataset.table_name or "").strip().lower() != table_name.strip().lower():
                continue
            columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
            existing_signature = self._namer.selection_signature(
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
    ) -> DatasetMetadata:
        selected_columns = self._build_selected_columns(
            workspace_id=request.workspace_id,
            columns=columns,
        )
        base_name = self._namer.render_name_template(
            request.naming_template,
            connection_id=request.connection_id,
            schema_name=schema_name,
            table_name=table_name,
        )
        final_name = await self._ensure_unique_dataset_name(
            workspace_id=request.workspace_id,
            base_name=base_name,
            suffix_seed=self._namer.selection_signature(
                schema_name,
                table_name,
                [column.name for column in selected_columns],
            ),
        )
        connector_kind = await self._dialects.connector_runtime_kind(
            workspace_id=request.workspace_id,
            connection_id=request.connection_id,
        )
        if connector_kind is None:
            raise ExecutionValidationError(
                f"Connector metadata is required to create dataset selections for connection '{request.connection_id}'."
            )

        now = datetime.now(timezone.utc)
        dataset_id = uuid.uuid4()
        source_kind = DatasetSourceKind.DATABASE
        storage_kind = DatasetStorageKind.TABLE
        materialization_mode = DatasetMaterializationMode.LIVE
        relation_identity = build_dataset_relation_identity(
            dataset_id=dataset_id,
            connector_id=request.connection_id,
            dataset_name=final_name,
            catalog_name=None,
            schema_name=schema_name,
            table_name=table_name,
            storage_uri=None,
            source_kind=source_kind,
            storage_kind=storage_kind,
        )
        execution_capabilities = build_dataset_execution_capabilities(
            source_kind=source_kind,
            storage_kind=storage_kind,
        )
        dataset = DatasetMetadata(
            id=dataset_id,
            workspace_id=request.workspace_id,
            connection_id=request.connection_id,
            created_by=request.actor_id,
            updated_by=request.actor_id,
            name=final_name,
            sql_alias=self._namer.dataset_sql_alias(final_name),
            description=None,
            tags=self._namer.normalize_tags(list(request.tags or [])),
            dataset_type=DatasetType.TABLE,
            materialization_mode=materialization_mode,
            source_kind=source_kind,
            connector_kind=connector_kind,
            storage_kind=storage_kind,
            dialect=self._dialects.connector_dialect(connector_kind),
            catalog_name=None,
            schema_name=schema_name,
            table_name=table_name,
            sql_text=None,
            source=DatasetSource(table=table_name),
            sync=None,
            relation_identity=relation_identity.model_dump(mode="json"),
            execution_capabilities=execution_capabilities.model_dump(mode="json"),
            referenced_dataset_ids=[],
            federated_plan=None,
            file_config=None,
            status=DatasetStatus.PUBLISHED,
            revision_id=None,
            row_count_estimate=None,
            bytes_estimate=None,
            last_profiled_at=None,
            created_at=now,
            updated_at=now,
            management_mode="runtime_managed",
            lifecycle_state="active",
        )
        self._dataset_repository.add(dataset)
        self._add_selected_columns(
            dataset=dataset,
            workspace_id=request.workspace_id,
            selected_columns=selected_columns,
            now=now,
        )
        policy = self._create_policy_from_defaults(
            dataset=dataset,
            request=request,
            now=now,
        )
        await self._create_dataset_revision(
            dataset=dataset,
            policy=policy,
            created_by=request.actor_id,
            change_summary="Auto-generated dataset created.",
        )
        await self._lineage_writer.replace_dataset_lineage(dataset)
        if self._dataset_repository is not None:
            await self._dataset_repository.save(dataset)
        return dataset

    def _build_selected_columns(
        self,
        *,
        workspace_id: uuid.UUID,
        columns: list[Any],
    ) -> list[DatasetColumnMetadata]:
        selected_columns: list[DatasetColumnMetadata] = []
        for index, column in enumerate(columns):
            column_name = str(getattr(column, "name", "")).strip()
            if not column_name:
                continue
            selected_columns.append(
                DatasetColumnMetadata(
                    id=uuid.uuid4(),
                    dataset_id=uuid.uuid4(),
                    workspace_id=workspace_id,
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
        return selected_columns

    def _add_selected_columns(
        self,
        *,
        dataset: DatasetMetadata,
        workspace_id: uuid.UUID,
        selected_columns: list[DatasetColumnMetadata],
        now: datetime,
    ) -> None:
        if not selected_columns:
            selected_columns.append(
                DatasetColumnMetadata(
                    id=uuid.uuid4(),
                    dataset_id=dataset.id,
                    workspace_id=workspace_id,
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

    def _create_policy_from_defaults(
        self,
        *,
        dataset: DatasetMetadata,
        request: CreateDatasetBulkCreateJobRequest,
        now: datetime,
    ) -> DatasetPolicyMetadata:
        defaults = request.policy_defaults
        max_preview_rows = (
            int(defaults.max_preview_rows)
            if defaults and defaults.max_preview_rows
            else settings.SQL_DEFAULT_MAX_PREVIEW_ROWS
        )
        max_export_rows = (
            int(defaults.max_export_rows)
            if defaults and defaults.max_export_rows
            else settings.SQL_DEFAULT_MAX_EXPORT_ROWS
        )
        max_preview_rows = max(1, min(max_preview_rows, settings.SQL_POLICY_MAX_PREVIEW_ROWS_UPPER_BOUND))
        max_export_rows = max(1, min(max_export_rows, settings.SQL_POLICY_MAX_EXPORT_ROWS_UPPER_BOUND))
        policy = DatasetPolicyMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=dataset.workspace_id,
            max_rows_preview=max_preview_rows,
            max_export_rows=max_export_rows,
            redaction_rules=dict(defaults.redaction_rules or {}) if defaults else {},
            row_filters=[],
            allow_dml=bool(defaults.allow_dml) if defaults else False,
            created_at=now,
            updated_at=now,
        )
        self._dataset_policy_repository.add(policy)
        return policy

    async def _ensure_unique_dataset_name(
        self,
        *,
        workspace_id: uuid.UUID,
        base_name: str,
        suffix_seed: str,
    ) -> str:
        rows = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
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

    async def _load_dataset_bundle(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> tuple[DatasetMetadata, list[DatasetColumnMetadata], DatasetPolicyMetadata]:
        if self._dataset_repository is not None:
            dataset = await self._dataset_repository.get_for_workspace(
                dataset_id=dataset_id,
                workspace_id=workspace_id,
            )
        elif self._dataset_provider is not None:
            dataset = await self._dataset_provider.get_dataset(
                dataset_id=dataset_id,
                workspace_id=workspace_id,
            )
        else:
            dataset = None
        if dataset is None:
            raise ExecutionValidationError("Dataset not found.")
        if self._dataset_column_repository is not None:
            columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        elif self._dataset_provider is not None:
            columns = await self._dataset_provider.get_dataset_columns(dataset_id=dataset.id)
        else:
            columns = []
        if self._dataset_policy_repository is not None:
            policy = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
        elif self._dataset_provider is not None:
            policy = await self._dataset_provider.get_dataset_policy(dataset_id=dataset.id)
        else:
            policy = None
        if policy is None:
            policy = DatasetPolicyMetadata(
                id=uuid.uuid4(),
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                max_rows_preview=settings.SQL_DEFAULT_MAX_PREVIEW_ROWS,
                max_export_rows=settings.SQL_DEFAULT_MAX_EXPORT_ROWS,
                redaction_rules={},
                row_filters=[],
                allow_dml=False,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            if self._dataset_policy_repository is not None:
                self._dataset_policy_repository.add(policy)
        return dataset, columns, policy

    async def _get_or_create_policy(self, dataset: DatasetMetadata) -> DatasetPolicyMetadata:
        policy = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
        if policy is not None:
            return policy

        policy = DatasetPolicyMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=dataset.workspace_id,
            max_rows_preview=settings.SQL_DEFAULT_MAX_PREVIEW_ROWS,
            max_export_rows=settings.SQL_DEFAULT_MAX_EXPORT_ROWS,
            redaction_rules={},
            row_filters=[],
            allow_dml=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self._dataset_policy_repository.add(policy)
        return policy

    async def _create_dataset_revision(
        self,
        *,
        dataset: DatasetMetadata,
        policy: DatasetPolicyMetadata,
        created_by: uuid.UUID | None,
        change_summary: str,
    ) -> None:
        if self._dataset_revision_repository is None:
            return
        await _flush_stores(
            self._dataset_repository,
            self._dataset_column_repository,
            self._dataset_policy_repository,
        )

        columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        next_revision = await self._dataset_revision_repository.next_revision_number(dataset_id=dataset.id)
        definition = self._metadata_builder.definition_snapshot(dataset)
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
        policy_snapshot = self._metadata_builder.policy_snapshot(policy)
        source_bindings = self._metadata_builder.source_bindings(dataset)
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
            DatasetRevision(
                id=revision_id,
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                revision_number=next_revision,
                revision_hash=stable_payload_hash(snapshot),
                change_summary=change_summary,
                definition=definition,
                schema_snapshot=schema_snapshot,
                policy=policy_snapshot,
                source_bindings=source_bindings,
                execution_characteristics=execution_characteristics,
                status=dataset.status_value,
                snapshot=snapshot,
                note=change_summary,
                created_by=created_by,
                created_at=datetime.now(timezone.utc),
            )
        )
        dataset.revision_id = revision_id
        if self._dataset_repository is not None:
            await self._dataset_repository.save(dataset)

    async def _replace_dataset_lineage(self, dataset: DatasetMetadata) -> None:
        await self._lineage_writer.replace_dataset_lineage(dataset)

    async def _build_workflow(self, *, dataset: DatasetMetadata):
        return await self._dataset_execution_resolver.build_workflow_for_dataset(dataset=dataset)
