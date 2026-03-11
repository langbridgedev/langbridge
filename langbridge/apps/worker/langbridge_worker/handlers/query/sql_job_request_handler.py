import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

import sqlglot
from sqlglot import exp
from pydantic import ValidationError

from langbridge.apps.worker.langbridge_worker.dataset_execution import DatasetExecutionResolver
from langbridge.apps.worker.langbridge_worker.secrets import SecretProviderRegistry
from langbridge.apps.worker.langbridge_worker.tools import FederatedQueryTool
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse
from langbridge.packages.common.langbridge_common.contracts.jobs.sql_job import (
    CreateSqlJobRequest,
)
from langbridge.packages.common.langbridge_common.db.sql import (
    SqlJobRecord,
    SqlJobResultArtifactRecord,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetRepository,
)
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlJobRepository,
    SqlJobResultArtifactRepository,
)
from langbridge.packages.common.langbridge_common.utils.datasets import (
    dataset_supports_structured_federation,
)
from langbridge.packages.common.langbridge_common.utils.sql import (
    apply_result_redaction,
    enforce_preview_limit,
    enforce_read_only_sql,
    enforce_table_allowlist,
    normalize_sql_dialect,
    render_sql_with_params,
    sanitize_sql_error_message,
    transpile_sql,
)
from langbridge.packages.connectors.langbridge_connectors.api import (
    ConnectorRuntimeTypeSqlDialectMap,
    SqlConnectorFactory,
    get_connector_config_factory,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType
from langbridge.packages.federation.models import FederationWorkflow, VirtualDataset, VirtualTableBinding
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.sql_job import (
    SqlJobRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler

RewriteExpression = Callable[[sqlglot.Expression], sqlglot.Expression]

class SqlJobRequestHandler(BaseMessageHandler):
    message_type: MessageType = MessageType.SQL_JOB_REQUEST

    def __init__(
        self,
        sql_job_repository: SqlJobRepository,
        sql_job_result_artifact_repository: SqlJobResultArtifactRepository,
        connector_repository: ConnectorRepository,
        dataset_repository: DatasetRepository | None = None,
        secret_provider_registry: SecretProviderRegistry | None = None,
        federated_query_tool: FederatedQueryTool | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._sql_job_repository = sql_job_repository
        self._sql_job_result_artifact_repository = sql_job_result_artifact_repository
        self._connector_repository = connector_repository
        self._dataset_repository = dataset_repository
        self._secret_provider_registry = secret_provider_registry or SecretProviderRegistry()
        self._sql_connector_factory = SqlConnectorFactory()
        self._federated_query_tool = federated_query_tool
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=self._dataset_repository,
        )

    async def handle(self, payload: SqlJobRequestMessage) -> None:
        request = self._parse_request(payload)
        job: SqlJobRecord = await self._sql_job_repository.get_by_id_for_workspace(
            sql_job_id=request.sql_job_id,
            workspace_id=request.workspace_id,
        )
        if job is None:
            raise BusinessValidationError("SQL job not found.")

        if job.status in {"succeeded", "failed", "cancelled"}:
            self._logger.info("SQL job %s already terminal (%s).", job.id, job.status)
            return None

        job.status = "running"
        if job.started_at is None:
            job.started_at = datetime.now(timezone.utc)
        job.updated_at = datetime.now(timezone.utc)

        try:
            if request.execution_mode == "federated":
                await self._execute_federated(job, request)
            else:
                await self._execute_single(job, request)
        except Exception as exc:
            self._logger.exception("SQL job %s failed: %s", job.id, exc)
            if job.status != "cancelled":
                job.status = "failed"
                job.error_json = {
                    "message": sanitize_sql_error_message(str(exc)),
                    "correlation_id": request.correlation_id,
                }
                job.finished_at = datetime.now(timezone.utc)
                job.updated_at = datetime.now(timezone.utc)

        return None

    def _parse_request(self, payload: SqlJobRequestMessage) -> CreateSqlJobRequest:
        try:
            return CreateSqlJobRequest.model_validate(payload.job_request)
        except ValidationError as exc:
            raise BusinessValidationError("Invalid SQL job request payload.") from exc

    async def _execute_single(
        self,
        job: SqlJobRecord,
        request: CreateSqlJobRequest,
    ) -> None:
        if request.connection_id is None:
            raise BusinessValidationError("connection_id is required for single datasource SQL jobs.")

        connector = await self._connector_repository.get_by_id(request.connection_id)
        if connector is None:
            raise BusinessValidationError("SQL connector not found.")

        connector_response = ConnectorResponse.from_connector(
            connector,
            organization_id=request.workspace_id,
            project_id=request.project_id,
        )
        if connector_response.connector_type is None:
            raise BusinessValidationError("Connector type is missing.")

        connector_type = ConnectorRuntimeType(connector_response.connector_type.upper())
        connector_sqlglot_dialect = self._sqlglot_dialect_for_connector(connector_type)
        sql_connector = await self._create_sql_connector(
            connector_type=connector_type,
            connector_payload=self._resolve_connector_config(connector_response),
        )

        source_sqlglot_dialect = normalize_sql_dialect(request.query_dialect, default="tsql")
        rendered_query = render_sql_with_params(request.query, request.params)
        enforce_read_only_sql(
            rendered_query,
            allow_dml=request.allow_dml,
            dialect=source_sqlglot_dialect,
        )
        enforce_table_allowlist(
            rendered_query,
            allowed_schemas=request.allowed_schemas,
            allowed_tables=request.allowed_tables,
            dialect=source_sqlglot_dialect,
        )
        executable_query = transpile_sql(
            rendered_query,
            source_dialect=source_sqlglot_dialect,
            target_dialect=connector_sqlglot_dialect,
        )

        if request.explain:
            await self._store_explain_result(
                job,
                request,
                executable_query,
                source_dialect=source_sqlglot_dialect,
                target_dialect=connector_sqlglot_dialect,
            )
            return

        executable_sql, effective_limit = enforce_preview_limit(
            executable_query,
            max_rows=request.enforced_limit,
            dialect=connector_sqlglot_dialect,
        )
        result = await sql_connector.execute(
            executable_sql,
            params={},
            max_rows=effective_limit,
            timeout_s=request.enforced_timeout_seconds,
        )

        rows: list[dict[str, Any]] = []
        for raw_row in result.rows:
            row = {
                str(column): raw_row[index] if index < len(raw_row) else None
                for index, column in enumerate(result.columns)
            }
            rows.append(row)

        redacted_rows, redaction_applied = apply_result_redaction(
            rows=rows,
            redaction_rules=request.redaction_rules,
        )
        columns_payload = [{"name": str(column), "type": None} for column in result.columns]

        now = datetime.now(timezone.utc)
        job.status = "succeeded"
        job.result_columns_json = columns_payload
        job.result_rows_json = redacted_rows
        job.row_count_preview = len(redacted_rows)
        job.total_rows_estimate = None
        job.bytes_scanned = None
        job.duration_ms = result.elapsed_ms
        job.result_cursor = "0"
        job.redaction_applied = redaction_applied
        job.error_json = None
        job.finished_at = now
        job.updated_at = now
        job.stats_json = {
            "rows_returned": len(redacted_rows),
            "duration_ms": result.elapsed_ms,
            "query_sql": executable_sql,
        }

        self._store_preview_artifact(
            job=job,
            columns_payload=columns_payload,
            rows=redacted_rows,
            now=now,
        )
        
    async def _execute_federated(
        self,
        job: SqlJobRecord,
        request: CreateSqlJobRequest,
    ) -> None:
        if not settings.SQL_FEDERATION_ENABLED or not request.allow_federation:
            raise BusinessValidationError("Federated SQL execution is disabled.")
        if self._federated_query_tool is None:
            raise BusinessValidationError("Federated query tool is not configured on this worker.")

        source_sqlglot_dialect = normalize_sql_dialect(request.query_dialect, default="tsql")
        rendered_query = render_sql_with_params(request.query, request.params)
        enforce_read_only_sql(
            rendered_query,
            allow_dml=request.allow_dml,
            dialect=source_sqlglot_dialect,
        )
        enforce_table_allowlist(
            rendered_query,
            allowed_schemas=request.allowed_schemas,
            allowed_tables=request.allowed_tables,
            dialect=source_sqlglot_dialect,
        )
        executable_sql, _ = enforce_preview_limit(
            rendered_query,
            max_rows=request.enforced_limit,
            dialect=source_sqlglot_dialect,
        )
        workflow, source_aliases = await self._build_federated_workflow(
            workspace_id=request.workspace_id,
            query=executable_sql,
            source_dialect=source_sqlglot_dialect,
            federated_datasets=[
                dataset.model_dump(mode="json") if hasattr(dataset, "model_dump") else dict(dataset)
                for dataset in (request.selected_datasets or request.federated_datasets)
            ],
            job=job,
        )
        tool_payload = {
            "workspace_id": str(request.workspace_id),
            "query": executable_sql,
            "dialect": source_sqlglot_dialect,
            "workflow": workflow.model_dump(mode="json"),
        }

        if request.explain:
            explain = await self._federated_query_tool.explain_federated_query(tool_payload)
            self._store_federated_explain_result(
                job=job,
                request=request,
                explain_payload=explain,
                query_sql=executable_sql,
                source_dialect=source_sqlglot_dialect,
                workflow=workflow,
            )
            return

        execution = await self._federated_query_tool.execute_federated_query(tool_payload)
        rows = self._extract_execution_rows(execution)
        redacted_rows, redaction_applied = apply_result_redaction(
            rows=rows,
            redaction_rules=request.redaction_rules,
        )
        columns_payload = self._extract_execution_columns(execution, redacted_rows)
        execution_meta = self._extract_execution_meta(execution)

        now = datetime.now(timezone.utc)
        job.status = "succeeded"
        job.result_columns_json = columns_payload
        job.result_rows_json = redacted_rows
        job.row_count_preview = len(redacted_rows)
        job.total_rows_estimate = None
        job.bytes_scanned = execution_meta["bytes_scanned"]
        job.duration_ms = execution_meta["duration_ms"]
        job.result_cursor = "0"
        job.redaction_applied = redaction_applied
        job.error_json = None
        job.finished_at = now
        job.updated_at = now
        job.stats_json = {
            "rows_returned": len(redacted_rows),
            "duration_ms": execution_meta["duration_ms"],
            "bytes_scanned": execution_meta["bytes_scanned"],
            "query_sql": executable_sql,
            "federated": True,
            "workflow_id": workflow.id,
            "source_aliases": source_aliases,
        }
        self._store_preview_artifact(
            job=job,
            columns_payload=columns_payload,
            rows=redacted_rows,
            now=now,
        )

    async def _store_explain_result(
        self,
        job: SqlJobRecord,
        request: CreateSqlJobRequest,
        rendered_query: str,
        *,
        source_dialect: str,
        target_dialect: str,
    ) -> None:
        try:
            expression = sqlglot.parse_one(rendered_query, read=target_dialect)
            normalized_sql = expression.sql(dialect=target_dialect)
            table_refs = [
                {
                    "schema": (table.db or None),
                    "table": table.name,
                }
                for table in expression.find_all(sqlglot.exp.Table)
            ]
        except sqlglot.ParseError as exc:
            raise BusinessValidationError(f"EXPLAIN parse failed: {exc}") from exc

        now = datetime.now(timezone.utc)
        job.status = "succeeded"
        job.result_columns_json = [
            {"name": "section", "type": "string"},
            {"name": "value", "type": "string"},
        ]
        job.result_rows_json = [
            {"section": "mode", "value": "logical"},
            {"section": "source_dialect", "value": source_dialect},
            {"section": "target_dialect", "value": target_dialect},
            {"section": "normalized_sql", "value": normalized_sql},
            {"section": "table_count", "value": str(len(table_refs))},
        ]
        job.row_count_preview = len(job.result_rows_json)
        job.total_rows_estimate = None
        job.bytes_scanned = None
        job.duration_ms = 0
        job.result_cursor = "0"
        job.redaction_applied = False
        job.error_json = None
        job.finished_at = now
        job.updated_at = now
        job.stats_json = {
            "explain": {
                "mode": "logical",
                "tables": table_refs,
                "query_hash": job.query_hash,
            }
        }

    async def _create_sql_connector(
        self,
        *,
        connector_type: ConnectorRuntimeType,
        connector_payload: dict[str, Any],
    ):
        dialect = ConnectorRuntimeTypeSqlDialectMap.get(connector_type)
        if dialect is None:
            raise BusinessValidationError(
                f"Connector type {connector_type.value} does not support SQL execution."
            )
        config_factory = get_connector_config_factory(connector_type)
        config_instance = config_factory.create(connector_payload.get("config", {}))
        sql_connector = self._sql_connector_factory.create_sql_connector(
            dialect,
            config_instance,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    def _resolve_connector_config(self, connector: ConnectorResponse) -> dict[str, Any]:
        resolved_payload = dict(connector.config or {})
        runtime_config = dict(resolved_payload.get("config") or {})

        if connector.connection_metadata is not None:
            metadata = connector.connection_metadata.model_dump(exclude_none=True)
            extra = metadata.pop("extra", {})
            for key, value in metadata.items():
                runtime_config.setdefault(key, value)
            if isinstance(extra, dict):
                for key, value in extra.items():
                    if value is not None:
                        runtime_config.setdefault(key, value)

        for secret_name, secret_ref in connector.secret_references.items():
            try:
                runtime_config[secret_name] = self._secret_provider_registry.resolve(secret_ref)
            except Exception as exc:  # pragma: no cover
                raise BusinessValidationError(
                    f"Unable to resolve connector secret '{secret_name}'."
                ) from exc

        resolved_payload["config"] = runtime_config
        return resolved_payload

    @staticmethod
    def _sqlglot_dialect_for_connector(connector_type: ConnectorRuntimeType) -> str:
        connector_map = {
            ConnectorRuntimeType.POSTGRES: "postgres",
            ConnectorRuntimeType.MYSQL: "mysql",
            ConnectorRuntimeType.MARIADB: "mysql",
            ConnectorRuntimeType.SNOWFLAKE: "snowflake",
            ConnectorRuntimeType.REDSHIFT: "redshift",
            ConnectorRuntimeType.BIGQUERY: "bigquery",
            ConnectorRuntimeType.SQLSERVER: "tsql",
            ConnectorRuntimeType.ORACLE: "oracle",
            ConnectorRuntimeType.SQLITE: "sqlite",
        }
        return connector_map.get(connector_type, "tsql")

    @staticmethod
    def _transpile(
        tree: exp.Select,
        *,
        dialect: str,
        rewrite_expression: RewriteExpression | None = None,
    ) -> str:
        if rewrite_expression:
            rewritten = tree.transform(lambda node: rewrite_expression(node))
            return rewritten.sql(dialect=dialect)
        return tree.sql(dialect=dialect)

    def _store_preview_artifact(
        self,
        *,
        job: SqlJobRecord,
        columns_payload: list[dict[str, Any]],
        rows: list[dict[str, Any]],
        now: datetime,
    ) -> None:
        snapshot_artifact_id = uuid.uuid4()
        snapshot_artifact = SqlJobResultArtifactRecord(
            id=snapshot_artifact_id,
            sql_job_id=job.id,
            workspace_id=job.workspace_id,
            created_by=job.user_id,
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
        self._sql_job_result_artifact_repository.add(snapshot_artifact)

    async def _build_federated_workflow(
        self,
        *,
        workspace_id: uuid.UUID,
        query: str,
        source_dialect: str,
        federated_datasets: list[dict[str, Any]],
        job: SqlJobRecord,
    ) -> tuple[FederationWorkflow, list[str]]:
        return await self._build_dataset_federated_workflow(
            workspace_id=workspace_id,
            query=query,
            source_dialect=source_dialect,
            federated_datasets=federated_datasets,
            job=job,
        )

    async def _build_dataset_federated_workflow(
        self,
        *,
        workspace_id: uuid.UUID,
        query: str,
        source_dialect: str,
        federated_datasets: list[dict[str, Any]],
        job: SqlJobRecord,
    ) -> tuple[FederationWorkflow, list[str]]:
        if self._dataset_repository is None:
            raise BusinessValidationError("Dataset repository is required for dataset-backed federated SQL.")

        dataset_map = self._normalize_federated_datasets(federated_datasets)
        datasets = await self._dataset_repository.get_by_ids_for_workspace(
            workspace_id=workspace_id,
            dataset_ids=list(dataset_map.keys()),
        )
        datasets_by_id = {dataset.id: dataset for dataset in datasets}
        for dataset_id, selection in dataset_map.items():
            if dataset_id not in datasets_by_id:
                raise BusinessValidationError(
                    f"Federated dataset '{selection.get('legacy_alias') or selection.get('sql_alias') or dataset_id}' references unknown dataset '{dataset_id}'."
                )

        table_bindings = self._extract_dataset_federated_table_bindings(
            dataset_map=dataset_map,
            datasets_by_id=datasets_by_id,
        )
        workflow_id = f"workflow_sql_{job.id.hex[:12]}"
        dataset_id = f"dataset_sql_{job.id.hex[:12]}"
        dataset_name = f"sql_job_{job.id.hex[:8]}"
        return (
            FederationWorkflow(
                id=workflow_id,
                workspace_id=str(workspace_id),
                dataset=VirtualDataset(
                    id=dataset_id,
                    name=dataset_name,
                    workspace_id=str(workspace_id),
                    tables=table_bindings,
                    relationships=[],
                ),
                broadcast_threshold_bytes=settings.FEDERATION_BROADCAST_THRESHOLD_BYTES,
                partition_count=settings.FEDERATION_PARTITION_COUNT,
                max_stage_retries=settings.FEDERATION_STAGE_MAX_RETRIES,
                stage_parallelism=settings.FEDERATION_STAGE_PARALLELISM,
            ),
            sorted(
                str(getattr(datasets_by_id[dataset_id], "sql_alias", None) or selection.get("sql_alias") or "").strip().lower()
                for dataset_id, selection in dataset_map.items()
            ),
        )

    @staticmethod
    def _normalize_federated_datasets(
        federated_datasets: list[dict[str, Any]],
    ) -> dict[uuid.UUID, dict[str, str | uuid.UUID | None]]:
        dataset_map: dict[uuid.UUID, dict[str, str | uuid.UUID | None]] = {}
        for raw_item in federated_datasets or []:
            item = dict(raw_item or {})
            raw_dataset_id = item.get("dataset_id") or item.get("datasetId")
            try:
                dataset_id = uuid.UUID(str(raw_dataset_id))
            except (TypeError, ValueError) as exc:
                raise BusinessValidationError(
                    "Federated dataset entry has an invalid dataset id."
                ) from exc
            legacy_alias = str(item.get("alias") or "").strip() or None
            sql_alias = str(item.get("sql_alias") or item.get("sqlAlias") or "").strip().lower() or None
            existing = dataset_map.get(dataset_id)
            if existing is not None:
                existing_sql_alias = str(existing.get("sql_alias") or "").strip().lower() or None
                if existing_sql_alias and sql_alias and existing_sql_alias != sql_alias:
                    raise BusinessValidationError(
                        f"Federated dataset '{dataset_id}' maps to multiple SQL aliases."
                    )
            dataset_map[dataset_id] = {
                "dataset_id": dataset_id,
                "legacy_alias": legacy_alias,
                "sql_alias": sql_alias,
            }

        if not dataset_map:
            raise BusinessValidationError(
                "federated_datasets must include at least one dataset mapping."
            )
        return dataset_map

    def _extract_dataset_federated_table_bindings(
        self,
        *,
        dataset_map: dict[uuid.UUID, dict[str, str | uuid.UUID | None]],
        datasets_by_id: dict[uuid.UUID, Any],
    ) -> dict[str, VirtualTableBinding]:
        table_bindings: dict[str, VirtualTableBinding] = {}
        for dataset_id, dataset in datasets_by_id.items():
            selection = dataset_map[dataset_id]
            descriptor = self._dataset_execution_resolver._build_dataset_execution_descriptor(dataset)
            if not dataset_supports_structured_federation(
                source_kind=descriptor.source_kind,
                storage_kind=descriptor.storage_kind,
                capabilities=descriptor.execution_capabilities,
            ):
                raise BusinessValidationError(
                    f"Dataset '{dataset.name}' does not support federated structured execution."
                )
            sql_alias = str(getattr(dataset, "sql_alias", None) or selection.get("sql_alias") or "").strip().lower()
            if not sql_alias:
                raise BusinessValidationError(
                    f"Dataset '{dataset.name}' is missing a SQL alias."
                )
            binding, _dialect = self._dataset_execution_resolver._build_binding_from_dataset_record(
                dataset=dataset,
                table_key=sql_alias,
                logical_schema=None,
                logical_table_name=sql_alias,
                catalog_name=None,
            )
            binding = self._with_dataset_logical_alias(binding=binding, dataset_alias=sql_alias)
            table_bindings[binding.table_key] = binding

            legacy_alias = str(selection.get("legacy_alias") or "").strip()
            if legacy_alias and legacy_alias.lower() != sql_alias and dataset.table_name:
                legacy_key = ".".join(
                    part for part in (legacy_alias, dataset.schema_name, dataset.table_name) if part
                )
                legacy_binding, _dialect = self._dataset_execution_resolver._build_binding_from_dataset_record(
                    dataset=dataset,
                    table_key=legacy_key,
                    logical_schema=dataset.schema_name,
                    logical_table_name=dataset.table_name,
                    catalog_name=legacy_alias,
                )
                table_bindings[legacy_binding.table_key] = self._with_dataset_logical_alias(
                    binding=legacy_binding,
                    dataset_alias=sql_alias,
                )

        return table_bindings

    @staticmethod
    def _with_dataset_logical_alias(
        *,
        binding: VirtualTableBinding,
        dataset_alias: str | None,
    ) -> VirtualTableBinding:
        metadata = dict(binding.metadata or {})
        if dataset_alias:
            metadata["dataset_alias"] = dataset_alias
        return binding.model_copy(
            update={
                "metadata": metadata,
            }
        )

    @staticmethod
    def _extract_execution_columns(
        execution: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        raw_columns = execution.get("columns") if isinstance(execution, dict) else []
        columns = []
        if isinstance(raw_columns, list):
            columns = [str(column) for column in raw_columns if str(column).strip()]
        if not columns and rows:
            columns = [str(column) for column in rows[0].keys()]
        return [{"name": column, "type": None} for column in columns]

    @staticmethod
    def _extract_execution_rows(execution: dict[str, Any]) -> list[dict[str, Any]]:
        rows_payload = execution.get("rows") if isinstance(execution, dict) else []
        if rows_payload is None:
            return []
        if not isinstance(rows_payload, list):
            raise BusinessValidationError("Federated SQL execution returned an invalid rows payload.")

        columns_payload = execution.get("columns") if isinstance(execution, dict) else []
        columns: list[str] = []
        if isinstance(columns_payload, list):
            columns = [str(column) for column in columns_payload if str(column).strip()]

        rows: list[dict[str, Any]] = []
        for row in rows_payload:
            if isinstance(row, dict):
                if columns:
                    rows.append({column: row.get(column) for column in columns})
                else:
                    rows.append({str(key): value for key, value in row.items()})
                continue
            if isinstance(row, (list, tuple)):
                if not columns:
                    columns = [f"column_{index + 1}" for index in range(len(row))]
                rows.append(
                    {
                        columns[index] if index < len(columns) else f"column_{index + 1}": value
                        for index, value in enumerate(row)
                    }
                )
                continue
            if not columns:
                columns = ["value"]
            rows.append({columns[0]: row})
        return rows

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

    def _store_federated_explain_result(
        self,
        *,
        job: SqlJobRecord,
        request: CreateSqlJobRequest,
        explain_payload: dict[str, Any],
        query_sql: str,
        source_dialect: str,
        workflow: FederationWorkflow,
    ) -> None:
        logical_plan = explain_payload.get("logical_plan") if isinstance(explain_payload, dict) else {}
        physical_plan = explain_payload.get("physical_plan") if isinstance(explain_payload, dict) else {}
        logical_tables = logical_plan.get("tables") if isinstance(logical_plan, dict) else {}
        logical_joins = logical_plan.get("joins") if isinstance(logical_plan, dict) else []
        physical_stages = physical_plan.get("stages") if isinstance(physical_plan, dict) else []

        now = datetime.now(timezone.utc)
        job.status = "succeeded"
        job.result_columns_json = [
            {"name": "section", "type": "string"},
            {"name": "value", "type": "string"},
        ]
        job.result_rows_json = [
            {"section": "mode", "value": "federated"},
            {"section": "source_dialect", "value": source_dialect},
            {"section": "normalized_sql", "value": query_sql},
            {
                "section": "source_alias_count",
                "value": str(len(request.selected_datasets or request.federated_datasets)),
            },
            {"section": "table_count", "value": str(len(logical_tables) if isinstance(logical_tables, dict) else 0)},
            {"section": "join_count", "value": str(len(logical_joins) if isinstance(logical_joins, list) else 0)},
            {"section": "stage_count", "value": str(len(physical_stages) if isinstance(physical_stages, list) else 0)},
        ]
        job.row_count_preview = len(job.result_rows_json)
        job.total_rows_estimate = None
        job.bytes_scanned = None
        job.duration_ms = 0
        job.result_cursor = "0"
        job.redaction_applied = False
        job.error_json = None
        job.finished_at = now
        job.updated_at = now
        job.stats_json = {
            "explain": {
                "mode": "federated",
                "query_hash": job.query_hash,
                "workflow": workflow.model_dump(mode="json"),
                "plan": explain_payload,
            }
        }
