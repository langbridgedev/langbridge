import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.runtime.execution import FederatedQueryTool
from langbridge.runtime.models import CreateSqlJobRequest, SqlJob
from langbridge.runtime.ports import DatasetCatalogStore, SqlJobArtifactStore
from langbridge.runtime.providers import (
    ConnectorMetadataProvider,
    CredentialProvider,
    DatasetMetadataProvider,
    SecretRegistryCredentialProvider,
)
from langbridge.runtime.security import SecretProviderRegistry
from langbridge.runtime.services.dataset_execution import DatasetExecutionResolver
from langbridge.runtime.services.errors import ExecutionValidationError
from langbridge.runtime.services.sql_query.artifacts import SqlJobArtifactWriter
from langbridge.runtime.services.sql_query.connectors import SqlConnectorRuntimeFactory
from langbridge.runtime.services.sql_query.explain import SqlExplainResultWriter
from langbridge.runtime.services.sql_query.federation import SqlFederatedWorkflowBuilder
from langbridge.runtime.services.sql_query.results import SqlExecutionResultParser
from langbridge.runtime.services.sql_query.types import (
    CreateSqlConnector,
    ResolveConnectorConfig,
)
from langbridge.runtime.settings import runtime_settings as settings
from langbridge.runtime.utils.sql import (
    apply_result_redaction,
    enforce_preview_limit,
    enforce_read_only_sql,
    enforce_table_allowlist,
    normalize_sql_dialect,
    render_sql_with_params,
    sanitize_sql_error_message,
    transpile_sql,
)


class SqlQueryService:
    """Executes single-connector and dataset-federated SQL jobs."""

    def __init__(
        self,
        dataset_repository: DatasetCatalogStore,
        secret_provider_registry: SecretProviderRegistry,
        federated_query_tool: FederatedQueryTool,
        connector_provider: ConnectorMetadataProvider,
        dataset_provider: DatasetMetadataProvider,
        credential_provider: CredentialProvider,
        sql_job_result_artifact_store: SqlJobArtifactStore | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._dataset_repository = dataset_repository
        self._dataset_provider = dataset_provider
        self._federated_query_tool = federated_query_tool
        self._secret_provider_registry = secret_provider_registry or SecretProviderRegistry()
        self._credential_provider = credential_provider or SecretRegistryCredentialProvider(
            registry=self._secret_provider_registry
        )
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=self._dataset_repository,
            dataset_provider=self._dataset_provider,
        )

        self._artifacts = SqlJobArtifactWriter(
            artifact_store=sql_job_result_artifact_store,
        )
        self._connectors = SqlConnectorRuntimeFactory(
            connector_provider=connector_provider,
            credential_provider=self._credential_provider,
            logger=self._logger,
        )
        self._federation = SqlFederatedWorkflowBuilder(
            dataset_repository=dataset_repository,
            dataset_provider=dataset_provider,
            dataset_execution_resolver=self._dataset_execution_resolver,
        )
        self._explain = SqlExplainResultWriter(federation=self._federation)
        self._results = SqlExecutionResultParser()

    async def execute_sql(
        self,
        *,
        request: CreateSqlJobRequest,
        job: SqlJob | None = None,
        create_sql_connector: CreateSqlConnector | None = None,
        resolve_connector_config: ResolveConnectorConfig | None = None,
    ) -> dict[str, Any]:
        existing_job = job is not None
        runtime_job = job or self._artifacts.build_transient_job(request)
        if runtime_job.started_at is None:
            runtime_job.started_at = datetime.now(timezone.utc)
        runtime_job.status = "running"
        runtime_job.updated_at = datetime.now(timezone.utc)

        await self.execute_job(
            job=runtime_job,
            request=request,
            create_sql_connector=create_sql_connector,
            resolve_connector_config=resolve_connector_config,
        )
        if existing_job:
            return self._artifacts.result_payload(runtime_job) if runtime_job.status == "succeeded" else {}
        if runtime_job.status != "succeeded":
            message = None
            if isinstance(runtime_job.error_json, dict):
                message = runtime_job.error_json.get("message")
            raise ExecutionValidationError(str(message or "SQL execution failed."))
        return self._artifacts.result_payload(runtime_job)

    async def execute_job(
        self,
        *,
        job: SqlJob,
        request: CreateSqlJobRequest,
        create_sql_connector: CreateSqlConnector | None = None,
        resolve_connector_config: ResolveConnectorConfig | None = None,
    ) -> None:
        try:
            if request.execution_mode == "federated":
                await self._execute_federated(job, request)
            else:
                await self._execute_single(
                    job,
                    request,
                    create_sql_connector=create_sql_connector or self._create_sql_connector,
                    resolve_connector_config=resolve_connector_config or self._resolve_connector_config,
                )
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

    async def _execute_single(
        self,
        job: SqlJob,
        request: CreateSqlJobRequest,
        *,
        create_sql_connector: CreateSqlConnector,
        resolve_connector_config: ResolveConnectorConfig,
    ) -> None:
        if request.connection_id is None:
            raise ExecutionValidationError("connection_id is required for single datasource SQL jobs.")

        connector_response = await self._get_connector_response(
            connection_id=request.connection_id,
            workspace_id=request.workspace_id,
        )
        if connector_response is None:
            raise ExecutionValidationError("SQL connector not found.")
        if connector_response.connector_type is None:
            raise ExecutionValidationError("Connector type is missing.")

        connector_type = ConnectorRuntimeType(connector_response.connector_type.upper())
        connector_sqlglot_dialect = self._connectors.sqlglot_dialect_for_connector(connector_type)
        sql_connector = await create_sql_connector(
            connector_type=connector_type,
            connector_payload=resolve_connector_config(connector_response),
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
            self._explain.store_single_result(
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

        rows = [
            {
                str(column): raw_row[index] if index < len(raw_row) else None
                for index, column in enumerate(result.columns)
            }
            for raw_row in result.rows
        ]
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
        self._artifacts.store_preview_artifact(
            job=job,
            columns_payload=columns_payload,
            rows=redacted_rows,
            now=now,
        )

    async def _execute_federated(self, job: SqlJob, request: CreateSqlJobRequest) -> None:
        if not settings.SQL_FEDERATION_ENABLED or not request.allow_federation:
            raise ExecutionValidationError("Federated SQL execution is disabled.")
        if self._federated_query_tool is None:
            raise ExecutionValidationError("Federated query tool is not configured on this runtime node.")

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
        workflow, federated_datasets = await self._federation.build_workflow(
            workspace_id=request.workspace_id,
            query=executable_sql,
            source_dialect=source_sqlglot_dialect,
            selected_dataset_ids=list(request.selected_datasets or []),
            job=job,
        )
        request.federated_datasets = federated_datasets
        job.selected_datasets_json = [dataset.model_dump(mode="json") for dataset in federated_datasets]
        source_aliases = sorted(
            str(dataset.sql_alias or "").strip().lower()
            for dataset in federated_datasets
            if str(dataset.sql_alias or "").strip()
        )
        tool_payload = {
            "workspace_id": str(request.workspace_id),
            "query": executable_sql,
            "dialect": source_sqlglot_dialect,
            "workflow": workflow.model_dump(mode="json"),
        }

        if request.explain:
            explain = await self._federated_query_tool.explain_federated_query(tool_payload)
            self._explain.store_federated_result(
                job=job,
                request=request,
                explain_payload=explain,
                query_sql=executable_sql,
                source_dialect=source_sqlglot_dialect,
                workflow=workflow,
            )
            return

        execution = await self._federated_query_tool.execute_federated_query(tool_payload)
        rows = self._results.extract_rows(execution)
        redacted_rows, redaction_applied = apply_result_redaction(
            rows=rows,
            redaction_rules=request.redaction_rules,
        )
        columns_payload = self._results.extract_columns(execution, redacted_rows)
        execution_meta = self._results.extract_meta(execution)
        federation_diagnostics = self._federation.build_diagnostics(
            workflow=workflow,
            planning_payload=execution.get("planning") if isinstance(execution, dict) else None,
            execution_payload=execution.get("execution") if isinstance(execution, dict) else None,
        )

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
            "federation_diagnostics": (
                federation_diagnostics.model_dump(mode="json")
                if federation_diagnostics is not None
                else None
            ),
        }
        self._artifacts.store_preview_artifact(
            job=job,
            columns_payload=columns_payload,
            rows=redacted_rows,
            now=now,
        )

    async def _create_sql_connector(
        self,
        *,
        connector_type: ConnectorRuntimeType,
        connector_payload: dict[str, Any],
    ) -> Any:
        return await self._connectors.create_sql_connector(
            connector_type=connector_type,
            connector_payload=connector_payload,
        )

    async def _get_connector_response(
        self,
        *,
        connection_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> Any:
        return await self._connectors.get_connector(
            connection_id=connection_id,
            workspace_id=workspace_id,
        )

    def _resolve_connector_config(self, connector) -> dict[str, Any]:
        return self._connectors.resolve_connector_config(connector)

    async def _resolve_federated_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        selected_dataset_ids: list[uuid.UUID],
    ):
        return await self._federation.resolve_datasets(
            workspace_id=workspace_id,
            selected_dataset_ids=selected_dataset_ids,
        )
