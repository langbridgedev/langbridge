from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

import sqlglot
from sqlglot import exp
from pydantic import ValidationError

from langbridge.apps.worker.langbridge_worker.secrets import SecretProviderRegistry
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
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlJobRepository,
    SqlJobResultArtifactRepository,
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
        secret_provider_registry: SecretProviderRegistry | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._sql_job_repository = sql_job_repository
        self._sql_job_result_artifact_repository = sql_job_result_artifact_repository
        self._connector_repository = connector_repository
        self._secret_provider_registry = secret_provider_registry or SecretProviderRegistry()
        self._sql_connector_factory = SqlConnectorFactory()

    async def handle(self, payload: SqlJobRequestMessage) -> None:
        request = self._parse_request(payload)
        job = await self._sql_job_repository.get_by_id_for_workspace(
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
                await self._execute_federated_stub(job, request)
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

        snapshot_artifact_id = uuid.uuid4()
        snapshot_artifact = SqlJobResultArtifactRecord(
            id=snapshot_artifact_id,
            sql_job_id=job.id,
            workspace_id=job.workspace_id,
            created_by=job.user_id,
            format="json_preview",
            mime_type="application/json",
            row_count=len(redacted_rows),
            byte_size=None,
            storage_backend="inline",
            storage_reference=f"inline://{snapshot_artifact_id}",
            payload_json={
                "columns": columns_payload,
                "rows": redacted_rows,
            },
            created_at=now,
        )
        self._sql_job_result_artifact_repository.add(snapshot_artifact)

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

    async def _execute_federated_stub(
        self,
        job: SqlJobRecord,
        request: CreateSqlJobRequest,
    ) -> None:
        if not settings.SQL_FEDERATION_ENABLED or not request.allow_federation:
            raise BusinessValidationError("Federated SQL execution is disabled.")
        raise BusinessValidationError(
            "Federated SQL interface is available, but no federated executor is configured."
        )

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
            ConnectorRuntimeType.SNOWFLAKE: "snowflake",
            ConnectorRuntimeType.REDSHIFT: "redshift",
            ConnectorRuntimeType.BIGQUERY: "bigquery",
            ConnectorRuntimeType.SQLSERVER: "tsql",
            ConnectorRuntimeType.ORACLE: "oracle",
            ConnectorRuntimeType.SQLITE: "sqlite",
            ConnectorRuntimeType.TRINO: "trino",
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
