from __future__ import annotations

import base64
import csv
import io
import uuid
from datetime import datetime, timezone
from typing import Any

import sqlglot

from langbridge.apps.api.langbridge_api.services.jobs.sql_job_request_service import (
    SqlJobRequestService,
)
from langbridge.apps.api.langbridge_api.services.request_context_provider import (
    RequestContextProvider,
)
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse
from langbridge.packages.common.langbridge_common.contracts.jobs.sql_job import (
    CreateSqlJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.sql import (
    SqlAssistMode,
    SqlAssistRequest,
    SqlAssistResponse,
    SqlCancelRequest,
    SqlCancelResponse,
    SqlColumnMetadata,
    SqlExecuteRequest,
    SqlExecuteResponse,
    SqlExecutionMode,
    SqlHistoryResponse,
    SqlJobResponse,
    SqlJobResultArtifactResponse,
    SqlJobResultsResponse,
    SqlJobStatus,
    SqlSavedQueryCreateRequest,
    SqlSavedQueryListResponse,
    SqlSavedQueryResponse,
    SqlSavedQueryUpdateRequest,
    SqlWorkspacePolicyBounds,
    SqlWorkspacePolicyResponse,
    SqlWorkspacePolicyUpdateRequest,
)
from langbridge.packages.common.langbridge_common.db.sql import (
    SqlJobRecord,
    SqlJobResultArtifactRecord,
    SqlSavedQueryRecord,
    SqlWorkspacePolicyRecord,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
    PermissionDeniedBusinessValidationError,
    QuotaExceededBusinessValidationError,
    ResourceNotFound,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.organization_repository import (
    OrganizationRepository,
)
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlJobRepository,
    SqlJobResultArtifactRepository,
    SqlSavedQueryRepository,
    SqlWorkspacePolicyRepository,
)
from langbridge.packages.common.langbridge_common.repositories.user_repository import (
    UserRepository,
)
from langbridge.packages.common.langbridge_common.utils.sql import (
    detect_sql_risk_hints,
    enforce_read_only_sql,
    enforce_table_allowlist,
    fingerprint_query,
    sanitize_sql_error_message,
)


class SqlService:
    def __init__(
        self,
        *,
        sql_job_repository: SqlJobRepository,
        sql_job_result_artifact_repository: SqlJobResultArtifactRepository,
        sql_saved_query_repository: SqlSavedQueryRepository,
        sql_workspace_policy_repository: SqlWorkspacePolicyRepository,
        connector_repository: ConnectorRepository,
        organization_repository: OrganizationRepository,
        user_repository: UserRepository,
        sql_job_request_service: SqlJobRequestService,
        request_context_provider: RequestContextProvider,
    ) -> None:
        self._sql_job_repository = sql_job_repository
        self._sql_job_result_artifact_repository = sql_job_result_artifact_repository
        self._sql_saved_query_repository = sql_saved_query_repository
        self._sql_workspace_policy_repository = sql_workspace_policy_repository
        self._connector_repository = connector_repository
        self._organization_repository = organization_repository
        self._user_repository = user_repository
        self._sql_job_request_service = sql_job_request_service
        self._request_context_provider = request_context_provider

    async def execute_sql(
        self,
        *,
        request: SqlExecuteRequest,
        current_user: UserResponse,
    ) -> SqlExecuteResponse:
        self._ensure_sql_feature_enabled()
        await self._assert_workspace_access(request.workspace_id, current_user)
        await self._assert_project_access(request.project_id, current_user)

        policy = await self._get_or_create_policy_record(request.workspace_id)
        if request.federated and not policy.allow_federation:
            raise PermissionDeniedBusinessValidationError(
                "Federated SQL execution is disabled for this workspace."
            )
        if request.federated and not settings.SQL_FEDERATION_ENABLED:
            raise BusinessValidationError(
                "Federated SQL execution is not enabled in this deployment."
            )

        connection_id = request.connection_id
        redaction_rules: dict[str, str] = {}
        if not request.federated:
            if connection_id is None:
                connection_id = policy.default_datasource_id
            if connection_id is None:
                raise BusinessValidationError(
                    "connection_id is required when no default datasource is configured."
                )
            connector = await self._connector_repository.get_by_id(connection_id)
            if connector is None:
                raise ResourceNotFound("Connection not found.")
            if not self._connector_is_in_workspace(connector, request.workspace_id):
                raise PermissionDeniedBusinessValidationError(
                    "Connection does not belong to this workspace."
                )
            connector_response = ConnectorResponse.from_connector(
                connector,
                organization_id=request.workspace_id,
                project_id=request.project_id,
            )
            if connector_response.connection_policy:
                redaction_rules = dict(connector_response.connection_policy.redaction_rules or {})

        active_jobs = await self._sql_job_repository.count_active_for_workspace(
            workspace_id=request.workspace_id
        )
        if active_jobs >= policy.max_concurrency:
            raise QuotaExceededBusinessValidationError(
                "Workspace SQL concurrency limit reached."
            )

        enforce_read_only_sql(
            request.query,
            allow_dml=policy.allow_dml,
            dialect=request.query_dialect.value,
        )
        enforce_table_allowlist(
            request.query,
            allowed_schemas=policy.allowed_schemas_json,
            allowed_tables=policy.allowed_tables_json,
            dialect=request.query_dialect.value,
        )

        effective_limit = min(
            request.requested_limit or policy.max_preview_rows,
            policy.max_preview_rows,
        )
        effective_timeout = min(
            request.requested_timeout_seconds or policy.max_runtime_seconds,
            policy.max_runtime_seconds,
        )

        risk_hints = detect_sql_risk_hints(request.query)
        warnings = list(risk_hints.get("warnings") or [])
        dangerous = risk_hints.get("dangerous_statements") or []
        if dangerous:
            warnings.append(
                f"Potentially dangerous statements detected: {', '.join(dangerous)}."
            )

        now = datetime.now(timezone.utc)
        sql_job_id = uuid.uuid4()
        sql_job = SqlJobRecord(
            id=sql_job_id,
            workspace_id=request.workspace_id,
            project_id=request.project_id,
            user_id=current_user.id,
            connection_id=connection_id,
            execution_mode=(
                SqlExecutionMode.federated.value
                if request.federated
                else SqlExecutionMode.single.value
            ),
            status=SqlJobStatus.queued.value,
            query_text=request.query,
            query_hash=fingerprint_query(request.query),
            query_params_json=dict(request.params or {}),
            requested_limit=request.requested_limit,
            enforced_limit=effective_limit,
            requested_timeout_seconds=request.requested_timeout_seconds,
            enforced_timeout_seconds=effective_timeout,
            is_explain=request.explain,
            is_federated=request.federated,
            correlation_id=self._request_context_provider.correlation_id,
            policy_snapshot_json={
                "allow_dml": policy.allow_dml,
                "allow_federation": policy.allow_federation,
                "allowed_schemas": policy.allowed_schemas_json,
                "allowed_tables": policy.allowed_tables_json,
                "max_preview_rows": policy.max_preview_rows,
                "max_export_rows": policy.max_export_rows,
                "max_runtime_seconds": policy.max_runtime_seconds,
                "max_concurrency": policy.max_concurrency,
                "redaction_rules": redaction_rules,
            },
            warning_json={
                "is_expensive": bool(risk_hints.get("is_expensive")),
                "warnings": warnings,
                "dangerous_statements": dangerous,
            },
            created_at=now,
            updated_at=now,
        )
        self._sql_job_repository.add(sql_job)

        try:
            await self._sql_job_request_service.dispatch_sql_job(
                CreateSqlJobRequest(
                    sql_job_id=sql_job_id,
                    workspace_id=request.workspace_id,
                    project_id=request.project_id,
                    user_id=current_user.id,
                    connection_id=connection_id,
                    execution_mode=(
                        SqlExecutionMode.federated.value
                        if request.federated
                        else SqlExecutionMode.single.value
                    ),
                    query=request.query,
                    query_dialect=request.query_dialect.value,
                    params=request.params,
                    requested_limit=request.requested_limit,
                    requested_timeout_seconds=request.requested_timeout_seconds,
                    enforced_limit=effective_limit,
                    enforced_timeout_seconds=effective_timeout,
                    allow_dml=policy.allow_dml,
                    allow_federation=policy.allow_federation,
                    allowed_schemas=list(policy.allowed_schemas_json or []),
                    allowed_tables=list(policy.allowed_tables_json or []),
                    redaction_rules=redaction_rules,
                    explain=request.explain,
                    correlation_id=self._request_context_provider.correlation_id,
                )
            )
        except Exception as exc:
            now = datetime.now(timezone.utc)
            sql_job.status = SqlJobStatus.failed.value
            sql_job.error_json = {
                "message": sanitize_sql_error_message(str(exc)),
                "correlation_id": self._request_context_provider.correlation_id,
            }
            sql_job.finished_at = now
            sql_job.updated_at = now
            raise BusinessValidationError(
                "Unable to enqueue SQL job for execution."
            ) from exc

        return SqlExecuteResponse(
            sql_job_id=sql_job_id,
            expensive_query=bool(risk_hints.get("is_expensive")),
            warnings=warnings,
        )

    async def cancel_sql_job(
        self,
        *,
        request: SqlCancelRequest,
        current_user: UserResponse,
    ) -> SqlCancelResponse:
        self._ensure_sql_feature_enabled()
        await self._assert_workspace_access(request.workspace_id, current_user)
        job = await self._sql_job_repository.get_by_id_for_workspace(
            sql_job_id=request.sql_job_id,
            workspace_id=request.workspace_id,
        )
        if job is None:
            raise ResourceNotFound("SQL job not found.")

        if job.status in {
            SqlJobStatus.succeeded.value,
            SqlJobStatus.failed.value,
            SqlJobStatus.cancelled.value,
        }:
            return SqlCancelResponse(
                accepted=True,
                status=SqlJobStatus(job.status),
            )

        if job.user_id != current_user.id and not await self._is_workspace_admin(
            request.workspace_id, current_user
        ):
            raise PermissionDeniedBusinessValidationError("You cannot cancel this SQL job.")

        job.status = SqlJobStatus.cancelled.value
        job.finished_at = datetime.now(timezone.utc)
        job.error_json = {"message": "Query cancelled by user."}
        job.updated_at = datetime.now(timezone.utc)
        return SqlCancelResponse(accepted=True, status=SqlJobStatus.cancelled)

    async def get_sql_job(
        self,
        *,
        sql_job_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> SqlJobResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        job = await self._sql_job_repository.get_by_id_for_workspace(
            sql_job_id=sql_job_id,
            workspace_id=workspace_id,
        )
        if job is None:
            raise ResourceNotFound("SQL job not found.")
        artifacts = await self._sql_job_result_artifact_repository.list_for_job(sql_job_id=job.id)
        return self._to_sql_job_response(job, artifacts=artifacts)

    async def get_sql_job_results(
        self,
        *,
        sql_job_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
        cursor: str | None = None,
        page_size: int = 100,
    ) -> SqlJobResultsResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        job = await self._sql_job_repository.get_by_id_for_workspace(
            sql_job_id=sql_job_id,
            workspace_id=workspace_id,
        )
        if job is None:
            raise ResourceNotFound("SQL job not found.")

        rows = list(job.result_rows_json or [])
        if page_size < 1:
            page_size = 1
        if page_size > 5000:
            page_size = 5000
        offset = 0
        if cursor:
            try:
                offset = max(0, int(cursor))
            except ValueError as exc:
                raise BusinessValidationError("Invalid cursor.") from exc
        page_rows = rows[offset : offset + page_size]
        next_cursor = str(offset + page_size) if offset + page_size < len(rows) else None
        artifacts = await self._sql_job_result_artifact_repository.list_for_job(sql_job_id=job.id)

        return SqlJobResultsResponse(
            sql_job_id=job.id,
            status=SqlJobStatus(job.status),
            columns=self._to_columns(job.result_columns_json or []),
            rows=[row for row in page_rows if isinstance(row, dict)],
            row_count_preview=job.row_count_preview,
            total_rows_estimate=job.total_rows_estimate,
            next_cursor=next_cursor,
            artifacts=[self._to_artifact_response(artifact) for artifact in artifacts],
        )

    async def download_sql_job_results(
        self,
        *,
        sql_job_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
        export_format: str,
    ) -> tuple[bytes, str, str]:
        await self._assert_workspace_access(workspace_id, current_user)
        job = await self._sql_job_repository.get_by_id_for_workspace(
            sql_job_id=sql_job_id,
            workspace_id=workspace_id,
        )
        if job is None:
            raise ResourceNotFound("SQL job not found.")
        if job.status != SqlJobStatus.succeeded.value:
            raise BusinessValidationError("Only succeeded SQL jobs can be exported.")

        policy = await self._get_or_create_policy_record(workspace_id)
        rows = [row for row in (job.result_rows_json or []) if isinstance(row, dict)]
        columns = [column.name for column in self._to_columns(job.result_columns_json or [])]
        max_rows = min(policy.max_export_rows, len(rows))
        export_rows = rows[:max_rows]
        now = datetime.now(timezone.utc)

        if export_format == "csv":
            content = self._build_csv(columns=columns, rows=export_rows).encode("utf-8")
            mime_type = "text/csv"
            extension = "csv"
        elif export_format == "parquet":
            content = self._build_parquet(columns=columns, rows=export_rows)
            mime_type = "application/octet-stream"
            extension = "parquet"
        else:
            raise BusinessValidationError("Unsupported export format. Use 'csv' or 'parquet'.")

        artifact_id = uuid.uuid4()
        artifact = SqlJobResultArtifactRecord(
            id=artifact_id,
            sql_job_id=job.id,
            workspace_id=workspace_id,
            created_by=current_user.id,
            format=export_format,
            mime_type=mime_type,
            row_count=len(export_rows),
            byte_size=len(content),
            storage_backend="inline",
            storage_reference=f"inline://{artifact_id}",
            payload_json={
                "base64": base64.b64encode(content).decode("ascii"),
            }
            if len(content) <= 5_000_000
            else None,
            created_at=now,
        )
        self._sql_job_result_artifact_repository.add(artifact)
        file_name = f"sql_job_{job.id}.{extension}"
        return content, mime_type, file_name

    async def list_history(
        self,
        *,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
        scope: str = "user",
        limit: int = 100,
    ) -> SqlHistoryResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        normalized_scope = (scope or "user").strip().lower()
        if normalized_scope not in {"user", "workspace"}:
            raise BusinessValidationError("scope must be 'user' or 'workspace'.")
        if limit < 1:
            limit = 1
        if limit > 500:
            limit = 500

        user_id: uuid.UUID | None = current_user.id
        if normalized_scope == "workspace":
            if not await self._is_workspace_admin(workspace_id, current_user):
                raise PermissionDeniedBusinessValidationError(
                    "Workspace history scope requires admin permissions."
                )
            user_id = None

        jobs = await self._sql_job_repository.list_history(
            workspace_id=workspace_id,
            user_id=user_id,
            limit=limit,
        )
        items: list[SqlJobResponse] = []
        for job in jobs:
            artifacts = await self._sql_job_result_artifact_repository.list_for_job(sql_job_id=job.id)
            items.append(self._to_sql_job_response(job, artifacts=artifacts))
        return SqlHistoryResponse(items=items)

    async def create_saved_query(
        self,
        *,
        request: SqlSavedQueryCreateRequest,
        current_user: UserResponse,
    ) -> SqlSavedQueryResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        await self._assert_project_access(request.project_id, current_user)
        now = datetime.now(timezone.utc)
        record = SqlSavedQueryRecord(
            id=uuid.uuid4(),
            workspace_id=request.workspace_id,
            project_id=request.project_id,
            created_by=current_user.id,
            updated_by=current_user.id,
            connection_id=request.connection_id,
            name=request.name.strip(),
            description=(request.description.strip() if request.description else None),
            query_text=request.query,
            query_hash=fingerprint_query(request.query),
            tags_json=list(request.tags or []),
            default_params_json=dict(request.default_params or {}),
            is_shared=bool(request.is_shared),
            last_sql_job_id=request.last_sql_job_id,
            created_at=now,
            updated_at=now,
        )
        if request.last_sql_job_id:
            job = await self._sql_job_repository.get_by_id_for_workspace(
                sql_job_id=request.last_sql_job_id,
                workspace_id=request.workspace_id,
            )
            if job is None:
                raise ResourceNotFound("Referenced SQL job was not found.")
            artifacts = await self._sql_job_result_artifact_repository.list_for_job(sql_job_id=job.id)
            if artifacts:
                record.last_result_artifact_id = artifacts[0].id

        self._sql_saved_query_repository.add(record)
        return self._to_saved_query_response(record)

    async def list_saved_queries(
        self,
        *,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> SqlSavedQueryListResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        records = await self._sql_saved_query_repository.list_for_workspace(
            workspace_id=workspace_id,
            user_id=current_user.id,
            include_shared=True,
        )
        return SqlSavedQueryListResponse(
            items=[self._to_saved_query_response(record) for record in records]
        )

    async def get_saved_query(
        self,
        *,
        saved_query_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> SqlSavedQueryResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        record = await self._sql_saved_query_repository.get_for_workspace(
            saved_query_id=saved_query_id,
            workspace_id=workspace_id,
        )
        if record is None:
            raise ResourceNotFound("Saved SQL query not found.")
        if not await self._can_view_saved_query(record, current_user):
            raise PermissionDeniedBusinessValidationError("You cannot access this saved SQL query.")
        return self._to_saved_query_response(record)

    async def update_saved_query(
        self,
        *,
        saved_query_id: uuid.UUID,
        request: SqlSavedQueryUpdateRequest,
        current_user: UserResponse,
    ) -> SqlSavedQueryResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        record = await self._sql_saved_query_repository.get_for_workspace(
            saved_query_id=saved_query_id,
            workspace_id=request.workspace_id,
        )
        if record is None:
            raise ResourceNotFound("Saved SQL query not found.")
        if not await self._can_manage_saved_query(record, current_user):
            raise PermissionDeniedBusinessValidationError("You cannot update this saved SQL query.")

        if request.project_id is not None:
            await self._assert_project_access(request.project_id, current_user)
            record.project_id = request.project_id
        if request.connection_id is not None:
            connector = await self._connector_repository.get_by_id(request.connection_id)
            if connector is None:
                raise ResourceNotFound("Connection not found.")
            if not self._connector_is_in_workspace(connector, request.workspace_id):
                raise PermissionDeniedBusinessValidationError(
                    "Connection does not belong to this workspace."
                )
            record.connection_id = request.connection_id
        if request.name is not None:
            record.name = request.name.strip()
        if request.description is not None:
            record.description = request.description.strip() or None
        if request.query is not None:
            record.query_text = request.query
            record.query_hash = fingerprint_query(request.query)
        if request.tags is not None:
            record.tags_json = list(request.tags)
        if request.default_params is not None:
            record.default_params_json = dict(request.default_params)
        if request.is_shared is not None:
            record.is_shared = bool(request.is_shared)
        if request.last_sql_job_id is not None:
            job = await self._sql_job_repository.get_by_id_for_workspace(
                sql_job_id=request.last_sql_job_id,
                workspace_id=request.workspace_id,
            )
            if job is None:
                raise ResourceNotFound("Referenced SQL job was not found.")
            record.last_sql_job_id = job.id
            artifacts = await self._sql_job_result_artifact_repository.list_for_job(sql_job_id=job.id)
            record.last_result_artifact_id = artifacts[0].id if artifacts else None

        record.updated_by = current_user.id
        record.updated_at = datetime.now(timezone.utc)
        return self._to_saved_query_response(record)

    async def delete_saved_query(
        self,
        *,
        saved_query_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> None:
        await self._assert_workspace_access(workspace_id, current_user)
        record = await self._sql_saved_query_repository.get_for_workspace(
            saved_query_id=saved_query_id,
            workspace_id=workspace_id,
        )
        if record is None:
            raise ResourceNotFound("Saved SQL query not found.")
        if not await self._can_manage_saved_query(record, current_user):
            raise PermissionDeniedBusinessValidationError("You cannot delete this saved SQL query.")
        await self._sql_saved_query_repository.delete(record)

    async def get_workspace_policy(
        self,
        *,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> SqlWorkspacePolicyResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        policy = await self._get_or_create_policy_record(workspace_id)
        return self._to_policy_response(policy)

    async def update_workspace_policy(
        self,
        *,
        request: SqlWorkspacePolicyUpdateRequest,
        current_user: UserResponse,
    ) -> SqlWorkspacePolicyResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        if not await self._is_workspace_admin(request.workspace_id, current_user):
            raise PermissionDeniedBusinessValidationError(
                "Workspace SQL policy updates require admin permissions."
            )

        policy = await self._get_or_create_policy_record(request.workspace_id)
        bounds = self._policy_bounds()

        if request.max_preview_rows is not None:
            if request.max_preview_rows > bounds.max_preview_rows_upper_bound:
                raise BusinessValidationError("max_preview_rows exceeds allowed upper bound.")
            policy.max_preview_rows = request.max_preview_rows
        if request.max_export_rows is not None:
            if request.max_export_rows > bounds.max_export_rows_upper_bound:
                raise BusinessValidationError("max_export_rows exceeds allowed upper bound.")
            policy.max_export_rows = request.max_export_rows
        if request.max_runtime_seconds is not None:
            if request.max_runtime_seconds > bounds.max_runtime_seconds_upper_bound:
                raise BusinessValidationError("max_runtime_seconds exceeds allowed upper bound.")
            policy.max_runtime_seconds = request.max_runtime_seconds
        if request.max_concurrency is not None:
            if request.max_concurrency > bounds.max_concurrency_upper_bound:
                raise BusinessValidationError("max_concurrency exceeds allowed upper bound.")
            policy.max_concurrency = request.max_concurrency
        if request.allow_dml is not None:
            policy.allow_dml = request.allow_dml
        if request.allow_federation is not None:
            policy.allow_federation = request.allow_federation
        if request.allowed_schemas is not None:
            policy.allowed_schemas_json = list(request.allowed_schemas)
        if request.allowed_tables is not None:
            policy.allowed_tables_json = list(request.allowed_tables)
        if "default_datasource" in request.model_fields_set:
            if request.default_datasource is not None:
                connector = await self._connector_repository.get_by_id(request.default_datasource)
                if connector is None:
                    raise ResourceNotFound("Default datasource connector not found.")
                if not self._connector_is_in_workspace(connector, request.workspace_id):
                    raise PermissionDeniedBusinessValidationError(
                        "Default datasource connector must belong to the workspace."
                    )
            policy.default_datasource_id = request.default_datasource
        if "budget_limit_bytes" in request.model_fields_set:
            policy.budget_limit_bytes = request.budget_limit_bytes

        policy.updated_by_user_id = current_user.id
        policy.updated_at = datetime.now(timezone.utc)
        return self._to_policy_response(policy)

    async def assist_sql(
        self,
        *,
        request: SqlAssistRequest,
        current_user: UserResponse,
    ) -> SqlAssistResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        if not settings.SQL_AI_HELPER_ENABLED:
            raise BusinessValidationError("SQL AI helper is disabled.")

        if request.mode == SqlAssistMode.generate:
            suggestion = (
                f"-- Prompt: {request.prompt.strip()}\n"
                "SELECT TOP 100\n"
                "    <columns>\n"
                "FROM <schema>.<table>\n"
                "WHERE <conditions>\n"
                "ORDER BY <column> DESC;"
            )
            return SqlAssistResponse(mode=request.mode, suggestion=suggestion, warnings=[])

        if request.mode == SqlAssistMode.fix:
            source = (request.query or request.prompt).strip()
            if not source:
                raise BusinessValidationError("query is required for fix mode.")
            try:
                expression = sqlglot.parse_one(source, read="tsql")
                suggestion = expression.sql(dialect="tsql")
            except sqlglot.ParseError as exc:
                raise BusinessValidationError(f"Unable to parse SQL: {exc}") from exc
            return SqlAssistResponse(mode=request.mode, suggestion=suggestion, warnings=[])

        if request.mode == SqlAssistMode.explain:
            source = (request.query or request.prompt).strip()
            if not source:
                raise BusinessValidationError("query is required for explain mode.")
            hints = detect_sql_risk_hints(source)
            warning_lines = hints.get("warnings") or []
            dangerous = hints.get("dangerous_statements") or []
            summary = (
                "This query reads tabular data and may include joins/filter predicates. "
                "Review selected columns, join cardinality, and predicates before running."
            )
            if dangerous:
                summary += f" Dangerous statements detected: {', '.join(dangerous)}."
            return SqlAssistResponse(mode=request.mode, suggestion=summary, warnings=warning_lines)

        lint_hints = detect_sql_risk_hints(request.query or request.prompt)
        lint_lines = lint_hints.get("warnings") or []
        if lint_hints.get("dangerous_statements"):
            lint_lines.append(
                "Potentially dangerous statements: "
                + ", ".join(lint_hints.get("dangerous_statements") or [])
            )
        suggestion = "No high-risk patterns detected." if not lint_lines else "\n".join(lint_lines)
        return SqlAssistResponse(mode=request.mode, suggestion=suggestion, warnings=lint_lines)

    def _to_sql_job_response(
        self,
        job: SqlJobRecord,
        *,
        artifacts: list[SqlJobResultArtifactRecord],
    ) -> SqlJobResponse:
        warning_payload = job.warning_json if isinstance(job.warning_json, dict) else None
        error_payload = job.error_json if isinstance(job.error_json, dict) else None
        return SqlJobResponse(
            id=job.id,
            workspace_id=job.workspace_id,
            project_id=job.project_id,
            user_id=job.user_id,
            connection_id=job.connection_id,
            execution_mode=SqlExecutionMode(job.execution_mode),
            status=SqlJobStatus(job.status),
            query_hash=job.query_hash,
            is_explain=job.is_explain,
            is_federated=job.is_federated,
            requested_limit=job.requested_limit,
            enforced_limit=job.enforced_limit,
            requested_timeout_seconds=job.requested_timeout_seconds,
            enforced_timeout_seconds=job.enforced_timeout_seconds,
            row_count_preview=job.row_count_preview,
            total_rows_estimate=job.total_rows_estimate,
            bytes_scanned=job.bytes_scanned,
            duration_ms=job.duration_ms,
            redaction_applied=job.redaction_applied,
            warning=warning_payload,
            error=error_payload,
            correlation_id=job.correlation_id,
            created_at=job.created_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            artifacts=[self._to_artifact_response(artifact) for artifact in artifacts],
        )

    @staticmethod
    def _to_columns(raw_columns: list[Any]) -> list[SqlColumnMetadata]:
        columns: list[SqlColumnMetadata] = []
        for raw in raw_columns:
            if isinstance(raw, dict):
                name = str(raw.get("name") or "").strip()
                if not name:
                    continue
                columns.append(
                    SqlColumnMetadata(
                        name=name,
                        type=(str(raw.get("type")) if raw.get("type") is not None else None),
                    )
                )
            elif isinstance(raw, str):
                columns.append(SqlColumnMetadata(name=raw, type=None))
        return columns

    @staticmethod
    def _to_artifact_response(artifact: SqlJobResultArtifactRecord) -> SqlJobResultArtifactResponse:
        return SqlJobResultArtifactResponse(
            id=artifact.id,
            format=artifact.format,
            mime_type=artifact.mime_type,
            row_count=artifact.row_count,
            byte_size=artifact.byte_size,
            storage_reference=artifact.storage_reference,
            created_at=artifact.created_at,
        )

    @staticmethod
    def _to_saved_query_response(record: SqlSavedQueryRecord) -> SqlSavedQueryResponse:
        return SqlSavedQueryResponse(
            id=record.id,
            workspace_id=record.workspace_id,
            project_id=record.project_id,
            created_by=record.created_by,
            updated_by=record.updated_by,
            connection_id=record.connection_id,
            name=record.name,
            description=record.description,
            query=record.query_text,
            query_hash=record.query_hash,
            tags=list(record.tags_json or []),
            default_params=dict(record.default_params_json or {}),
            is_shared=record.is_shared,
            last_sql_job_id=record.last_sql_job_id,
            last_result_artifact_id=record.last_result_artifact_id,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def _to_policy_response(self, policy: SqlWorkspacePolicyRecord) -> SqlWorkspacePolicyResponse:
        return SqlWorkspacePolicyResponse(
            workspace_id=policy.workspace_id,
            max_preview_rows=policy.max_preview_rows,
            max_export_rows=policy.max_export_rows,
            max_runtime_seconds=policy.max_runtime_seconds,
            max_concurrency=policy.max_concurrency,
            allow_dml=policy.allow_dml,
            allow_federation=policy.allow_federation,
            allowed_schemas=list(policy.allowed_schemas_json or []),
            allowed_tables=list(policy.allowed_tables_json or []),
            default_datasource=policy.default_datasource_id,
            budget_limit_bytes=policy.budget_limit_bytes,
            bounds=self._policy_bounds(),
            updated_at=policy.updated_at,
        )

    @staticmethod
    def _build_csv(*, columns: list[str], rows: list[dict[str, Any]]) -> str:
        buffer = io.StringIO()
        fieldnames = columns or (sorted(rows[0].keys()) if rows else [])
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return buffer.getvalue()

    @staticmethod
    def _build_parquet(*, columns: list[str], rows: list[dict[str, Any]]) -> bytes:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:  # pragma: no cover
            raise BusinessValidationError("Parquet export is unavailable: pyarrow is missing.") from exc

        ordered_columns = columns or (sorted(rows[0].keys()) if rows else [])
        data = {column: [row.get(column) for row in rows] for column in ordered_columns}
        table = pa.table(data)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return buffer.getvalue()

    def _policy_bounds(self) -> SqlWorkspacePolicyBounds:
        return SqlWorkspacePolicyBounds(
            max_preview_rows_upper_bound=settings.SQL_POLICY_MAX_PREVIEW_ROWS_UPPER_BOUND,
            max_export_rows_upper_bound=settings.SQL_POLICY_MAX_EXPORT_ROWS_UPPER_BOUND,
            max_runtime_seconds_upper_bound=settings.SQL_POLICY_MAX_RUNTIME_SECONDS_UPPER_BOUND,
            max_concurrency_upper_bound=settings.SQL_POLICY_MAX_CONCURRENCY_UPPER_BOUND,
        )

    async def _get_or_create_policy_record(
        self,
        workspace_id: uuid.UUID,
    ) -> SqlWorkspacePolicyRecord:
        policy = await self._sql_workspace_policy_repository.get_by_workspace_id(
            workspace_id=workspace_id
        )
        if policy is not None:
            return policy
        now = datetime.now(timezone.utc)
        policy = SqlWorkspacePolicyRecord(
            id=uuid.uuid4(),
            workspace_id=workspace_id,
            max_preview_rows=settings.SQL_DEFAULT_MAX_PREVIEW_ROWS,
            max_export_rows=settings.SQL_DEFAULT_MAX_EXPORT_ROWS,
            max_runtime_seconds=settings.SQL_DEFAULT_MAX_RUNTIME_SECONDS,
            max_concurrency=settings.SQL_DEFAULT_MAX_CONCURRENCY,
            allow_dml=settings.SQL_DEFAULT_ALLOW_DML,
            allow_federation=settings.SQL_DEFAULT_ALLOW_FEDERATION,
            allowed_schemas_json=[],
            allowed_tables_json=[],
            default_datasource_id=None,
            budget_limit_bytes=None,
            updated_by_user_id=None,
            created_at=now,
            updated_at=now,
        )
        self._sql_workspace_policy_repository.add(policy)
        return policy

    async def _assert_workspace_access(
        self,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> None:
        if self._is_internal_user(current_user):
            return
        allowed = {str(item) for item in (current_user.available_organizations or [])}
        if str(workspace_id) not in allowed:
            raise PermissionDeniedBusinessValidationError("Forbidden")

    async def _assert_project_access(
        self,
        project_id: uuid.UUID | None,
        current_user: UserResponse,
    ) -> None:
        if project_id is None or self._is_internal_user(current_user):
            return
        allowed = {str(item) for item in (current_user.available_projects or [])}
        if str(project_id) not in allowed:
            raise PermissionDeniedBusinessValidationError("Forbidden")

    async def _is_workspace_admin(
        self,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> bool:
        if self._is_internal_user(current_user):
            return True
        organization = await self._organization_repository.get_by_id(workspace_id)
        if organization is None:
            raise ResourceNotFound("Workspace not found.")
        user = await self._user_repository.get_by_id(current_user.id)
        if user is None:
            raise ResourceNotFound("User not found.")
        role = await self._organization_repository.get_member_role(organization, user)
        return role in {"owner", "admin"}

    async def _can_view_saved_query(
        self,
        record: SqlSavedQueryRecord,
        current_user: UserResponse,
    ) -> bool:
        if self._is_internal_user(current_user):
            return True
        if record.created_by == current_user.id or record.is_shared:
            return True
        return await self._is_workspace_admin(record.workspace_id, current_user)

    async def _can_manage_saved_query(
        self,
        record: SqlSavedQueryRecord,
        current_user: UserResponse,
    ) -> bool:
        if self._is_internal_user(current_user):
            return True
        if record.created_by == current_user.id:
            return True
        return await self._is_workspace_admin(record.workspace_id, current_user)

    @staticmethod
    def _connector_is_in_workspace(connector: Any, workspace_id: uuid.UUID) -> bool:
        organizations = getattr(connector, "organizations", None) or []
        return any(str(getattr(org, "id", "")) == str(workspace_id) for org in organizations)

    def _ensure_sql_feature_enabled(self) -> None:
        if not settings.SQL_FEATURE_ENABLED:
            raise BusinessValidationError("SQL feature is disabled.")

    @staticmethod
    def _is_internal_user(user: UserResponse) -> bool:
        return user.id.int == 0

    @staticmethod
    def sanitize_error_message(error: str) -> str:
        return sanitize_sql_error_message(error)
