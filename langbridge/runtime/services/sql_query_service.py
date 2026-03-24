import logging
import hashlib
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import sqlglot
from sqlglot import exp
from langbridge.runtime.models import (
    CreateSqlJobRequest,
    SqlJob,
    SqlJobResultArtifact,
    SqlSelectedDataset,
)
from .errors import ExecutionValidationError
from langbridge.runtime.utils.datasets import (
    dataset_supports_structured_federation,
)
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
from langbridge.connectors.base import (
    SqlConnectorFactory,
    get_connector_config_factory,
)
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.federation.models import FederationWorkflow, VirtualDataset, VirtualTableBinding
from langbridge.runtime.execution import FederatedQueryTool
from langbridge.runtime.ports import (
    DatasetCatalogStore,
    SqlJobArtifactStore,
)
from langbridge.runtime.providers import (
    ConnectorMetadataProvider,
    CredentialProvider,
    DatasetMetadataProvider,
    SecretRegistryCredentialProvider,
)
from langbridge.runtime.models import ConnectorMetadata
from langbridge.runtime.security import SecretProviderRegistry
from langbridge.runtime.services.dataset_execution import DatasetExecutionResolver
from langbridge.runtime.settings import runtime_settings as settings

RewriteExpression = Callable[[sqlglot.Expression], sqlglot.Expression]
CreateSqlConnector = Callable[..., Awaitable[Any]]
ResolveConnectorConfig = Callable[[ConnectorMetadata], dict[str, Any]]

class SqlQueryService:
    def __init__(
        self,
        sql_job_result_artifact_store: SqlJobArtifactStore | None,
        dataset_repository: DatasetCatalogStore | None = None,
        secret_provider_registry: SecretProviderRegistry | None = None,
        federated_query_tool: FederatedQueryTool | None = None,
        connector_provider: ConnectorMetadataProvider | None = None,
        dataset_provider: DatasetMetadataProvider | None = None,
        credential_provider: CredentialProvider | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._sql_job_result_artifact_store = sql_job_result_artifact_store
        self._dataset_repository = dataset_repository
        self._connector_provider = connector_provider
        self._dataset_provider = dataset_provider
        self._credential_provider = credential_provider or SecretRegistryCredentialProvider(
            registry=secret_provider_registry or SecretProviderRegistry()
        )
        self._secret_provider_registry = secret_provider_registry or SecretProviderRegistry()
        self._sql_connector_factory = SqlConnectorFactory()
        self._federated_query_tool = federated_query_tool
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=self._dataset_repository,
            dataset_provider=self._dataset_provider,
        )

    async def execute_sql(
        self,
        *,
        request: CreateSqlJobRequest,
        job: SqlJob | None = None,
        create_sql_connector: CreateSqlConnector | None = None,
        resolve_connector_config: ResolveConnectorConfig | None = None,
    ) -> dict[str, Any]:
        existing_job = job is not None
        runtime_job = job or self._build_transient_job(request)
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
            return self._result_payload(runtime_job) if runtime_job.status == "succeeded" else {}
        if runtime_job.status != "succeeded":
            message = None
            if isinstance(runtime_job.error_json, dict):
                message = runtime_job.error_json.get("message")
            raise ExecutionValidationError(str(message or "SQL execution failed."))
        return self._result_payload(runtime_job)

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
                    resolve_connector_config=resolve_connector_config
                    or self._resolve_connector_config,
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

        return None

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
        connector_sqlglot_dialect = self._sqlglot_dialect_for_connector(connector_type)
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
        job: SqlJob,
        request: CreateSqlJobRequest,
    ) -> None:
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
        workflow, federated_datasets = await self._build_federated_workflow(
            workspace_id=request.workspace_id,
            query=executable_sql,
            source_dialect=source_sqlglot_dialect,
            selected_dataset_ids=list(request.selected_datasets or []),
            job=job,
        )
        request.federated_datasets = federated_datasets
        job.selected_datasets_json = [
            dataset.model_dump(mode="json")
            for dataset in federated_datasets
        ]
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
        job: SqlJob,
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
            raise ExecutionValidationError(f"EXPLAIN parse failed: {exc}") from exc

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
        try:
            self._sql_connector_factory.get_sql_connector_class_reference(connector_type)
        except ValueError as exc:
            raise ExecutionValidationError(
                f"Connector type {connector_type.value} does not support SQL execution."
            ) from exc
        config_factory = get_connector_config_factory(connector_type)
        config_instance = config_factory.create(connector_payload.get("config", {}))
        sql_connector = self._sql_connector_factory.create_sql_connector(
            connector_type,
            config_instance,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    async def _get_connector_response(
        self,
        *,
        connection_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> ConnectorMetadata | None:
        if self._connector_provider is not None:
            return await self._connector_provider.get_connector(
                workspace_id=workspace_id,
                connector_id=connection_id,
            )
        raise ExecutionValidationError("Connector metadata provider is required for SQL execution.")

    def _resolve_connector_config(self, connector: ConnectorMetadata) -> dict[str, Any]:
        resolved_payload = dict(connector.config or {})
        runtime_config = dict(resolved_payload.get("config") or {})

        if connector.connection_metadata is not None:
            metadata = connector.connection_metadata.model_dump(exclude_none=True, by_alias=True)
            extra = metadata.pop("extra", {})
            for key, value in metadata.items():
                runtime_config.setdefault(key, value)
            if isinstance(extra, dict):
                for key, value in extra.items():
                    if value is not None:
                        runtime_config.setdefault(key, value)

        for secret_name, secret_ref in connector.secret_references.items():
            try:
                runtime_config[secret_name] = self._credential_provider.resolve_secret(secret_ref)
            except Exception as exc:  # pragma: no cover
                raise ExecutionValidationError(
                    f"Unable to resolve connector secret '{secret_name}'."
                ) from exc

        resolved_payload["config"] = runtime_config
        return resolved_payload

    @staticmethod
    def _sqlglot_dialect_for_connector(connector_type: ConnectorRuntimeType) -> str:
        try:
            return SqlConnectorFactory.get_sqlglot_dialect(connector_type)
        except ValueError:
            return "tsql"

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
        job: SqlJob,
        columns_payload: list[dict[str, Any]],
        rows: list[dict[str, Any]],
        now: datetime,
    ) -> None:
        if self._sql_job_result_artifact_store is None:
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
        self._sql_job_result_artifact_store.add(snapshot_artifact)

    async def _build_federated_workflow(
        self,
        *,
        workspace_id: uuid.UUID,
        query: str,
        source_dialect: str,
        selected_dataset_ids: list[uuid.UUID],
        job: SqlJob,
    ) -> tuple[FederationWorkflow, list[SqlSelectedDataset]]:
        return await self._build_dataset_federated_workflow(
            workspace_id=workspace_id,
            query=query,
            source_dialect=source_dialect,
            selected_dataset_ids=selected_dataset_ids,
            job=job,
        )

    async def _build_dataset_federated_workflow(
        self,
        *,
        workspace_id: uuid.UUID,
        query: str,
        source_dialect: str,
        selected_dataset_ids: list[uuid.UUID],
        job: SqlJob,
    ) -> tuple[FederationWorkflow, list[SqlSelectedDataset]]:
        federated_datasets, datasets_by_id = await self._resolve_federated_datasets(
            workspace_id=workspace_id,
            selected_dataset_ids=selected_dataset_ids,
        )
        table_bindings = self._extract_dataset_federated_table_bindings(
            selected_datasets=federated_datasets,
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
            federated_datasets,
        )

    async def _resolve_federated_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        selected_dataset_ids: list[uuid.UUID],
    ) -> tuple[list[SqlSelectedDataset], dict[uuid.UUID, Any]]:
        if selected_dataset_ids:
            datasets = await self._get_datasets_for_workspace(
                workspace_id=workspace_id,
                dataset_ids=selected_dataset_ids,
            )
            datasets_by_id = {dataset.id: dataset for dataset in datasets}
            missing_dataset_ids = [
                dataset_id
                for dataset_id in selected_dataset_ids
                if dataset_id not in datasets_by_id
            ]
            if missing_dataset_ids:
                missing = ", ".join(str(dataset_id) for dataset_id in missing_dataset_ids)
                raise ExecutionValidationError(
                    f"Selected federated datasets were not found in workspace '{workspace_id}': {missing}."
                )
        else:
            datasets = await self._list_datasets_for_workspace(workspace_id=workspace_id)
            datasets_by_id = {dataset.id: dataset for dataset in datasets}

        eligible_datasets: list[Any] = []
        ineligible_dataset_names: list[str] = []
        for dataset in datasets:
            descriptor = self._dataset_execution_resolver._build_dataset_execution_descriptor(dataset)
            if dataset_supports_structured_federation(
                source_kind=descriptor.source_kind,
                storage_kind=descriptor.storage_kind,
                capabilities=descriptor.execution_capabilities,
            ):
                eligible_datasets.append(dataset)
            else:
                ineligible_dataset_names.append(str(getattr(dataset, "name", dataset.id)))

        if selected_dataset_ids and ineligible_dataset_names:
            raise ExecutionValidationError(
                "Selected datasets do not support federated structured execution: "
                + ", ".join(sorted(ineligible_dataset_names))
            )
        if not eligible_datasets:
            raise ExecutionValidationError(
                "No eligible datasets are available for federated SQL in this workspace."
            )

        ordered_datasets = sorted(
            eligible_datasets,
            key=lambda dataset: (
                str(getattr(dataset, "name", "")).strip().lower(),
                str(getattr(dataset, "id", "")),
            ),
        )
        sql_aliases = self._derive_federated_sql_aliases(ordered_datasets)
        resolved = [
            SqlSelectedDataset(
                alias=sql_aliases[dataset.id],
                sql_alias=sql_aliases[dataset.id],
                dataset_id=dataset.id,
                dataset_name=str(getattr(dataset, "name", "")).strip() or None,
                canonical_reference=self._dataset_canonical_reference(dataset),
                connector_id=getattr(dataset, "connection_id", None),
                source_kind=str(getattr(dataset, "source_kind", "")).strip().lower() or None,
                storage_kind=str(getattr(dataset, "storage_kind", "")).strip().lower() or None,
            )
            for dataset in ordered_datasets
        ]
        return resolved, {dataset.id: dataset for dataset in ordered_datasets}

    async def _list_datasets_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
    ) -> list[Any]:
        if self._dataset_repository is None:
            raise ExecutionValidationError(
                "Dataset catalog store is required to enumerate workspace datasets for federated SQL."
            )
        limit = max(1, settings.SQL_FEDERATION_MAX_ELIGIBLE_DATASETS)
        datasets = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            limit=limit + 1,
            offset=0,
        )
        if len(datasets) > limit:
            raise ExecutionValidationError(
                "Federated SQL scope exceeds the default dataset limit for this workspace. "
                "Pass selected_datasets to narrow planner scope."
            )
        return datasets

    @classmethod
    def _derive_federated_sql_aliases(
        cls,
        datasets: list[Any],
    ) -> dict[uuid.UUID, str]:
        aliases: dict[uuid.UUID, str] = {}
        used_aliases: set[str] = set()
        for dataset in datasets:
            base_alias = cls._normalize_dataset_sql_alias(
                getattr(dataset, "sql_alias", None) or getattr(dataset, "name", None)
            )
            alias = base_alias
            suffix = 2
            while alias in used_aliases:
                alias = f"{base_alias}_{suffix}"
                suffix += 1
            aliases[dataset.id] = alias
            used_aliases.add(alias)
        return aliases

    @staticmethod
    def _normalize_dataset_sql_alias(value: Any) -> str:
        alias = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
        alias = re.sub(r"_+", "_", alias).strip("_")
        if not alias:
            alias = "dataset"
        if alias[0].isdigit():
            alias = f"dataset_{alias}"
        return alias

    @staticmethod
    def _dataset_canonical_reference(dataset: Any) -> str | None:
        relation_identity = getattr(dataset, "relation_identity_json", None)
        if relation_identity is None:
            relation_identity = getattr(dataset, "relation_identity", None)
        if isinstance(relation_identity, dict):
            value = str(relation_identity.get("canonical_reference") or "").strip()
            return value or None
        return None

    def _extract_dataset_federated_table_bindings(
        self,
        *,
        selected_datasets: list[SqlSelectedDataset],
        datasets_by_id: dict[uuid.UUID, Any],
    ) -> dict[str, VirtualTableBinding]:
        table_bindings: dict[str, VirtualTableBinding] = {}
        for selection in selected_datasets:
            dataset = datasets_by_id[selection.dataset_id]
            descriptor = self._dataset_execution_resolver._build_dataset_execution_descriptor(dataset)
            if not dataset_supports_structured_federation(
                source_kind=descriptor.source_kind,
                storage_kind=descriptor.storage_kind,
                capabilities=descriptor.execution_capabilities,
            ):
                raise ExecutionValidationError(
                    f"Dataset '{dataset.name}' does not support federated structured execution."
                )
            sql_alias = str(selection.sql_alias or "").strip().lower()
            if not sql_alias:
                raise ExecutionValidationError(
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

        return table_bindings

    async def _get_datasets_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids: list[uuid.UUID],
    ) -> list[Any]:
        if self._dataset_repository is not None:
            return await self._dataset_repository.get_by_ids_for_workspace(
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )
        if self._dataset_provider is not None:
            return await self._dataset_provider.get_datasets(
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )
        raise ExecutionValidationError("Dataset metadata provider is required for dataset-backed federated SQL.")

    @staticmethod
    def _result_payload(job: SqlJob) -> dict[str, Any]:
        return {
            "columns": list(job.result_columns_json or []),
            "rows": list(job.result_rows_json or []),
            "row_count_preview": int(job.row_count_preview or 0),
            "total_rows_estimate": job.total_rows_estimate,
            "bytes_scanned": job.bytes_scanned,
            "duration_ms": job.duration_ms,
            "result_cursor": job.result_cursor,
            "redaction_applied": job.redaction_applied,
            "stats": dict(job.stats_json or {}),
        }

    @staticmethod
    def _selected_datasets_payload(request: CreateSqlJobRequest) -> list[dict[str, Any]]:
        if request.federated_datasets:
            return [
                dataset.model_dump(mode="json") if hasattr(dataset, "model_dump") else dict(dataset)
                for dataset in request.federated_datasets
            ]
        return [
            {"dataset_id": str(dataset_id)}
            for dataset_id in request.selected_datasets
        ]

    @staticmethod
    def _build_transient_job(request: CreateSqlJobRequest) -> SqlJob:
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
            selected_datasets_json=SqlQueryService._selected_datasets_payload(request),
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
            raise ExecutionValidationError("Federated SQL execution returned an invalid rows payload.")

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
        job: SqlJob,
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
                "value": str(len(request.federated_datasets or [])),
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
