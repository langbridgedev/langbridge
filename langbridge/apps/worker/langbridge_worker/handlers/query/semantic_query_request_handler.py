import json
import logging
import os
import uuid
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import sqlglot
from pydantic import ValidationError

from langbridge.apps.worker.langbridge_worker.handlers.jobs.job_event_emitter import (
    BrokerJobEventEmitter,
)
from langbridge.apps.worker.langbridge_worker.dataset_execution import (
    DatasetExecutionResolver,
)
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse
from langbridge.packages.common.langbridge_common.contracts.jobs.semantic_query_job import (
    CreateSemanticQueryJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticQueryResponse,
    UnifiedSemanticQueryResponse,
)
from langbridge.packages.common.langbridge_common.db.job import JobRecord, JobStatus
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.interfaces.agent_events import (
    AgentEventVisibility,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetColumnRepository,
    DatasetRepository,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import (
    SemanticModelRepository,
)
from langbridge.packages.common.langbridge_common.utils.sql import (
    normalize_sql_dialect,
    transpile_sql,
)
from langbridge.packages.connectors.langbridge_connectors.api import (
    SqlConnector,
    SqlConnectorFactory,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType
from langbridge.packages.messaging.langbridge_messaging.broker.base import MessageBroker
from langbridge.packages.messaging.langbridge_messaging.broker.redis import RedisStreams
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.event import JobEventMessage
from langbridge.packages.messaging.langbridge_messaging.contracts.messages import (
    MessageEnvelope,
    MessageHeaders,
)
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.semantic_query import (
    SemanticQueryRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler
from langbridge.apps.worker.langbridge_worker.tools import FederatedQueryTool
from langbridge.packages.semantic.langbridge_semantic.loader import (
    SemanticModelError,
    load_semantic_model,
)
from langbridge.apps.worker.langbridge_worker.secrets import SecretProviderRegistry
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery, SemanticQueryEngine
from langbridge.apps.worker.langbridge_worker.semantic_query_execution_service import SemanticQueryExecutionService


class SemanticQueryRequestHandler(BaseMessageHandler):
    message_type: MessageType = MessageType.SEMANTIC_QUERY_REQUEST

    def __init__(
        self,
        message_broker: MessageBroker,
        job_repository: JobRepository | None = None,
        semantic_model_repository: SemanticModelRepository | None = None,
        connector_repository: ConnectorRepository | None = None,
        dataset_repository: DatasetRepository | None = None,
        dataset_column_repository: DatasetColumnRepository | None = None,
        secret_provider_registry: SecretProviderRegistry | None = None,
        federated_query_tool: FederatedQueryTool | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._job_repository = job_repository
        self._semantic_model_repository = semantic_model_repository
        self._connector_repository = connector_repository
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._message_broker = message_broker
        self._secret_provider_registry = secret_provider_registry or SecretProviderRegistry()
        self._federated_query_tool = federated_query_tool
        self._engine = SemanticQueryEngine()
        self._sql_connector_factory = SqlConnectorFactory()
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=self._dataset_repository,
        )
        self._query_execution_service = SemanticQueryExecutionService(
            semantic_model_repository=self._semantic_model_repository,
            dataset_repository=self._dataset_repository,
            federated_query_tool=self._federated_query_tool,
            logger=self._logger,
        )

    async def handle(self, payload: SemanticQueryRequestMessage) -> None:
        if (
            self._job_repository is None
            or os.environ.get("WORKER_EXECUTION_MODE", "").strip().lower()
            in {"customer_runtime", "customer-runtime", "edge"}
        ) and payload.job_request and payload.semantic_model_yaml and payload.connector:
            await self._handle_runtime_payload(payload)
            return None

        if self._job_repository is None:
            raise BusinessValidationError("Job repository is required for hosted semantic query handling.")
        if self._semantic_model_repository is None or self._connector_repository is None:
            raise BusinessValidationError(
                "Semantic query repositories are required for hosted semantic query handling."
            )

        self._logger.info("Received semantic query job request %s", payload.job_id)
        job_record = await self._job_repository.get_by_id(payload.job_id)
        if job_record is None:
            raise BusinessValidationError(f"Job with ID {payload.job_id} does not exist.")

        if job_record.status in {
            JobStatus.succeeded,
            JobStatus.failed,
            JobStatus.cancelled,
        }:
            self._logger.info(
                "Job %s already in terminal state %s; skipping.",
                job_record.id,
                job_record.status,
            )
            return None

        event_emitter = BrokerJobEventEmitter(
            job_record=job_record,
            broker_client=self._message_broker,
            logger=self._logger,
        )
        job_record.status = JobStatus.running
        job_record.progress = 5
        job_record.status_message = "Semantic query started."
        if job_record.started_at is None:
            job_record.started_at = datetime.now(timezone.utc)
        await event_emitter.emit(
            event_type="SemanticQueryStarted",
            message="Semantic query started.",
            visibility=AgentEventVisibility.public,
            source="worker",
            details={"job_id": str(job_record.id)},
        )

        try:
            request = self._parse_job_payload(job_record)
            semantic_response: UnifiedSemanticQueryResponse | SemanticQueryResponse = await self._run_query(job_record, request, event_emitter)

            row_count = len(semantic_response.data)
            job_record.result = {
                "result": semantic_response.model_dump(mode="json"),
                "summary": f"Semantic query completed with {row_count} rows.",
            }
            job_record.status = JobStatus.succeeded
            job_record.progress = 100
            job_record.status_message = "Semantic query completed."
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = None
            await event_emitter.emit(
                event_type="SemanticQueryCompleted",
                message="Semantic query completed.",
                visibility=AgentEventVisibility.public,
                source="worker",
                details={"job_id": str(job_record.id), "row_count": row_count},
            )
        except Exception as exc:  # pragma: no cover - defensive background worker guard
            self._logger.exception("Semantic query job %s failed: %s", job_record.id, exc)
            job_record.status = JobStatus.failed
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.status_message = "Semantic query failed."
            job_record.error = {"message": str(exc)}
            await event_emitter.emit(
                event_type="SemanticQueryFailed",
                message="Semantic query failed.",
                visibility=AgentEventVisibility.public,
                source="worker",
                details={"job_id": str(job_record.id), "error": str(exc)},
            )

        return None

    async def _handle_runtime_payload(self, payload: SemanticQueryRequestMessage) -> None:
        if payload.job_request is None or payload.semantic_model_yaml is None or payload.connector is None:
            raise BusinessValidationError("Runtime semantic query payload is incomplete.")

        request = CreateSemanticQueryJobRequest.model_validate(payload.job_request)
        if request.query_scope != "semantic_model":
            raise BusinessValidationError(
                "Customer-runtime semantic execution currently supports semantic_model scope only."
            )

        await self._emit_runtime_event(
            job_id=payload.job_id,
            event_type="SemanticQueryStarted",
            message="Semantic query started.",
            details={"job_id": str(payload.job_id)},
        )

        try:
            connector_response = ConnectorResponse.model_validate(payload.connector)
            semantic_model = self._load_model_payload(payload.semantic_model_yaml)
            semantic_query = self._load_query_payload(request.query)
            resolved_connector_config = self._resolve_connector_config(connector_response)

            if connector_response.connector_type is None:
                raise BusinessValidationError("Connector type is required for semantic query execution.")

            connector_type = ConnectorRuntimeType(connector_response.connector_type.upper())
            sql_connector = await self._create_sql_connector(
                connector_type=connector_type,
                connector_config=resolved_connector_config,
            )

            await self._emit_runtime_event(
                job_id=payload.job_id,
                event_type="SemanticQueryCompiling",
                message="Compiling semantic query.",
                details={"query_scope": request.query_scope},
            )

            rewrite_expression = None
            if getattr(sql_connector, "EXPRESSION_REWRITE", False):
                rewrite_expression = getattr(sql_connector, "rewrite_expression", None)
                if rewrite_expression is None:
                    raise BusinessValidationError(
                        "Semantic query translation failed: connector expression rewriter missing."
                    )

            plan = self._engine.compile(
                semantic_query,
                semantic_model,
                dialect=sql_connector.DIALECT.value.lower(),
                rewrite_expression=rewrite_expression,
            )
            target_dialect = normalize_sql_dialect(sql_connector.DIALECT.value.lower(), default="tsql")
            source_dialect = self._extract_source_dialect(request.query, fallback=target_dialect)
            execution_sql = self._transpile_sql_if_needed(
                plan.sql,
                source_dialect=source_dialect,
                target_dialect=target_dialect,
            )
            await self._emit_runtime_event(
                job_id=payload.job_id,
                event_type="SemanticQueryExecuting",
                message="Executing semantic query SQL.",
                details={"sql": execution_sql},
            )

            query_result = await sql_connector.execute(execution_sql)
            data = self._engine.format_rows(query_result.columns, query_result.rows)
            row_count = len(data)
            response = SemanticQueryResponse(
                id=uuid.uuid4(),
                organization_id=request.organisation_id,
                project_id=request.project_id,
                semantic_model_id=request.semantic_model_id
                if request.semantic_model_id is not None
                else uuid.uuid4(),
                data=data,
                annotations=plan.annotations,
                metadata=plan.metadata,
            )
            await self._emit_runtime_event(
                job_id=payload.job_id,
                event_type="SemanticQueryCompleted",
                message="Semantic query completed.",
                details={
                    "job_id": str(payload.job_id),
                    "row_count": row_count,
                    "result": {
                        "result": response.model_dump(mode="json"),
                        "summary": f"Semantic query completed with {row_count} rows.",
                    },
                },
            )
        except Exception as exc:
            await self._emit_runtime_event(
                job_id=payload.job_id,
                event_type="SemanticQueryFailed",
                message="Semantic query failed.",
                details={"job_id": str(payload.job_id), "error": str(exc)},
            )
            raise

    async def _emit_runtime_event(
        self,
        *,
        job_id: uuid.UUID,
        event_type: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        payload = JobEventMessage(
            job_id=job_id,
            event_type=event_type,
            message=message,
            visibility=AgentEventVisibility.public.value,
            source="worker",
            details=details or {},
        )
        envelope = MessageEnvelope(
            message_type=payload.message_type,
            payload=payload,
            headers=MessageHeaders.default(),
        )
        await self._message_broker.publish(envelope, stream=RedisStreams.API)

    def _parse_job_payload(self, job_record: JobRecord) -> CreateSemanticQueryJobRequest:
        raw_payload = job_record.payload
        if isinstance(raw_payload, str):
            try:
                payload_data = json.loads(raw_payload)
            except json.JSONDecodeError as exc:
                raise BusinessValidationError(
                    f"Job payload for {job_record.id} is not valid JSON."
                ) from exc
        elif isinstance(raw_payload, dict):
            payload_data = raw_payload
        else:
            raise BusinessValidationError(
                f"Job payload for {job_record.id} must be an object or JSON string."
            )

        try:
            return CreateSemanticQueryJobRequest.model_validate(payload_data)
        except ValidationError as exc:
            raise BusinessValidationError(
                f"Job payload for {job_record.id} is invalid for semantic query execution."
            ) from exc

    async def _run_query(
        self,
        job_record: JobRecord,
        request: CreateSemanticQueryJobRequest,
        event_emitter: BrokerJobEventEmitter,
    ) -> SemanticQueryResponse | UnifiedSemanticQueryResponse:
        job_record.progress = 20
        job_record.status_message = "Loading semantic model."
        await event_emitter.emit(
            event_type="SemanticQueryLoadingModel",
            message="Loading semantic model.",
            visibility=AgentEventVisibility.public,
            source="worker",
            details={"query_scope": request.query_scope},
        )

        semantic_query = self._load_query_payload(request.query)
        
        if request.query_scope == "unified":
            return await self._run_federated_query(
                semantic_query=semantic_query,
                job_record=job_record,
                request=request,
                event_emitter=event_emitter,
            )
        else:            
            return await self._run_semantic_query(
                semantic_query=semantic_query,
                job_record=job_record,
                request=request,
                event_emitter=event_emitter,
            )

        

    async def _run_semantic_query(
        self,
        semantic_query: SemanticQuery,
        job_record: JobRecord,
        request: CreateSemanticQueryJobRequest,
        event_emitter: BrokerJobEventEmitter,
    ) -> SemanticQueryResponse:
        if request.semantic_model_id is None:
            raise BusinessValidationError(
                "semantic_model_id is required for semantic_model query scope."
            )

        semantic_model_record = await self._semantic_model_repository.get_for_scope(
            model_id=request.semantic_model_id,
            organization_id=request.organisation_id,
        )
        if semantic_model_record is None:
            raise BusinessValidationError("Semantic model not found.")
        raw_model_payload = self._parse_semantic_model_payload(semantic_model_record)
        semantic_model = self._load_model_payload(semantic_model_record.content_yaml)
        semantic_model_id = request.semantic_model_id

        return await self._run_dataset_backed_semantic_query(
            semantic_query=semantic_query,
            semantic_model=semantic_model,
            semantic_model_id=semantic_model_id,
            organization_id=request.organisation_id,
            project_id=request.project_id,
            connector_fallbacks={
                table_key: semantic_model_record.connector_id
                for table_key in semantic_model.tables.keys()
            },
            raw_model_payload=raw_model_payload,
            event_emitter=event_emitter,
        )

    async def _run_dataset_backed_semantic_query(
        self,
        *,
        semantic_query: SemanticQuery,
        semantic_model: SemanticModel,
        semantic_model_id: uuid.UUID,
        organization_id: uuid.UUID,
        project_id: uuid.UUID | None,
        connector_fallbacks: Mapping[str, uuid.UUID],
        raw_model_payload: Mapping[str, Any],
        event_emitter: BrokerJobEventEmitter,
    ) -> SemanticQueryResponse:
        if self._dataset_repository is None or self._dataset_column_repository is None:
            raise BusinessValidationError("Dataset repositories are required for dataset-backed semantic queries.")
        if self._federated_query_tool is None:
            raise BusinessValidationError("Federated query tool is required for dataset-backed semantic queries.")

        workflow, workflow_dialect = await self._dataset_execution_resolver.build_semantic_workflow(
            organization_id=organization_id,
            workflow_id=f"workflow_semantic_dataset_{semantic_model_id.hex[:12]}",
            dataset_name=f"semantic_dataset_{semantic_model_id.hex[:12]}",
            semantic_model=semantic_model,
            connector_fallbacks=connector_fallbacks,
            raw_tables_payload=(
                raw_model_payload.get("tables")
                if isinstance(raw_model_payload.get("tables"), Mapping)
                else None
            ),
        )

        try:
            plan = self._engine.compile(
                semantic_query,
                semantic_model,
                dialect=workflow_dialect,
            )
        except Exception as exc:
            raise BusinessValidationError(f"Semantic query translation failed: {exc}") from exc

        await event_emitter.emit(
            event_type="SemanticQueryExecuting",
            message="Executing semantic query via datasets.",
            visibility=AgentEventVisibility.public,
            source="worker",
            details={"sql": plan.sql, "query_scope": "semantic_model_dataset"},
        )
        execution = await self._federated_query_tool.execute_federated_query(
            {
                "workspace_id": str(organization_id),
                "query": semantic_query.model_dump(by_alias=True, exclude_none=True),
                "dialect": workflow_dialect,
                "workflow": workflow.model_dump(mode="json"),
                "semantic_model": semantic_model.model_dump(by_alias=True, exclude_none=True),
            }
        )
        rows_payload = execution.get("rows", [])
        if not isinstance(rows_payload, list):
            raise BusinessValidationError("Dataset-backed semantic query returned an invalid row payload.")
        data_payload = [row for row in rows_payload if isinstance(row, dict)]
        return SemanticQueryResponse(
            id=uuid.uuid4(),
            organization_id=organization_id,
            project_id=project_id,
            semantic_model_id=semantic_model_id,
            data=data_payload,
            annotations=plan.annotations,
            metadata=plan.metadata,
        )

    @staticmethod
    def _parse_semantic_model_payload(semantic_model_record) -> dict[str, Any]:
        content_json = getattr(semantic_model_record, "content_json", None)
        if isinstance(content_json, str) and content_json.strip():
            try:
                parsed = json.loads(content_json)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass
        content_yaml = getattr(semantic_model_record, "content_yaml", None)
        if isinstance(content_yaml, str) and content_yaml.strip():
            import yaml

            try:
                parsed_yaml = yaml.safe_load(content_yaml)
                if isinstance(parsed_yaml, dict):
                    return parsed_yaml
            except Exception:
                return {}
        return {}

    async def _run_federated_query(
        self,
        semantic_query: SemanticQuery,
        job_record: JobRecord,
        request: CreateSemanticQueryJobRequest,
        event_emitter: BrokerJobEventEmitter,
    ) -> UnifiedSemanticQueryResponse:
        if not request.semantic_model_ids:
            raise BusinessValidationError(
                "semantic_model_ids must include at least one model id for unified query scope."
            )

        job_record.progress = 45
        job_record.status_message = "Compiling semantic query."
        await event_emitter.emit(
            event_type="SemanticQueryCompiling",
            message="Compiling semantic query.",
            visibility=AgentEventVisibility.public,
            source="worker",
        )

        job_record.progress = 70
        job_record.status_message = "Executing federated semantic query."
        execution = await self._query_execution_service.execute_unified_query(
            organization_id=request.organisation_id,
            project_id=request.project_id,
            semantic_query=semantic_query,
            semantic_model_ids=request.semantic_model_ids,
            joins=request.joins,
            metrics=request.metrics,
        )
        await event_emitter.emit(
            event_type="SemanticQueryExecuting",
            message="Executing semantic query SQL.",
            visibility=AgentEventVisibility.public,
            source="worker",
            details={"sql": execution.compiled_sql, "query_scope": request.query_scope},
        )
        return execution.response

    async def _create_sql_connector(
        self,
        *,
        connector_type: ConnectorRuntimeType,
        connector_config: dict[str, Any],
    ) -> SqlConnector:
        return await self._query_execution_service.create_sql_connector(
            connector_type=connector_type,
            connector_config=connector_config,
            sql_connector_factory=self._sql_connector_factory,
            logger=self._logger,
        )

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
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                raise BusinessValidationError(
                    f"Unable to resolve connector secret '{secret_name}'."
                ) from exc

        resolved_payload["config"] = runtime_config
        return resolved_payload

    @staticmethod
    def _load_model_payload(content_yaml: str) -> SemanticModel:
        try:
            return load_semantic_model(content_yaml)
        except SemanticModelError as exc:
            raise BusinessValidationError(
                f"Semantic model failed validation: {exc}"
            ) from exc

    @staticmethod
    def _load_query_payload(query_payload: Mapping[str, Any] | dict[str, Any]) -> SemanticQuery:
        try:
            return SemanticQuery.model_validate(query_payload)
        except Exception as exc:
            raise BusinessValidationError(
                f"Semantic query payload failed validation: {exc}"
            ) from exc

    @staticmethod
    def _normalize_model_ids(semantic_model_ids: list[uuid.UUID]) -> list[uuid.UUID]:
        ordered_unique: list[uuid.UUID] = []
        seen: set[uuid.UUID] = set()
        for model_id in semantic_model_ids:
            if model_id in seen:
                continue
            seen.add(model_id)
            ordered_unique.append(model_id)
        if not ordered_unique:
            raise BusinessValidationError(
                "semantic_model_ids must include at least one model id."
            )
        return ordered_unique

    @staticmethod
    def _extract_source_dialect(
        query_payload: Mapping[str, Any] | dict[str, Any],
        *,
        fallback: str,
    ) -> str:
        for key in ("queryDialect", "query_dialect", "dialect"):
            raw = query_payload.get(key)
            if isinstance(raw, str) and raw.strip():
                return normalize_sql_dialect(raw, default=fallback)
        return normalize_sql_dialect(fallback, default="tsql")

    def _transpile_sql_if_needed(
        self,
        sql: str,
        *,
        source_dialect: str,
        target_dialect: str,
    ) -> str:
        normalized_source = normalize_sql_dialect(source_dialect, default=target_dialect)
        normalized_target = normalize_sql_dialect(target_dialect, default="tsql")
        if normalized_source == normalized_target:
            return sql
        try:
            return transpile_sql(
                sql,
                source_dialect=normalized_source,
                target_dialect=normalized_target,
            )
        except ValueError:
            self._logger.warning(
                "Semantic SQL transpile fallback (source=%s target=%s).",
                normalized_source,
                normalized_target,
            )
            try:
                expression = sqlglot.parse_one(sql, read=normalized_target)
                return expression.sql(dialect=normalized_target)
            except sqlglot.ParseError as exc:
                raise BusinessValidationError(
                    f"Semantic query transpilation failed: {exc}"
                ) from exc
