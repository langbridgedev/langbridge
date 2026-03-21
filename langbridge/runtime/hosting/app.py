from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from langbridge.runtime.hosting.auth import RuntimeAuthConfig, RuntimeAuthResolver
from langbridge.runtime.local_config import (
    ConfiguredLocalRuntimeHost,
    build_configured_local_runtime,
)
from langbridge.runtime.models.jobs import (
    CreateDatasetPreviewJobRequest,
    CreateSqlJobRequest,
    SqlWorkbenchMode,
)
from langbridge.runtime.services.runtime_host import RuntimeHost
from langbridge.runtime.hosting.api_models import (
    RuntimeAgentAskRequest,
    RuntimeAgentAskResponse,
    RuntimeConnectorListResponse,
    RuntimeDatasetListResponse,
    RuntimeDatasetPreviewRequest,
    RuntimeDatasetPreviewResponse,
    RuntimeInfoResponse,
    RuntimeSemanticQueryRequest,
    RuntimeSemanticQueryResponse,
    RuntimeSyncRequest,
    RuntimeSyncResourceListResponse,
    RuntimeSyncResponse,
    RuntimeSyncStateListResponse,
    RuntimeSqlQueryRequest,
    RuntimeSqlQueryResponse,
)

_CONFIG_PATH_ENV = "LANGBRIDGE_RUNTIME_CONFIG_PATH"


def create_runtime_api_app(
    *,
    config_path: str | Path | None = None,
    runtime_host: ConfiguredLocalRuntimeHost | None = None,
    auth_config: RuntimeAuthConfig | None = None,
) -> FastAPI:
    host = runtime_host
    if host is None:
        if config_path is None:
            raise ValueError("config_path is required when runtime_host is not supplied.")
        host = build_configured_local_runtime(config_path=str(config_path))

    app = FastAPI(
        title="Langbridge Runtime Host",
        version="0.1.0",
        docs_url="/api/runtime/docs",
        openapi_url="/api/runtime/openapi.json",
    )
    app.state.runtime_host = host
    app.state.runtime_auth = RuntimeAuthResolver(
        config=auth_config or RuntimeAuthConfig.from_env(),
        default_context=host.context,
    )

    @app.get("/api/runtime/v1/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/runtime/v1/info", response_model=RuntimeInfoResponse)
    async def info(request: Request) -> RuntimeInfoResponse:
        configured_host = await _resolve_request_host(request)
        connector_items = await configured_host.list_connectors()
        capabilities = [
            "datasets.list",
            "datasets.preview",
            "semantic.query",
            "sql.query",
            "agents.ask",
        ]
        if connector_items:
            capabilities.append("connectors.list")
        if any(bool(item.get("supports_sync")) for item in connector_items):
            capabilities.extend(
                [
                    "sync.resources",
                    "sync.states",
                    "sync.run",
                ]
            )
        return RuntimeInfoResponse(
            runtime_mode="configured_local",
            config_path=str(configured_host._config_path),
            workspace_id=configured_host.context.workspace_id,
            actor_id=configured_host.context.actor_id,
            roles=list(configured_host.context.roles),
            default_semantic_model=configured_host._default_semantic_model_name,
            default_agent=configured_host._default_agent.config.name if configured_host._default_agent else None,
            capabilities=capabilities,
        )

    @app.get("/api/runtime/v1/datasets", response_model=RuntimeDatasetListResponse)
    async def list_datasets(request: Request) -> RuntimeDatasetListResponse:
        configured_host = await _resolve_request_host(request)
        items = await configured_host.list_datasets()
        return RuntimeDatasetListResponse(items=items, total=len(items))

    @app.post("/api/runtime/v1/datasets/{dataset_ref}/preview", response_model=RuntimeDatasetPreviewResponse)
    async def preview_dataset(
        request: Request,
        dataset_ref: str,
        body: RuntimeDatasetPreviewRequest,
    ) -> RuntimeDatasetPreviewResponse:
        configured_host = await _resolve_request_host(request)
        dataset_id = await _resolve_dataset_id(configured_host, dataset_ref)
        try:
            payload = await configured_host.query_dataset(
                request=CreateDatasetPreviewJobRequest(
                    dataset_id=dataset_id,
                    workspace_id=configured_host.context.workspace_id,
                    actor_id=configured_host.context.actor_id,
                    requested_limit=body.limit,
                    enforced_limit=body.limit or 100,
                    filters=body.filters,
                    sort=body.sort,
                    user_context=body.user_context,
                    correlation_id=configured_host.context.request_id,
                )
            )
        except Exception as exc:
            return RuntimeDatasetPreviewResponse(
                dataset_id=dataset_id,
                status="failed",
                error=str(exc),
            )
        return RuntimeDatasetPreviewResponse(
            dataset_id=dataset_id,
            dataset_name=payload.get("dataset_name"),
            status="succeeded",
            columns=list(payload.get("columns", [])),
            rows=list(payload.get("rows", [])),
            row_count_preview=int(payload.get("row_count_preview") or 0),
            effective_limit=payload.get("effective_limit"),
            redaction_applied=bool(payload.get("redaction_applied")),
            duration_ms=payload.get("duration_ms"),
            bytes_scanned=payload.get("bytes_scanned"),
            generated_sql=payload.get("generated_sql"),
        )

    @app.post("/api/runtime/v1/semantic/query", response_model=RuntimeSemanticQueryResponse)
    async def query_semantic(
        request: Request,
        body: RuntimeSemanticQueryRequest,
    ) -> RuntimeSemanticQueryResponse:
        configured_host = await _resolve_request_host(request)
        if not body.semantic_models:
            raise HTTPException(status_code=400, detail="semantic_models is required.")
        try:
            payload = await configured_host.query_semantic_models(
                semantic_models=body.semantic_models,
                measures=list(body.measures or []),
                dimensions=list(body.dimensions or []),
                filters=list(body.filters or []),
                time_dimensions=list(body.time_dimensions or []),
                limit=body.limit,
                order=body.order,
            )
        except Exception as exc:
            return RuntimeSemanticQueryResponse(
                status="failed",
                error=str(exc),
            )
        semantic_model_id = payload.get("semantic_model_id")
        connector_id = payload.get("connector_id")
        return RuntimeSemanticQueryResponse(
            status="succeeded",
            semantic_model_id=uuid.UUID(str(semantic_model_id)) if semantic_model_id is not None else None,
            semantic_model_ids=[
                uuid.UUID(str(value))
                for value in payload.get("semantic_model_ids", [])
            ],
            connector_id=uuid.UUID(str(connector_id)) if connector_id is not None else None,
            data=list(payload.get("rows", [])),
            annotations=list(payload.get("annotations", [])),
            metadata=payload.get("metadata"),
            generated_sql=payload.get("generated_sql"),
        )

    @app.post("/api/runtime/v1/sql/query", response_model=RuntimeSqlQueryResponse)
    async def query_sql(
        request: Request,
        body: RuntimeSqlQueryRequest,
    ) -> RuntimeSqlQueryResponse:
        configured_host = await _resolve_request_host(request)
        return await _execute_runtime_sql(configured_host, body)

    @app.post("/api/runtime/v1/agents/ask", response_model=RuntimeAgentAskResponse)
    async def ask_agent(
        request: Request,
        body: RuntimeAgentAskRequest,
    ) -> RuntimeAgentAskResponse:
        configured_host = await _resolve_request_host(request)
        agent_name = _resolve_agent_name(
            configured_host,
            agent_id=body.agent_id,
            agent_name=body.agent_name,
        )
        try:
            result = await configured_host.ask_agent(
                prompt=body.message,
                agent_name=agent_name,
            )
        except Exception as exc:
            return RuntimeAgentAskResponse(
                status="failed",
                error={"message": str(exc)},
            )
        payload_thread_id = result.get("thread_id")
        payload_job_id = result.get("job_id")
        return RuntimeAgentAskResponse(
            thread_id=uuid.UUID(str(payload_thread_id)) if payload_thread_id is not None else body.thread_id,
            status="succeeded",
            job_id=uuid.UUID(str(payload_job_id)) if payload_job_id is not None else None,
            summary=result.get("summary"),
            result=result.get("result"),
            visualization=result.get("visualization"),
            error=result.get("error"),
            events=list(result.get("events", [])),
        )

    @app.get("/api/runtime/v1/connectors", response_model=RuntimeConnectorListResponse)
    async def list_connectors(request: Request) -> RuntimeConnectorListResponse:
        configured_host = await _resolve_request_host(request)
        items = await configured_host.list_connectors()
        return RuntimeConnectorListResponse(items=items, total=len(items))

    @app.get(
        "/api/runtime/v1/connectors/{connector_name}/sync/resources",
        response_model=RuntimeSyncResourceListResponse,
    )
    async def list_sync_resources(request: Request, connector_name: str) -> RuntimeSyncResourceListResponse:
        configured_host = await _resolve_request_host(request)
        try:
            items = await configured_host.list_sync_resources(connector_name=connector_name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return RuntimeSyncResourceListResponse(items=items, total=len(items))

    @app.get(
        "/api/runtime/v1/connectors/{connector_name}/sync/states",
        response_model=RuntimeSyncStateListResponse,
    )
    async def list_sync_states(request: Request, connector_name: str) -> RuntimeSyncStateListResponse:
        configured_host = await _resolve_request_host(request)
        try:
            items = await configured_host.list_sync_states(connector_name=connector_name)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return RuntimeSyncStateListResponse(items=items, total=len(items))

    @app.post(
        "/api/runtime/v1/connectors/{connector_name}/sync",
        response_model=RuntimeSyncResponse,
    )
    async def sync_connector(
        request: Request,
        connector_name: str,
        body: RuntimeSyncRequest,
    ) -> RuntimeSyncResponse:
        configured_host = await _resolve_request_host(request)
        try:
            payload = await configured_host.sync_connector_resources(
                connector_name=connector_name,
                resources=list(body.resource_names or []),
                sync_mode=body.sync_mode,
                force_full_refresh=bool(body.force_full_refresh),
            )
        except Exception as exc:
            return RuntimeSyncResponse(
                status="failed",
                connector_name=connector_name,
                sync_mode=body.sync_mode,
                error=str(exc),
            )
        return RuntimeSyncResponse.model_validate(payload)

    return app


def create_runtime_api_app_from_env() -> FastAPI:
    config_path = os.getenv(_CONFIG_PATH_ENV)
    if not config_path:
        raise RuntimeError(f"{_CONFIG_PATH_ENV} must be set before starting the runtime host.")
    return create_runtime_api_app(config_path=config_path)


def _require_configured_host(runtime_host: RuntimeHost) -> ConfiguredLocalRuntimeHost:
    if isinstance(runtime_host, ConfiguredLocalRuntimeHost):
        return runtime_host
    raise HTTPException(
        status_code=501,
        detail="This runtime host only supports configured local runtimes in the current release.",
    )


async def _resolve_request_host(request: Request) -> ConfiguredLocalRuntimeHost:
    configured_host = _require_configured_host(request.app.state.runtime_host)
    auth_resolver = request.app.state.runtime_auth
    principal = await auth_resolver.authenticate(request)
    return configured_host.with_context(
        auth_resolver.build_context(request=request, principal=principal)
    )


def _resolve_agent_name(
    runtime_host: ConfiguredLocalRuntimeHost,
    *,
    agent_id: uuid.UUID | None,
    agent_name: str | None,
) -> str | None:
    normalized_name = str(agent_name or "").strip()
    if normalized_name:
        return normalized_name
    if agent_id is None:
        return None
    for candidate_name, record in runtime_host._agents.items():
        if record.id == agent_id:
            return candidate_name
    raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' was not found.")


async def _resolve_dataset_id(
    runtime_host: ConfiguredLocalRuntimeHost,
    dataset_ref: str,
) -> uuid.UUID:
    normalized_ref = str(dataset_ref or "").strip()
    if not normalized_ref:
        raise HTTPException(status_code=400, detail="dataset_ref is required.")
    try:
        return uuid.UUID(normalized_ref)
    except ValueError:
        pass

    datasets = await runtime_host.list_datasets()
    for item in datasets:
        if str(item.get("name") or "").strip() == normalized_ref:
            item_id = item.get("id")
            if item_id is None:
                break
            try:
                return uuid.UUID(str(item_id))
            except (TypeError, ValueError):
                break
    raise HTTPException(status_code=404, detail=f"Dataset '{dataset_ref}' was not found.")


async def _execute_runtime_sql(
    runtime_host: ConfiguredLocalRuntimeHost,
    request: RuntimeSqlQueryRequest,
) -> RuntimeSqlQueryResponse:
    selected_datasets = list(request.selected_datasets or [])
    if not selected_datasets and request.connection_name:
        try:
            payload = await runtime_host.execute_sql_text(
                query=request.query,
                connection_name=request.connection_name,
                requested_limit=request.requested_limit,
            )
        except Exception as exc:
            return RuntimeSqlQueryResponse(
                sql_job_id=uuid.uuid4(),
                status="failed",
                error={"message": str(exc)},
                query=request.query,
            )
        return RuntimeSqlQueryResponse(
            sql_job_id=uuid.uuid4(),
            status="succeeded",
            columns=list(payload.get("columns", [])),
            rows=list(payload.get("rows", [])),
            row_count_preview=int(payload.get("row_count_preview") or 0),
            total_rows_estimate=payload.get("total_rows_estimate"),
            bytes_scanned=payload.get("bytes_scanned"),
            duration_ms=payload.get("duration_ms"),
            redaction_applied=bool(payload.get("redaction_applied")),
            query=request.query,
            generated_sql=payload.get("generated_sql"),
        )

    sql_job_id = uuid.uuid4()
    create_request = CreateSqlJobRequest(
        sql_job_id=sql_job_id,
        workspace_id=runtime_host.context.workspace_id,
        actor_id=runtime_host.context.actor_id,
        workbench_mode=(SqlWorkbenchMode.dataset if selected_datasets else SqlWorkbenchMode.direct_sql),
        connection_id=request.connection_id,
        execution_mode=("federated" if selected_datasets else "single"),
        query=request.query,
        query_dialect=str(request.query_dialect or "tsql").strip().lower() or "tsql",
        params=dict(request.params or {}),
        requested_limit=request.requested_limit,
        requested_timeout_seconds=request.requested_timeout_seconds,
        enforced_limit=request.requested_limit or 100,
        enforced_timeout_seconds=request.requested_timeout_seconds or 30,
        allow_dml=False,
        allow_federation=bool(selected_datasets),
        selected_datasets=selected_datasets,
        federated_datasets=selected_datasets,
        explain=bool(request.explain),
        correlation_id=runtime_host.context.request_id,
    )
    try:
        payload = await runtime_host.execute_sql(request=create_request)
    except Exception as exc:
        return RuntimeSqlQueryResponse(
            sql_job_id=sql_job_id,
            status="failed",
            error={"message": str(exc)},
            query=request.query,
        )
    return RuntimeSqlQueryResponse(
        sql_job_id=sql_job_id,
        status="succeeded",
        columns=list(payload.get("columns", [])),
        rows=list(payload.get("rows", [])),
        row_count_preview=int(payload.get("row_count_preview") or 0),
        total_rows_estimate=payload.get("total_rows_estimate"),
        bytes_scanned=payload.get("bytes_scanned"),
        duration_ms=payload.get("duration_ms"),
        redaction_applied=bool(payload.get("redaction_applied")),
        query=request.query,
        generated_sql=payload.get("generated_sql"),
    )
