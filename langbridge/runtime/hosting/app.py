import logging
import os
import json
import uuid
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
import inspect
from pathlib import Path
from typing import Any
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from langbridge.mcp import DEFAULT_MCP_MOUNT_PATH, build_runtime_mcp_server
from langbridge.ui import register_runtime_ui
from langbridge.runtime.hosting.auth import (
    RuntimeAuthConfig,
    RuntimeAuthMode,
    RuntimeAuthPrincipal,
    RuntimeAuthResolver,
)
from langbridge.runtime.hosting.odbc import RuntimeOdbcEndpoint, RuntimeOdbcEndpointConfig
from langbridge.runtime.hosting.background import (
    BackgroundTaskSchedule,
    RuntimeBackgroundTaskDefinition,
    RuntimeBackgroundTaskManager,
    build_semantic_vector_refresh_default_task,
    build_dataset_sync_default_task,
    build_cleanup_default_task,
)
from langbridge.runtime.bootstrap import (
    ConfiguredLocalRuntimeHost,
    build_configured_local_runtime,
)
from langbridge.runtime.application.errors import ApplicationError, BusinessValidationError
from langbridge.runtime.models.jobs import (
    CreateDatasetPreviewJobRequest,
)
from langbridge.runtime.models.streaming import RuntimeRunStreamEvent
from langbridge.runtime.services.errors import ExecutionValidationError
from langbridge.runtime.services.runtime_host import RuntimeHost
from langbridge.runtime.hosting.api_models import (
    RuntimeAgentAskRequest,
    RuntimeAgentAskResponse,
    RuntimeActorCreateRequest,
    RuntimeActorListResponse,
    RuntimeActorResetPasswordRequest,
    RuntimeActorSummary,
    RuntimeActorUpdateRequest,
    RuntimeAuthBootstrapRequest,
    RuntimeAuthLoginRequest,
    RuntimeConnectorCreateRequest,
    RuntimeConnectorConfigSchemaResponse,
    RuntimeConnectorListResponse,
    RuntimeConnectorSummary,
    RuntimeConnectorTypesListResponse,
    RuntimeDatasetCreateRequest,
    RuntimeDatasetListResponse,
    RuntimeDatasetPreviewRequest,
    RuntimeDatasetPreviewResponse,
    RuntimeDatasetSyncRequest,
    RuntimeDatasetSyncStateResponse,
    RuntimeConnectorUpdateRequest,
    RuntimeDatasetUpdateRequest,
    RuntimeInfoResponse,
    RuntimeSemanticModelCreateRequest,
    RuntimeSemanticModelListResponse,
    RuntimeSemanticModelUpdateRequest,
    RuntimeSemanticQueryRequest,
    RuntimeSemanticQueryResponse,
    RuntimeSyncResourceListResponse,
    RuntimeSyncResponse,
    RuntimeSyncStateListResponse,
    RuntimeThreadCreateRequest,
    RuntimeThreadUpdateRequest,
    RuntimeSqlQueryRequest,
    RuntimeSqlQueryResponse,
)

_CONFIG_PATH_ENV = "LANGBRIDGE_RUNTIME_CONFIG_PATH"
_FEATURES_ENV = "LANGBRIDGE_RUNTIME_FEATURES"
_DEBUG_ENV = "LANGBRIDGE_RUNTIME_DEBUG"
_ODBC_HOST_ENV = "LANGBRIDGE_RUNTIME_ODBC_HOST"
_ODBC_PORT_ENV = "LANGBRIDGE_RUNTIME_ODBC_PORT"
_SEMANTIC_VECTOR_REFRESH_TASK_NAME = "semantic-vector-refresh"
_RUNTIME_CLEANUP_TASK_NAME = "runtime-cleanup"
_RUNTIME_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_DEBUG_HANDLER_MARKER = "_langbridge_runtime_debug_handler"


def create_runtime_api_app(
    *,
    config_path: str | Path | None = None,
    runtime_host: ConfiguredLocalRuntimeHost | None = None,
    auth_config: RuntimeAuthConfig | None = None,
    features: Iterable[str] | None = None,
    debug: bool = False,
    odbc_host: str | None = None,
    odbc_port: int | None = None,
    default_background_tasks: Iterable[RuntimeBackgroundTaskDefinition] | None = None,
    background_tasks: Iterable[RuntimeBackgroundTaskDefinition] | None = None,
    background_task_manager: RuntimeBackgroundTaskManager | None = None,
) -> FastAPI:
    _configure_runtime_logging(debug=debug)
    host = runtime_host
    if host is None:
        if config_path is None:
            raise ValueError("config_path is required when runtime_host is not supplied.")
        host = build_configured_local_runtime(config_path=str(config_path))
    enabled_features = _normalize_runtime_features(features)
    mcp_enabled = "mcp" in enabled_features
    ui_enabled = "ui" in enabled_features
    odbc_enabled = "odbc" in enabled_features
    auth_resolver = RuntimeAuthResolver(
        config=auth_config
        or RuntimeAuthConfig.from_env(
            config_path=getattr(host, "_config_path", config_path),
        ),
        default_context=host.context,
        runtime_host=host,
    )
    task_manager = RuntimeBackgroundTaskManager(
        runtime_host=host,
        default_tasks=default_background_tasks,
        custom_tasks=background_tasks,
    )
    mcp_server = None
    mcp_app = None
    odbc_server = None
    if mcp_enabled:
        mcp_server, mcp_app = build_runtime_mcp_server(
            runtime_host=host,
            auth_resolver=auth_resolver,
            mount_path=DEFAULT_MCP_MOUNT_PATH,
            debug=debug,
        )
    if odbc_enabled:
        odbc_server = RuntimeOdbcEndpoint(
            runtime_host=host,
            auth_config=auth_resolver.config,
            config=RuntimeOdbcEndpointConfig.from_env(
                host=odbc_host,
                port=odbc_port,
            ),
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if odbc_server is not None:
            await odbc_server.start()
        await _register_runtime_dataset_background_tasks(
            task_manager=task_manager,
            runtime_host=host,
        )
        await _register_runtime_semantic_vector_refresh_task(
            task_manager=task_manager,
            runtime_host=host,
        )
        await _register_runtime_cleanup_task(
            task_manager=task_manager,
            runtime_host=host,
        )
        await task_manager.start()
        try:
            if mcp_server is None:
                yield
                return
            if debug:
                logging.getLogger("langbridge.runtime.mcp").debug(
                    "MCP debug logging enabled for %s/.",
                    DEFAULT_MCP_MOUNT_PATH,
                )
            async with mcp_server.session_manager.run():
                yield
        finally:
            await task_manager.stop()
            if odbc_server is not None:
                await odbc_server.close()
            await _close_runtime_host(host)

    app = FastAPI(
        title="Langbridge Runtime Host",
        version="0.1.0",
        docs_url="/api/runtime/docs",
        openapi_url="/api/runtime/openapi.json",
        lifespan=lifespan,
    )
    app.state.runtime_host = host
    app.state.runtime_features = enabled_features
    app.state.runtime_auth = auth_resolver
    app.state.runtime_background_tasks = task_manager
    app.state.runtime_debug = bool(debug)
    app.state.runtime_odbc = odbc_server

    if mcp_enabled:
        @app.middleware("http")
        async def _normalize_mcp_mount_path(request: Request, call_next):
            normalized_no_slash = request.url.path == DEFAULT_MCP_MOUNT_PATH
            if normalized_no_slash:
                request.scope["path"] = f"{DEFAULT_MCP_MOUNT_PATH}/"
                request.scope["raw_path"] = f"{DEFAULT_MCP_MOUNT_PATH}/".encode("ascii")
            response = await call_next(request)
            if normalized_no_slash and debug:
                logging.getLogger("langbridge.runtime.mcp").debug(
                    "Normalized MCP request path from %s to %s/ to avoid a redirect.",
                    DEFAULT_MCP_MOUNT_PATH,
                    DEFAULT_MCP_MOUNT_PATH,
                )
            return response

    @app.get("/api/runtime/v1/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/runtime/v1/auth/bootstrap")
    async def runtime_auth_bootstrap_status() -> dict[str, Any]:
        return await _build_runtime_auth_status(auth_resolver)

    @app.post("/api/runtime/v1/auth/bootstrap")
    async def runtime_auth_bootstrap(
        request: Request,
        body: RuntimeAuthBootstrapRequest,
    ) -> JSONResponse:
        local_auth = _require_local_auth_manager(auth_resolver)
        try:
            session = await local_auth.bootstrap_admin(
                username=body.username,
                email=body.email,
                password=body.password,
            )
        except ValueError as exc:
            detail = str(exc)
            status_code = 409 if "already" in detail.lower() else 400
            raise HTTPException(status_code=status_code, detail=detail) from exc
        principal = await auth_resolver.sync_local_session(session)
        token = await local_auth.issue_session_token(session)
        response = JSONResponse(
            {
                "ok": True,
                "auth_mode": auth_resolver.mode.value,
                "user": _serialize_runtime_principal_user(principal),
            }
        )
        _set_runtime_session_cookie(
            request=request,
            response=response,
            auth_resolver=auth_resolver,
            token=token,
        )
        return response

    @app.post("/api/runtime/v1/auth/login")
    async def runtime_auth_login(
        request: Request,
        body: RuntimeAuthLoginRequest,
    ) -> JSONResponse:
        local_auth = _require_local_auth_manager(auth_resolver)
        try:
            session = await local_auth.authenticate(
                identifier=str(body.identifier or ""),
                password=body.password,
            )
        except ValueError as exc:
            detail = str(exc)
            status_code = 409 if "bootstrap" in detail.lower() else 401
            raise HTTPException(status_code=status_code, detail=detail) from exc
        principal = await auth_resolver.sync_local_session(session)
        token = await local_auth.issue_session_token(session)
        response = JSONResponse(
            {
                "ok": True,
                "auth_mode": auth_resolver.mode.value,
                "user": _serialize_runtime_principal_user(principal),
            }
        )
        _set_runtime_session_cookie(
            request=request,
            response=response,
            auth_resolver=auth_resolver,
            token=token,
        )
        return response

    @app.post("/api/runtime/v1/auth/logout")
    async def runtime_auth_logout() -> JSONResponse:
        response = JSONResponse({"ok": True, "auth_mode": auth_resolver.mode.value})
        local_auth = auth_resolver.local_auth
        if local_auth is not None:
            local_auth.delete_session_cookie(response)
        return response

    @app.get("/api/runtime/v1/auth/me")
    async def runtime_auth_me(request: Request) -> dict[str, Any]:
        if auth_resolver.mode == RuntimeAuthMode.none:
            configured_host = _require_configured_host(app.state.runtime_host)
            return {
                "auth_enabled": False,
                "auth_mode": auth_resolver.mode.value,
                "user": {
                    "id": str(configured_host.context.actor_id) if configured_host.context.actor_id else None,
                    "username": "runtime",
                    "email": None,
                    "roles": list(configured_host.context.roles),
                    "provider": "runtime_none",
                },
            }
        principal = await auth_resolver.authenticate(request)
        return {
            "auth_enabled": True,
            "auth_mode": auth_resolver.mode.value,
            "user": _serialize_runtime_principal_user(principal),
        }

    @app.get("/api/runtime/v1/actors", response_model=RuntimeActorListResponse)
    async def list_runtime_actors(request: Request) -> RuntimeActorListResponse:
        principal = await _require_runtime_admin_principal(request)
        _ = principal
        local_auth = _require_local_auth_manager(auth_resolver)
        items = [RuntimeActorSummary.model_validate(_serialize_runtime_actor(actor)) for actor in await local_auth.list_actors()]
        return RuntimeActorListResponse(items=items, total=len(items))

    @app.post("/api/runtime/v1/actors", status_code=201, response_model=RuntimeActorSummary)
    async def create_runtime_actor(
        request: Request,
        body: RuntimeActorCreateRequest,
    ) -> RuntimeActorSummary:
        principal = await _require_runtime_admin_principal(request)
        _ = principal
        local_auth = _require_local_auth_manager(auth_resolver)
        try:
            actor = await local_auth.create_actor(
                username=body.username,
                email=body.email,
                display_name=body.display_name,
                actor_type=body.actor_type,
                password=body.password,
                roles=list(body.roles or []),
            )
        except ValueError as exc:
            raise HTTPException(status_code=_runtime_mutation_status_code(str(exc)), detail=str(exc)) from exc
        return RuntimeActorSummary.model_validate(_serialize_runtime_actor(actor))

    @app.patch("/api/runtime/v1/actors/{actor_id}", response_model=RuntimeActorSummary)
    async def update_runtime_actor(
        request: Request,
        actor_id: uuid.UUID,
        body: RuntimeActorUpdateRequest,
    ) -> RuntimeActorSummary:
        principal = await _require_runtime_admin_principal(request)
        _ = principal
        local_auth = _require_local_auth_manager(auth_resolver)
        try:
            actor = await local_auth.update_actor(
                actor_id=actor_id,
                email=body.email,
                display_name=body.display_name,
                actor_type=body.actor_type,
                status=body.status,
                roles=None if body.roles is None else list(body.roles),
            )
        except ValueError as exc:
            detail = str(exc)
            status_code = 404 if "not found" in detail.lower() else _runtime_mutation_status_code(detail)
            raise HTTPException(status_code=status_code, detail=detail) from exc
        return RuntimeActorSummary.model_validate(_serialize_runtime_actor(actor))

    @app.post("/api/runtime/v1/actors/{actor_id}/reset-password", response_model=RuntimeActorSummary)
    async def reset_runtime_actor_password(
        request: Request,
        actor_id: uuid.UUID,
        body: RuntimeActorResetPasswordRequest,
    ) -> RuntimeActorSummary:
        principal = await _require_runtime_admin_principal(request)
        _ = principal
        local_auth = _require_local_auth_manager(auth_resolver)
        try:
            actor = await local_auth.reset_password(
                actor_id=actor_id,
                password=body.password,
                must_rotate_password=body.must_rotate_password,
            )
        except ValueError as exc:
            detail = str(exc)
            status_code = 404 if "not found" in detail.lower() else _runtime_mutation_status_code(detail)
            raise HTTPException(status_code=status_code, detail=detail) from exc
        return RuntimeActorSummary.model_validate(_serialize_runtime_actor(actor))

    @app.get("/api/runtime/v1/info", response_model=RuntimeInfoResponse)
    async def info(request: Request) -> RuntimeInfoResponse:
        configured_host = await _resolve_request_host(request)
        connector_items = await configured_host.list_connectors()
        capabilities = [
            "datasets.list",
            "datasets.get",
            "datasets.create",
            "datasets.update",
            "datasets.delete",
            "datasets.preview",
            "connectors.get",
            "connectors.create",
            "connectors.update",
            "connectors.delete",
            "semantic_models.list",
            "semantic_models.get",
            "semantic_models.create",
            "semantic_models.update",
            "semantic_models.delete",
            "semantic.query",
            "sql.query",
            "agents.list",
            "agents.get",
            "agents.ask",
            "threads.create",
            "threads.list",
            "threads.get",
            "threads.update",
            "threads.delete",
            "threads.messages.list",
        ]
        if connector_items:
            capabilities.append("connectors.list")
        if auth_resolver.local_auth_enabled:
            capabilities.extend(
                [
                    "auth.bootstrap",
                    "auth.login",
                    "auth.logout",
                    "auth.me",
                    "actors.list",
                    "actors.create",
                    "actors.update",
                    "actors.reset_password",
                ]
            )
        if any(bool(item.get("supports_sync")) for item in connector_items):
            capabilities.extend(
                [
                    "connectors.sync.resources",
                    "connectors.sync.states",
                    "datasets.sync.get",
                    "datasets.sync.run",
                ]
            )
        if ui_enabled:
            capabilities.append("ui")
        if mcp_enabled:
            capabilities.append("mcp")
        if odbc_enabled:
            capabilities.append("odbc")
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

    @app.post("/api/runtime/v1/datasets", status_code=201)
    async def create_dataset(
        request: Request,
        body: RuntimeDatasetCreateRequest,
    ) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.create_dataset(request=body)
        except (ValueError, ApplicationError) as exc:
            raise HTTPException(
                status_code=_runtime_mutation_status_code(str(exc)),
                detail=str(exc),
            ) from exc

    @app.patch("/api/runtime/v1/datasets/{dataset_ref}")
    async def update_dataset(
        request: Request,
        dataset_ref: str,
        body: RuntimeDatasetUpdateRequest,
    ) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.update_dataset(dataset_ref=dataset_ref, request=body)
        except (ValueError, ApplicationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_runtime_resource(detail) else _runtime_mutation_status_code(detail)
            raise HTTPException(status_code=status_code, detail=detail) from exc

    @app.delete("/api/runtime/v1/datasets/{dataset_ref}")
    async def delete_dataset(request: Request, dataset_ref: str) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.delete_dataset(dataset_ref=dataset_ref)
        except (ValueError, ApplicationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_runtime_resource(detail) else _runtime_mutation_status_code(detail)
            raise HTTPException(status_code=status_code, detail=detail) from exc

    @app.get("/api/runtime/v1/datasets/{dataset_ref}")
    async def get_dataset(request: Request, dataset_ref: str) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.get_dataset(dataset_ref=dataset_ref)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

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
        except (ValueError, ExecutionValidationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_runtime_resource(detail) else 400
            raise HTTPException(status_code=status_code, detail=detail) from exc
        except HTTPException:
            raise
        except Exception as exc:
            _raise_runtime_internal_server_error("dataset preview", exc)

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

    @app.get(
        "/api/runtime/v1/datasets/{dataset_ref}/sync",
        response_model=RuntimeDatasetSyncStateResponse,
    )
    async def get_dataset_sync(
        request: Request,
        dataset_ref: str,
    ) -> RuntimeDatasetSyncStateResponse:
        configured_host = await _resolve_request_host(request)
        try:
            payload = await configured_host.get_dataset_sync(dataset_ref=dataset_ref)
        except (ValueError, BusinessValidationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_runtime_resource(detail) else 400
            raise HTTPException(status_code=status_code, detail=detail) from exc
        return RuntimeDatasetSyncStateResponse.model_validate(payload)

    @app.post(
        "/api/runtime/v1/datasets/{dataset_ref}/sync",
        response_model=RuntimeSyncResponse,
    )
    async def sync_dataset(
        request: Request,
        dataset_ref: str,
        body: RuntimeDatasetSyncRequest,
    ) -> RuntimeSyncResponse:
        configured_host = await _resolve_request_host(request)
        try:
            payload = await configured_host.sync_dataset(
                dataset_ref=dataset_ref,
                sync_mode=body.sync_mode,
                force_full_refresh=bool(body.force_full_refresh),
            )
        except (ValueError, BusinessValidationError, ExecutionValidationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_runtime_resource(detail) else 400
            raise HTTPException(status_code=status_code, detail=detail) from exc
        except HTTPException:
            raise
        except Exception as exc:
            _raise_runtime_internal_server_error("dataset sync", exc)
        return RuntimeSyncResponse.model_validate(payload)

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
        except (ValueError, ExecutionValidationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_semantic_resource(detail) else 400
            raise HTTPException(status_code=status_code, detail=detail) from exc
        except HTTPException:
            raise
        except Exception as exc:
            _raise_runtime_internal_server_error("semantic query", exc)
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
            federation_diagnostics=payload.get("federation_diagnostics"),
        )

    @app.get("/api/runtime/v1/semantic-models", response_model=RuntimeSemanticModelListResponse)
    async def list_semantic_models(request: Request) -> RuntimeSemanticModelListResponse:
        configured_host = await _resolve_request_host(request)
        items = await configured_host.list_semantic_models()
        return RuntimeSemanticModelListResponse(items=items, total=len(items))

    @app.post("/api/runtime/v1/semantic-models", status_code=201)
    async def create_semantic_model(
        request: Request,
        body: RuntimeSemanticModelCreateRequest,
    ) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.create_semantic_model(request=body)
        except (ValueError, ApplicationError) as exc:
            raise HTTPException(
                status_code=_runtime_mutation_status_code(str(exc)),
                detail=str(exc),
            ) from exc

    @app.patch("/api/runtime/v1/semantic-models/{model_ref}")
    async def update_semantic_model(
        request: Request,
        model_ref: str,
        body: RuntimeSemanticModelUpdateRequest,
    ) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.update_semantic_model(model_ref=model_ref, request=body)
        except (ValueError, ApplicationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_semantic_resource(detail) else _runtime_mutation_status_code(detail)
            raise HTTPException(status_code=status_code, detail=detail) from exc

    @app.delete("/api/runtime/v1/semantic-models/{model_ref}")
    async def delete_semantic_model(request: Request, model_ref: str) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.delete_semantic_model(model_ref=model_ref)
        except (ValueError, ApplicationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_semantic_resource(detail) else _runtime_mutation_status_code(detail)
            raise HTTPException(status_code=status_code, detail=detail) from exc

    @app.get("/api/runtime/v1/semantic-models/{model_ref}")
    async def get_semantic_model(request: Request, model_ref: str) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.get_semantic_model(model_ref=model_ref)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/api/runtime/v1/sql/query", response_model=RuntimeSqlQueryResponse)
    async def query_sql(
        request: Request,
        body: RuntimeSqlQueryRequest,
    ) -> RuntimeSqlQueryResponse:
        configured_host = await _resolve_request_host(request)
        try:
            return await _execute_runtime_sql(configured_host, body)
        except HTTPException:
            raise
        except (ValueError, ApplicationError, ExecutionValidationError) as exc:
            detail = str(exc)
            raise HTTPException(
                status_code=_runtime_query_status_code(detail),
                detail=detail,
            ) from exc

    @app.get("/api/runtime/v1/agents")
    async def list_agents(request: Request) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        items = await configured_host.list_agents()
        return {"items": items, "total": len(items)}

    @app.get("/api/runtime/v1/agents/{agent_ref}")
    async def get_agent(request: Request, agent_ref: str) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.get_agent(agent_ref=agent_ref)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

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
            ask_kwargs = {
                "prompt": body.message,
                "agent_name": agent_name,
                "thread_id": body.thread_id,
                "title": body.title,
            }
            if body.agent_mode is not None:
                ask_kwargs["agent_mode"] = body.agent_mode
            if body.metadata_json is not None:
                ask_kwargs["metadata_json"] = body.metadata_json
            result = await configured_host.ask_agent(**ask_kwargs)
        except HTTPException:
            raise
        except Exception as exc:
            _raise_runtime_internal_server_error("agent ask", exc)
        payload_thread_id = result.get("thread_id")
        payload_job_id = result.get("job_id")
        return RuntimeAgentAskResponse(
            thread_id=uuid.UUID(str(payload_thread_id)) if payload_thread_id is not None else body.thread_id,
            status="succeeded",
            run_id=uuid.UUID(str(payload_job_id)) if payload_job_id is not None else None,
            job_id=uuid.UUID(str(payload_job_id)) if payload_job_id is not None else None,
            message_id=(
                uuid.UUID(str(result.get("message_id")))
                if result.get("message_id") is not None
                else None
            ),
            summary=result.get("summary"),
            result=result.get("result"),
            visualization=result.get("visualization"),
            error=result.get("error"),
            events=list(result.get("events", [])),
        )

    @app.post("/api/runtime/v1/agents/ask/stream")
    async def ask_agent_stream(
        request: Request,
        body: RuntimeAgentAskRequest,
    ) -> StreamingResponse:
        configured_host = await _resolve_request_host(request)
        agent_name = _resolve_agent_name(
            configured_host,
            agent_id=body.agent_id,
            agent_name=body.agent_name,
        )
        stream_kwargs = {
            "prompt": body.message,
            "agent_name": agent_name,
            "thread_id": body.thread_id,
            "title": body.title,
            "agent_mode": body.agent_mode,
        }
        if body.agent_mode is not None:
            stream_kwargs["agent_mode"] = body.agent_mode
        if body.metadata_json is not None:
            stream_kwargs["metadata_json"] = body.metadata_json
        return _build_runtime_sse_response(
            _stream_runtime_run_events(
                request=request,
                events=configured_host.ask_agent_stream(**stream_kwargs),
            )
        )

    @app.get("/api/runtime/v1/runs/{run_id}/stream")
    async def stream_run(
        request: Request,
        run_id: uuid.UUID,
        after_sequence: int = 0,
    ) -> StreamingResponse:
        configured_host = await _resolve_request_host(request)
        try:
            events = await configured_host.stream_run(
                run_id=run_id,
                after_sequence=after_sequence,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Run '{run_id}' was not found.") from exc
        return _build_runtime_sse_response(
            _stream_runtime_run_events(
                request=request,
                events=events,
            )
        )

    @app.post("/api/runtime/v1/threads")
    async def create_thread(
        request: Request,
        body: RuntimeThreadCreateRequest,
    ) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        return await configured_host.create_thread(title=body.title)

    @app.get("/api/runtime/v1/threads")
    async def list_threads(request: Request) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        items = await configured_host.list_threads()
        return {"items": items, "total": len(items)}

    @app.get("/api/runtime/v1/threads/{thread_id}")
    async def get_thread(request: Request, thread_id: uuid.UUID) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.get_thread(thread_id=thread_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.patch("/api/runtime/v1/threads/{thread_id}")
    async def update_thread(
        request: Request,
        thread_id: uuid.UUID,
        body: RuntimeThreadUpdateRequest,
    ) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.update_thread(thread_id=thread_id, title=body.title)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.delete("/api/runtime/v1/threads/{thread_id}")
    async def delete_thread(request: Request, thread_id: uuid.UUID) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.delete_thread(thread_id=thread_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/runtime/v1/threads/{thread_id}/messages")
    async def list_thread_messages(request: Request, thread_id: uuid.UUID) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            items = await configured_host.list_thread_messages(thread_id=thread_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"items": items, "total": len(items)}

    @app.get("/api/runtime/v1/connectors", response_model=RuntimeConnectorListResponse)
    async def list_connectors(request: Request) -> RuntimeConnectorListResponse:
        configured_host = await _resolve_request_host(request)
        items = await configured_host.list_connectors()
        return RuntimeConnectorListResponse(items=items, total=len(items))
    
    
    @app.get("/api/runtime/v1/connector/types", response_model=RuntimeConnectorTypesListResponse)
    async def list_connector_types(request: Request) -> RuntimeConnectorTypesListResponse:
        configured_host = await _resolve_request_host(request)
        items = await configured_host.list_connector_types()
        return RuntimeConnectorTypesListResponse(items=items, total=len(items))

    @app.get(
        "/api/runtime/v1/connector/type/{connector_type}/config",
        response_model=RuntimeConnectorConfigSchemaResponse,
    )
    async def get_connector_type_config(
        request: Request,
        connector_type: str,
    ) -> RuntimeConnectorConfigSchemaResponse:
        configured_host = await _resolve_request_host(request)
        try:
            payload = await configured_host.get_connector_type_config(
                connector_type=connector_type
            )
        except BusinessValidationError as exc:
            raise HTTPException(status_code=400, detail=exc.message) from exc
        return RuntimeConnectorConfigSchemaResponse.model_validate(payload)

    @app.get("/api/runtime/v1/connectors/{connector_name}")
    async def get_connector(request: Request, connector_name: str) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.get_connector(connector_name=connector_name)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post(
        "/api/runtime/v1/connectors",
        response_model=RuntimeConnectorSummary,
        status_code=201,
    )
    async def create_connector(
        request: Request,
        body: RuntimeConnectorCreateRequest,
    ) -> RuntimeConnectorSummary:
        configured_host = await _resolve_request_host(request)
        try:
            payload = await configured_host.create_connector(request=body)
        except (ValueError, ApplicationError) as exc:
            raise HTTPException(
                status_code=_runtime_mutation_status_code(str(exc)),
                detail=str(exc),
            ) from exc
        return RuntimeConnectorSummary.model_validate(payload)

    @app.patch("/api/runtime/v1/connectors/{connector_name}")
    async def update_connector(
        request: Request,
        connector_name: str,
        body: RuntimeConnectorUpdateRequest,
    ) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.update_connector(connector_name=connector_name, request=body)
        except (ValueError, ApplicationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_runtime_resource(detail) else _runtime_mutation_status_code(detail)
            raise HTTPException(status_code=status_code, detail=detail) from exc

    @app.delete("/api/runtime/v1/connectors/{connector_name}")
    async def delete_connector(request: Request, connector_name: str) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        try:
            return await configured_host.delete_connector(connector_name=connector_name)
        except (ValueError, ApplicationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_runtime_resource(detail) else _runtime_mutation_status_code(detail)
            raise HTTPException(status_code=status_code, detail=detail) from exc

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

    @app.get("/api/runtime/ui/v1/summary")
    async def runtime_ui_summary(request: Request) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        connector_items = await configured_host.list_connectors()
        dataset_items = await configured_host.list_datasets()
        semantic_model_items = await configured_host.list_semantic_models()
        agent_items = await configured_host.list_agents()
        thread_items = await configured_host.list_threads()
        return {
            "health": {"status": "ok"},
            "features": list(enabled_features),
            "auth": await _build_runtime_auth_status(auth_resolver),
            "runtime": {
                "mode": "configured_local",
                "workspace_id": str(configured_host.context.workspace_id),
                "actor_id": str(configured_host.context.actor_id) if configured_host.context.actor_id else None,
                "default_semantic_model": configured_host._default_semantic_model_name,
                "default_agent": configured_host._default_agent.config.name if configured_host._default_agent else None,
            },
            "counts": {
                "datasets": len(dataset_items),
                "connectors": len(connector_items),
                "semantic_models": len(semantic_model_items),
                "agents": len(agent_items),
                "threads": len(thread_items),
            },
            "datasets": [
                {
                    "id": _stringify_optional_uuid(item.get("id")),
                    "name": item.get("name"),
                    "connector": item.get("connector"),
                    "semantic_model": item.get("semantic_model"),
                    "management_mode": item.get("management_mode"),
                    "managed": bool(item.get("managed")),
                }
                for item in dataset_items[:8]
            ],
            "connectors": [
                {
                    "id": _stringify_optional_uuid(item.get("id")),
                    "name": item.get("name"),
                    "connector_type": item.get("connector_type"),
                    "supports_sync": bool(item.get("supports_sync")),
                    "management_mode": item.get("management_mode"),
                    "managed": bool(item.get("managed")),
                }
                for item in connector_items[:8]
            ],
            "semantic_models": semantic_model_items[:6],
            "agents": agent_items[:6],
            "threads": thread_items[:6],
        }

    if ui_enabled:

        register_runtime_ui(app)

    if mcp_enabled and mcp_app is not None:
        app.mount(DEFAULT_MCP_MOUNT_PATH, mcp_app)

    return app


def create_runtime_api_app_from_env() -> FastAPI:
    config_path = os.getenv(_CONFIG_PATH_ENV)
    if not config_path:
        raise RuntimeError(f"{_CONFIG_PATH_ENV} must be set before starting the runtime host.")
    return create_runtime_api_app(
        config_path=config_path,
        features=_parse_runtime_features_env(os.getenv(_FEATURES_ENV)),
        debug=_parse_runtime_debug_env(os.getenv(_DEBUG_ENV)),
        odbc_host=os.getenv(_ODBC_HOST_ENV),
        odbc_port=_parse_runtime_port_env(os.getenv(_ODBC_PORT_ENV)),
    )


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
        if normalized_name in runtime_host._agents:
            return normalized_name
        raise HTTPException(status_code=404, detail=f"Agent '{normalized_name}' was not found.")
    if agent_id is None:
        return None
    for candidate_name, record in runtime_host._agents.items():
        if record.id == agent_id:
            return candidate_name
    raise HTTPException(status_code=404, detail=f"Agent '{agent_id}' was not found.")


def _encode_sse_payload(*, event_name: str, payload: RuntimeRunStreamEvent) -> str:
    serialized = json.dumps(payload.model_dump(mode="json"))
    stream_id = payload.run_id or payload.job_id
    event_id = (
        f"{stream_id}:{payload.sequence}"
        if stream_id is not None
        else str(payload.sequence)
    )
    return f"id: {event_id}\nevent: {event_name}\ndata: {serialized}\n\n"


def _encode_sse_comment(comment: str | None = None, *, padding: int = 0) -> str:
    message = f": {str(comment or '').strip()}".rstrip()
    if padding > 0:
        message = f"{message}{' ' * padding}"
    return f"{message}\n\n"


def _build_runtime_sse_response(event_stream: AsyncIterator[str]) -> StreamingResponse:
    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(
        event_stream,
        media_type="text/event-stream",
        headers=headers,
    )


async def _stream_runtime_run_events(
    *,
    request: Request,
    events: AsyncIterator[RuntimeRunStreamEvent | None],
) -> AsyncIterator[str]:
    yield _encode_sse_comment("stream-open", padding=2048)
    async for event in events:
        if await request.is_disconnected():
            return
        if event is None:
            yield _encode_sse_comment("keep-alive")
            continue
        yield _encode_sse_payload(
            event_name=event.event,
            payload=event,
        )


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
    try:
        payload = await runtime_host.query_sql(request=request)
    except HTTPException:
        raise
    except (ValueError, ApplicationError, ExecutionValidationError):
        raise
    except Exception as exc:
        _raise_runtime_internal_server_error(f"{request.query_scope.value} SQL query", exc)

    payload_sql_job_id = payload.get("sql_job_id")
    try:
        sql_job_id = uuid.UUID(str(payload_sql_job_id)) if payload_sql_job_id is not None else uuid.uuid4()
    except (TypeError, ValueError):
        sql_job_id = uuid.uuid4()

    return RuntimeSqlQueryResponse(
        sql_job_id=sql_job_id,
        query_scope=payload.get("query_scope") or request.query_scope,
        status="succeeded",
        semantic_model_id=payload.get("semantic_model_id"),
        semantic_model_ids=list(payload.get("semantic_model_ids", [])),
        connector_id=payload.get("connector_id"),
        columns=list(payload.get("columns", [])),
        rows=list(payload.get("rows", [])),
        row_count_preview=int(payload.get("row_count_preview") or 0),
        total_rows_estimate=payload.get("total_rows_estimate"),
        bytes_scanned=payload.get("bytes_scanned"),
        duration_ms=payload.get("duration_ms"),
        redaction_applied=bool(payload.get("redaction_applied")),
        query=payload.get("query") or request.query,
        generated_sql=payload.get("generated_sql"),
        federation_diagnostics=payload.get("federation_diagnostics"),
    )


def _parse_runtime_features_env(value: str | None) -> tuple[str, ...]:
    return _normalize_runtime_features(str(value or "").split(","))


def _raise_runtime_internal_server_error(operation: str, exc: Exception) -> None:
    logging.getLogger(__name__).exception("Runtime API %s failed", operation)
    raise HTTPException(status_code=500, detail=str(exc)) from exc


def _parse_runtime_debug_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "debug"}


def _parse_runtime_port_env(value: str | None) -> int | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    return int(normalized)


def _require_local_auth_manager(auth_resolver: RuntimeAuthResolver):
    if auth_resolver.local_auth is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Runtime local operator login is not enabled. "
                "Use a secured runtime auth mode and keep LANGBRIDGE_RUNTIME_AUTH_LOCAL_ENABLED enabled "
                "to use bootstrap and login endpoints."
            ),
        )
    return auth_resolver.local_auth


async def _build_runtime_auth_status(auth_resolver: RuntimeAuthResolver) -> dict[str, Any]:
    if auth_resolver.mode == RuntimeAuthMode.none:
        return {
            "auth_enabled": False,
            "auth_mode": auth_resolver.mode.value,
            "bootstrap_required": False,
            "has_admin": False,
            "login_allowed": False,
        }

    if auth_resolver.local_auth is None:
        return {
            "auth_enabled": True,
            "auth_mode": auth_resolver.mode.value,
            "bootstrap_required": False,
            "has_admin": False,
            "login_allowed": False,
            "detail": "This runtime uses bearer authentication only. Local operator browser sessions are disabled.",
        }

    status = await auth_resolver.local_auth.auth_status()
    return {
        "auth_enabled": True,
        "auth_mode": auth_resolver.mode.value,
        "bootstrap_required": bool(status["bootstrap_required"]),
        "has_admin": bool(status["has_admin"]),
        "login_allowed": True,
        "session_cookie_name": auth_resolver.local_auth.cookie_name,
        "detail": (
            "Bearer clients can continue using the configured runtime auth mode. "
            "The runtime UI uses a local operator session cookie."
        ),
    }


def _set_runtime_session_cookie(
    *,
    request: Request,
    response: JSONResponse,
    auth_resolver: RuntimeAuthResolver,
    token: str,
    ) -> None:
    local_auth = _require_local_auth_manager(auth_resolver)
    forwarded_proto = str(request.headers.get("x-forwarded-proto") or "").strip().lower()
    secure = forwarded_proto == "https" or request.url.scheme == "https"
    response.set_cookie(
        key=local_auth.cookie_name,
        value=token,
        max_age=local_auth.session_max_age_seconds,
        httponly=True,
        samesite="lax",
        secure=secure,
        path="/",
    )


def _serialize_runtime_principal_user(principal: RuntimeAuthPrincipal) -> dict[str, Any]:
    username = principal.username or principal.subject or principal.display_name or "runtime"
    return {
        "id": str(principal.actor_id) if principal.actor_id else None,
        "username": username,
        "display_name": principal.display_name or username,
        "email": principal.email,
        "roles": list(principal.roles),
        "provider": principal.provider,
    }


def _serialize_runtime_actor(actor: Any) -> dict[str, Any]:
    return {
        "id": actor.id,
        "workspace_id": actor.workspace_id,
        "subject": actor.subject,
        "username": actor.username,
        "email": actor.email,
        "display_name": actor.display_name,
        "actor_type": actor.actor_type,
        "status": actor.status,
        "roles": list(actor.roles),
        "auth_provider": "local_password",
        "password_algorithm": actor.password_algorithm,
        "password_updated_at": actor.password_updated_at,
        "must_rotate_password": bool(actor.must_rotate_password),
        "created_at": actor.created_at,
        "updated_at": actor.updated_at,
    }


async def _require_runtime_admin_principal(request: Request) -> RuntimeAuthPrincipal:
    auth_resolver = request.app.state.runtime_auth
    principal = await auth_resolver.authenticate(request)
    if not _principal_has_runtime_admin(principal):
        raise HTTPException(status_code=403, detail="Runtime admin access is required.")
    return principal


def _principal_has_runtime_admin(principal: RuntimeAuthPrincipal) -> bool:
    normalized_roles = {str(role or "").strip().lower() for role in principal.roles}
    return "admin" in normalized_roles or "runtime:admin" in normalized_roles


async def _register_runtime_dataset_background_tasks(
    *,
    task_manager: RuntimeBackgroundTaskManager,
    runtime_host: RuntimeHost,
) -> None:
    if not isinstance(runtime_host, ConfiguredLocalRuntimeHost):
        return
    existing_names = {
        str(task.name or "").strip()
        for task in task_manager.list_tasks()
        if str(task.name or "").strip()
    }
    dataset_tasks = await runtime_host._applications.datasets.build_scheduled_sync_tasks()
    for task in dataset_tasks:
        if task.name in existing_names:
            continue
        task_manager.register_default_task(task)
        existing_names.add(task.name)
        
async def _register_runtime_semantic_vector_refresh_task(
    *,
    task_manager: RuntimeBackgroundTaskManager,
    runtime_host: RuntimeHost,
) -> None:
    if runtime_host.services.semantic_vector_search is None:
        return
    can_refresh = await runtime_host.can_refresh_semantic_vector_search()
    if not can_refresh:
        return
    existing_names = {
        str(task.name or "").strip()
        for task in task_manager.list_tasks()
        if str(task.name or "").strip()
    }
    if _SEMANTIC_VECTOR_REFRESH_TASK_NAME in existing_names:
        return
    task_manager.register_default_task(
        build_semantic_vector_refresh_default_task(
            name=_SEMANTIC_VECTOR_REFRESH_TASK_NAME,
            run_on_startup=True,
            schedule=BackgroundTaskSchedule.interval(seconds=60),
            description=(
                "Check semantic vector indexes every minute and refresh any that are due."
            ),
        )
    )

async def _register_runtime_cleanup_task(
    *,
    task_manager: RuntimeBackgroundTaskManager,
    runtime_host: RuntimeHost,
) -> None:
    if not isinstance(runtime_host, ConfiguredLocalRuntimeHost):
        return
    existing_names = {
        str(task.name or "").strip()
        for task in task_manager.list_tasks()
        if str(task.name or "").strip()
    }
    if _RUNTIME_CLEANUP_TASK_NAME in existing_names:
        return
    task_manager.register_default_task(
        build_cleanup_default_task(
            name=_RUNTIME_CLEANUP_TASK_NAME,
            run_on_startup=True,
            schedule=BackgroundTaskSchedule.interval(seconds=300),
            description=(
                "Perform routine cleanup of runtime resources every 5 minutes."
            ),
        )
    )

def _normalize_runtime_features(features: Iterable[str] | None) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw_feature in features or ():
        feature = str(raw_feature or "").strip().lower()
        if feature and feature not in normalized:
            normalized.append(feature)
    return tuple(normalized)


def _configure_runtime_logging(*, debug: bool) -> None:
    if not debug:
        return
    logger = logging.getLogger("langbridge")
    logger.setLevel(logging.DEBUG)
    for handler in logger.handlers:
        if getattr(handler, _DEBUG_HANDLER_MARKER, False):
            return
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(_RUNTIME_LOG_FORMAT))
    setattr(handler, _DEBUG_HANDLER_MARKER, True)
    logger.addHandler(handler)


async def _close_runtime_host(runtime_host: RuntimeHost) -> None:
    aclose = getattr(runtime_host, "aclose", None)
    if callable(aclose):
        result = aclose()
        if inspect.isawaitable(result):
            await result
        return
    close = getattr(runtime_host, "close", None)
    if callable(close):
        result = close()
        if inspect.isawaitable(result):
            await result


def _stringify_optional_uuid(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _runtime_mutation_status_code(detail: str) -> int:
    normalized = str(detail or "").strip().lower()
    if "already exists" in normalized or "already in use" in normalized:
        return 409
    return 400


def _runtime_query_status_code(detail: str) -> int:
    if _is_missing_semantic_resource(detail) or _is_missing_runtime_resource(detail):
        return 404
    return 400


def _is_missing_semantic_resource(detail: str) -> bool:
    normalized = str(detail or "").strip().lower()
    return (
        "unknown semantic model" in normalized
        or "semantic model" in normalized and "not found" in normalized
        or "dataset" in normalized and "not found" in normalized
    )


def _is_missing_runtime_resource(detail: str) -> bool:
    normalized = str(detail or "").strip().lower()
    return "not found" in normalized or "unknown " in normalized
