import logging
import os
import uuid
from collections.abc import Iterable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from langbridge.mcp import DEFAULT_MCP_MOUNT_PATH, build_runtime_mcp_server
from langbridge.ui import register_runtime_ui
from langbridge.runtime.hosting.auth import (
    RuntimeAuthConfig,
    RuntimeAuthMode,
    RuntimeAuthResolver,
)
from langbridge.runtime.hosting.background import (
    BackgroundTaskSchedule,
    RuntimeBackgroundTaskDefinition,
    RuntimeBackgroundTaskManager,
    build_semantic_vector_refresh_default_task,
)
from langbridge.runtime.local_config import (
    ConfiguredLocalRuntimeHost,
    build_configured_local_runtime,
)
from langbridge.runtime.models.jobs import (
    CreateDatasetPreviewJobRequest,
    CreateSqlJobRequest,
    SqlWorkbenchMode,
)
from langbridge.runtime.services.errors import ExecutionValidationError
from langbridge.runtime.services.runtime_host import RuntimeHost
from langbridge.runtime.hosting.api_models import (
    RuntimeAgentAskRequest,
    RuntimeAgentAskResponse,
    RuntimeAuthBootstrapRequest,
    RuntimeAuthLoginRequest,
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
    RuntimeThreadCreateRequest,
    RuntimeThreadUpdateRequest,
    RuntimeSqlQueryRequest,
    RuntimeSqlQueryResponse,
)

_CONFIG_PATH_ENV = "LANGBRIDGE_RUNTIME_CONFIG_PATH"
_FEATURES_ENV = "LANGBRIDGE_RUNTIME_FEATURES"
_DEBUG_ENV = "LANGBRIDGE_RUNTIME_DEBUG"
_SEMANTIC_VECTOR_REFRESH_TASK_NAME = "semantic-vector-refresh"
_RUNTIME_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_DEBUG_HANDLER_MARKER = "_langbridge_runtime_debug_handler"


def create_runtime_api_app(
    *,
    config_path: str | Path | None = None,
    runtime_host: ConfiguredLocalRuntimeHost | None = None,
    auth_config: RuntimeAuthConfig | None = None,
    features: Iterable[str] | None = None,
    debug: bool = False,
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
    auth_resolver = RuntimeAuthResolver(
        config=auth_config
        or RuntimeAuthConfig.from_env(
            config_path=getattr(host, "_config_path", config_path),
        ),
        default_context=host.context,
    )
    resolved_default_background_tasks = _resolve_default_background_tasks(
        runtime_host=host,
        default_background_tasks=default_background_tasks,
    )
    task_manager = background_task_manager
    if task_manager is None:
        task_manager = RuntimeBackgroundTaskManager(
            runtime_host=host,
            default_tasks=resolved_default_background_tasks,
            custom_tasks=background_tasks,
        )
    else:
        if task_manager.runtime_host is not host:
            raise ValueError("background_task_manager must be bound to the same runtime_host.")
        for task in resolved_default_background_tasks:
            task_manager.register_default_task(task)
        for task in background_tasks or ():
            task_manager.register_custom_task(task)
    mcp_server = None
    mcp_app = None
    if mcp_enabled:
        mcp_server, mcp_app = build_runtime_mcp_server(
            runtime_host=host,
            auth_resolver=auth_resolver,
            mount_path=DEFAULT_MCP_MOUNT_PATH,
            debug=debug,
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
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
        return _build_runtime_auth_status(auth_resolver)

    @app.post("/api/runtime/v1/auth/bootstrap")
    async def runtime_auth_bootstrap(
        request: Request,
        body: RuntimeAuthBootstrapRequest,
    ) -> JSONResponse:
        local_auth = _require_local_auth_manager(auth_resolver)
        try:
            session = local_auth.bootstrap_admin(
                username=body.username,
                email=body.email,
                password=body.password,
            )
        except ValueError as exc:
            detail = str(exc)
            status_code = 409 if "already" in detail.lower() else 400
            raise HTTPException(status_code=status_code, detail=detail) from exc
        token = local_auth.issue_session_token(session)
        response = JSONResponse(
            {
                "ok": True,
                "auth_mode": auth_resolver.mode.value,
                "user": session.model_dump(mode="json"),
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
            session = local_auth.authenticate(
                identifier=str(body.identifier or ""),
                password=body.password,
            )
        except ValueError as exc:
            detail = str(exc)
            status_code = 409 if "bootstrap" in detail.lower() else 401
            raise HTTPException(status_code=status_code, detail=detail) from exc
        token = local_auth.issue_session_token(session)
        response = JSONResponse(
            {
                "ok": True,
                "auth_mode": auth_resolver.mode.value,
                "user": session.model_dump(mode="json"),
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
        if auth_resolver.mode == RuntimeAuthMode.local:
            local_auth = _require_local_auth_manager(auth_resolver)
            try:
                session = local_auth.authenticate_request(request)
            except ValueError as exc:
                raise HTTPException(status_code=401, detail=str(exc)) from exc
            return {
                "auth_enabled": True,
                "auth_mode": auth_resolver.mode.value,
                "user": session.model_dump(mode="json"),
            }

        principal = await auth_resolver.authenticate(request)
        return {
            "auth_enabled": True,
            "auth_mode": auth_resolver.mode.value,
            "user": {
                "id": str(principal.actor_id) if principal.actor_id else None,
                "username": principal.subject or "runtime",
                "email": None,
                "roles": list(principal.roles),
                "provider": auth_resolver.mode.value,
            },
        }

    @app.get("/api/runtime/v1/info", response_model=RuntimeInfoResponse)
    async def info(request: Request) -> RuntimeInfoResponse:
        configured_host = await _resolve_request_host(request)
        connector_items = await configured_host.list_connectors()
        capabilities = [
            "datasets.list",
            "datasets.get",
            "datasets.preview",
            "semantic_models.list",
            "semantic_models.get",
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
        if auth_resolver.mode == RuntimeAuthMode.local:
            capabilities.extend(
                [
                    "auth.bootstrap",
                    "auth.login",
                    "auth.logout",
                    "auth.me",
                ]
            )
        if any(bool(item.get("supports_sync")) for item in connector_items):
            capabilities.extend(
                [
                    "sync.resources",
                    "sync.states",
                    "sync.run",
                ]
            )
        if ui_enabled:
            capabilities.append("ui")
        if mcp_enabled:
            capabilities.append("mcp")
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
        except (ValueError, ExecutionValidationError) as exc:
            detail = str(exc)
            status_code = 404 if _is_missing_semantic_resource(detail) else 400
            raise HTTPException(status_code=status_code, detail=detail) from exc
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

    @app.get("/api/runtime/v1/semantic-models")
    async def list_semantic_models(request: Request) -> dict[str, Any]:
        configured_host = await _resolve_request_host(request)
        items = await configured_host.list_semantic_models()
        return {"items": items, "total": len(items)}

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
        return await _execute_runtime_sql(configured_host, body)

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
            result = await configured_host.ask_agent(
                prompt=body.message,
                agent_name=agent_name,
                thread_id=body.thread_id,
                title=body.title,
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

    if ui_enabled:
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
                "auth": _build_runtime_auth_status(auth_resolver),
                "runtime": {
                    "mode": "configured_local",
                    "workspace_id": str(configured_host.context.workspace_id),
                    "actor_id": str(configured_host.context.actor_id) if configured_host.context.actor_id else None,
                    "default_semantic_model": configured_host._default_semantic_model_name,
                    "default_agent": (
                        configured_host._default_agent.config.name if configured_host._default_agent else None
                    ),
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
                        "managed": bool(item.get("managed")),
                    }
                    for item in connector_items[:8]
                ],
                "semantic_models": semantic_model_items[:6],
                "agents": agent_items[:6],
                "threads": thread_items[:6],
            }

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
    explicit_direct = request.connection_id is not None or bool(request.connection_name)

    if explicit_direct and request.connection_name:
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
    is_federated = not explicit_direct
    create_request = CreateSqlJobRequest(
        sql_job_id=sql_job_id,
        workspace_id=runtime_host.context.workspace_id,
        actor_id=runtime_host.context.actor_id,
        workbench_mode=(SqlWorkbenchMode.dataset if is_federated else SqlWorkbenchMode.direct_sql),
        connection_id=request.connection_id,
        execution_mode=("federated" if is_federated else "single"),
        query=request.query,
        query_dialect=str(request.query_dialect or "tsql").strip().lower() or "tsql",
        params=dict(request.params or {}),
        requested_limit=request.requested_limit,
        requested_timeout_seconds=request.requested_timeout_seconds,
        enforced_limit=request.requested_limit or 100,
        enforced_timeout_seconds=request.requested_timeout_seconds or 30,
        allow_dml=False,
        allow_federation=is_federated,
        selected_datasets=selected_datasets,
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


def _parse_runtime_features_env(value: str | None) -> tuple[str, ...]:
    return _normalize_runtime_features(str(value or "").split(","))


def _parse_runtime_debug_env(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "debug"}


def _require_local_auth_manager(auth_resolver: RuntimeAuthResolver):
    if auth_resolver.mode != RuntimeAuthMode.local or auth_resolver.local_auth is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Runtime local login is not enabled. "
                "Set LANGBRIDGE_RUNTIME_AUTH_MODE=local to use bootstrap and login endpoints."
            ),
        )
    return auth_resolver.local_auth


def _build_runtime_auth_status(auth_resolver: RuntimeAuthResolver) -> dict[str, Any]:
    if auth_resolver.mode == RuntimeAuthMode.none:
        return {
            "auth_enabled": False,
            "auth_mode": auth_resolver.mode.value,
            "bootstrap_required": False,
            "has_admin": False,
            "login_allowed": False,
        }
    if auth_resolver.mode != RuntimeAuthMode.local:
        return {
            "auth_enabled": True,
            "auth_mode": auth_resolver.mode.value,
            "bootstrap_required": False,
            "has_admin": True,
            "login_allowed": False,
            "detail": "This runtime uses bearer-token or JWT authentication instead of browser-managed local sessions.",
        }
    local_auth = _require_local_auth_manager(auth_resolver)
    status = local_auth.auth_status()
    return {
        "auth_enabled": True,
        "auth_mode": auth_resolver.mode.value,
        "bootstrap_required": bool(status["bootstrap_required"]),
        "has_admin": bool(status["has_admin"]),
        "login_allowed": True,
        "session_cookie_name": local_auth.cookie_name,
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


def _resolve_default_background_tasks(
    *,
    runtime_host: RuntimeHost,
    default_background_tasks: Iterable[RuntimeBackgroundTaskDefinition] | None,
) -> tuple[RuntimeBackgroundTaskDefinition, ...]:
    tasks = list(default_background_tasks or ())
    registered_names = {
        str(task.name or "").strip()
        for task in tasks
        if str(task.name or "").strip()
    }
    if (
        runtime_host.services.semantic_vector_search is not None
        and runtime_host.can_refresh_semantic_vector_search()
        and _SEMANTIC_VECTOR_REFRESH_TASK_NAME not in registered_names
    ):
        tasks.append(
            build_semantic_vector_refresh_default_task(
                name=_SEMANTIC_VECTOR_REFRESH_TASK_NAME,
                schedule=BackgroundTaskSchedule.interval(seconds=60),
                description=(
                    "Check semantic vector indexes every minute and refresh any that are due."
                ),
            )
        )
    return tuple(tasks)


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


def _stringify_optional_uuid(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _is_missing_semantic_resource(detail: str) -> bool:
    normalized = str(detail or "").strip().lower()
    return (
        "unknown semantic model" in normalized
        or "semantic model" in normalized and "not found" in normalized
        or "dataset" in normalized and "not found" in normalized
    )
