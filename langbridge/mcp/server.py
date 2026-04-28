import logging
import uuid
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from mcp import types as mcp_types
from fastapi import HTTPException
from mcp.server.fastmcp import Context, FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from langbridge.runtime.hosting.auth import RuntimeAuthPrincipal, RuntimeAuthResolver
from langbridge.runtime.bootstrap import ConfiguredLocalRuntimeHost
from langbridge.runtime.models.jobs import (
    CreateDatasetPreviewJobRequest,
    SqlQueryRequest,
    SqlQueryScope,
)

DEFAULT_MCP_MOUNT_PATH = "/mcp"
_MCP_DEBUG_BODY_LIMIT = 4096
_MCP_LOGGER = logging.getLogger("langbridge.runtime.mcp")
_MCP_RUNTIME_TOOL_NAME = "runtime_info"
_MCP_LIST_DATASETS_TOOL_NAME = "list_datasets"
_MCP_PREVIEW_DATASET_TOOL_NAME = "preview_dataset"
_MCP_QUERY_SEMANTIC_TOOL_NAME = "query_semantic"
_MCP_QUERY_SQL_TOOL_NAME = "query_sql"
_MCP_ASK_AGENT_TOOL_NAME = "ask_agent"


def build_runtime_mcp_server(
    *,
    runtime_host: ConfiguredLocalRuntimeHost,
    auth_resolver: RuntimeAuthResolver,
    mount_path: str = DEFAULT_MCP_MOUNT_PATH,
    debug: bool = False,
) -> tuple[FastMCP, Any]:
    tool_availability = _build_mcp_tool_availability(runtime_host)
    available_mcp_tools = [
        tool_name
        for tool_name, item in tool_availability.items()
        if bool(item.get("available"))
    ]
    server = FastMCP(
        name="Langbridge Runtime MCP",
        instructions=(
            "Use these tools to inspect and query the configured Langbridge runtime. "
            "Tool calls execute against the runtime workspace available to the current caller."
        ),
        streamable_http_path="/",
    )
    _disable_unused_fastmcp_handlers(server)

    async def resolve_runtime_host(context: Context) -> ConfiguredLocalRuntimeHost:
        request = _require_request(context)
        principal = _resolve_principal(request=request, auth_resolver=auth_resolver)
        return runtime_host.with_context(
            auth_resolver.build_context(
                request=request,
                principal=principal,
            )
        )

    @server.tool(name=_MCP_RUNTIME_TOOL_NAME)
    async def runtime_info(context: Context) -> dict[str, Any]:
        """Return runtime metadata, enabled capabilities, and the MCP endpoint path."""
        configured_host = await resolve_runtime_host(context)
        connector_items = await configured_host.list_connectors()
        capabilities = _filter_runtime_capabilities_for_mcp_tools(
            capabilities=_build_runtime_capabilities(
                connector_items=connector_items,
                features=("mcp",),
            ),
            tool_availability=tool_availability,
        )
        return _to_jsonable(
            {
                "runtime_mode": "configured_local",
                "config_path": str(configured_host._config_path),
                "workspace_id": configured_host.context.workspace_id,
                "actor_id": configured_host.context.actor_id,
                "roles": list(configured_host.context.roles),
                "default_semantic_model": configured_host._default_semantic_model_name,
                "default_agent": (
                    configured_host._default_agent.config.name if configured_host._default_agent else None
                ),
                "capabilities": capabilities,
                "mcp_endpoint": mount_path,
                "available_mcp_tools": available_mcp_tools,
                "mcp_tool_status": tool_availability,
                "resource_summary": _build_runtime_resource_summary(runtime_host),
            }
        )

    @server.tool(name=_MCP_LIST_DATASETS_TOOL_NAME)
    async def list_datasets(search: str | None = None, context: Context = None) -> dict[str, Any]:
        """List datasets visible to the current runtime workspace."""
        configured_host = await resolve_runtime_host(context)
        items = await configured_host.list_datasets()
        normalized_search = str(search or "").strip().lower()
        if normalized_search:
            items = [
                item
                for item in items
                if normalized_search in str(item.get("name") or "").lower()
                or normalized_search in str(item.get("description") or "").lower()
            ]
        return _to_jsonable(
            {
                "items": items,
                "total": len(items),
            }
        )

    if tool_availability[_MCP_PREVIEW_DATASET_TOOL_NAME]["available"]:
        @server.tool(name=_MCP_PREVIEW_DATASET_TOOL_NAME)
        async def preview_dataset(
            dataset: str,
            limit: int = 10,
            context: Context = None,
        ) -> dict[str, Any]:
            """Preview rows from a dataset by name or UUID."""
            configured_host = await resolve_runtime_host(context)
            dataset_id = await _resolve_dataset_id(configured_host, dataset)
            try:
                payload = await configured_host.query_dataset(
                    request=CreateDatasetPreviewJobRequest(
                        dataset_id=dataset_id,
                        workspace_id=configured_host.context.workspace_id,
                        actor_id=configured_host.context.actor_id,
                        requested_limit=limit,
                        enforced_limit=limit or 100,
                        correlation_id=configured_host.context.request_id,
                    )
                )
            except Exception as exc:
                return {
                    "dataset_id": str(dataset_id),
                    "status": "failed",
                    "error": str(exc),
                }
            return _to_jsonable(
                {
                    "dataset_id": dataset_id,
                    "dataset_name": payload.get("dataset_name"),
                    "status": "succeeded",
                    "columns": list(payload.get("columns", [])),
                    "rows": list(payload.get("rows", [])),
                    "row_count_preview": int(payload.get("row_count_preview") or 0),
                    "effective_limit": payload.get("effective_limit"),
                    "redaction_applied": bool(payload.get("redaction_applied")),
                    "duration_ms": payload.get("duration_ms"),
                    "bytes_scanned": payload.get("bytes_scanned"),
                    "generated_sql": payload.get("generated_sql"),
                }
            )

    if tool_availability[_MCP_QUERY_SEMANTIC_TOOL_NAME]["available"]:
        @server.tool(name=_MCP_QUERY_SEMANTIC_TOOL_NAME)
        async def query_semantic(
            semantic_models: list[str] | None = None,
            measures: list[str] | None = None,
            dimensions: list[str] | None = None,
            filters: list[dict[str, Any]] | None = None,
            limit: int | None = None,
            order: dict[str, str] | None = None,
            context: Context = None,
        ) -> dict[str, Any]:
            """Run a semantic query against one or more semantic models."""
            configured_host = await resolve_runtime_host(context)
            selected_models = [item for item in (semantic_models or []) if str(item).strip()]
            if not selected_models:
                default_model = str(configured_host._default_semantic_model_name or "").strip()
                if default_model:
                    selected_models = [default_model]
            if not selected_models:
                raise ValueError("semantic_models is required when the runtime does not define a default semantic model.")
            try:
                payload = await configured_host.query_semantic_models(
                    semantic_models=selected_models,
                    measures=list(measures or []),
                    dimensions=list(dimensions or []),
                    filters=list(filters or []),
                    time_dimensions=[],
                    limit=limit,
                    order=order,
                )
            except Exception as exc:
                return {
                    "status": "failed",
                    "error": str(exc),
                }
            return _to_jsonable(
                {
                    "status": "succeeded",
                    "semantic_model_id": payload.get("semantic_model_id"),
                    "semantic_model_ids": list(payload.get("semantic_model_ids", [])),
                    "connector_id": payload.get("connector_id"),
                    "data": list(payload.get("rows", [])),
                    "annotations": list(payload.get("annotations", [])),
                    "metadata": payload.get("metadata"),
                    "generated_sql": payload.get("generated_sql"),
                }
            )

    if tool_availability[_MCP_QUERY_SQL_TOOL_NAME]["available"]:
        @server.tool(name=_MCP_QUERY_SQL_TOOL_NAME)
        async def query_sql(
            query: str,
            query_scope: str | None = None,
            connection_name: str | None = None,
            selected_datasets: list[str] | None = None,
            requested_limit: int | None = None,
            requested_timeout_seconds: int | None = None,
            explain: bool = False,
            context: Context = None,
        ) -> dict[str, Any]:
            """Run semantic, dataset, or source SQL against the runtime."""
            configured_host = await resolve_runtime_host(context)
            normalized_datasets = [uuid.UUID(str(dataset_id)) for dataset_id in (selected_datasets or [])]
            resolved_scope = (
                SqlQueryScope(str(query_scope).strip().lower())
                if query_scope is not None
                else (SqlQueryScope.source if connection_name else SqlQueryScope.dataset)
            )
            try:
                payload = await configured_host.query_sql(
                    request=SqlQueryRequest(
                        query_scope=resolved_scope,
                        query=query,
                        connection_name=connection_name,
                        selected_datasets=normalized_datasets,
                        query_dialect="tsql",
                        params={},
                        requested_limit=requested_limit,
                        requested_timeout_seconds=requested_timeout_seconds,
                        explain=bool(explain),
                    )
                )
            except Exception as exc:
                return {
                    "sql_job_id": str(uuid.uuid4()),
                    "status": "failed",
                    "error": {"message": str(exc)},
                    "query": query,
                }
            return _to_jsonable(
                {
                    "sql_job_id": payload.get("sql_job_id") or uuid.uuid4(),
                    "query_scope": payload.get("query_scope") or resolved_scope.value,
                    "status": "succeeded",
                    "semantic_model_id": payload.get("semantic_model_id"),
                    "semantic_model_ids": list(payload.get("semantic_model_ids", [])),
                    "connector_id": payload.get("connector_id"),
                    "columns": list(payload.get("columns", [])),
                    "rows": list(payload.get("rows", [])),
                    "row_count_preview": int(payload.get("row_count_preview") or 0),
                    "total_rows_estimate": payload.get("total_rows_estimate"),
                    "bytes_scanned": payload.get("bytes_scanned"),
                    "duration_ms": payload.get("duration_ms"),
                    "redaction_applied": bool(payload.get("redaction_applied")),
                    "query": payload.get("query") or query,
                    "generated_sql": payload.get("generated_sql"),
                }
            )

    if tool_availability[_MCP_ASK_AGENT_TOOL_NAME]["available"]:
        @server.tool(name=_MCP_ASK_AGENT_TOOL_NAME)
        async def ask_agent(
            message: str,
            agent_name: str | None = None,
            context: Context = None,
        ) -> dict[str, Any]:
            """Ask the default runtime agent, or a named agent, a grounded analytics question."""
            configured_host = await resolve_runtime_host(context)
            try:
                payload = await configured_host.ask_agent(
                    prompt=message,
                    agent_name=str(agent_name or "").strip() or None,
                )
            except Exception as exc:
                return {
                    "status": "failed",
                    "error": {"message": str(exc)},
                }
            return _to_jsonable(
                {
                    "status": "succeeded",
                    "thread_id": payload.get("thread_id"),
                    "job_id": payload.get("job_id"),
                    "summary": payload.get("summary"),
                    "result": payload.get("result"),
                    "visualization": payload.get("visualization"),
                    "error": payload.get("error"),
                    "events": list(payload.get("events", [])),
                }
            )

    mounted_app = _RuntimeMCPAuthMiddleware(
        app=server.streamable_http_app(),
        auth_resolver=auth_resolver,
        debug=debug,
    )
    return server, mounted_app


class _RuntimeMCPAuthMiddleware:
    def __init__(
        self,
        *,
        app: Any,
        auth_resolver: RuntimeAuthResolver,
        debug: bool = False,
        debug_body_limit: int = _MCP_DEBUG_BODY_LIMIT,
    ) -> None:
        self._app = app
        self._auth_resolver = auth_resolver
        self._debug = bool(debug)
        self._debug_body_limit = max(256, int(debug_body_limit))

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self._app(scope, receive, send)
            return

        request_headers = _normalize_headers(scope.get("headers", ()))
        request_body = bytearray()
        response_status: int | None = None
        response_headers: dict[str, str] = {}
        response_body = bytearray()
        request = Request(scope, receive=receive)

        async def logged_receive():
            message = await receive()
            if self._debug and message.get("type") == "http.request":
                _append_body_preview(
                    request_body,
                    message.get("body", b""),
                    limit=self._debug_body_limit,
                )
            return message

        async def logged_send(message) -> None:
            nonlocal response_status, response_headers
            if self._debug:
                if message.get("type") == "http.response.start":
                    response_status = int(message.get("status", 0))
                    response_headers = _normalize_headers(message.get("headers", ()))
                elif message.get("type") == "http.response.body":
                    _append_body_preview(
                        response_body,
                        message.get("body", b""),
                        limit=self._debug_body_limit,
                    )
            await send(message)

        try:
            principal = await self._auth_resolver.authenticate(request)
        except HTTPException as exc:
            response = JSONResponse(
                {"detail": exc.detail},
                status_code=exc.status_code,
            )
            await response(scope, receive, logged_send)
            self._log_exchange(
                scope=scope,
                request_headers=request_headers,
                request_body=request_body,
                response_status=response_status,
                response_headers=response_headers,
                response_body=response_body,
                auth_detail=str(exc.detail),
            )
            return

        scope.setdefault("state", {})
        scope["state"]["runtime_principal"] = principal
        try:
            await self._app(scope, logged_receive, logged_send)
        except Exception:
            self._log_exchange(
                scope=scope,
                request_headers=request_headers,
                request_body=request_body,
                response_status=response_status,
                response_headers=response_headers,
                response_body=response_body,
                include_traceback=True,
            )
            raise
        self._log_exchange(
            scope=scope,
            request_headers=request_headers,
            request_body=request_body,
            response_status=response_status,
            response_headers=response_headers,
            response_body=response_body,
        )

    def _log_exchange(
        self,
        *,
        scope: dict[str, Any],
        request_headers: dict[str, str],
        request_body: bytearray,
        response_status: int | None,
        response_headers: dict[str, str],
        response_body: bytearray,
        auth_detail: str | None = None,
        include_traceback: bool = False,
    ) -> None:
        if not self._debug:
            return
        target = _format_scope_target(scope)
        status_text = str(response_status) if response_status is not None else "no-response"
        body_preview = _decode_log_body(bytes(request_body))
        response_preview = _decode_log_body(bytes(response_body))
        message = (
            "MCP HTTP %s %s -> %s headers=%s request_body=%s response_headers=%s response_body=%s"
        )
        args = (
            scope.get("method", "UNKNOWN"),
            target,
            status_text,
            _sanitize_headers(request_headers),
            body_preview,
            _sanitize_headers(response_headers),
            response_preview,
        )
        if include_traceback:
            _MCP_LOGGER.exception(message, *args)
            return
        if auth_detail is not None:
            rendered_message = message % args
            _MCP_LOGGER.warning("%s auth_error=%s", rendered_message, auth_detail)
            return
        if response_status is not None and response_status >= 400:
            _MCP_LOGGER.warning(message, *args)
            return
        _MCP_LOGGER.debug(message, *args)


def _require_request(context: Context) -> Request:
    request = context.request_context.request
    if not isinstance(request, Request):
        raise RuntimeError("Langbridge MCP tools require an HTTP request context.")
    return request


def _resolve_principal(
    *,
    request: Request,
    auth_resolver: RuntimeAuthResolver,
) -> RuntimeAuthPrincipal:
    principal = getattr(request.state, "runtime_principal", None)
    if isinstance(principal, RuntimeAuthPrincipal):
        return principal
    raise RuntimeError("Langbridge MCP request authentication context is missing.")


def _build_mcp_tool_availability(runtime_host: ConfiguredLocalRuntimeHost) -> dict[str, dict[str, Any]]:
    summary = _build_runtime_resource_summary(runtime_host)
    runtime_services = getattr(getattr(runtime_host, "_runtime_host", None), "services", None)
    tool_status: dict[str, dict[str, Any]] = {
        _MCP_RUNTIME_TOOL_NAME: {
            "available": True,
            "reason": None,
        },
        _MCP_LIST_DATASETS_TOOL_NAME: {
            "available": True,
            "reason": None,
        },
        _MCP_PREVIEW_DATASET_TOOL_NAME: _availability_entry(
            available=summary["datasets"] > 0,
            reason="No datasets are configured for this runtime." if summary["datasets"] == 0 else None,
        ),
        _MCP_QUERY_SEMANTIC_TOOL_NAME: _availability_entry(
            available=(
                summary["semantic_models"] > 0
                and getattr(runtime_services, "semantic_query", None) is not None
            ),
            reason=_semantic_query_unavailable_reason(
                summary=summary,
                runtime_services=runtime_services,
            ),
        ),
        _MCP_QUERY_SQL_TOOL_NAME: _availability_entry(
            available=(
                (summary["connectors"] > 0 or summary["datasets"] > 0)
                and getattr(runtime_services, "sql_query", None) is not None
            ),
            reason=_sql_query_unavailable_reason(
                summary=summary,
                runtime_services=runtime_services,
            ),
        ),
        _MCP_ASK_AGENT_TOOL_NAME: _availability_entry(
            available=(
                summary["agents"] > 0
                and getattr(runtime_services, "agent_execution", None) is not None
            ),
            reason=_ask_agent_unavailable_reason(
                summary=summary,
                runtime_services=runtime_services,
            ),
        ),
    }
    return tool_status


def _build_runtime_resource_summary(runtime_host: ConfiguredLocalRuntimeHost) -> dict[str, int]:
    connectors = getattr(runtime_host, "_connectors", {}) or {}
    datasets = getattr(runtime_host, "_datasets", {}) or {}
    semantic_models = getattr(runtime_host, "_semantic_models", {}) or {}
    agents = getattr(runtime_host, "_agents", {}) or {}
    supports_sync = getattr(runtime_host, "_connector_supports_sync", None)
    syncable_connectors = 0
    if callable(supports_sync):
        syncable_connectors = sum(
            1
            for connector in connectors.values()
            if supports_sync(connector)
        )
    return {
        "connectors": len(connectors),
        "datasets": len(datasets),
        "semantic_models": len(semantic_models),
        "agents": len(agents),
        "syncable_connectors": syncable_connectors,
    }


def _availability_entry(*, available: bool, reason: str | None) -> dict[str, Any]:
    return {
        "available": bool(available),
        "reason": None if available else reason,
    }


def _semantic_query_unavailable_reason(*, summary: dict[str, int], runtime_services: Any) -> str | None:
    if summary["semantic_models"] == 0:
        return "No semantic models are configured for this runtime."
    if getattr(runtime_services, "semantic_query", None) is None:
        return "Semantic query execution is not configured for this runtime."
    return None


def _sql_query_unavailable_reason(*, summary: dict[str, int], runtime_services: Any) -> str | None:
    if summary["connectors"] == 0 and summary["datasets"] == 0:
        return "No connectors or datasets are configured for SQL execution."
    if getattr(runtime_services, "sql_query", None) is None:
        return "SQL execution is not configured for this runtime."
    return None


def _ask_agent_unavailable_reason(*, summary: dict[str, int], runtime_services: Any) -> str | None:
    if summary["agents"] == 0:
        return "No agents are configured for this runtime."
    if getattr(runtime_services, "agent_execution", None) is None:
        return "Agent execution is not configured for this runtime."
    return None


def _filter_runtime_capabilities_for_mcp_tools(
    *,
    capabilities: list[str],
    tool_availability: dict[str, dict[str, Any]],
) -> list[str]:
    capability_by_tool = {
        _MCP_LIST_DATASETS_TOOL_NAME: "datasets.list",
        _MCP_PREVIEW_DATASET_TOOL_NAME: "datasets.preview",
        _MCP_QUERY_SEMANTIC_TOOL_NAME: "semantic.query",
        _MCP_QUERY_SQL_TOOL_NAME: "sql.query",
        _MCP_ASK_AGENT_TOOL_NAME: "agents.ask",
    }
    filtered: list[str] = []
    for capability in capabilities:
        matching_tool = next(
            (
                tool_name
                for tool_name, mapped_capability in capability_by_tool.items()
                if mapped_capability == capability
            ),
            None,
        )
        if matching_tool is not None and not bool(tool_availability[matching_tool]["available"]):
            continue
        filtered.append(capability)
    return filtered


def _disable_unused_fastmcp_handlers(server: FastMCP) -> None:
    request_handlers = getattr(getattr(server, "_mcp_server", None), "request_handlers", None)
    if not isinstance(request_handlers, dict):
        return
    for request_type in (
        mcp_types.ListResourcesRequest,
        mcp_types.ListResourceTemplatesRequest,
        mcp_types.ReadResourceRequest,
        mcp_types.ListPromptsRequest,
        mcp_types.GetPromptRequest,
    ):
        request_handlers.pop(request_type, None)


async def _resolve_dataset_id(
    runtime_host: ConfiguredLocalRuntimeHost,
    dataset_ref: str,
) -> uuid.UUID:
    normalized_ref = str(dataset_ref or "").strip()
    if not normalized_ref:
        raise ValueError("dataset is required.")
    try:
        return uuid.UUID(normalized_ref)
    except ValueError:
        pass

    datasets = await runtime_host.list_datasets()
    for item in datasets:
        if str(item.get("name") or "").strip() != normalized_ref:
            continue
        item_id = item.get("id")
        if item_id is None:
            break
        try:
            return uuid.UUID(str(item_id))
        except (TypeError, ValueError):
            break
    raise ValueError(f"Dataset '{dataset_ref}' was not found.")


def _build_runtime_capabilities(
    *,
    connector_items: Sequence[dict[str, Any]],
    features: Sequence[str],
) -> list[str]:
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
                "connectors.sync.resources",
                "connectors.sync.states",
                "datasets.sync.get",
                "datasets.sync.run",
            ]
        )
    for feature in features:
        normalized = str(feature or "").strip().lower()
        if normalized and normalized not in capabilities:
            capabilities.append(normalized)
    return capabilities


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "model_dump"):
        return _to_jsonable(value.model_dump(mode="json"))
    return value


def _append_body_preview(buffer: bytearray, chunk: bytes | None, *, limit: int) -> None:
    if not chunk or len(buffer) >= limit:
        return
    remaining = limit - len(buffer)
    buffer.extend(chunk[:remaining])


def _decode_log_body(body: bytes) -> str:
    if not body:
        return "<empty>"
    return body.decode("utf-8", errors="replace")


def _format_scope_target(scope: dict[str, Any]) -> str:
    path = str(scope.get("path") or "")
    query_string = scope.get("query_string", b"")
    if not query_string:
        return path
    return f"{path}?{query_string.decode('utf-8', errors='replace')}"


def _normalize_headers(headers: Sequence[tuple[bytes, bytes]] | Sequence[tuple[str, str]]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in headers:
        normalized[(str(key, "latin-1") if isinstance(key, bytes) else str(key)).lower()] = (
            str(value, "latin-1") if isinstance(value, bytes) else str(value)
        )
    return normalized


def _sanitize_headers(headers: dict[str, str]) -> dict[str, str]:
    sanitized: dict[str, str] = {}
    for name, value in headers.items():
        lowered = str(name).lower()
        if lowered in {"authorization", "cookie", "set-cookie"}:
            sanitized[lowered] = "<redacted>"
            continue
        sanitized[lowered] = value
    return sanitized
