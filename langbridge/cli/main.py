from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx

from langbridge.client import LangbridgeClient
from langbridge.runtime import run_runtime_api


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if getattr(args, "command", None) is None:
        parser.print_help()
        return 1

    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 1

    try:
        return int(handler(args) or 0)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Langbridge runtime CLI")
    subparsers = parser.add_subparsers(dest="command")

    serve = subparsers.add_parser("serve", help="Run the runtime host HTTP server.")
    serve.add_argument("--config", required=True, help="Path to langbridge_config.yml")
    serve.add_argument("--host", default="127.0.0.1", help="Bind host")
    serve.add_argument("--port", type=int, default=8000, help="Bind port")
    serve.add_argument(
        "--features",
        default="",
        help="Comma-separated runtime features to enable. Currently supported: mcp, ui",
    )
    serve.add_argument("--debug", action="store_true", help="Enable verbose runtime and MCP debug logging")
    serve.add_argument("--reload", action="store_true", help="Enable auto reload")
    serve.set_defaults(handler=_handle_serve)

    info = subparsers.add_parser("info", help="Show runtime host information.")
    _add_client_source_args(info)
    info.set_defaults(handler=_handle_info)

    datasets = subparsers.add_parser("datasets", help="Dataset commands.")
    dataset_subparsers = datasets.add_subparsers(dest="datasets_command")

    datasets_list = dataset_subparsers.add_parser("list", help="List datasets.")
    _add_client_source_args(datasets_list)
    datasets_list.add_argument("--search", default=None, help="Optional dataset search string")
    datasets_list.set_defaults(handler=_handle_datasets_list)

    datasets_preview = dataset_subparsers.add_parser("preview", help="Preview a dataset.")
    _add_client_source_args(datasets_preview)
    datasets_preview.add_argument("--dataset", required=True, help="Dataset name or UUID")
    datasets_preview.add_argument("--limit", type=int, default=10, help="Preview row limit")
    datasets_preview.set_defaults(handler=_handle_datasets_preview)

    semantic = subparsers.add_parser("semantic", help="Semantic query commands.")
    semantic_subparsers = semantic.add_subparsers(dest="semantic_command")

    semantic_query = semantic_subparsers.add_parser("query", help="Run a semantic query.")
    _add_client_source_args(semantic_query)
    semantic_query.add_argument("--model", required=True, help="Semantic model name")
    semantic_query.add_argument("--measure", action="append", default=[], help="Measure member")
    semantic_query.add_argument("--dimension", action="append", default=[], help="Dimension member")
    semantic_query.add_argument(
        "--filter",
        action="append",
        default=[],
        help="Semantic filter in member:operator:value form. Repeat for multiple filters.",
    )
    semantic_query.add_argument("--limit", type=int, default=None, help="Result limit")
    semantic_query.set_defaults(handler=_handle_semantic_query)

    sql = subparsers.add_parser("sql", help="SQL query commands.")
    sql_subparsers = sql.add_subparsers(dest="sql_command")

    sql_query = sql_subparsers.add_parser("query", help="Run a SQL query.")
    _add_client_source_args(sql_query)
    sql_query.add_argument("--query", required=True, help="SQL text")
    sql_query.add_argument("--connection", default=None, help="Connection name for direct SQL")
    sql_query.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Dataset name or UUID used to narrow federated SQL scope. Repeat for multiple datasets.",
    )
    sql_query.add_argument("--limit", type=int, default=None, help="Requested limit")
    sql_query.set_defaults(handler=_handle_sql_query)

    agents = subparsers.add_parser("agents", help="Agent commands.")
    agents_subparsers = agents.add_subparsers(dest="agents_command")

    agents_ask = agents_subparsers.add_parser("ask", help="Ask a configured agent.")
    _add_client_source_args(agents_ask)
    agents_ask.add_argument("--message", required=True, help="Prompt text")
    agents_ask.add_argument("--agent", default=None, help="Optional agent name")
    agents_ask.set_defaults(handler=_handle_agents_ask)

    connectors = subparsers.add_parser("connectors", help="Connector commands.")
    connector_subparsers = connectors.add_subparsers(dest="connectors_command")

    connectors_list = connector_subparsers.add_parser("list", help="List configured connectors.")
    _add_client_source_args(connectors_list)
    connectors_list.set_defaults(handler=_handle_connectors_list)

    sync = subparsers.add_parser("sync", help="Connector sync commands.")
    sync_subparsers = sync.add_subparsers(dest="sync_command")

    sync_resources = sync_subparsers.add_parser("resources", help="List syncable resources for a connector.")
    _add_client_source_args(sync_resources)
    sync_resources.add_argument("--connector", required=True, help="Connector name")
    sync_resources.set_defaults(handler=_handle_sync_resources)

    sync_states = sync_subparsers.add_parser("states", help="List sync states for a connector.")
    _add_client_source_args(sync_states)
    sync_states.add_argument("--connector", required=True, help="Connector name")
    sync_states.set_defaults(handler=_handle_sync_states)

    sync_run = sync_subparsers.add_parser("run", help="Run connector sync for explicit resources.")
    _add_client_source_args(sync_run)
    sync_run.add_argument("--connector", required=True, help="Connector name")
    sync_run.add_argument(
        "--resource",
        action="append",
        default=[],
        help="Resource name to sync. Repeat for multiple resources.",
    )
    sync_run.add_argument(
        "--mode",
        default="INCREMENTAL",
        choices=["INCREMENTAL", "FULL_REFRESH", "incremental", "full_refresh"],
        help="Requested sync mode",
    )
    sync_run.add_argument(
        "--full-refresh",
        action="store_true",
        help="Force a full refresh even if incremental sync is supported.",
    )
    sync_run.set_defaults(handler=_handle_sync_run)

    return parser


def _add_client_source_args(parser: argparse.ArgumentParser) -> None:
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--config", help="Local runtime config path")
    source.add_argument("--url", help="Runtime host base URL")
    parser.add_argument("--token", default=None, help="Optional bearer token for remote hosts")


def _handle_serve(args: argparse.Namespace) -> int:
    run_runtime_api(
        config_path=args.config,
        host=args.host,
        port=args.port,
        features=_parse_feature_flags(args.features),
        debug=bool(args.debug),
        reload=bool(args.reload),
    )
    return 0


def _handle_info(args: argparse.Namespace) -> int:
    if args.config:
        client = LangbridgeClient.local(config_path=args.config)
        try:
            payload = {
                "runtime_mode": "configured_local",
                "config_path": str(Path(args.config).resolve()),
                "workspace_id": client.default_workspace_id,
                "actor_id": client.default_actor_id,
            }
        finally:
            client.close()
        _print_json(payload)
        return 0

    payload = _fetch_remote_runtime_info(base_url=args.url, token=args.token)
    _print_json(payload)
    return 0


def _handle_datasets_list(args: argparse.Namespace) -> int:
    client = _build_client_from_args(args)
    try:
        result = client.datasets.list(search=args.search)
        _print_json(result.model_dump(mode="json"))
    finally:
        client.close()
    return 0


def _handle_datasets_preview(args: argparse.Namespace) -> int:
    client = _build_client_from_args(args)
    try:
        result = client.datasets.query(dataset=args.dataset, limit=args.limit)
        _print_json(result.model_dump(mode="json"))
    finally:
        client.close()
    return 0


def _handle_semantic_query(args: argparse.Namespace) -> int:
    client = _build_client_from_args(args)
    try:
        filters = [_parse_semantic_filter(value) for value in (args.filter or [])]
        result = client.semantic.query(
            args.model,
            measures=list(args.measure or []),
            dimensions=list(args.dimension or []),
            filters=filters,
            limit=args.limit,
        )
        _print_json(result.model_dump(mode="json"))
    finally:
        client.close()
    return 0


def _handle_sql_query(args: argparse.Namespace) -> int:
    client = _build_client_from_args(args)
    try:
        selected_datasets = [_resolve_dataset_alias(client, spec) for spec in (args.dataset or [])]
        result = client.sql.query(
            query=args.query,
            connection_name=args.connection,
            selected_datasets=selected_datasets,
            requested_limit=args.limit,
        )
        _print_json(result.model_dump(mode="json"))
    finally:
        client.close()
    return 0


def _handle_agents_ask(args: argparse.Namespace) -> int:
    client = _build_client_from_args(args)
    try:
        result = client.agents.ask(message=args.message, agent_name=args.agent)
        _print_json(result.model_dump(mode="json"))
    finally:
        client.close()
    return 0


def _handle_connectors_list(args: argparse.Namespace) -> int:
    client = _build_client_from_args(args)
    try:
        result = client.connectors.list()
        _print_json(result.model_dump(mode="json"))
    finally:
        client.close()
    return 0


def _handle_sync_resources(args: argparse.Namespace) -> int:
    client = _build_client_from_args(args)
    try:
        result = client.sync.resources(connector_name=args.connector)
        _print_json(result.model_dump(mode="json"))
    finally:
        client.close()
    return 0


def _handle_sync_states(args: argparse.Namespace) -> int:
    client = _build_client_from_args(args)
    try:
        result = client.sync.states(connector_name=args.connector)
        _print_json(result.model_dump(mode="json"))
    finally:
        client.close()
    return 0


def _handle_sync_run(args: argparse.Namespace) -> int:
    resources = [str(item).strip() for item in (args.resource or []) if str(item).strip()]
    if not resources:
        raise ValueError("At least one --resource value is required.")
    client = _build_client_from_args(args)
    try:
        result = client.sync.run(
            connector_name=args.connector,
            resource_names=resources,
            sync_mode=args.mode,
            force_full_refresh=bool(args.full_refresh),
        )
        _print_json(result.model_dump(mode="json"))
    finally:
        client.close()
    return 0


def _build_client_from_args(args: argparse.Namespace) -> LangbridgeClient:
    if getattr(args, "config", None):
        return LangbridgeClient.local(config_path=args.config)

    info_payload = _fetch_remote_runtime_info(base_url=args.url, token=args.token)
    workspace_id = _uuid_or_none(info_payload.get("workspace_id"))
    actor_id = _uuid_or_none(info_payload.get("actor_id"))
    return LangbridgeClient.for_runtime_host(
        base_url=args.url,
        token=args.token,
        default_workspace_id=workspace_id,
        default_actor_id=actor_id,
    )


def _fetch_remote_runtime_info(*, base_url: str, token: str | None) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with httpx.Client(base_url=base_url.rstrip("/"), timeout=30.0, headers=headers) as client:
        response = client.get("/api/runtime/v1/info")
        response.raise_for_status()
        return dict(response.json())


def _resolve_dataset_alias(client: LangbridgeClient, value: str) -> str:
    dataset_ref = str(value or "").strip()
    if not dataset_ref:
        raise ValueError("--dataset entries must contain a dataset name or UUID.")
    datasets = client.datasets.list(search=dataset_ref)
    for item in datasets.items:
        if item.name == dataset_ref or str(item.id) == dataset_ref:
            return str(item.id)
    raise ValueError(f"Dataset '{dataset_ref}' was not found.")


def _parse_semantic_filter(value: str) -> dict[str, Any]:
    member, separator, remainder = str(value or "").partition(":")
    if not separator:
        raise ValueError("Semantic filters must use member:operator:value form.")
    operator, separator, raw_values = remainder.partition(":")
    if not separator:
        raise ValueError("Semantic filters must use member:operator:value form.")
    values = [item.strip() for item in raw_values.split(",") if item.strip()]
    if operator == "set":
        values = []
    return {
        "member": member.strip(),
        "operator": operator.strip(),
        "values": values,
    }


def _parse_feature_flags(value: str | None) -> list[str]:
    supported = {"mcp", "ui"}
    normalized: list[str] = []
    for raw_feature in str(value or "").split(","):
        feature = raw_feature.strip().lower()
        if not feature:
            continue
        if feature not in supported:
            supported_values = ", ".join(sorted(supported))
            raise ValueError(f"Unsupported serve feature '{feature}'. Supported values: {supported_values}.")
        if feature not in normalized:
            normalized.append(feature)
    return normalized


def _uuid_or_none(value: Any) -> uuid.UUID | None:
    if value is None:
        return None
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, default=_json_default))


def _json_default(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
