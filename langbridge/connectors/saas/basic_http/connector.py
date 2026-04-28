import base64
from datetime import datetime, timezone
from typing import Any, Mapping

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ApiExtractResult, ApiResource
from langbridge.connectors.base.errors import ConnectorError
from langbridge.connectors.base.http import (
    ApiResourceDefinition,
    HttpApiConnector,
    parse_link_header_cursor,
)
from langbridge.connectors.base.resource_paths import describe_api_child_resources

from .config import BasicHttpConnectorConfig
from .models import (
    BasicHttpAuthType,
    BasicHttpCursorType,
    BasicHttpPaginationStrategy,
    BasicHttpResourceConfig,
)
from langbridge.runtime.datasets.contracts import DatasetExtractionConfig, DatasetRequestConfig


class BasicHttpConnector(HttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.BASIC_HTTP

    config: BasicHttpConnectorConfig

    def __init__(self, config: BasicHttpConnectorConfig, logger=None, **kwargs: Any) -> None:
        super().__init__(
            config=config,
            logger=logger,
            timeout_s=float(config.timeout_s or 30.0),
            **kwargs,
        )
        self._resource_configs = {resource.key: resource for resource in config.resources}
        self._resource_definitions = {
            resource.key: ApiResourceDefinition(
                resource=ApiResource(
                    name=resource.key,
                    label=resource.label or resource.key,
                    path=resource.key,
                    primary_key=resource.primary_key,
                    cursor_field=resource.cursor_param,
                    incremental_cursor_field=resource.incremental_cursor_field,
                    supports_incremental=resource.supports_incremental,
                    default_sync_mode=resource.default_sync_mode,
                ),
                path=resource.path,
                response_key=resource.response_items_field,
                request_params=resource.request_params,
            )
            for resource in config.resources
        }

    async def discover_resources(self) -> list[ApiResource]:
        return [definition.resource for definition in self._resource_definitions.values()]

    async def test_connection(self) -> None:
        resource_config = next(iter(self._resource_configs.values()), None)
        test_path = str(self.config.test_connection_path or "").strip() or (
            resource_config.path if resource_config is not None else ""
        )
        if not test_path:
            raise ConnectorError("Basic HTTP connector does not define any resource paths to test.")
        params = None
        if resource_config is not None:
            params = self._build_request_params(resource=resource_config, cursor=None, limit=1, since=None)
        await self._request_json("GET", test_path, params=params)

    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiExtractResult:
        definition = self._require_resource(resource_name)
        resource = self._require_resource_config(resource_name)
        params = self._build_request_params(
            resource=resource,
            cursor=cursor,
            limit=limit,
            since=since,
        )
        payload, response = await self._request_json("GET", definition.path, params=params)
        records = _extract_records(payload, field_path=resource.response_items_field)
        records = self._filter_incremental_records(
            resource=resource,
            records=records,
            since=since,
        )
        structured_records = [record for record in records if isinstance(record, dict)]
        return ApiExtractResult(
            resource=definition.resource.name,
            status="success",
            records=structured_records,
            next_cursor=self._next_cursor(
                resource=resource,
                payload=payload,
                response=response,
                records=structured_records,
                current_cursor=cursor,
                page_size=_resolved_page_size(resource=resource, limit=limit),
            ),
            checkpoint_cursor=self._checkpoint_cursor(resource=resource, records=structured_records),
            child_resources=list(
                describe_api_child_resources(
                    resource_path=definition.resource.path or definition.resource.name,
                    records=structured_records,
                )
            ),
        )

    async def extract_request(
        self,
        request: Mapping[str, Any],
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        extraction: Mapping[str, Any] | None = None,
    ) -> ApiExtractResult:
        request_config = (
            request
            if isinstance(request, DatasetRequestConfig)
            else DatasetRequestConfig.model_validate(request)
        )
        extraction_config = (
            extraction
            if isinstance(extraction, DatasetExtractionConfig)
            else DatasetExtractionConfig.model_validate(extraction or {})
        )
        method = str(request_config.method or "get").strip().upper()
        path = str(request_config.path or "").strip()
        if not path:
            raise ConnectorError("Basic HTTP request extraction requires a request path.")
        params = dict(request_config.params or {})
        if since is not None:
            params.setdefault("since", since)
        if cursor is not None:
            params.setdefault("cursor", cursor)
        if limit is not None:
            params.setdefault("limit", limit)
        body = dict(request_config.body or {})
        payload, _ = await self._request_json(
            method,
            path,
            params=params,
            json_payload=(body or None),
            headers=dict(request_config.headers or {}),
        )
        records = _extract_records(
            payload,
            field_path=str((extraction_config.options or {}).get("path") or "").strip() or None,
        )
        structured_records = [record for record in records if isinstance(record, dict)]
        return ApiExtractResult(
            resource=path,
            status="success",
            records=structured_records,
            next_cursor=None,
            checkpoint_cursor=None,
            child_resources=list(
                describe_api_child_resources(
                    resource_path=_request_resource_path(path),
                    records=structured_records,
                )
            ),
        )

    def _require_resource(self, resource_name: str) -> ApiResourceDefinition:
        definition = self._resource_definitions.get(resource_name)
        if definition is None:
            raise ConnectorError(f"Unsupported basic HTTP resource '{resource_name}'.")
        return definition

    def _require_resource_config(self, resource_name: str) -> BasicHttpResourceConfig:
        resource = self._resource_configs.get(resource_name)
        if resource is None:
            raise ConnectorError(f"Unsupported basic HTTP resource '{resource_name}'.")
        return resource

    def _base_url(self) -> str:
        return str(self.config.api_base_url or "").rstrip("/")

    def _default_headers(self) -> dict[str, str]:
        headers = dict(self.config.static_headers or {})
        if self.config.auth_type == BasicHttpAuthType.NONE:
            return headers
        if self.config.auth_type == BasicHttpAuthType.BEARER:
            header_name = str(self.config.auth_header_name or "Authorization")
            prefix = self.config.auth_header_value_prefix
            if prefix is None:
                prefix = "Bearer "
            headers[header_name] = f"{prefix}{self.config.auth_token}"
            return headers
        if self.config.auth_type == BasicHttpAuthType.API_KEY_HEADER:
            header_name = str(self.config.auth_header_name or "X-API-Key")
            prefix = self.config.auth_header_value_prefix or ""
            headers[header_name] = f"{prefix}{self.config.auth_token}"
            return headers
        credentials = f"{self.config.username}:{self.config.password}".encode("utf-8")
        headers["Authorization"] = "Basic " + base64.b64encode(credentials).decode("ascii")
        return headers

    def _build_request_params(
        self,
        *,
        resource: BasicHttpResourceConfig,
        cursor: str | None,
        limit: int | None,
        since: str | None,
    ) -> dict[str, Any]:
        params = dict(resource.request_params or {})
        page_size = _resolved_page_size(resource=resource, limit=limit)
        if page_size is not None and str(resource.limit_param or "").strip():
            params[str(resource.limit_param).strip()] = page_size
        if cursor and str(resource.cursor_param or "").strip():
            if resource.pagination_strategy == BasicHttpPaginationStrategy.OFFSET:
                params[str(resource.cursor_param).strip()] = int(cursor)
            else:
                params[str(resource.cursor_param).strip()] = cursor
        if (
            since
            and resource.supports_incremental
            and str(resource.incremental_request_param or "").strip()
        ):
            params[str(resource.incremental_request_param).strip()] = _normalize_incremental_value(
                since,
                cursor_type=resource.incremental_cursor_type,
            )
        return params

    def _filter_incremental_records(
        self,
        *,
        resource: BasicHttpResourceConfig,
        records: list[dict[str, Any]],
        since: str | None,
    ) -> list[dict[str, Any]]:
        field_name = str(resource.incremental_cursor_field or "").strip()
        if not since or not resource.supports_incremental or not field_name:
            return records
        if resource.incremental_cursor_type == BasicHttpCursorType.UNIX_TIMESTAMP:
            threshold = _coerce_numeric_cursor(since)
            if threshold is None:
                return records
            return [
                record
                for record in records
                if (
                    value := _extract_path(record, field_name)
                ) is None or (
                    parsed := _coerce_numeric_cursor(value)
                ) is None or parsed >= threshold
            ]
        if resource.incremental_cursor_type == BasicHttpCursorType.ISO_DATETIME:
            threshold = _coerce_datetime_cursor(since)
            if threshold is None:
                return records
            return [
                record
                for record in records
                if (
                    value := _extract_path(record, field_name)
                ) is None or (
                    parsed := _coerce_datetime_cursor(value)
                ) is None or parsed >= threshold
            ]
        threshold = str(since).strip()
        return [
            record
            for record in records
            if (value := _extract_path(record, field_name)) is None or str(value).strip() >= threshold
        ]

    def _next_cursor(
        self,
        *,
        resource: BasicHttpResourceConfig,
        payload: Any,
        response: Any,
        records: list[dict[str, Any]],
        current_cursor: str | None,
        page_size: int | None,
    ) -> str | None:
        if resource.pagination_strategy == BasicHttpPaginationStrategy.NONE:
            return None
        if resource.pagination_strategy == BasicHttpPaginationStrategy.LINK_HEADER:
            return parse_link_header_cursor(
                response.headers.get("Link"),
                param_name=str(resource.link_header_param or resource.cursor_param or "cursor"),
            )
        if resource.pagination_strategy == BasicHttpPaginationStrategy.OFFSET:
            current_offset = int(str(current_cursor or "0").strip() or "0")
            if _truthy_flag(_extract_path(payload, resource.response_is_last_field)):
                return None
            total = _coerce_numeric_cursor(_extract_path(payload, resource.response_total_field))
            if total is not None and current_offset + len(records) >= total:
                return None
            if not records:
                return None
            next_offset = current_offset + len(records)
            if page_size is not None and len(records) < page_size and total is None:
                return None
            return str(next_offset)

        has_more_field = str(resource.response_has_more_field or "").strip()
        if has_more_field and not _truthy_flag(_extract_path(payload, has_more_field)):
            return None
        if not records:
            return None
        next_cursor_field = str(resource.next_cursor_field or "").strip()
        if next_cursor_field:
            top_level_cursor = _extract_path(payload, next_cursor_field)
            if top_level_cursor is not None:
                return str(top_level_cursor)
        last_record = records[-1]
        raw_cursor = None
        if next_cursor_field:
            raw_cursor = _extract_path(last_record, next_cursor_field)
        if raw_cursor is None and str(resource.primary_key or "").strip():
            raw_cursor = last_record.get(str(resource.primary_key).strip())
        return None if raw_cursor is None else str(raw_cursor)

    def _checkpoint_cursor(
        self,
        *,
        resource: BasicHttpResourceConfig,
        records: list[dict[str, Any]],
    ) -> str | None:
        field_name = str(resource.incremental_cursor_field or "").strip()
        if not field_name:
            return None
        raw_values = [_extract_path(record, field_name) for record in records]
        values = [value for value in raw_values if value is not None]
        if not values:
            return None
        if resource.incremental_cursor_type == BasicHttpCursorType.UNIX_TIMESTAMP:
            parsed = [_coerce_numeric_cursor(value) for value in values]
            valid = [value for value in parsed if value is not None]
            return str(max(valid)) if valid else None
        if resource.incremental_cursor_type == BasicHttpCursorType.ISO_DATETIME:
            parsed = [_coerce_datetime_cursor(value) for value in values]
            valid = [value for value in parsed if value is not None]
            if not valid:
                return None
            latest = max(valid)
            return latest.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        normalized = [str(value).strip() for value in values if str(value).strip()]
        return max(normalized) if normalized else None


def _resolved_page_size(*, resource: BasicHttpResourceConfig, limit: int | None) -> int | None:
    if not str(resource.limit_param or "").strip():
        return None
    resolved_limit = limit if limit is not None else resource.default_page_size
    if resolved_limit is None:
        return None
    if resource.max_page_size is not None:
        return max(1, min(int(resolved_limit), int(resource.max_page_size)))
    return max(1, int(resolved_limit))


def _request_resource_path(path: str) -> str:
    normalized = str(path or "").strip().split("?", 1)[0].strip("/")
    if not normalized:
        return "request"
    return ".".join(segment for segment in normalized.split("/") if segment)


def _extract_records(payload: Any, *, field_path: str | None) -> list[dict[str, Any]]:
    if not str(field_path or "").strip():
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, Mapping)]
        if isinstance(payload, Mapping):
            return [dict(payload)]
        return []
    value = _extract_path(payload, field_path)
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    if isinstance(value, Mapping):
        return [dict(value)]
    return []


def _extract_path(payload: Any, field_path: str | None) -> Any:
    if not str(field_path or "").strip():
        return payload
    current = payload
    for segment in str(field_path).split("."):
        key = segment.strip()
        if not key:
            continue
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _normalize_incremental_value(raw_value: str, *, cursor_type: BasicHttpCursorType) -> str:
    normalized = str(raw_value or "").strip()
    if not normalized:
        return normalized
    if cursor_type == BasicHttpCursorType.UNIX_TIMESTAMP:
        numeric = _coerce_numeric_cursor(normalized)
        return str(numeric) if numeric is not None else normalized
    if cursor_type == BasicHttpCursorType.ISO_DATETIME:
        parsed = _coerce_datetime_cursor(normalized)
        if parsed is None:
            return normalized
        return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return normalized


def _coerce_numeric_cursor(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    normalized = str(value or "").strip()
    if normalized.isdigit():
        return int(normalized)
    return None


def _coerce_datetime_cursor(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    normalized = str(value or "").strip()
    if not normalized:
        return None
    candidate = normalized.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}
