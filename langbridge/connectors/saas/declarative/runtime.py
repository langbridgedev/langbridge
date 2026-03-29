
from datetime import datetime, timezone
from typing import Any, Mapping

from langbridge.connectors.base.config import BaseConnectorConfig
from langbridge.connectors.base.connector import ApiExtractResult, ApiResource
from langbridge.connectors.base.errors import ConnectorError
from langbridge.connectors.base.http import (
    ApiResourceDefinition,
    HttpApiConnector,
    flatten_api_records,
    parse_link_header_cursor,
)

from .manifest import DeclarativeConnectorManifest, DeclarativeConnectorResource


class DeclarativeHttpApiConnector(HttpApiConnector):
    """Manifest-driven HTTP API connector for sync-oriented SaaS resources."""

    MANIFEST: DeclarativeConnectorManifest | None = None

    def __init__(
        self,
        config: BaseConnectorConfig,
        logger=None,
        **kwargs: Any,
    ) -> None:
        super().__init__(config=config, logger=logger, **kwargs)
        self._manifest = self._require_manifest()
        self._resource_definitions = {
            resource.key: _build_resource_definition(resource, manifest=self._manifest)
            for resource in self._manifest.resources
        }

    async def discover_resources(self) -> list[ApiResource]:
        return [definition.resource for definition in self._resource_definitions.values()]

    async def test_connection(self) -> None:
        test_path = self._manifest.test_connection_path
        params: dict[str, Any] | None = None
        if not test_path:
            if not self._manifest.resources:
                raise ConnectorError(
                    f"Declarative connector '{self._manifest.id}' does not define any resources."
                )
            test_path = self._manifest.resources[0].path
            params = {self._manifest.pagination.limit_param: 1}
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
        params = self._build_request_params(
            definition=definition,
            since=since,
            cursor=cursor,
            limit=limit,
        )
        page_size = int(params.get(self._manifest.pagination.limit_param, 0) or 0)
        payload, response = await self._request_json("GET", definition.path, params=params)
        records = _extract_records(payload, definition.response_key)
        records = self._filter_incremental_records(
            definition=definition,
            records=records,
            since=since,
        )
        flattened_records, child_records = flatten_api_records(
            resource_name=definition.resource.name,
            records=[record for record in records if isinstance(record, dict)],
            primary_key=definition.resource.primary_key,
        )
        return ApiExtractResult(
            resource=definition.resource.name,
            status="success",
            records=flattened_records,
            next_cursor=self._next_cursor(
                payload=payload,
                response=response,
                records=records,
                definition=definition,
                current_cursor=cursor,
                page_size=page_size,
            ),
            checkpoint_cursor=self._checkpoint_cursor(records=records),
            child_records=child_records,
        )

    def _require_manifest(self) -> DeclarativeConnectorManifest:
        if self.MANIFEST is None:
            raise ConnectorError(
                f"{type(self).__name__} must define MANIFEST to use the declarative runtime."
            )
        return self.MANIFEST

    def _require_resource(self, resource_name: str) -> ApiResourceDefinition:
        definition = self._resource_definitions.get(resource_name)
        if definition is None:
            raise ConnectorError(
                f"Unsupported declarative resource '{resource_name}' for connector '{self._manifest.id}'."
            )
        return definition

    def _base_url(self) -> str:
        override = str(getattr(self.config, "api_base_url", "") or "").strip()
        return (override or self._manifest.base_url).rstrip("/")

    def _default_headers(self) -> dict[str, str]:
        config_values = _config_values(self.config)
        headers = {
            self._manifest.auth.header_name: self._manifest.auth.header_template.format(**config_values)
        }
        for header in self._manifest.auth.static_headers:
            headers[header.header_name] = header.value
        for header in self._manifest.auth.optional_headers:
            value = config_values.get(header.field)
            if value is None:
                continue
            normalized = str(value).strip()
            if normalized:
                headers[header.header_name] = normalized
        return headers

    def _build_request_params(
        self,
        *,
        definition: ApiResourceDefinition,
        since: str | None,
        cursor: str | None,
        limit: int | None,
    ) -> dict[str, Any]:
        page_size = self._clamp_limit(
            limit,
            default=self._manifest.pagination.default_page_size,
            maximum=self._manifest.pagination.max_page_size,
        )
        params: dict[str, Any] = {
            self._manifest.pagination.limit_param: page_size,
            **dict(definition.request_params or {}),
        }
        if cursor:
            params[self._manifest.pagination.cursor_param] = cursor
            return params
        if since and definition.resource.supports_incremental:
            if self._manifest.incremental.strategy == "request_param":
                params[self._manifest.incremental.request_param] = _normalize_incremental_value(
                    since,
                    cursor_type=self._manifest.incremental.cursor_type,
                )
        return params

    def _filter_incremental_records(
        self,
        *,
        definition: ApiResourceDefinition,
        records: list[Mapping[str, Any]],
        since: str | None,
    ) -> list[Mapping[str, Any]]:
        if (
            not since
            or not definition.resource.supports_incremental
            or self._manifest.incremental.strategy != "client_filter"
        ):
            return records
        field_name = self._manifest.incremental.cursor_field
        cursor_type = self._manifest.incremental.cursor_type
        if cursor_type == "unix_timestamp":
            threshold = _coerce_numeric_cursor(since)
            if threshold is None:
                return records
            return [
                record
                for record in records
                if (
                    raw_value := _extract_path(record, field_name)
                ) is None or (
                    coerced := _coerce_numeric_cursor(raw_value)
                ) is None or coerced >= threshold
            ]
        if cursor_type in {"iso_datetime", "datetime"}:
            threshold = _coerce_datetime_cursor(since)
            if threshold is None:
                return records
            return [
                record
                for record in records
                if (
                    raw_value := _extract_path(record, field_name)
                ) is None or (
                    coerced := _coerce_datetime_cursor(raw_value)
                ) is None or coerced >= threshold
            ]
        threshold = str(since).strip()
        if not threshold:
            return records
        return [
            record
            for record in records
            if (raw_value := _extract_path(record, field_name)) is None or str(raw_value).strip() >= threshold
        ]

    def _next_cursor(
        self,
        *,
        payload: Any,
        response: Any,
        records: list[Mapping[str, Any]],
        definition: ApiResourceDefinition,
        current_cursor: str | None,
        page_size: int,
    ) -> str | None:
        pagination = self._manifest.pagination
        if pagination.next_cursor_source == "link_header":
            param_name = pagination.link_header_param or pagination.cursor_param
            return parse_link_header_cursor(response.headers.get("Link"), param_name=param_name)
        if pagination.next_cursor_source == "response":
            if pagination.response_has_more_field and not _truthy_flag(
                _extract_path(payload, pagination.response_has_more_field)
            ):
                return None
            return _normalized_cursor(_extract_path(payload, pagination.next_cursor_field))
        if pagination.strategy == "offset":
            current_offset = int(str(current_cursor or "0").strip() or "0")
            if _truthy_flag(_extract_path(payload, pagination.response_is_last_field)):
                return None
            total = _coerce_numeric_cursor(_extract_path(payload, pagination.response_total_field))
            if total is not None and current_offset + len(records) >= total:
                return None
            if not records:
                return None
            next_offset = current_offset + len(records)
            if page_size and len(records) < page_size and total in {None, ""}:
                return None
            return str(next_offset)
        has_more = _extract_path(payload, pagination.response_has_more_field)
        if pagination.response_has_more_field and not has_more:
            return None
        if not records:
            return None
        last_record = records[-1]
        raw_cursor = _extract_path(last_record, pagination.next_cursor_field)
        if raw_cursor is None:
            raw_cursor = last_record.get(definition.resource.primary_key or "id")
        return _normalized_cursor(raw_cursor)

    def _checkpoint_cursor(self, *, records: list[Mapping[str, Any]]) -> str | None:
        field_name = self._manifest.incremental.cursor_field
        if not field_name:
            return None
        raw_values = [
            _extract_path(record, field_name)
            for record in records
        ]
        values = [value for value in raw_values if value is not None]
        if not values:
            return None
        cursor_type = self._manifest.incremental.cursor_type
        if cursor_type == "unix_timestamp":
            normalized = [_coerce_numeric_cursor(value) for value in values]
            valid = [value for value in normalized if value is not None]
            return str(max(valid)) if valid else None
        if cursor_type in {"iso_datetime", "datetime"}:
            normalized = [_coerce_datetime_cursor(value) for value in values]
            valid = [value for value in normalized if value is not None]
            if not valid:
                return None
            latest = max(valid)
            return latest.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        normalized = [str(value).strip() for value in values if str(value).strip()]
        return max(normalized) if normalized else None


def _build_resource_definition(
    resource: DeclarativeConnectorResource,
    *,
    manifest: DeclarativeConnectorManifest,
) -> ApiResourceDefinition:
    return ApiResourceDefinition(
        resource=ApiResource(
            name=resource.key,
            label=resource.label,
            primary_key=resource.primary_key,
            cursor_field=manifest.pagination.cursor_param,
            incremental_cursor_field=(
                manifest.incremental.cursor_field if resource.supports_incremental else None
            ),
            supports_incremental=resource.supports_incremental,
            default_sync_mode=resource.default_sync_mode,
        ),
        path=resource.path,
        response_key=resource.response_items_field or manifest.pagination.response_items_field,
        request_params=resource.request_params,
    )


def _config_values(config: BaseConnectorConfig) -> dict[str, Any]:
    if hasattr(config, "model_dump"):
        payload = config.model_dump(mode="json")
        if isinstance(payload, dict):
            return payload
    return dict(getattr(config, "__dict__", {}))


def _extract_records(payload: Any, field_path: str) -> list[Mapping[str, Any]]:
    if not str(field_path or "").strip():
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, Mapping)]
        return []
    value = _extract_path(payload, field_path)
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _extract_path(payload: Any, field_path: str) -> Any:
    current = payload
    for segment in str(field_path or "").split("."):
        key = segment.strip()
        if not key:
            continue
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _normalize_incremental_value(raw_value: str, *, cursor_type: str) -> str:
    normalized = str(raw_value or "").strip()
    if not normalized:
        return normalized
    if cursor_type == "unix_timestamp":
        numeric = _coerce_numeric_cursor(normalized)
        return str(numeric) if numeric is not None else normalized
    if cursor_type in {"iso_datetime", "datetime"}:
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
    parsed = _coerce_datetime_cursor(normalized)
    if parsed is None:
        return None
    return int(parsed.timestamp())


def _coerce_datetime_cursor(value: Any) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalized_cursor(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes"}
