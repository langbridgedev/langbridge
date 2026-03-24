from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from langbridge.connectors.base.config import BaseConnectorConfig
from langbridge.connectors.base.connector import ApiExtractResult, ApiResource
from langbridge.connectors.base.errors import ConnectorError
from langbridge.connectors.base.http import (
    ApiResourceDefinition,
    HttpApiConnector,
    flatten_api_records,
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
            resource=definition.resource,
            since=since,
            cursor=cursor,
            limit=limit,
        )
        payload, _ = await self._request_json("GET", definition.path, params=params)
        records = _extract_records(payload, self._manifest.pagination.response_items_field)
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
                records=records,
                definition=definition,
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
        resource: ApiResource,
        since: str | None,
        cursor: str | None,
        limit: int | None,
    ) -> dict[str, Any]:
        page_size = self._clamp_limit(
            limit,
            default=self._manifest.pagination.default_page_size,
            maximum=self._manifest.pagination.max_page_size,
        )
        params: dict[str, Any] = {self._manifest.pagination.limit_param: page_size}
        if cursor:
            params[self._manifest.pagination.cursor_param] = cursor
            return params
        if since and resource.supports_incremental:
            params[self._manifest.incremental.request_param] = _normalize_incremental_value(
                since,
                cursor_type=self._manifest.incremental.cursor_type,
            )
        return params

    def _next_cursor(
        self,
        *,
        payload: Any,
        records: list[Mapping[str, Any]],
        definition: ApiResourceDefinition,
    ) -> str | None:
        has_more = _extract_path(payload, self._manifest.pagination.response_has_more_field)
        if not has_more or not records:
            return None
        last_record = records[-1]
        raw_cursor = _extract_path(last_record, self._manifest.pagination.next_cursor_field)
        if raw_cursor is None:
            raw_cursor = last_record.get(definition.resource.primary_key or "id")
        if raw_cursor is None:
            return None
        normalized = str(raw_cursor).strip()
        return normalized or None

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
    )


def _config_values(config: BaseConnectorConfig) -> dict[str, Any]:
    if hasattr(config, "model_dump"):
        payload = config.model_dump(mode="json")
        if isinstance(payload, dict):
            return payload
    return dict(getattr(config, "__dict__", {}))


def _extract_records(payload: Any, field_path: str) -> list[Mapping[str, Any]]:
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
