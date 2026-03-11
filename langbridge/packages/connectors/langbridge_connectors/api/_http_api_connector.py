import json
import os
import re
import ssl
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse

import httpx

from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.errors.connector_errors import (
    AuthError,
    ConnectorError,
)

from .connector import ApiConnector, ApiExtractResult, ApiResource, ApiSyncResult

_KEY_SANITIZER = re.compile(r"[^0-9A-Za-z_]+")
_SCALAR_TYPES = (str, int, float, bool, type(None))


@dataclass(frozen=True, slots=True)
class ApiResourceDefinition:
    resource: ApiResource
    path: str
    response_key: str | None = None


class HttpApiConnector(ApiConnector):
    RESOURCE_DEFINITIONS: Mapping[str, ApiResourceDefinition] = {}

    def __init__(
        self,
        config: Any,
        logger=None,
        *,
        transport: Any | None = None,
        timeout_s: float = 30.0,
    ) -> None:
        super().__init__(config=config, logger=logger)
        self._transport = transport
        self._timeout_s = timeout_s
        self._verify = _build_http_verify()

    async def discover_resources(self) -> list[ApiResource]:
        return [definition.resource for definition in self.RESOURCE_DEFINITIONS.values()]

    async def sync_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiSyncResult:
        result = await self.extract_resource(
            resource_name,
            since=since,
            cursor=cursor,
            limit=limit,
        )
        child_count = sum(len(rows) for rows in (result.child_records or {}).values())
        return ApiSyncResult(
            resource=resource_name,
            status=result.status,
            records_synced=len(result.records) + child_count,
            datasets_created=[],
        )

    def _require_resource(self, resource_name: str) -> ApiResourceDefinition:
        definition = self.RESOURCE_DEFINITIONS.get(resource_name)
        if definition is None:
            raise ConnectorError(f"Unsupported resource '{resource_name}'.")
        return definition

    @staticmethod
    def _clamp_limit(limit: int | None, *, default: int, maximum: int) -> int:
        if limit is None:
            return default
        return max(1, min(int(limit), maximum))

    async def _request(
        self,
        method: str,
        path_or_url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json_payload: Any | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        url = self._resolve_url(path_or_url)
        request_headers = {
            "Accept": "application/json",
            **self._default_headers(),
            **dict(headers or {}),
        }
        try:
            async with httpx.AsyncClient(
                transport=self._transport,
                timeout=httpx.Timeout(self._timeout_s),
                follow_redirects=True,
                verify=self._verify,
            ) as client:
                response = await client.request(
                    method=method,
                    url=url,
                    headers=request_headers,
                    params=params,
                    json=json_payload,
                    data=data,
                )
        except httpx.RequestError as exc:
            raise ConnectorError(f"Request to {url} failed: {exc}") from exc

        if response.status_code in {401, 403}:
            raise AuthError(self._error_message(response, fallback=f"Authentication failed for {url}."))
        if response.status_code >= 400:
            raise ConnectorError(
                self._error_message(
                    response,
                    fallback=f"Request to {url} failed with status {response.status_code}.",
                )
            )
        return response

    async def _request_json(
        self,
        method: str,
        path_or_url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json_payload: Any | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> tuple[Any, httpx.Response]:
        response = await self._request(
            method,
            path_or_url,
            headers=headers,
            params=params,
            json_payload=json_payload,
            data=data,
        )
        try:
            return response.json(), response
        except ValueError as exc:
            raise ConnectorError(f"Response from {response.request.url} was not valid JSON.") from exc

    def _resolve_url(self, path_or_url: str) -> str:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            return path_or_url
        base_url = self._base_url().rstrip("/")
        path = path_or_url if path_or_url.startswith("/") else f"/{path_or_url}"
        return f"{base_url}{path}"

    def _base_url(self) -> str:
        raise NotImplementedError

    def _default_headers(self) -> dict[str, str]:
        return {}

    @staticmethod
    def _error_message(response: httpx.Response, *, fallback: str) -> str:
        try:
            payload = response.json()
        except ValueError:
            text = response.text.strip()
            return text or fallback

        if isinstance(payload, dict):
            for key in ("error_description", "error", "message"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            if isinstance(payload.get("errors"), list) and payload["errors"]:
                first = payload["errors"][0]
                if isinstance(first, str) and first.strip():
                    return first.strip()
                if isinstance(first, dict):
                    for key in ("message", "detail", "code"):
                        value = first.get(key)
                        if isinstance(value, str) and value.strip():
                            return value.strip()
        return fallback

def flatten_api_records(
    *,
    resource_name: str,
    records: list[dict[str, Any]],
    primary_key: str | None = "id",
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    flattened_records: list[dict[str, Any]] = []
    child_records: dict[str, list[dict[str, Any]]] = {}

    for record_index, raw_record in enumerate(records):
        if not isinstance(raw_record, Mapping):
            flattened_records.append({"value": raw_record})
            continue

        flattened: dict[str, Any] = {}
        record_id = raw_record.get(primary_key) if primary_key else raw_record.get("id")
        _flatten_mapping(
            payload=raw_record,
            row=flattened,
            child_records=child_records,
            table_name=resource_name,
            parent_table=resource_name,
            parent_id=record_id,
            path_segments=[],
        )
        if record_id is None:
            flattened.setdefault("_record_index", record_index)
        flattened_records.append(flattened)

    return flattened_records, child_records


def parse_link_header_cursor(link_header: str | None, *, param_name: str = "page_info") -> str | None:
    if not link_header:
        return None

    for fragment in link_header.split(","):
        if 'rel="next"' not in fragment:
            continue
        start = fragment.find("<")
        end = fragment.find(">", start + 1)
        if start == -1 or end == -1:
            continue
        next_url = fragment[start + 1 : end]
        parsed = parse_qs(urlparse(next_url).query)
        values = parsed.get(param_name)
        if values:
            return values[0]
    return None


def _flatten_mapping(
    *,
    payload: Mapping[str, Any],
    row: dict[str, Any],
    child_records: dict[str, list[dict[str, Any]]],
    table_name: str,
    parent_table: str,
    parent_id: Any,
    path_segments: list[str],
) -> None:
    for raw_key, value in payload.items():
        key = _normalize_key(raw_key)
        if not key:
            continue
        next_segments = [*path_segments, key]
        field_name = "__".join(next_segments)

        if isinstance(value, _SCALAR_TYPES):
            row[field_name] = value
            continue

        if isinstance(value, Mapping):
            _flatten_mapping(
                payload=value,
                row=row,
                child_records=child_records,
                table_name=table_name,
                parent_table=parent_table,
                parent_id=parent_id,
                path_segments=next_segments,
            )
            continue

        if isinstance(value, list):
            child_table_name = f"{table_name}__{field_name}"
            for child_index, child_value in enumerate(value):
                child_row = {
                    "_parent_table": parent_table,
                    "_parent_id": parent_id,
                    "_child_index": child_index,
                }
                if isinstance(child_value, Mapping):
                    child_record_id = child_value.get("id", parent_id)
                    _flatten_mapping(
                        payload=child_value,
                        row=child_row,
                        child_records=child_records,
                        table_name=child_table_name,
                        parent_table=child_table_name,
                        parent_id=child_record_id,
                        path_segments=[],
                    )
                elif isinstance(child_value, _SCALAR_TYPES):
                    child_row["value"] = child_value
                else:
                    child_row["value"] = _json_string(child_value)
                child_records.setdefault(child_table_name, []).append(child_row)
            continue

        row[field_name] = _json_string(value)


def _normalize_key(value: Any) -> str:
    key = _KEY_SANITIZER.sub("_", str(value or "")).strip("_")
    return key


def _json_string(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _build_http_verify() -> ssl.SSLContext | bool:
    if settings.API_HTTP_SKIP_TLS_VERIFY:
        return False

    ssl_context = ssl.create_default_context()
    extra_ca_bundle = _extra_ca_bundle()
    if extra_ca_bundle:
        ssl_context.load_verify_locations(cafile=extra_ca_bundle)
    return ssl_context


def _extra_ca_bundle() -> str | None:
    for candidate in (
        settings.API_HTTP_CA_BUNDLE,
        os.environ.get("REQUESTS_CA_BUNDLE"),
        os.environ.get("CURL_CA_BUNDLE"),
    ):
        value = str(candidate or "").strip()
        if value:
            return value
    return None
