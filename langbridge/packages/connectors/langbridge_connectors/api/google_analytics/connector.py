from __future__ import annotations

import base64
import json
import time
from typing import Any, Mapping

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from langbridge.packages.common.langbridge_common.errors.connector_errors import (
    AuthError,
    ConnectorError,
)
from langbridge.packages.connectors.langbridge_connectors.api._http_api_connector import (
    ApiResourceDefinition,
    HttpApiConnector,
    flatten_api_records,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import (
    ConnectorRuntimeType,
)
from langbridge.packages.connectors.langbridge_connectors.api.connector import (
    ApiExtractResult,
    ApiResource,
)

from .config import (
    GOOGLE_ANALYTICS_SUPPORTED_RESOURCES,
    GoogleAnalyticsConnectorConfig,
)

_GOOGLE_SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
_GOOGLE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"
_DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token"
_DEFAULT_DATE_RANGES = [{"startDate": "30daysAgo", "endDate": "today"}]


class GoogleAnalyticsApiConnector(HttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.GOOGLE_ANALYTICS
    SUPPORTED_RESOURCES = GOOGLE_ANALYTICS_SUPPORTED_RESOURCES
    RESOURCE_DEFINITIONS = {
        "events": ApiResourceDefinition(
            resource=ApiResource(
                name="events",
                label="Events",
                primary_key=None,
                cursor_field="offset",
                supports_incremental=False,
            ),
            path="",
        ),
        "pages": ApiResourceDefinition(
            resource=ApiResource(
                name="pages",
                label="Pages",
                primary_key=None,
                cursor_field="offset",
                supports_incremental=False,
            ),
            path="",
        ),
        "sessions": ApiResourceDefinition(
            resource=ApiResource(
                name="sessions",
                label="Sessions",
                primary_key=None,
                cursor_field="offset",
                supports_incremental=False,
            ),
            path="",
        ),
        "traffic_sources": ApiResourceDefinition(
            resource=ApiResource(
                name="traffic_sources",
                label="Traffic Sources",
                primary_key=None,
                cursor_field="offset",
                supports_incremental=False,
            ),
            path="",
        ),
    }

    _REPORT_DEFINITIONS = {
        "events": {
            "dimensions": ["date", "eventName"],
            "metrics": ["eventCount", "totalUsers"],
        },
        "pages": {
            "dimensions": ["date", "pagePath", "pageTitle"],
            "metrics": ["screenPageViews", "activeUsers"],
        },
        "sessions": {
            "dimensions": ["date", "sessionDefaultChannelGroup"],
            "metrics": ["sessions", "engagedSessions", "totalUsers"],
        },
        "traffic_sources": {
            "dimensions": ["date", "sessionSource", "sessionMedium"],
            "metrics": ["sessions", "totalUsers"],
        },
    }

    def __init__(self, config: GoogleAnalyticsConnectorConfig, logger=None, **kwargs: Any) -> None:
        super().__init__(config=config, logger=logger, **kwargs)
        self._access_token: str | None = None
        self._access_token_expiry: float = 0.0

    def _base_url(self) -> str:
        return self.config.api_base_url.rstrip("/")

    async def test_connection(self) -> None:
        await self._run_report(
            resource_name="sessions",
            offset=0,
            limit=1,
        )

    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiExtractResult:
        self._require_resource(resource_name)
        try:
            offset = max(0, int(cursor or "0"))
        except ValueError as exc:
            raise ConnectorError("Google Analytics cursor must be an integer offset.") from exc

        page_size = self._clamp_limit(limit, default=1000, maximum=10000)
        payload = await self._run_report(
            resource_name=resource_name,
            offset=offset,
            limit=page_size,
        )
        records, row_count = self._records_from_report(payload)
        flattened_records, child_records = flatten_api_records(
            resource_name=resource_name,
            records=records,
            primary_key=None,
        )
        next_cursor = None
        if offset + len(records) < row_count:
            next_cursor = str(offset + len(records))

        return ApiExtractResult(
            resource=resource_name,
            status="success",
            records=flattened_records,
            next_cursor=next_cursor,
            child_records=child_records,
        )

    async def _run_report(
        self,
        *,
        resource_name: str,
        offset: int,
        limit: int,
    ) -> dict[str, Any]:
        report_definition = self._REPORT_DEFINITIONS.get(resource_name)
        if report_definition is None:
            raise ConnectorError(f"Unsupported Google Analytics resource '{resource_name}'.")

        payload, _ = await self._request_json(
            "POST",
            f"/v1beta/properties/{self.config.property_id}:runReport",
            headers=await self._authorization_headers(),
            json_payload={
                "dateRanges": list(_DEFAULT_DATE_RANGES),
                "dimensions": [{"name": name} for name in report_definition["dimensions"]],
                "metrics": [{"name": name} for name in report_definition["metrics"]],
                "limit": str(limit),
                "offset": str(offset),
            },
        )
        if not isinstance(payload, dict):
            raise ConnectorError("Google Analytics response was not a valid report payload.")
        return payload

    def _records_from_report(self, payload: Mapping[str, Any]) -> tuple[list[dict[str, Any]], int]:
        dimension_headers = payload.get("dimensionHeaders") if isinstance(payload.get("dimensionHeaders"), list) else []
        metric_headers = payload.get("metricHeaders") if isinstance(payload.get("metricHeaders"), list) else []
        rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
        row_count = int(payload.get("rowCount") or 0)

        records: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            record: dict[str, Any] = {}
            dimension_values = row.get("dimensionValues") if isinstance(row.get("dimensionValues"), list) else []
            metric_values = row.get("metricValues") if isinstance(row.get("metricValues"), list) else []

            for index, header in enumerate(dimension_headers):
                if not isinstance(header, Mapping) or index >= len(dimension_values):
                    continue
                header_name = str(header.get("name") or "").strip()
                value = dimension_values[index]
                if header_name and isinstance(value, Mapping):
                    record[header_name] = value.get("value")

            for index, header in enumerate(metric_headers):
                if not isinstance(header, Mapping) or index >= len(metric_values):
                    continue
                header_name = str(header.get("name") or "").strip()
                metric_type = str(header.get("type") or "").strip().upper()
                value = metric_values[index]
                if header_name and isinstance(value, Mapping):
                    record[header_name] = _coerce_metric_value(metric_type, value.get("value"))

            if record:
                records.append(record)

        return records, row_count

    async def _authorization_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {await self._access_token_value()}"}

    async def _access_token_value(self) -> str:
        now = time.time()
        if self._access_token and now < self._access_token_expiry - 60:
            return self._access_token

        credentials = self._credentials_payload()
        token_uri = str(credentials.get("token_uri") or _DEFAULT_TOKEN_URI).strip()
        assertion = self._build_service_account_assertion(credentials=credentials, token_uri=token_uri)
        payload, _ = await self._request_json(
            "POST",
            token_uri,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": _GOOGLE_GRANT_TYPE,
                "assertion": assertion,
            },
        )
        if not isinstance(payload, dict):
            raise AuthError("Google token endpoint returned an invalid response.")
        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            raise AuthError("Google token endpoint did not return an access token.")

        expires_in = int(payload.get("expires_in") or 3600)
        self._access_token = access_token
        self._access_token_expiry = now + max(60, expires_in)
        return access_token

    def _credentials_payload(self) -> dict[str, Any]:
        raw_credentials = self.config.credentials_json
        if isinstance(raw_credentials, Mapping):
            payload = dict(raw_credentials)
        else:
            try:
                parsed = json.loads(str(raw_credentials))
            except ValueError as exc:
                raise AuthError("Google service account credentials_json must be valid JSON.") from exc
            if not isinstance(parsed, dict):
                raise AuthError("Google service account credentials_json must decode to an object.")
            payload = parsed

        if not str(payload.get("client_email") or "").strip():
            raise AuthError("Google service account credentials are missing client_email.")
        if not str(payload.get("private_key") or "").strip():
            raise AuthError("Google service account credentials are missing private_key.")
        return payload

    def _build_service_account_assertion(
        self,
        *,
        credentials: Mapping[str, Any],
        token_uri: str,
    ) -> str:
        now = int(time.time())
        header = {"alg": "RS256", "typ": "JWT"}
        claims = {
            "iss": str(credentials["client_email"]),
            "scope": _GOOGLE_SCOPE,
            "aud": token_uri,
            "iat": now,
            "exp": now + 3600,
        }
        encoded_header = _urlsafe_b64(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
        encoded_claims = _urlsafe_b64(json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8"))
        signing_input = f"{encoded_header}.{encoded_claims}".encode("utf-8")
        private_key = serialization.load_pem_private_key(
            str(credentials["private_key"]).encode("utf-8"),
            password=None,
        )
        signature = private_key.sign(
            signing_input,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return f"{encoded_header}.{encoded_claims}.{_urlsafe_b64(signature)}"


def _coerce_metric_value(metric_type: str, raw_value: Any) -> Any:
    value = str(raw_value or "")
    if value == "":
        return None
    if metric_type in {"TYPE_INTEGER", "TYPE_SECONDS", "TYPE_MILLISECONDS", "TYPE_MINUTES", "TYPE_HOURS"}:
        try:
            return int(value)
        except ValueError:
            return value
    if metric_type in {"TYPE_FLOAT", "TYPE_STANDARD", "TYPE_CURRENCY"}:
        try:
            return float(value)
        except ValueError:
            return value
    return value


def _urlsafe_b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")
