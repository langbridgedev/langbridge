from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

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

from .config import STRIPE_SUPPORTED_RESOURCES, StripeConnectorConfig


class StripeApiConnector(HttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.STRIPE
    SUPPORTED_RESOURCES = STRIPE_SUPPORTED_RESOURCES
    RESOURCE_DEFINITIONS = {
        "customers": ApiResourceDefinition(
            resource=ApiResource(
                name="customers",
                label="Customers",
                primary_key="id",
                cursor_field="starting_after",
                incremental_cursor_field="created",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/v1/customers",
        ),
        "charges": ApiResourceDefinition(
            resource=ApiResource(
                name="charges",
                label="Charges",
                primary_key="id",
                cursor_field="starting_after",
                incremental_cursor_field="created",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/v1/charges",
        ),
        "invoices": ApiResourceDefinition(
            resource=ApiResource(
                name="invoices",
                label="Invoices",
                primary_key="id",
                cursor_field="starting_after",
                incremental_cursor_field="created",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/v1/invoices",
        ),
        "subscriptions": ApiResourceDefinition(
            resource=ApiResource(
                name="subscriptions",
                label="Subscriptions",
                primary_key="id",
                cursor_field="starting_after",
                incremental_cursor_field="created",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/v1/subscriptions",
        ),
    }

    def __init__(self, config: StripeConnectorConfig, logger=None, **kwargs: Any) -> None:
        super().__init__(config=config, logger=logger, **kwargs)

    def _base_url(self) -> str:
        return self.config.api_base_url.rstrip("/")

    def _default_headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
        }
        if self.config.account_id:
            headers["Stripe-Account"] = self.config.account_id
        return headers

    async def test_connection(self) -> None:
        await self._request_json("GET", "/v1/account")

    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiExtractResult:
        definition = self._require_resource(resource_name)
        page_size = self._clamp_limit(limit, default=100, maximum=100)
        params: dict[str, Any] = {"limit": page_size}
        if cursor:
            params["starting_after"] = cursor
        elif since:
            params["created[gte]"] = _coerce_unix_timestamp(since)

        payload, _ = await self._request_json(
            "GET",
            definition.path,
            params=params,
        )
        records = payload.get("data", []) if isinstance(payload, dict) else []
        if not isinstance(records, list):
            records = []

        flattened_records, child_records = flatten_api_records(
            resource_name=definition.resource.name,
            records=[record for record in records if isinstance(record, dict)],
            primary_key=definition.resource.primary_key,
        )
        next_cursor = None
        if payload.get("has_more") and records:
            last_record = records[-1]
            if isinstance(last_record, dict):
                raw_next = last_record.get(definition.resource.primary_key or "id")
                if raw_next:
                    next_cursor = str(raw_next)

        return ApiExtractResult(
            resource=definition.resource.name,
            status="success",
            records=flattened_records,
            next_cursor=next_cursor,
            checkpoint_cursor=_max_numeric_cursor(flattened_records, "created"),
            child_records=child_records,
        )


def _coerce_unix_timestamp(raw_value: str) -> str:
    normalized = str(raw_value or "").strip()
    if normalized.isdigit():
        return normalized
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return normalized
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return str(int(parsed.timestamp()))


def _max_numeric_cursor(rows: list[dict[str, Any]], field_name: str) -> str | None:
    values: list[int] = []
    for row in rows:
        value = row.get(field_name)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            values.append(int(value))
            continue
        if isinstance(value, str) and value.strip().isdigit():
            values.append(int(value.strip()))
    if not values:
        return None
    return str(max(values))
