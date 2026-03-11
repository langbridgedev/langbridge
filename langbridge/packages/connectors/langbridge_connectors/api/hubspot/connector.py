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

from .config import HUBSPOT_SUPPORTED_RESOURCES, HubSpotConnectorConfig


class HubSpotApiConnector(HttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.HUBSPOT
    SUPPORTED_RESOURCES = HUBSPOT_SUPPORTED_RESOURCES
    RESOURCE_DEFINITIONS = {
        "contacts": ApiResourceDefinition(
            resource=ApiResource(
                name="contacts",
                label="Contacts",
                primary_key="id",
                cursor_field="after",
                incremental_cursor_field="updatedAt",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/crm/v3/objects/contacts",
            response_key="results",
        ),
        "companies": ApiResourceDefinition(
            resource=ApiResource(
                name="companies",
                label="Companies",
                primary_key="id",
                cursor_field="after",
                incremental_cursor_field="updatedAt",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/crm/v3/objects/companies",
            response_key="results",
        ),
        "deals": ApiResourceDefinition(
            resource=ApiResource(
                name="deals",
                label="Deals",
                primary_key="id",
                cursor_field="after",
                incremental_cursor_field="updatedAt",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/crm/v3/objects/deals",
            response_key="results",
        ),
        "tickets": ApiResourceDefinition(
            resource=ApiResource(
                name="tickets",
                label="Tickets",
                primary_key="id",
                cursor_field="after",
                incremental_cursor_field="updatedAt",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/crm/v3/objects/tickets",
            response_key="results",
        ),
    }
    config: HubSpotConnectorConfig

    def __init__(self, config: HubSpotConnectorConfig, logger=None, **kwargs: Any) -> None:
        super().__init__(config=config, logger=logger, **kwargs)

    def _base_url(self) -> str:
        return self.config.api_base_url.rstrip("/")

    def _default_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.service_key}",
        }

    async def test_connection(self) -> None:
        await self._request_json(
            "GET",
            "/crm/v3/objects/contacts",
            params={"limit": 1, "archived": "false"},
        )

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
        params: dict[str, Any] = {
            "limit": page_size,
            "archived": "false",
        }
        if cursor:
            params["after"] = cursor

        payload, _ = await self._request_json(
            "GET",
            definition.path,
            params=params,
        )
        records = payload.get(definition.response_key or "results", []) if isinstance(payload, dict) else []
        if not isinstance(records, list):
            records = []
        if since:
            records = [record for record in records if _is_newer_hubspot_record(record, since)]

        flattened_records, child_records = flatten_api_records(
            resource_name=definition.resource.name,
            records=[record for record in records if isinstance(record, dict)],
            primary_key=definition.resource.primary_key,
        )
        next_cursor = None
        paging = payload.get("paging") if isinstance(payload, dict) else None
        if isinstance(paging, dict):
            next_payload = paging.get("next")
            if isinstance(next_payload, dict):
                raw_after = next_payload.get("after")
                if raw_after:
                    next_cursor = str(raw_after)

        return ApiExtractResult(
            resource=definition.resource.name,
            status="success",
            records=flattened_records,
            next_cursor=next_cursor,
            checkpoint_cursor=_max_updated_at(flattened_records, "updatedAt"),
            child_records=child_records,
        )


def _is_newer_hubspot_record(record: Any, since: str) -> bool:
    if not isinstance(record, dict):
        return False
    record_value = record.get("updatedAt")
    if record_value is None:
        return True
    return _parse_datetime(record_value) >= _parse_datetime(since)


def _max_updated_at(rows: list[dict[str, Any]], field_name: str) -> str | None:
    values = [str(row.get(field_name) or "").strip() for row in rows if str(row.get(field_name) or "").strip()]
    if not values:
        return None
    return max(values)


def _parse_datetime(value: Any) -> datetime:
    normalized = str(value or "").strip().replace("Z", "+00:00")
    if not normalized:
        return datetime.min.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
