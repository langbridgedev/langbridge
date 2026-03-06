from datetime import datetime, timezone
from typing import Any

from langbridge.packages.connectors.langbridge_connectors.api._http_api_connector import (
    ApiResourceDefinition,
    HttpApiConnector,
    flatten_api_records,
    parse_link_header_cursor,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import (
    ConnectorRuntimeType,
)
from langbridge.packages.connectors.langbridge_connectors.api.connector import (
    ApiExtractResult,
    ApiResource,
)

from langbridge.packages.common.langbridge_common.config import settings
from .config import SHOPIFY_SUPPORTED_RESOURCES, ShopifyConnectorConfig


class ShopifyApiConnector(HttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.SHOPIFY
    SUPPORTED_RESOURCES = SHOPIFY_SUPPORTED_RESOURCES
    API_VERSION = "2025-01"
    RESOURCE_DEFINITIONS = {
        "orders": ApiResourceDefinition(
            resource=ApiResource(
                name="orders",
                label="Orders",
                primary_key="id",
                cursor_field="page_info",
                incremental_cursor_field="updated_at",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/orders.json",
            response_key="orders",
        ),
        "customers": ApiResourceDefinition(
            resource=ApiResource(
                name="customers",
                label="Customers",
                primary_key="id",
                cursor_field="page_info",
                incremental_cursor_field="updated_at",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/customers.json",
            response_key="customers",
        ),
        "products": ApiResourceDefinition(
            resource=ApiResource(
                name="products",
                label="Products",
                primary_key="id",
                cursor_field="page_info",
                incremental_cursor_field="updated_at",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="/products.json",
            response_key="products",
        ),
    }
    OAUTH_TOKEN_URL = "https://{shop_domain}/admin/oauth/access_token"
    config: ShopifyConnectorConfig
    _access_token: str | None = None
    def __init__(self, config: ShopifyConnectorConfig, logger=None, **kwargs: Any) -> None:
        super().__init__(config=config, logger=logger, **kwargs)

    def _base_url(self) -> str:
        return f"https://{self.config.shop_domain.strip().rstrip('/')}/admin/api/{self.API_VERSION}"


    def _default_headers(self) -> dict[str, str]:
        if not self._access_token:
            return {}
        return {
            "X-Shopify-Access-Token": self._access_token,
        }
    
    async def _authenticate(self) -> None:
        client_id: str | None = settings.SHOPIFY_APP_CLIENT_ID
        client_secret: str | None = settings.SHOPIFY_APP_CLIENT_SECRET

        if not client_id or not client_secret:
            raise ValueError("Shopify app credentials are required for authentication")
        
        payload, response = await self._request_json(
            "POST",
            self.OAUTH_TOKEN_URL.format(shop_domain=self.config.shop_domain),
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
        )
        access_token = payload.get("access_token")
        if not access_token:
            raise ValueError("Failed to obtain access token from Shopify")
        self._access_token = access_token

    async def test_connection(self) -> None:
        await self._authenticate()
        await self._request_json("GET", "/shop.json")

    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiExtractResult:
        definition = self._require_resource(resource_name)
        page_size = self._clamp_limit(limit, default=50, maximum=250)
        params: dict[str, Any] = {"limit": page_size}
        if cursor:
            params["page_info"] = cursor
        else:
            if resource_name == "orders":
                params["status"] = "any"
            if since:
                params["updated_at_min"] = _normalize_shopify_since(since)
        await self._authenticate()
        payload, response = await self._request_json(
            "GET",
            definition.path,
            params=params,
        )
        records = payload.get(definition.response_key or definition.resource.name, [])
        if not isinstance(records, list):
            records = []

        flattened_records, child_records = flatten_api_records(
            resource_name=definition.resource.name,
            records=[record for record in records if isinstance(record, dict)],
            primary_key=definition.resource.primary_key,
        )
        return ApiExtractResult(
            resource=definition.resource.name,
            status="success",
            records=flattened_records,
            next_cursor=parse_link_header_cursor(response.headers.get("Link")),
            checkpoint_cursor=_max_string_cursor(flattened_records, "updated_at"),
            child_records=child_records,
        )


def _normalize_shopify_since(raw_value: str) -> str:
    normalized = str(raw_value or "").strip()
    if not normalized:
        return normalized
    if normalized.endswith("Z"):
        return normalized
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return normalized
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _max_string_cursor(rows: list[dict[str, Any]], field_name: str) -> str | None:
    values = [str(row.get(field_name) or "").strip() for row in rows if str(row.get(field_name) or "").strip()]
    if not values:
        return None
    return max(values)
