import re
from typing import Any

from langbridge.connectors.base import ApiResource, ApiResourceDefinition
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.errors import AuthError, ConnectorError
from langbridge.connectors.saas.declarative import DeclarativeHttpApiConnector

from .config import SHOPIFY_MANIFEST, ShopifyConnectorConfig

SHOPIFY_API_VERSION = "2026-01"
_OAUTH_TOKEN_URL = "https://{shop_domain}/admin/oauth/access_token"
_RESOURCE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class ShopifyApiConnector(DeclarativeHttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.SHOPIFY
    MANIFEST = SHOPIFY_MANIFEST
    config: ShopifyConnectorConfig

    def __init__(self, config: ShopifyConnectorConfig, logger=None, **kwargs: Any) -> None:
        super().__init__(config=config, logger=logger, **kwargs)
        self._access_token: str | None = None

    def _base_url(self) -> str:
        if self.config.api_base_url:
            return self.config.api_base_url.rstrip("/")
        shop_domain = self.config.shop_domain.strip().rstrip("/")
        return f"https://{shop_domain}/admin/api/{SHOPIFY_API_VERSION}"

    def _default_headers(self) -> dict[str, str]:
        access_token = self._access_token or str(self.config.access_token or "").strip()
        if not access_token:
            return {}
        return {"X-Shopify-Access-Token": access_token}

    def resolve_resource(self, resource_name: str) -> ApiResource:
        return self._require_resource(resource_name).resource

    async def test_connection(self) -> None:
        await self._ensure_access_token()
        await super().test_connection()

    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ):
        await self._ensure_access_token()
        return await super().extract_resource(
            resource_name,
            since=since,
            cursor=cursor,
            limit=limit,
        )

    async def _ensure_access_token(self) -> str:
        if self._access_token:
            return self._access_token

        direct_token = str(self.config.access_token or "").strip()
        if direct_token:
            self._access_token = direct_token
            return self._access_token

        client_id = str(self.config.shopify_app_client_id or "").strip()
        client_secret = str(self.config.shopify_app_client_secret or "").strip()
        if not client_id or not client_secret:
            raise AuthError(
                "Shopify connector requires either access_token or app client credentials."
            )

        payload, _ = await self._request_json(
            "POST",
            _OAUTH_TOKEN_URL.format(shop_domain=self.config.shop_domain),
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
        )
        access_token = str(payload.get("access_token") or "").strip() if isinstance(payload, dict) else ""
        if not access_token:
            raise AuthError("Failed to obtain access token from Shopify.")
        self._access_token = access_token
        return self._access_token

    def _require_resource(self, resource_name: str) -> ApiResourceDefinition:
        normalized_name = str(resource_name or "").strip()
        definition = self._resource_definitions.get(normalized_name)
        if definition is not None:
            return definition
        if not normalized_name or _RESOURCE_NAME_RE.fullmatch(normalized_name) is None:
            raise ConnectorError(f"Unsupported Shopify resource '{resource_name}'.")

        definition = ApiResourceDefinition(
            resource=ApiResource(
                name=normalized_name,
                label=normalized_name.replace("_", " ").title(),
                path=normalized_name,
                primary_key="id",
                cursor_field=SHOPIFY_MANIFEST.pagination.cursor_param,
                incremental_cursor_field=SHOPIFY_MANIFEST.incremental.cursor_field,
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path=f"/{normalized_name}.json",
            response_key=normalized_name,
            request_params={"status": "any"} if normalized_name == "orders" else None,
        )
        self._resource_definitions[normalized_name] = definition
        return definition


ShopifyDeclarativeApiConnector = ShopifyApiConnector
