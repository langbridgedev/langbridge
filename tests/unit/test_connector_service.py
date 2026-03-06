from __future__ import annotations

import httpx
import pytest

from langbridge.apps.api.langbridge_api.services.connector_service import ConnectorService
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.connectors.langbridge_connectors.api.config import (
    ConnectorRuntimeType,
)
from langbridge.packages.connectors.langbridge_connectors.api.shopify.config import (
    ShopifyConnectorConfig,
)
from langbridge.packages.connectors.langbridge_connectors.api.shopify.connector import (
    ShopifyApiConnector,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _UnusedConnectorRepository:
    async def get_by_id(self, connector_id):
        return None

    async def get_all(self):
        return []

    async def delete(self, connector):
        return None

    def add(self, connector):
        return None


class _UnusedOrganizationRepository:
    async def get_by_id(self, organization_id):
        return None


class _UnusedProjectRepository:
    async def get_by_id(self, project_id):
        return None


def _service() -> ConnectorService:
    return ConnectorService(
        connector_repository=_UnusedConnectorRepository(),
        organization_repository=_UnusedOrganizationRepository(),
        project_repository=_UnusedProjectRepository(),
    )


def test_connector_service_lists_api_plugins_and_schema_metadata() -> None:
    service = _service()

    connector_types = service.list_connector_types()
    assert "SHOPIFY" in connector_types
    assert "STRIPE" in connector_types

    plugins = service.list_connector_plugins()
    shopify_plugin = next(
        item for item in plugins if item.connector_type == ConnectorRuntimeType.SHOPIFY.value
    )
    assert shopify_plugin.connector_family.value == "API"
    assert shopify_plugin.supported_resources == ["orders", "customers", "products"]
    assert [field.field for field in shopify_plugin.auth_schema] == ["shop_domain"]
    assert shopify_plugin.sync_strategy is not None

    schema = service.get_connector_config_schema("shopify")
    assert schema.plugin_metadata is not None
    assert schema.plugin_metadata.connector_type == ConnectorRuntimeType.SHOPIFY.value
    assert schema.plugin_metadata.connector_family.value == "API"
    assert schema.plugin_metadata.supported_resources == ["orders", "customers", "products"]


@pytest.mark.anyio
async def test_connector_service_can_instantiate_api_connector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "SHOPIFY_APP_CLIENT_ID", "client-id")
    monkeypatch.setattr(settings, "SHOPIFY_APP_CLIENT_SECRET", "client-secret")
    service = _service()
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/oauth/access_token"):
            return httpx.Response(200, json={"access_token": "oauth-token"})
        return httpx.Response(200, json={"shop": {"id": 1, "name": "Acme"}})

    service._api_connector_factory.create_api_connector = (
        lambda connector_type, config, logger: ShopifyApiConnector(
            ShopifyConnectorConfig.model_validate(config.model_dump()),
            logger=logger,
            transport=httpx.MockTransport(handler),
        )
    )

    connector = await service.async_create_api_connector(
        ConnectorRuntimeType.SHOPIFY,
        {
            "config": {
                "shop_domain": "acme.myshopify.com",
            }
        },
    )

    resources = await connector.discover_resources()
    assert connector.RUNTIME_TYPE == ConnectorRuntimeType.SHOPIFY
    assert [resource.name for resource in resources] == ["orders", "customers", "products"]
    assert len(requests) == 2
    assert requests[1].headers["X-Shopify-Access-Token"] == "oauth-token"
