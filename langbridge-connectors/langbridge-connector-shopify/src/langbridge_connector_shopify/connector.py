
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.saas.declarative import DeclarativeHttpApiConnector

from .config import SHOPIFY_MANIFEST, ShopifyDeclarativeConnectorConfig

SHOPIFY_API_VERSION = "2026-01"


class ShopifyDeclarativeApiConnector(DeclarativeHttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.SHOPIFY
    MANIFEST = SHOPIFY_MANIFEST
    config: ShopifyDeclarativeConnectorConfig

    def _base_url(self) -> str:
        if self.config.api_base_url:
            return self.config.api_base_url.rstrip("/")
        shop_domain = self.config.shop_domain.strip().rstrip("/")
        return f"https://{shop_domain}/admin/api/{SHOPIFY_API_VERSION}"
