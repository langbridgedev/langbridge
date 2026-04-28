
import json
import ssl
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from langbridge.connectors.base import http as http_api_connector_module

CONNECTOR_SRC_DIRS = [
    Path(__file__).resolve().parents[3]
    / "langbridge-connectors"
    / package_name
    / "src"
    for package_name in (
        "langbridge-connector-stripe",
        "langbridge-connector-shopify",
        "langbridge-connector-hubspot",
        "langbridge-connector-google-analytics",
        "langbridge-connector-salesforce",
    )
]
for src_dir in CONNECTOR_SRC_DIRS:
    src_path = str(src_dir)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

from langbridge_connector_google_analytics.config import GoogleAnalyticsConnectorConfig
from langbridge_connector_google_analytics.connector import GoogleAnalyticsApiConnector
from langbridge_connector_hubspot.config import HubSpotConnectorConfig
from langbridge_connector_hubspot.connector import HubSpotApiConnector
from langbridge_connector_salesforce.config import SalesforceConnectorConfig
from langbridge_connector_salesforce.connector import SalesforceApiConnector
from langbridge_connector_shopify.config import ShopifyConnectorConfig
from langbridge_connector_shopify.connector import ShopifyApiConnector
from langbridge_connector_stripe.config import StripeDeclarativeConnectorConfig
from langbridge_connector_stripe.connector import StripeDeclarativeApiConnector


@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.mark.anyio
async def test_stripe_connector_handles_bearer_auth_and_has_more_pagination() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/account":
            assert request.headers["Authorization"] == "Bearer sk_test_123"
            assert request.headers["Stripe-Account"] == "acct_456"
            return httpx.Response(200, json={"id": "acct_456"})
        if request.url.path == "/v1/customers":
            assert request.url.params["limit"] == "2"
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "cus_001",
                            "email": "ada@example.com",
                            "address": {"city": "London"},
                            "subscriptions": {
                                "data": [
                                    {"id": "sub_001", "status": "active"},
                                ]
                            },
                        }
                    ],
                    "has_more": True,
                },
            )
        raise AssertionError(f"Unexpected Stripe request: {request.method} {request.url}")

    connector = StripeDeclarativeApiConnector(
        StripeDeclarativeConnectorConfig(
            api_key="sk_test_123",
            account_id="acct_456",
        ),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource("customers", limit=2)

    assert len(requests) == 2
    assert result.records[0]["address"]["city"] == "London"
    assert {child.path for child in result.child_resources or []} >= {
        "customers.address",
        "customers.subscriptions",
        "customers.subscriptions.data",
    }
    assert result.next_cursor == "cus_001"


@pytest.mark.anyio
async def test_hubspot_connector_handles_bearer_auth_and_after_cursor() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/crm/v3/objects/contacts":
            assert request.headers["Authorization"] == "Bearer hubspot-token"
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "id": "1",
                            "properties": {
                                "firstname": "Ada",
                                "lastname": "Lovelace",
                            },
                            "associations": {
                                "companies": {
                                    "results": [
                                        {"id": "2", "type": "contact_to_company"},
                                    ]
                                }
                            },
                        }
                    ],
                    "paging": {"next": {"after": "cursor-3"}},
                },
            )
        raise AssertionError(f"Unexpected HubSpot request: {request.method} {request.url}")

    connector = HubSpotApiConnector(
        HubSpotConnectorConfig(service_key="hubspot-token"),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource("contacts", limit=1)

    assert len(requests) == 2
    assert result.records[0]["properties"]["firstname"] == "Ada"
    assert {child.path for child in result.child_resources or []} >= {
        "contacts.properties",
        "contacts.associations",
        "contacts.associations.companies",
        "contacts.associations.companies.results",
    }
    assert result.next_cursor == "cursor-3"


@pytest.mark.anyio
async def test_hubspot_connector_accepts_legacy_access_token_config() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer legacy-hubspot-token"
        return httpx.Response(200, json={"results": []})

    connector = HubSpotApiConnector(
        HubSpotConnectorConfig(access_token="legacy-hubspot-token"),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()

    assert len(requests) == 1


@pytest.mark.anyio
async def test_shopify_connector_supports_legacy_app_credentials_flow() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/admin/oauth/access_token":
            payload = parse_qs(request.content.decode("utf-8"))
            assert payload["client_id"] == ["legacy-client-id"]
            assert payload["client_secret"] == ["legacy-client-secret"]
            assert payload["grant_type"] == ["client_credentials"]
            return httpx.Response(200, json={"access_token": "shpat_legacy"})
        if request.url.path == "/admin/api/2025-01/shop.json":
            assert request.headers["X-Shopify-Access-Token"] == "shpat_legacy"
            return httpx.Response(200, json={"shop": {"id": 1}})
        if request.url.path == "/admin/api/2025-01/orders.json":
            assert request.headers["X-Shopify-Access-Token"] == "shpat_legacy"
            assert request.url.params["limit"] == "2"
            assert request.url.params["status"] == "any"
            assert request.url.params["updated_at_min"] == "2025-01-01T00:00:00Z"
            return httpx.Response(
                200,
                json={
                    "orders": [
                        {
                            "id": 101,
                            "updated_at": "2025-01-02T00:00:00Z",
                            "customer": {"email": "ada@example.com"},
                        }
                    ]
                },
                headers={
                    "Link": '<https://acme.myshopify.com/admin/api/2025-01/orders.json?page_info=cursor-2&limit=2>; rel="next"'
                },
            )
        raise AssertionError(f"Unexpected Shopify request: {request.method} {request.url}")

    connector = ShopifyApiConnector(
        ShopifyConnectorConfig(
            shop_domain="acme.myshopify.com",
            shopify_app_client_id="legacy-client-id",
            shopify_app_client_secret="legacy-client-secret",
        ),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource(
        "orders",
        since="2025-01-01T00:00:00Z",
        limit=2,
    )

    assert len(requests) == 3
    assert result.records[0]["customer"]["email"] == "ada@example.com"
    assert result.next_cursor == "cursor-2"

@pytest.mark.anyio
async def test_google_analytics_connector_uses_service_account_jwt_and_run_report() -> None:
    token_requests = 0
    report_requests = 0
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_requests, report_requests
        if request.url == httpx.URL("https://oauth2.googleapis.com/token"):
            token_requests += 1
            payload = parse_qs(request.content.decode("utf-8"))
            assert payload["grant_type"] == ["urn:ietf:params:oauth:grant-type:jwt-bearer"]
            assert payload["assertion"][0].count(".") == 2
            return httpx.Response(200, json={"access_token": "ya29.test", "expires_in": 3600})
        if request.url.path == "/v1beta/properties/123456:runReport":
            report_requests += 1
            assert request.headers["Authorization"] == "Bearer ya29.test"
            body = json.loads(request.content.decode("utf-8"))
            assert body["limit"] == "1"
            return httpx.Response(
                200,
                json={
                    "dimensionHeaders": [
                        {"name": "date"},
                        {"name": "sessionDefaultChannelGroup"},
                    ],
                    "metricHeaders": [
                        {"name": "sessions", "type": "TYPE_INTEGER"},
                        {"name": "engagedSessions", "type": "TYPE_INTEGER"},
                        {"name": "totalUsers", "type": "TYPE_INTEGER"},
                    ],
                    "rows": [
                        {
                            "dimensionValues": [
                                {"value": "20250101"},
                                {"value": "Organic Search"},
                            ],
                            "metricValues": [
                                {"value": "12"},
                                {"value": "9"},
                                {"value": "7"},
                            ],
                        }
                    ],
                    "rowCount": 4,
                },
            )
        raise AssertionError(f"Unexpected Google Analytics request: {request.method} {request.url}")

    connector = GoogleAnalyticsApiConnector(
        GoogleAnalyticsConnectorConfig(
            property_id="123456",
            credentials_json={
                "client_email": "svc@example.iam.gserviceaccount.com",
                "private_key": private_key_pem,
                "token_uri": "https://oauth2.googleapis.com/token",
            },
        ),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource("sessions", limit=1)

    assert token_requests == 1
    assert report_requests == 2
    assert result.records[0]["sessions"] == 12
    assert result.next_cursor == "1"


@pytest.mark.anyio
async def test_salesforce_connector_uses_refresh_token_and_query_cursor() -> None:
    token_requests = 0
    query_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_requests, query_requests
        if request.url.path == "/services/oauth2/token":
            token_requests += 1
            payload = parse_qs(request.content.decode("utf-8"))
            assert payload["grant_type"] == ["refresh_token"]
            assert payload["client_id"] == ["client-id"]
            return httpx.Response(
                200,
                json={
                    "access_token": "sf-token",
                    "instance_url": "https://acme.my.salesforce.com",
                },
            )
        if request.url.path == "/services/data/v61.0/":
            assert request.headers["Authorization"] == "Bearer sf-token"
            return httpx.Response(200, json={"versions": []})
        if request.url.path == "/services/data/v61.0/query":
            query_requests += 1
            assert "FROM Account" in request.url.params["q"]
            return httpx.Response(
                200,
                json={
                    "records": [
                        {
                            "attributes": {"type": "Account"},
                            "Id": "001-test",
                            "Name": "Acme",
                            "Owner": {"attributes": {"type": "User"}, "Id": "005-test", "Name": "Ada"},
                        }
                    ],
                    "done": False,
                    "nextRecordsUrl": "/services/data/v61.0/query/01g-test",
                },
            )
        raise AssertionError(f"Unexpected Salesforce request: {request.method} {request.url}")

    connector = SalesforceApiConnector(
        SalesforceConnectorConfig(
            instance_url="https://acme.my.salesforce.com",
            client_id="client-id",
            client_secret="client-secret",
            refresh_token="refresh-token",
        ),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource("accounts", limit=100)

    assert token_requests == 1
    assert query_requests == 1
    assert result.records[0]["Owner"]["Name"] == "Ada"
    assert {child.path for child in result.child_resources or []} >= {"accounts.Owner"}
    assert result.next_cursor == "/services/data/v61.0/query/01g-test"
