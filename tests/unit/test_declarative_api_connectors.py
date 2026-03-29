
import sys
from pathlib import Path

import httpx
import pytest

CONNECTOR_SRC_DIRS = [
    Path(__file__).resolve().parents[2]
    / "langbridge-connectors"
    / package_name
    / "src"
    for package_name in (
        "langbridge-connector-shopify",
        "langbridge-connector-hubspot",
        "langbridge-connector-github",
        "langbridge-connector-jira",
        "langbridge-connector-asana",
    )
]
for src_dir in CONNECTOR_SRC_DIRS:
    src_path = str(src_dir)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

from langbridge_connector_asana.config import AsanaDeclarativeConnectorConfig
from langbridge_connector_asana.connector import AsanaDeclarativeApiConnector
from langbridge_connector_github.config import GitHubDeclarativeConnectorConfig
from langbridge_connector_github.connector import GitHubDeclarativeApiConnector
from langbridge_connector_hubspot.config import HubSpotDeclarativeConnectorConfig
from langbridge_connector_hubspot.connector import HubSpotDeclarativeApiConnector
from langbridge_connector_jira.config import JiraDeclarativeConnectorConfig
from langbridge_connector_jira.connector import JiraDeclarativeApiConnector
from langbridge_connector_shopify.config import ShopifyDeclarativeConnectorConfig
from langbridge_connector_shopify.connector import ShopifyDeclarativeApiConnector


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_declarative_shopify_connector_uses_shop_domain_and_link_pagination() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/admin/api/2026-01/shop.json":
            assert request.url.host == "acme.myshopify.com"
            assert request.headers["X-Shopify-Access-Token"] == "shpat_test"
            return httpx.Response(200, json={"shop": {"id": 1}})
        if request.url.path == "/admin/api/2026-01/customers.json":
            assert request.url.params["limit"] == "2"
            assert request.url.params["updated_at_min"] == "2025-01-01T00:00:00Z"
            return httpx.Response(
                200,
                json={
                    "customers": [
                        {
                            "id": 101,
                            "email": "ada@example.com",
                            "default_address": {"city": "London"},
                        }
                    ]
                },
                headers={
                    "Link": '<https://acme.myshopify.com/admin/api/2026-01/customers.json?page_info=cursor-2&limit=2>; rel="next"'
                },
            )
        raise AssertionError(f"Unexpected Shopify request: {request.method} {request.url}")

    connector = ShopifyDeclarativeApiConnector(
        ShopifyDeclarativeConnectorConfig(
            shop_domain="acme.myshopify.com",
            access_token="shpat_test",
        ),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource(
        "customers",
        since="2025-01-01T00:00:00Z",
        limit=2,
    )

    assert len(requests) == 2
    assert result.records[0]["default_address__city"] == "London"
    assert result.next_cursor == "cursor-2"


@pytest.mark.anyio
async def test_declarative_hubspot_connector_uses_response_cursor_and_client_filtering() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer hubspot-token"
        assert request.url.path == "/crm/v3/objects/contacts"
        if len(requests) == 1:
            return httpx.Response(200, json={"results": []})
        assert request.url.params["limit"] == "2"
        assert request.url.params["archived"] == "false"
        assert "updatedAfter" not in request.url.params
        return httpx.Response(
            200,
            json={
                "results": [
                    {"id": "1", "updatedAt": "2025-01-01T00:00:00Z", "properties": {"firstname": "Old"}},
                    {"id": "2", "updatedAt": "2025-01-03T00:00:00Z", "properties": {"firstname": "Ada"}},
                ],
                "paging": {"next": {"after": "cursor-2"}},
            },
        )

    connector = HubSpotDeclarativeApiConnector(
        HubSpotDeclarativeConnectorConfig(access_token="hubspot-token"),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource(
        "contacts",
        since="2025-01-02T00:00:00Z",
        limit=2,
    )

    assert len(requests) == 2
    assert [record["id"] for record in result.records] == ["2"]
    assert result.records[0]["properties__firstname"] == "Ada"
    assert result.next_cursor == "cursor-2"


@pytest.mark.anyio
async def test_declarative_github_connector_handles_top_level_lists_and_link_headers() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer ghp_test"
        assert request.headers["Accept"] == "application/vnd.github+json"
        assert request.headers["X-GitHub-Api-Version"] == "2022-11-28"
        if request.url.path == "/user":
            return httpx.Response(200, json={"login": "ada"})
        if request.url.path == "/issues":
            assert request.url.params["per_page"] == "2"
            assert request.url.params["since"] == "2025-01-01T00:00:00Z"
            assert request.url.params["state"] == "all"
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 9001,
                        "updated_at": "2025-01-02T00:00:00Z",
                        "repository": {"full_name": "acme/platform"},
                    }
                ],
                headers={
                    "Link": '<https://api.github.com/issues?page=2&per_page=2>; rel="next"'
                },
            )
        raise AssertionError(f"Unexpected GitHub request: {request.method} {request.url}")

    connector = GitHubDeclarativeApiConnector(
        GitHubDeclarativeConnectorConfig(access_token="ghp_test"),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource(
        "issues",
        since="2025-01-01T00:00:00Z",
        limit=2,
    )

    assert len(requests) == 2
    assert result.records[0]["repository__full_name"] == "acme/platform"
    assert result.next_cursor == "2"


@pytest.mark.anyio
async def test_declarative_jira_connector_uses_cloud_id_base_url_and_offset_pagination() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer jira-token"
        if request.url.path == "/ex/jira/cloud-123/rest/api/3/myself":
            return httpx.Response(200, json={"accountId": "user-1"})
        if request.url.path == "/ex/jira/cloud-123/rest/api/3/project/search":
            assert request.url.params["maxResults"] == "1"
            return httpx.Response(
                200,
                json={
                    "values": [
                        {"id": "10000", "key": "OPS", "lead": {"displayName": "Ada"}},
                    ],
                    "isLast": False,
                },
            )
        raise AssertionError(f"Unexpected Jira request: {request.method} {request.url}")

    connector = JiraDeclarativeApiConnector(
        JiraDeclarativeConnectorConfig(
            cloud_id="cloud-123",
            access_token="jira-token",
        ),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource("projects", limit=1)

    assert len(requests) == 2
    assert result.records[0]["lead__displayName"] == "Ada"
    assert result.next_cursor == "1"


@pytest.mark.anyio
async def test_declarative_asana_connector_uses_workspace_base_url_and_response_offsets() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer asana-token"
        if request.url.path == "/api/1.0/workspaces/1200/users":
            if len(requests) == 1:
                return httpx.Response(200, json={"data": []})
            assert request.url.params["limit"] == "2"
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"gid": "42", "name": "Ada", "email": "ada@example.com"},
                    ],
                    "next_page": {"offset": "offset-2"},
                },
            )
        raise AssertionError(f"Unexpected Asana request: {request.method} {request.url}")

    connector = AsanaDeclarativeApiConnector(
        AsanaDeclarativeConnectorConfig(
            workspace_gid="1200",
            access_token="asana-token",
        ),
        transport=httpx.MockTransport(handler),
    )

    await connector.test_connection()
    result = await connector.extract_resource("users", limit=2)

    assert len(requests) == 2
    assert result.records[0]["email"] == "ada@example.com"
    assert result.next_cursor == "offset-2"
