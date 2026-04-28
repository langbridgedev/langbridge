import httpx
import pytest

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.saas.basic_http import (
    BasicHttpConnector,
    BasicHttpConnectorConfigFactory,
    BasicHttpConnectorConfigSchemaFactory,
)
from langbridge.runtime.utils.connector_runtime import resolve_supported_resources


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_basic_http_connector_schema_exposes_textarea_resource_config() -> None:
    schema = BasicHttpConnectorConfigSchemaFactory.create({})

    assert schema.name == "Basic HTTP"
    assert schema.plugin_metadata is not None
    assert schema.plugin_metadata.connector_type == ConnectorRuntimeType.BASIC_HTTP
    assert any(entry.field == "resources" and entry.type == "textarea" for entry in schema.config)
    assert any(entry.field == "static_headers" and entry.type == "textarea" for entry in schema.config)


def test_basic_http_connector_factory_parses_json_resource_config() -> None:
    config = BasicHttpConnectorConfigFactory.create(
        {
            "api_base_url": "https://api.test",
            "auth_type": "bearer",
            "auth_token": "secret-token",
            "resources": """
            [
              {
                "key": "customers",
                "path": "/customers",
                "response_items_field": "data",
                "request_params": {"status": "active"}
              }
            ]
            """,
            "static_headers": "{\"X-Test\": \"1\"}",
        }
    )

    assert config.api_base_url == "https://api.test"
    assert config.auth_type.value == "bearer"
    assert config.static_headers == {"X-Test": "1"}
    assert len(config.resources) == 1
    assert config.resources[0].request_params == {"status": "active"}


@pytest.mark.anyio
async def test_basic_http_connector_extracts_list_resource_with_bearer_auth() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer secret-token"
        assert request.headers["X-Test"] == "1"
        assert request.url.params["status"] == "active"
        assert request.url.params["limit"] == "2"
        return httpx.Response(
            200,
            json={
                "data": [
                    {"id": "cus_001", "name": "Ada"},
                    {"id": "cus_002", "name": "Grace"},
                ],
                "has_more": True,
            },
            request=request,
        )

    config = BasicHttpConnectorConfigFactory.create(
        {
            "api_base_url": "https://api.test",
            "auth_type": "bearer",
            "auth_token": "secret-token",
            "static_headers": {"X-Test": "1"},
            "resources": [
                {
                    "key": "customers",
                    "path": "/customers",
                    "response_items_field": "data",
                    "request_params": {"status": "active"},
                    "pagination_strategy": "cursor",
                    "limit_param": "limit",
                    "default_page_size": 2,
                    "max_page_size": 50,
                    "cursor_param": "cursor",
                    "response_has_more_field": "has_more",
                    "primary_key": "id",
                }
            ],
        }
    )

    connector = BasicHttpConnector(config=config, transport=httpx.MockTransport(handler))
    result = await connector.extract_resource("customers")

    assert len(requests) == 1
    assert [record["id"] for record in result.records] == ["cus_001", "cus_002"]
    assert result.next_cursor == "cus_002"


@pytest.mark.anyio
async def test_basic_http_connector_returns_single_record_when_payload_is_object() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"result": "ok", "service": "health"},
            request=request,
        )

    config = BasicHttpConnectorConfigFactory.create(
        {
            "api_base_url": "https://api.test",
            "resources": [
                {
                    "key": "health",
                    "path": "/health",
                }
            ],
        }
    )

    connector = BasicHttpConnector(config=config, transport=httpx.MockTransport(handler))
    result = await connector.extract_resource("health")

    assert result.records == [{"result": "ok", "service": "health"}]
    assert result.next_cursor is None


@pytest.mark.anyio
async def test_basic_http_connector_extracts_request_payload_without_named_resource() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/latest"
        assert request.url.params["base"] == "USD"
        return httpx.Response(
            200,
            json={
                "rates": {
                    "GBP": 0.8,
                    "EUR": 0.92,
                }
            },
            request=request,
        )

    config = BasicHttpConnectorConfigFactory.create(
        {
            "api_base_url": "https://api.test",
            "resources": [],
        }
    )

    connector = BasicHttpConnector(config=config, transport=httpx.MockTransport(handler))
    result = await connector.extract_request(
        {
            "method": "get",
            "path": "/latest",
            "params": {"base": "USD"},
        },
        extraction={"type": "json", "options": {"path": "rates"}},
    )

    assert result.resource == "/latest"
    assert result.records == [{"GBP": 0.8, "EUR": 0.92}]


def test_resolve_supported_resources_uses_dynamic_config_resources() -> None:
    config = BasicHttpConnectorConfigFactory.create(
        {
            "api_base_url": "https://api.test",
            "resources": [
                {"key": "customers", "path": "/customers"},
                {"key": "rates", "path": "/rates"},
            ],
        }
    )

    assert resolve_supported_resources(plugin=None, connector_config=config) == ["customers", "rates"]
