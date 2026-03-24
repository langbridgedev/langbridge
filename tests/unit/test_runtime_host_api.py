from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient
from jose import jwt
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from langbridge import LangbridgeClient
from langbridge.runtime import build_configured_local_runtime
from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.hosting import create_runtime_api_app
from langbridge.runtime.hosting.auth import RuntimeAuthConfig, RuntimeAuthMode
from langbridge.runtime.models import RuntimeMessageRole, RuntimeThreadMessage
from tests.unit._runtime_host_sync_helpers import (
    mock_stripe_api,
    runtime_storage_dirs,
    write_sync_runtime_config,
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _build_runtime(tmp_path: Path):
    db_path = tmp_path / "example.db"
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.executescript(
        """
        CREATE TABLE orders_enriched (
            order_id TEXT PRIMARY KEY,
            order_date TEXT NOT NULL,
            country TEXT NOT NULL,
            net_revenue REAL NOT NULL,
            gross_margin REAL NOT NULL,
            acquisition_channel TEXT NOT NULL,
            loyalty_tier TEXT NOT NULL,
            order_status TEXT NOT NULL,
            customer_id INTEGER NOT NULL
        );
        INSERT INTO orders_enriched VALUES
            ('O-1', '2025-04-08', 'United Kingdom', 180.0, 72.0, 'Direct', 'Gold', 'fulfilled', 1001),
            ('O-2', '2025-05-14', 'United States', 210.0, 84.0, 'Paid Search', 'Silver', 'fulfilled', 1002),
            ('O-3', '2025-05-18', 'United Kingdom', 260.0, 101.0, 'Email', 'Gold', 'fulfilled', 1003);
        """
    )
    connection.commit()
    connection.close()

    config_path = tmp_path / "langbridge.yml"
    config_path.write_text(
        f"""
version: 1
connectors:
  - name: commerce_demo
    type: sqlite
    connection:
      path: {db_path.name}
datasets:
  - name: shopify_orders
    connector: commerce_demo
    semantic_model: commerce_performance
    default_time_dimension: order_date
    source:
      table: orders_enriched
semantic_models:
  - name: commerce_performance
    default: true
    model:
      version: "1"
      name: commerce_performance
      datasets:
        shopify_orders:
          relation_name: orders_enriched
          dimensions:
            - name: country
              expression: country
              type: string
            - name: order_date
              expression: order_date
              type: time
          measures:
            - name: net_sales
              expression: net_revenue
              type: number
              aggregation: sum
llm_connections:
  - name: local_openai
    provider: openai
    model: gpt-4o-mini
    api_key: test-key
    default: true
agents:
  - name: commerce_analyst
    llm_connection: local_openai
    default: true
    definition:
      prompt:
        system_prompt: You are a commerce analytics agent.
      memory:
        strategy: database
      features:
        bi_copilot_enabled: false
        deep_research_enabled: false
        visualization_enabled: true
        mcp_enabled: false
      tools:
        - name: commerce_semantic_sql
          tool_type: sql
          config:
            semantic_model_ids: [commerce_performance]
      access_policy:
        allowed_connectors: [commerce_demo]
        denied_connectors: []
      execution:
        mode: iterative
        response_mode: analyst
        max_iterations: 3
        max_steps_per_iteration: 5
        allow_parallel_tools: false
      output:
        format: markdown
      guardrails:
        moderation_enabled: true
      observability:
        log_level: info
        emit_traces: false
        capture_prompts: false
        audit_fields: []
""".strip(),
        encoding="utf-8",
    )

    runtime = build_configured_local_runtime(config_path=str(config_path))

    async def fake_agent_execute(*, job_id, request, event_emitter=None):
        response_payload = {
            "summary": f"{runtime._agents['commerce_analyst'].config.name} answered runtime prompt",
            "result": {"text": "ok"},
            "visualization": None,
            "error": None,
            "events": [],
        }
        thread = await runtime._thread_repository.get_by_id(request.thread_id)
        if thread is not None:
            assistant_message = RuntimeThreadMessage(
                id=uuid.uuid4(),
                thread_id=request.thread_id,
                parent_message_id=thread.last_message_id,
                role=RuntimeMessageRole.assistant,
                content={
                    "summary": response_payload["summary"],
                    "result": response_payload["result"],
                    "visualization": response_payload["visualization"],
                },
                created_at=datetime.now(timezone.utc),
            )
            runtime._thread_message_repository.add(assistant_message)
            thread.last_message_id = assistant_message.id
        return SimpleNamespace(
            response=response_payload
        )

    runtime._runtime_host.services.agent_execution.execute = fake_agent_execute  # type: ignore[assignment]
    return runtime


def _build_runtime_with_relational_semantic_models(tmp_path: Path):
    db_path = tmp_path / "semantic_runtime.db"
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.executescript(
        """
        CREATE TABLE orders_enriched (
            order_id TEXT PRIMARY KEY,
            order_date TEXT NOT NULL,
            country TEXT NOT NULL,
            net_revenue REAL NOT NULL,
            order_status TEXT NOT NULL,
            customer_id INTEGER NOT NULL
        );
        CREATE TABLE customer_profiles (
            customer_id INTEGER PRIMARY KEY,
            region TEXT NOT NULL,
            loyalty_tier TEXT NOT NULL
        );
        CREATE TABLE campaign_touchpoints (
            campaign_id TEXT PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            spend REAL NOT NULL
        );
        INSERT INTO orders_enriched VALUES
            ('O-1', '2025-04-08', 'United Kingdom', 180.0, 'fulfilled', 1001),
            ('O-2', '2025-05-14', 'United States', 210.0, 'fulfilled', 1002),
            ('O-3', '2025-05-18', 'United Kingdom', 260.0, 'pending', 1003);
        INSERT INTO customer_profiles VALUES
            (1001, 'Europe', 'Gold'),
            (1002, 'North America', 'Silver'),
            (1003, 'Europe', 'Gold');
        INSERT INTO campaign_touchpoints VALUES
            ('C-1', 1001, 'Email', 45.0),
            ('C-2', 1002, 'Paid Search', 80.0),
            ('C-3', 1003, 'Affiliate', 30.0);
        """
    )
    connection.commit()
    connection.close()

    config_path = tmp_path / "langbridge_semantic_runtime.yml"
    config_path.write_text(
        f"""
version: 1
connectors:
  - name: commerce_demo
    type: sqlite
    connection:
      path: {db_path.name}
datasets:
  - name: shopify_orders
    connector: commerce_demo
    semantic_model: commerce_performance
    default_time_dimension: order_date
    source:
      table: orders_enriched
  - name: shopify_customers
    connector: commerce_demo
    semantic_model: commerce_performance
    source:
      table: customer_profiles
  - name: campaign_touchpoints
    connector: commerce_demo
    semantic_model: marketing_performance
    source:
      table: campaign_touchpoints
semantic_models:
  - name: commerce_performance
    default: true
    model:
      version: "1"
      name: commerce_performance
      datasets:
        shopify_orders:
          relation_name: orders_enriched
          dimensions:
            - name: order_id
              expression: order_id
              type: string
              primary_key: true
            - name: order_date
              expression: order_date
              type: time
            - name: country
              expression: country
              type: string
            - name: order_status
              expression: order_status
              type: string
            - name: customer_id
              expression: customer_id
              type: integer
          measures:
            - name: net_sales
              expression: net_revenue
              type: number
              aggregation: sum
        shopify_customers:
          relation_name: customer_profiles
          dimensions:
            - name: customer_id
              expression: customer_id
              type: integer
              primary_key: true
            - name: region
              expression: region
              type: string
            - name: loyalty_tier
              expression: loyalty_tier
              type: string
      relationships:
        - name: orders_to_customers
          source_dataset: shopify_orders
          source_field: customer_id
          target_dataset: shopify_customers
          target_field: customer_id
          type: left
  - name: marketing_performance
    model:
      version: "1"
      name: marketing_performance
      datasets:
        campaign_touchpoints:
          relation_name: campaign_touchpoints
          dimensions:
            - name: campaign_id
              expression: campaign_id
              type: string
              primary_key: true
            - name: customer_id
              expression: customer_id
              type: integer
            - name: channel
              expression: channel
              type: string
          measures:
            - name: marketing_spend
              expression: spend
              type: number
              aggregation: sum
llm_connections:
  - name: local_openai
    provider: openai
    model: gpt-4o-mini
    api_key: test-key
    default: true
""".strip(),
        encoding="utf-8",
    )

    return build_configured_local_runtime(config_path=str(config_path))


def _raw_mcp_initialize_payload() -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": "initialize-1",
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {
                "name": "langbridge-test-client",
                "version": "1.0.0",
            },
        },
    }


def _extract_sse_payload(response_text: str) -> dict[str, object]:
    for line in response_text.splitlines():
        if line.startswith("data: "):
            return json.loads(line.removeprefix("data: "))
    raise AssertionError(f"No SSE payload found in response: {response_text}")


def test_runtime_host_api_exposes_runtime_features(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime)
    client = TestClient(app)

    info = client.get("/api/runtime/v1/info")
    assert info.status_code == 200
    assert info.json()["runtime_mode"] == "configured_local"
    assert info.json()["workspace_id"] == str(runtime.context.workspace_id)
    assert info.json()["actor_id"] == str(runtime.context.actor_id)
    assert info.json()["roles"] == list(runtime.context.roles)

    datasets = client.get("/api/runtime/v1/datasets")
    assert datasets.status_code == 200
    dataset_id = datasets.json()["items"][0]["id"]
    dataset_name = datasets.json()["items"][0]["name"]

    preview = client.post(f"/api/runtime/v1/datasets/{dataset_id}/preview", json={"limit": 2})
    assert preview.status_code == 200
    assert preview.json()["status"] == "succeeded"
    assert len(preview.json()["rows"]) == 2

    preview_by_name = client.post(f"/api/runtime/v1/datasets/{dataset_name}/preview", json={"limit": 1})
    assert preview_by_name.status_code == 200
    assert preview_by_name.json()["status"] == "succeeded"
    assert len(preview_by_name.json()["rows"]) == 1

    semantic = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_models": ["commerce_performance"],
            "measures": ["shopify_orders.net_sales"],
            "dimensions": ["shopify_orders.country"],
            "order": {"shopify_orders.net_sales": "desc"},
            "limit": 5,
        },
    )
    assert semantic.status_code == 200
    assert semantic.json()["status"] == "succeeded"
    assert semantic.json()["data"][0]["shopify_orders.country"] == "United Kingdom"

    sql = client.post(
        "/api/runtime/v1/sql/query",
        json={
            "query": (
                "SELECT country, SUM(net_revenue) AS net_sales "
                "FROM orders_enriched "
                "GROUP BY country "
                "ORDER BY net_sales DESC"
            ),
            "connection_name": "commerce_demo",
        },
    )
    assert sql.status_code == 200
    assert sql.json()["status"] == "succeeded"
    assert sql.json()["rows"][0]["country"] == "United Kingdom"

    federated_sql = client.post(
        "/api/runtime/v1/sql/query",
        json={
            "query": (
                "SELECT country, SUM(net_revenue) AS net_sales "
                "FROM shopify_orders "
                "GROUP BY country "
                "ORDER BY net_sales DESC"
            ),
        },
    )
    assert federated_sql.status_code == 200
    assert federated_sql.json()["status"] == "succeeded"
    assert federated_sql.json()["rows"][0]["country"] == "United Kingdom"

    agent = client.post(
        "/api/runtime/v1/agents/ask",
        json={
            "message": "Summarize revenue",
            "agent_name": "commerce_analyst",
        },
    )
    assert agent.status_code == 200
    assert agent.json()["status"] == "succeeded"
    assert "commerce_analyst" in agent.json()["summary"]


def test_runtime_host_api_executes_joined_semantic_query_with_runtime_response_shape(tmp_path: Path) -> None:
    runtime = _build_runtime_with_relational_semantic_models(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime)
    client = TestClient(app)

    semantic = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_model": "commerce_performance",
            "measures": ["net_sales"],
            "dimensions": ["region"],
            "filters": [
                {
                    "member": "order_status",
                    "operator": "equals",
                    "values": ["fulfilled"],
                }
            ],
            "order": {"net_sales": "desc"},
        },
    )

    assert semantic.status_code == 200
    payload = semantic.json()
    assert payload["status"] == "succeeded"
    assert payload["semantic_model_id"] is not None
    assert payload["semantic_model_ids"]
    assert payload["connector_id"] is None
    assert payload["data"] == [
        {
            "shopify_customers.region": "North America",
            "shopify_orders.net_sales": 210.0,
        },
        {
            "shopify_customers.region": "Europe",
            "shopify_orders.net_sales": 180.0,
        },
    ]
    assert "LEFT JOIN customer_profiles" in payload["generated_sql"]


def test_runtime_host_api_executes_federated_sql_join_across_runtime_datasets(tmp_path: Path) -> None:
    runtime = _build_runtime_with_relational_semantic_models(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime)
    client = TestClient(app)

    response = client.post(
        "/api/runtime/v1/sql/query",
        json={
            "query": (
                "SELECT c.region, SUM(o.net_revenue) AS net_sales "
                "FROM shopify_orders AS o "
                "JOIN shopify_customers AS c ON o.customer_id = c.customer_id "
                "GROUP BY c.region "
                "ORDER BY net_sales DESC"
            ),
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "succeeded"
    assert payload["rows"] == [
        {"net_sales": 440.0, "region": "Europe"},
        {"net_sales": 210.0, "region": "North America"},
    ]
    assert payload["generated_sql"] is not None


def test_runtime_host_api_serves_ui_when_feature_enabled(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime, features=["ui"])
    client = TestClient(app)

    shell = client.get("/")
    assert shell.status_code == 200
    assert "Langbridge Runtime UI" in shell.text

    connectors_route = client.get("/connectors")
    assert connectors_route.status_code == 200
    assert "Langbridge Runtime UI" in connectors_route.text

    chat_route = client.get("/chat")
    assert chat_route.status_code == 200
    assert "Langbridge Runtime UI" in chat_route.text

    summary = client.get("/api/runtime/ui/v1/summary")
    assert summary.status_code == 200
    payload = summary.json()
    assert payload["health"]["status"] == "ok"
    assert "ui" in payload["features"]
    assert payload["counts"]["datasets"] == 1
    assert payload["counts"]["connectors"] == 1
    assert payload["counts"]["semantic_models"] == 1
    assert payload["counts"]["agents"] == 1
    assert payload["datasets"][0]["name"] == "shopify_orders"
    assert payload["connectors"][0]["name"] == "commerce_demo"

    info = client.get("/api/runtime/v1/info")
    assert info.status_code == 200
    assert "ui" in info.json()["capabilities"]


@pytest.mark.anyio
async def test_runtime_host_api_serves_mcp_when_feature_enabled(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime, features=["mcp"])

    @asynccontextmanager
    async def httpx_client_factory(**kwargs):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
            **kwargs,
        ) as client:
            yield client

    async with app.router.lifespan_context(app):
        async with streamablehttp_client(
            "http://testserver/mcp/",
            httpx_client_factory=httpx_client_factory,
        ) as streams:
            async with ClientSession(*streams[:2]) as session:
                initialize_result = await session.initialize()
                assert initialize_result.serverInfo.name == "Langbridge Runtime MCP"
                assert initialize_result.capabilities.resources is None
                assert initialize_result.capabilities.prompts is None

                tools = await session.list_tools()
                tool_names = {tool.name for tool in tools.tools}
                assert "runtime_info" in tool_names
                assert "query_sql" in tool_names
                assert "ask_agent" in tool_names

                info_result = await session.call_tool("runtime_info", {})
                assert info_result.isError is False
                assert info_result.structuredContent["runtime_mode"] == "configured_local"
                assert info_result.structuredContent["mcp_endpoint"] == "/mcp"
                assert "mcp" in info_result.structuredContent["capabilities"]
                assert "ask_agent" in info_result.structuredContent["available_mcp_tools"]
                assert info_result.structuredContent["mcp_tool_status"]["ask_agent"]["available"] is True
                assert info_result.structuredContent["resource_summary"]["agents"] == 1

                datasets_result = await session.call_tool("list_datasets", {})
                assert datasets_result.isError is False
                assert datasets_result.structuredContent["total"] == 1
                assert datasets_result.structuredContent["items"][0]["name"] == "shopify_orders"

    info = TestClient(app).get("/api/runtime/v1/info")
    assert info.status_code == 200
    assert "mcp" in info.json()["capabilities"]


@pytest.mark.anyio
async def test_runtime_host_api_requires_auth_for_mcp_requests(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(
        runtime_host=runtime,
        features=["mcp"],
        auth_config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.static_token,
            static_token="runtime-token",
        ),
    )

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.post(
                "/mcp/",
                json=_raw_mcp_initialize_payload(),
                headers={
                    "accept": "application/json, text/event-stream",
                    "content-type": "application/json",
                },
            )
            assert response.status_code == 401

            authorized_response = await client.post(
                "/mcp/",
                json=_raw_mcp_initialize_payload(),
                headers={
                    "accept": "application/json, text/event-stream",
                    "content-type": "application/json",
                    "authorization": "Bearer runtime-token",
                },
            )
            assert authorized_response.status_code == 200


@pytest.mark.anyio
async def test_runtime_host_api_logs_mcp_debug_details_for_bad_requests(tmp_path: Path, caplog) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime, features=["mcp"], debug=True)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        with caplog.at_level("DEBUG", logger="langbridge.runtime.mcp"):
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                bad_response = await client.post(
                    "/mcp",
                    json={"jsonrpc": "2.0"},
                    headers={
                        "accept": "application/json, text/event-stream",
                        "content-type": "application/json",
                    },
                )
                assert bad_response.status_code == 400

    messages = [record.getMessage() for record in caplog.records if record.name == "langbridge.runtime.mcp"]
    assert any("Normalized MCP request path from /mcp to /mcp/" in message for message in messages)
    assert any("MCP HTTP POST /mcp/" in message for message in messages)
    assert any("response_body=" in message for message in messages)


@pytest.mark.anyio
async def test_runtime_host_api_accepts_mcp_requests_without_trailing_slash(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime, features=["mcp"])

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            headers = {
                "accept": "application/json, text/event-stream",
                "content-type": "application/json",
            }

            initialize = await client.post("/mcp", json=_raw_mcp_initialize_payload(), headers=headers)
            assert initialize.status_code == 200
            session_id = initialize.headers.get("mcp-session-id")
            assert session_id
            initialize_payload = _extract_sse_payload(initialize.text)
            assert initialize_payload["result"]["serverInfo"]["name"] == "Langbridge Runtime MCP"

            initialized = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                headers={**headers, "mcp-session-id": session_id},
            )
            assert initialized.status_code == 202

            tools = await client.post(
                "/mcp",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
                headers={**headers, "mcp-session-id": session_id},
            )
            assert tools.status_code == 200
            tools_payload = _extract_sse_payload(tools.text)
            tool_names = {item["name"] for item in tools_payload["result"]["tools"]}
            assert "runtime_info" in tool_names
            assert "query_sql" in tool_names


@pytest.mark.anyio
async def test_runtime_host_api_omits_unavailable_mcp_tools_from_runtime_info(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    runtime._agents = {}
    runtime._default_agent = None
    runtime._runtime_host.services.agent_execution = None  # type: ignore[assignment]
    app = create_runtime_api_app(runtime_host=runtime, features=["mcp"])

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            headers = {
                "accept": "application/json, text/event-stream",
                "content-type": "application/json",
            }

            initialize = await client.post("/mcp/", json=_raw_mcp_initialize_payload(), headers=headers)
            assert initialize.status_code == 200
            session_id = initialize.headers["mcp-session-id"]

            initialized = await client.post(
                "/mcp/",
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
                headers={**headers, "mcp-session-id": session_id},
            )
            assert initialized.status_code == 202

            tools = await client.post(
                "/mcp/",
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
                headers={**headers, "mcp-session-id": session_id},
            )
            assert tools.status_code == 200
            tools_payload = _extract_sse_payload(tools.text)
            tool_names = {tool["name"] for tool in tools_payload["result"]["tools"]}
            assert "runtime_info" in tool_names
            assert "ask_agent" not in tool_names

            runtime_info = await client.post(
                "/mcp/",
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "runtime_info", "arguments": {}},
                },
                headers={**headers, "mcp-session-id": session_id},
            )
            assert runtime_info.status_code == 200
            runtime_info_payload = _extract_sse_payload(runtime_info.text)
            tool_result = runtime_info_payload["result"]
            assert "ask_agent" not in tool_result["structuredContent"]["available_mcp_tools"]
            assert tool_result["structuredContent"]["mcp_tool_status"]["ask_agent"] == {
                "available": False,
                "reason": "No agents are configured for this runtime.",
            }
            assert "agents.ask" not in tool_result["structuredContent"]["capabilities"]


def test_runtime_host_api_does_not_serve_ui_by_default(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime)
    client = TestClient(app)

    shell = client.get("/")
    assert shell.status_code == 404

    summary = client.get("/api/runtime/ui/v1/summary")
    assert summary.status_code == 404


def test_remote_sdk_can_use_runtime_host_api(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime)

    with TestClient(app) as http_client:
        client = LangbridgeClient.remote(
            base_url=str(http_client.base_url),
            http_client=http_client,
        )
        try:
            datasets = client.datasets.list()
            assert datasets.total == 1

            semantic = client.semantic.query(
                "commerce_performance",
                measures=["shopify_orders.net_sales"],
                dimensions=["shopify_orders.country"],
                order={"shopify_orders.net_sales": "desc"},
                limit=5,
            )
            assert semantic.status == "succeeded"
            assert semantic.rows[0]["shopify_orders.country"] == "United Kingdom"

            sql = client.sql.query(
                query=(
                    "SELECT country, SUM(net_revenue) AS net_sales "
                    "FROM orders_enriched "
                    "GROUP BY country "
                    "ORDER BY net_sales DESC"
                ),
                connection_name="commerce_demo",
            )
            assert sql.status == "succeeded"
            assert sql.rows[0]["country"] == "United Kingdom"

            agent = client.agents.ask(
                message="Summarize revenue",
                agent_name="commerce_analyst",
            )
            assert agent.status == "succeeded"
        finally:
            client.close()


def test_runtime_host_api_supports_connector_sync(tmp_path: Path) -> None:
    with mock_stripe_api() as api_base_url, runtime_storage_dirs(tmp_path):
        config_path = write_sync_runtime_config(tmp_path, api_base_url=api_base_url)
        runtime = build_configured_local_runtime(config_path=str(config_path))
        app = create_runtime_api_app(runtime_host=runtime)
        client = TestClient(app)

        info = client.get("/api/runtime/v1/info")
        assert info.status_code == 200
        assert "connectors.list" in info.json()["capabilities"]
        assert "sync.run" in info.json()["capabilities"]

        connectors = client.get("/api/runtime/v1/connectors")
        assert connectors.status_code == 200
        connector_payload = connectors.json()
        assert connector_payload["total"] == 1
        assert connector_payload["items"][0]["name"] == "billing_demo"
        assert connector_payload["items"][0]["supports_sync"] is True

        resources = client.get("/api/runtime/v1/connectors/billing_demo/sync/resources")
        assert resources.status_code == 200
        customers = next(item for item in resources.json()["items"] if item["name"] == "customers")
        assert customers["status"] == "never_synced"
        assert customers["dataset_names"] == []

        sync = client.post(
            "/api/runtime/v1/connectors/billing_demo/sync",
            json={
                "resource_names": ["customers"],
                "sync_mode": "INCREMENTAL",
            },
        )
        assert sync.status_code == 200
        sync_payload = sync.json()
        assert sync_payload["status"] == "succeeded"
        assert sync_payload["resources"][0]["resource_name"] == "customers"
        assert sync_payload["resources"][0]["records_synced"] == 2

        datasets = client.get("/api/runtime/v1/datasets")
        assert datasets.status_code == 200
        assert datasets.json()["total"] == 1
        synced_dataset_name = datasets.json()["items"][0]["name"]

        preview = client.post(
            f"/api/runtime/v1/datasets/{synced_dataset_name}/preview",
            json={"limit": 5},
        )
        assert preview.status_code == 200
        assert preview.json()["status"] == "succeeded"
        assert preview.json()["row_count_preview"] == 2
        assert preview.json()["rows"][0]["id"] == "cus_001"

        states = client.get("/api/runtime/v1/connectors/billing_demo/sync/states")
        assert states.status_code == 200
        assert states.json()["total"] == 1
        assert states.json()["items"][0]["resource_name"] == "customers"
        assert states.json()["items"][0]["status"] == "succeeded"
        assert states.json()["items"][0]["dataset_names"] == [synced_dataset_name]


def test_runtime_host_api_static_token_auth_scopes_runtime_requests(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    authenticated_workspace_id = runtime.context.workspace_id
    authenticated_actor_id = uuid.uuid4()
    captured: dict[str, object] = {}

    async def fake_query_dataset(*, request):
        captured["workspace_id"] = request.workspace_id
        captured["actor_id"] = request.actor_id
        captured["request_id"] = request.correlation_id
        return {
            "dataset_name": "shopify_orders",
            "columns": [{"name": "country"}],
            "rows": [{"country": "United Kingdom"}],
            "row_count_preview": 1,
            "effective_limit": request.enforced_limit,
            "redaction_applied": False,
        }

    runtime._runtime_host.services.dataset_query.query_dataset = fake_query_dataset  # type: ignore[assignment]
    app = create_runtime_api_app(
        runtime_host=runtime,
        auth_config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.static_token,
            static_token="runtime-token",
            static_workspace_id=authenticated_workspace_id,
            static_actor_id=authenticated_actor_id,
            static_roles=("runtime:viewer",),
        ),
    )
    client = TestClient(app)
    headers = {
        "Authorization": "Bearer runtime-token",
        "X-Request-Id": "req-static-auth",
    }

    info = client.get("/api/runtime/v1/info", headers=headers)
    assert info.status_code == 200
    assert info.json()["workspace_id"] == str(authenticated_workspace_id)
    assert info.json()["actor_id"] == str(authenticated_actor_id)
    assert info.json()["roles"] == ["runtime:viewer"]

    datasets = client.get("/api/runtime/v1/datasets", headers=headers)
    dataset_id = datasets.json()["items"][0]["id"]

    preview = client.post(
        f"/api/runtime/v1/datasets/{dataset_id}/preview",
        headers=headers,
        json={"limit": 1},
    )
    assert preview.status_code == 200
    assert preview.json()["status"] == "succeeded"
    assert captured == {
        "workspace_id": authenticated_workspace_id,
        "actor_id": authenticated_actor_id,
        "request_id": "req-static-auth",
    }


def test_runtime_host_api_local_auth_bootstrap_login_and_logout(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    auth_store_path = tmp_path / ".langbridge" / "auth.json"
    app = create_runtime_api_app(
        runtime_host=runtime,
        auth_config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.local,
            local_store_path=str(auth_store_path),
            local_session_secret="runtime-local-auth-secret",
        ),
    )
    client = TestClient(app)

    bootstrap_status = client.get("/api/runtime/v1/auth/bootstrap")
    assert bootstrap_status.status_code == 200
    assert bootstrap_status.json() == {
        "auth_enabled": True,
        "auth_mode": "local",
        "bootstrap_required": True,
        "has_admin": False,
        "login_allowed": True,
        "session_cookie_name": "langbridge_runtime_session",
    }

    unauthorized_info = client.get("/api/runtime/v1/info")
    assert unauthorized_info.status_code == 401

    bootstrap = client.post(
        "/api/runtime/v1/auth/bootstrap",
        json={
            "username": "runtime-admin",
            "email": "admin@example.com",
            "password": "Password123!",
        },
    )
    assert bootstrap.status_code == 200
    assert bootstrap.json()["user"]["username"] == "runtime-admin"
    assert bootstrap.json()["user"]["email"] == "admin@example.com"

    me = client.get("/api/runtime/v1/auth/me")
    assert me.status_code == 200
    assert me.json()["auth_mode"] == "local"
    assert me.json()["user"]["username"] == "runtime-admin"
    assert me.json()["user"]["roles"] == ["runtime:admin"]

    authenticated_info = client.get("/api/runtime/v1/info")
    assert authenticated_info.status_code == 200
    assert "auth.bootstrap" in authenticated_info.json()["capabilities"]

    bootstrap_status_after = client.get("/api/runtime/v1/auth/bootstrap")
    assert bootstrap_status_after.status_code == 200
    assert bootstrap_status_after.json()["bootstrap_required"] is False
    assert bootstrap_status_after.json()["has_admin"] is True

    second_bootstrap = client.post(
        "/api/runtime/v1/auth/bootstrap",
        json={
            "username": "ignored",
            "email": "ignored@example.com",
            "password": "Password123!",
        },
    )
    assert second_bootstrap.status_code == 409

    logout = client.post("/api/runtime/v1/auth/logout")
    assert logout.status_code == 200

    client.cookies.clear()
    unauthorized_me = client.get("/api/runtime/v1/auth/me")
    assert unauthorized_me.status_code == 401

    login = client.post(
        "/api/runtime/v1/auth/login",
        json={
            "identifier": "runtime-admin",
            "password": "Password123!",
        },
    )
    assert login.status_code == 200
    assert login.json()["user"]["username"] == "runtime-admin"

    authenticated_again = client.get("/api/runtime/v1/info")
    assert authenticated_again.status_code == 200


def test_runtime_host_api_exposes_semantic_models_agents_and_threads(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime)
    client = TestClient(app)

    semantic_models = client.get("/api/runtime/v1/semantic-models")
    assert semantic_models.status_code == 200
    assert semantic_models.json()["total"] == 1
    semantic_model_id = semantic_models.json()["items"][0]["id"]

    semantic_model = client.get(f"/api/runtime/v1/semantic-models/{semantic_model_id}")
    assert semantic_model.status_code == 200
    semantic_payload = semantic_model.json()
    assert semantic_payload["name"] == "commerce_performance"
    assert "content_yaml" in semantic_payload
    assert semantic_payload["dataset_count"] == 1

    agents = client.get("/api/runtime/v1/agents")
    assert agents.status_code == 200
    assert agents.json()["total"] == 1
    agent_id = agents.json()["items"][0]["id"]

    agent = client.get(f"/api/runtime/v1/agents/{agent_id}")
    assert agent.status_code == 200
    assert agent.json()["name"] == "commerce_analyst"
    assert agent.json()["tool_count"] == 1

    ask = client.post(
        "/api/runtime/v1/agents/ask",
        json={
            "message": "Summarize revenue",
            "agent_name": "commerce_analyst",
        },
    )
    assert ask.status_code == 200
    thread_id = ask.json()["thread_id"]

    threads = client.get("/api/runtime/v1/threads")
    assert threads.status_code == 200
    assert threads.json()["total"] == 1
    assert threads.json()["items"][0]["id"] == thread_id

    thread = client.get(f"/api/runtime/v1/threads/{thread_id}")
    assert thread.status_code == 200
    assert thread.json()["id"] == thread_id
    assert thread.json()["title"] == "commerce_analyst"

    messages = client.get(f"/api/runtime/v1/threads/{thread_id}/messages")
    assert messages.status_code == 200
    assert messages.json()["total"] == 2
    assert messages.json()["items"][0]["role"] == "user"
    assert messages.json()["items"][1]["role"] == "assistant"

    follow_up = client.post(
        "/api/runtime/v1/agents/ask",
        json={
            "message": "Break that down by country",
            "agent_name": "commerce_analyst",
            "thread_id": thread_id,
        },
    )
    assert follow_up.status_code == 200
    assert follow_up.json()["thread_id"] == thread_id

    updated_messages = client.get(f"/api/runtime/v1/threads/{thread_id}/messages")
    assert updated_messages.status_code == 200
    assert updated_messages.json()["total"] == 4


def test_runtime_host_api_supports_thread_crud(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    app = create_runtime_api_app(runtime_host=runtime)
    client = TestClient(app)

    create_response = client.post(
        "/api/runtime/v1/threads",
        json={"title": "Investigate connector drift"},
    )
    assert create_response.status_code == 200
    thread_id = create_response.json()["id"]
    assert create_response.json()["title"] == "Investigate connector drift"
    assert create_response.json()["state"] == "awaiting_user_input"

    threads = client.get("/api/runtime/v1/threads")
    assert threads.status_code == 200
    assert threads.json()["total"] == 1
    assert threads.json()["items"][0]["id"] == thread_id

    update_response = client.patch(
        f"/api/runtime/v1/threads/{thread_id}",
        json={"title": "Investigate connector freshness"},
    )
    assert update_response.status_code == 200
    assert update_response.json()["id"] == thread_id
    assert update_response.json()["title"] == "Investigate connector freshness"

    delete_response = client.delete(f"/api/runtime/v1/threads/{thread_id}")
    assert delete_response.status_code == 200
    assert delete_response.json() == {
        "status": "deleted",
        "thread_id": thread_id,
    }

    missing_thread = client.get(f"/api/runtime/v1/threads/{thread_id}")
    assert missing_thread.status_code == 404

    remaining_threads = client.get("/api/runtime/v1/threads")
    assert remaining_threads.status_code == 200
    assert remaining_threads.json()["total"] == 0


@pytest.mark.anyio
async def test_configured_runtime_threads_use_fallback_actor_when_context_actor_is_missing(
    tmp_path: Path,
) -> None:
    base_runtime = _build_runtime(tmp_path)
    runtime = base_runtime.with_context(
        RuntimeContext.build(
            workspace_id=base_runtime.context.workspace_id,
            actor_id=None,
        )
    )

    created = await runtime.create_thread(title="Fallback actor thread")
    threads = await runtime.list_threads()

    assert len(threads) == 1
    assert threads[0]["id"] == created["id"]
    assert threads[0]["title"] == "Fallback actor thread"


def test_runtime_host_api_jwt_auth_exposes_authenticated_identity(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    token = jwt.encode(
        {
            "workspace_id": str(workspace_id),
            "actor_id": str(actor_id),
            "roles": ["runtime:editor", "sql:query"],
            "sub": str(actor_id),
        },
        "runtime-jwt-secret",
        algorithm="HS256",
    )
    app = create_runtime_api_app(
        runtime_host=runtime,
        auth_config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.jwt,
            jwt_secret="runtime-jwt-secret",
        ),
    )
    client = TestClient(app)

    info = client.get(
        "/api/runtime/v1/info",
        headers={
            "Authorization": f"Bearer {token}",
            "X-Request-Id": "req-jwt-auth",
        },
    )
    assert info.status_code == 200
    assert info.json()["workspace_id"] == str(workspace_id)
    assert info.json()["actor_id"] == str(actor_id)
    assert info.json()["roles"] == ["runtime:editor", "sql:query"]
