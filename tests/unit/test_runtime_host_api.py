from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient
from jose import jwt

from langbridge import LangbridgeClient
from langbridge.runtime import build_configured_local_runtime
from langbridge.runtime.hosting import create_runtime_api_app
from langbridge.runtime.hosting.auth import RuntimeAuthConfig, RuntimeAuthMode
from tests.unit._runtime_host_sync_helpers import (
    mock_stripe_api,
    runtime_storage_dirs,
    write_sync_runtime_config,
)


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
    semantic_model: commerce_performance
    dataset: shopify_orders
    default: true
""".strip(),
        encoding="utf-8",
    )

    runtime = build_configured_local_runtime(config_path=str(config_path))

    async def fake_agent_execute(*, job_id, request, event_emitter=None):
        return SimpleNamespace(
            response={
                "summary": f"{runtime._agents['commerce_analyst'].config.name} answered runtime prompt",
                "result": {"text": "ok"},
                "visualization": None,
                "error": None,
                "events": [],
            }
        )

    runtime._runtime_host.services.agent_execution.execute = fake_agent_execute  # type: ignore[assignment]
    return runtime


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
