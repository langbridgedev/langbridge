
import sqlite3
import uuid
from pathlib import Path

from fastapi.testclient import TestClient

from langbridge.runtime import build_configured_local_runtime
from langbridge.runtime.hosting import create_runtime_api_app
from langbridge.runtime.hosting.auth import RuntimeAuthConfig, RuntimeAuthMode
from langbridge.runtime.bootstrap.configured_runtime import _stable_uuid


def _build_runtime_with_configured_semantic_graphs(
    tmp_path: Path,
    *,
    include_relationships: bool = True,
    duplicate_graph_definition: bool = False,
):
    workspace_id = uuid.UUID("00000000-0000-0000-0000-000000000321")
    commerce_model_id = _stable_uuid(
        "semantic-model",
        f"{workspace_id}:commerce_performance",
    )
    marketing_model_id = _stable_uuid(
        "semantic-model",
        f"{workspace_id}:marketing_performance",
    )

    db_path = tmp_path / "runtime_semantic_graph.db"
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

    graph_relationships = ""
    if include_relationships:
        graph_relationships = f"""
      relationships:
        - name: commerce_to_marketing
          source_semantic_model_id: "{commerce_model_id}"
          source_field: shopify_orders.customer_id
          target_semantic_model_id: "{marketing_model_id}"
          target_field: campaign_touchpoints.customer_id
          relationship_type: left
"""

    duplicate_graph_block = ""
    if duplicate_graph_definition:
        duplicate_graph_block = f"""
  - name: commerce_marketing_graph_duplicate
    model:
      version: "1.0"
      name: commerce_marketing_graph_duplicate
      description: Duplicate semantic graph definition for ambiguity tests.
      source_models:
        - id: "{commerce_model_id}"
          alias: Commerce
          name: Commerce
        - id: "{marketing_model_id}"
          alias: Marketing
          name: Marketing
{graph_relationships.rstrip()}
"""

    config_path = tmp_path / "langbridge_runtime_semantic_graph.yml"
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
    materialization_mode: live
    semantic_model: commerce_performance
    default_time_dimension: order_date
    source:
      table: orders_enriched
  - name: shopify_customers
    connector: commerce_demo
    materialization_mode: live
    semantic_model: commerce_performance
    source:
      table: customer_profiles
  - name: campaign_touchpoints
    connector: commerce_demo
    materialization_mode: live
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
          relation_name: shopify_orders
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
          relation_name: shopify_customers
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
  - name: commerce_marketing_graph
    model:
      version: "1.0"
      name: commerce_marketing_graph
      description: Configured semantic graph for runtime API boundary tests.
      source_models:
        - id: "{commerce_model_id}"
          alias: Commerce
          name: Commerce
        - id: "{marketing_model_id}"
          alias: Marketing
          name: Marketing
{graph_relationships.rstrip()}
{duplicate_graph_block.rstrip()}
""".strip(),
        encoding="utf-8",
    )

    return build_configured_local_runtime(
        config_path=str(config_path),
        workspace_id=workspace_id,
    )


def _create_runtime_app(runtime):
    return create_runtime_api_app(
        runtime_host=runtime,
        auth_config=RuntimeAuthConfig(mode=RuntimeAuthMode.none),
    )


def test_runtime_semantic_graph_query_executes_joined_dimension_query(tmp_path: Path) -> None:
    runtime = _build_runtime_with_configured_semantic_graphs(tmp_path)
    client = TestClient(_create_runtime_app(runtime))

    response = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_models": ["commerce_performance", "marketing_performance"],
            "dimensions": [
                "shopify_orders.order_id",
                "shopify_customers.region",
                "campaign_touchpoints.channel",
            ],
            "order": {"shopify_orders.order_id": "asc"},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "succeeded"
    assert payload["semantic_model_id"] is None
    assert len(payload["semantic_model_ids"]) == 2
    assert payload["connector_id"] is not None
    assert payload["data"] == [
        {
            "shopify_orders.order_id": "O-1",
            "shopify_customers.region": "Europe",
            "campaign_touchpoints.channel": "Email",
        },
        {
            "shopify_orders.order_id": "O-2",
            "shopify_customers.region": "North America",
            "campaign_touchpoints.channel": "Paid Search",
        },
        {
            "shopify_orders.order_id": "O-3",
            "shopify_customers.region": "Europe",
            "campaign_touchpoints.channel": "Affiliate",
        },
    ]
    assert "Commerce__shopify_customers" in payload["generated_sql"]
    assert "Marketing__campaign_touchpoints" in payload["generated_sql"]


def test_runtime_semantic_graph_query_filters_across_model_boundaries(tmp_path: Path) -> None:
    runtime = _build_runtime_with_configured_semantic_graphs(tmp_path)
    client = TestClient(_create_runtime_app(runtime))

    response = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_models": ["commerce_performance", "marketing_performance"],
            "measures": ["shopify_orders.net_sales", "campaign_touchpoints.marketing_spend"],
            "dimensions": ["shopify_customers.region"],
            "filters": [
                {
                    "member": "shopify_orders.order_status",
                    "operator": "equals",
                    "values": ["fulfilled"],
                },
                {
                    "member": "campaign_touchpoints.channel",
                    "operator": "equals",
                    "values": ["Email"],
                },
            ],
            "order": {"shopify_orders.net_sales": "desc"},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "succeeded"
    assert payload["data"] == [
        {
            "shopify_customers.region": "Europe",
            "shopify_orders.net_sales": 180.0,
            "campaign_touchpoints.marketing_spend": 45.0,
        }
    ]
    assert "WHERE" in payload["generated_sql"]
    assert "order_status" in payload["generated_sql"]
    assert "channel" in payload["generated_sql"]


def test_runtime_semantic_graph_query_groups_and_aggregates_across_models(tmp_path: Path) -> None:
    runtime = _build_runtime_with_configured_semantic_graphs(tmp_path)
    client = TestClient(_create_runtime_app(runtime))

    response = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_models": ["commerce_performance", "marketing_performance"],
            "measures": ["shopify_orders.net_sales", "campaign_touchpoints.marketing_spend"],
            "dimensions": ["shopify_customers.region", "campaign_touchpoints.channel"],
            "order": {"shopify_orders.net_sales": "desc"},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "succeeded"
    assert payload["data"] == [
        {
            "shopify_customers.region": "Europe",
            "campaign_touchpoints.channel": "Affiliate",
            "shopify_orders.net_sales": 260.0,
            "campaign_touchpoints.marketing_spend": 30.0,
        },
        {
            "shopify_customers.region": "North America",
            "campaign_touchpoints.channel": "Paid Search",
            "shopify_orders.net_sales": 210.0,
            "campaign_touchpoints.marketing_spend": 80.0,
        },
        {
            "shopify_customers.region": "Europe",
            "campaign_touchpoints.channel": "Email",
            "shopify_orders.net_sales": 180.0,
            "campaign_touchpoints.marketing_spend": 45.0,
        },
    ]
    assert "GROUP BY" in payload["generated_sql"]


def test_runtime_semantic_graph_query_rejects_missing_cross_model_relationship(tmp_path: Path) -> None:
    runtime = _build_runtime_with_configured_semantic_graphs(
        tmp_path,
        include_relationships=False,
    )
    client = TestClient(_create_runtime_app(runtime))

    response = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_models": ["commerce_performance", "marketing_performance"],
            "measures": ["shopify_orders.net_sales"],
            "dimensions": ["campaign_touchpoints.channel"],
        },
    )

    assert response.status_code == 400
    assert "No join path" in response.json()["detail"]


def test_runtime_semantic_graph_query_rejects_unknown_semantic_model(tmp_path: Path) -> None:
    runtime = _build_runtime_with_configured_semantic_graphs(tmp_path)
    client = TestClient(_create_runtime_app(runtime))

    response = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_models": ["commerce_performance", "unknown_semantic_model"],
            "dimensions": ["shopify_orders.order_id"],
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown semantic model 'unknown_semantic_model'."


def test_runtime_semantic_graph_query_rejects_incompatible_graph_request_shape(tmp_path: Path) -> None:
    runtime = _build_runtime_with_configured_semantic_graphs(tmp_path)
    client = TestClient(_create_runtime_app(runtime))

    response = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_models": [
                "commerce_marketing_graph",
                "commerce_performance",
            ],
            "dimensions": ["shopify_orders.order_id"],
        },
    )

    assert response.status_code == 400
    assert "cannot be combined with other semantic_models" in response.json()["detail"]


def test_runtime_semantic_graph_query_executes_named_configured_graph(tmp_path: Path) -> None:
    runtime = _build_runtime_with_configured_semantic_graphs(tmp_path)
    client = TestClient(_create_runtime_app(runtime))

    response = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_models": ["commerce_marketing_graph"],
            "dimensions": ["shopify_orders.order_id", "campaign_touchpoints.channel"],
            "order": {"shopify_orders.order_id": "asc"},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "succeeded"
    assert payload["semantic_model_id"] is None
    assert len(payload["semantic_model_ids"]) == 2
    assert payload["data"][0]["shopify_orders.order_id"] == "O-1"


def test_runtime_semantic_graph_query_rejects_ambiguous_configured_graph_match(tmp_path: Path) -> None:
    runtime = _build_runtime_with_configured_semantic_graphs(
        tmp_path,
        duplicate_graph_definition=True,
    )
    client = TestClient(_create_runtime_app(runtime))

    response = client.post(
        "/api/runtime/v1/semantic/query",
        json={
            "semantic_models": ["commerce_performance", "marketing_performance"],
            "dimensions": ["shopify_orders.order_id", "campaign_touchpoints.channel"],
        },
    )

    assert response.status_code == 400
    assert "Multiple configured semantic graphs match" in response.json()["detail"]
