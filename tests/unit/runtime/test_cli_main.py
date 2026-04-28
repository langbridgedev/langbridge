
import json
import sqlite3
from pathlib import Path

from langbridge.cli.main import main
from tests.unit._runtime_host_sync_helpers import (
    mock_stripe_api,
    runtime_storage_dirs,
    write_sync_runtime_config,
)


def _write_config(tmp_path: Path) -> Path:
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
            ('O-2', '2025-05-14', 'United States', 210.0, 84.0, 'Paid Search', 'Silver', 'fulfilled', 1002);
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
    materialization_mode: live
    semantic_model: commerce_performance
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
ai:
  profiles:
    - name: commerce_analyst
      llm:
        llm_connection: local_openai
      scope:
        semantic_models: [commerce_performance]
      prompts:
        system: You are a commerce analytics agent.
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
    return config_path


def test_cli_lists_datasets_for_local_config(tmp_path: Path, capsys) -> None:
    config_path = _write_config(tmp_path)

    exit_code = main(["datasets", "list", "--config", str(config_path)])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["total"] == 1
    assert payload["items"][0]["name"] == "shopify_orders"


def test_cli_serve_delegates_to_runtime_api(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_config(tmp_path)
    captured: dict[str, object] = {}

    def fake_run_runtime_api(*, config_path, host, port, features, debug, reload, odbc_host, odbc_port):
        captured["config_path"] = config_path
        captured["host"] = host
        captured["port"] = port
        captured["features"] = features
        captured["debug"] = debug
        captured["reload"] = reload
        captured["odbc_host"] = odbc_host
        captured["odbc_port"] = odbc_port

    monkeypatch.setattr("langbridge.cli.main.run_runtime_api", fake_run_runtime_api)

    exit_code = main(
        [
            "serve",
            "--config",
            str(config_path),
            "--host",
            "0.0.0.0",
            "--port",
            "9100",
            "--features",
            "ui,mcp,odbc",
            "--odbc-host",
            "0.0.0.0",
            "--odbc-port",
            "15432",
            "--debug",
            "--reload",
        ]
    )

    assert exit_code == 0
    assert captured == {
        "config_path": str(config_path),
        "host": "0.0.0.0",
        "port": 9100,
        "features": ["ui", "mcp", "odbc"],
        "debug": True,
        "reload": True,
        "odbc_host": "0.0.0.0",
        "odbc_port": 15432,
    }


def test_cli_serve_rejects_unknown_feature(tmp_path: Path, capsys) -> None:
    config_path = _write_config(tmp_path)

    exit_code = main(
        [
            "serve",
            "--config",
            str(config_path),
            "--features",
            "widgets",
        ]
    )

    assert exit_code == 1
    assert "Unsupported serve feature 'widgets'" in capsys.readouterr().err


def test_cli_supports_local_sync_commands(tmp_path: Path, capsys) -> None:
    with mock_stripe_api() as api_base_url, runtime_storage_dirs(tmp_path):
        config_path = write_sync_runtime_config(
            tmp_path,
            api_base_url=api_base_url,
            declared_synced_datasets=[{"name": "billing_customers", "resource": "customers"}],
        )

        exit_code = main(["connectors", "list", "--config", str(config_path)])
        assert exit_code == 0
        connectors_payload = json.loads(capsys.readouterr().out)
        assert connectors_payload["items"][0]["name"] == "billing_demo"

        exit_code = main(
            [
                "sync",
                "run",
                "--config",
                str(config_path),
                "--dataset",
                "billing_customers",
            ]
        )
        assert exit_code == 0
        sync_payload = json.loads(capsys.readouterr().out)
        assert sync_payload["status"] == "succeeded"
        assert sync_payload["dataset_name"] == "billing_customers"
        assert sync_payload["resources"][0]["resource_name"] == "customers"

        exit_code = main(
            [
                "sync",
                "resources",
                "--config",
                str(config_path),
                "--connector",
                "billing_demo",
            ]
        )
        assert exit_code == 0
        resources_payload = json.loads(capsys.readouterr().out)
        assert resources_payload["total"] >= 1
        assert any(item["name"] == "customers" for item in resources_payload["items"])
