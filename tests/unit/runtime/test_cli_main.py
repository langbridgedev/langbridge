
import json
import os
import sqlite3
from pathlib import Path

from langbridge.cli.main import main
from langbridge.runtime.hosting import server as runtime_server
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

    def fake_run_runtime_api(*, config_path, host, port, features, debug, reload, workers, odbc_host, odbc_port):
        captured["config_path"] = config_path
        captured["host"] = host
        captured["port"] = port
        captured["features"] = features
        captured["debug"] = debug
        captured["reload"] = reload
        captured["workers"] = workers
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
        "workers": 1,
        "odbc_host": "0.0.0.0",
        "odbc_port": 15432,
    }


def test_cli_serve_forwards_worker_count(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_config(tmp_path)
    captured: dict[str, object] = {}

    def fake_run_runtime_api(*, config_path, host, port, features, debug, reload, workers, odbc_host, odbc_port):
        captured["config_path"] = config_path
        captured["host"] = host
        captured["port"] = port
        captured["features"] = features
        captured["debug"] = debug
        captured["reload"] = reload
        captured["workers"] = workers
        captured["odbc_host"] = odbc_host
        captured["odbc_port"] = odbc_port

    monkeypatch.setattr("langbridge.cli.main.run_runtime_api", fake_run_runtime_api)

    exit_code = main(
        [
            "serve",
            "--config",
            str(config_path),
            "--workers",
            "4",
        ]
    )

    assert exit_code == 0
    assert captured == {
        "config_path": str(config_path),
        "host": "127.0.0.1",
        "port": 8000,
        "features": [],
        "debug": False,
        "reload": False,
        "workers": 4,
        "odbc_host": None,
        "odbc_port": None,
    }


def test_cli_serve_rejects_reload_with_multiple_workers(tmp_path: Path, capsys) -> None:
    config_path = _write_config(tmp_path)

    exit_code = main(
        [
            "serve",
            "--config",
            str(config_path),
            "--reload",
            "--workers",
            "2",
        ]
    )

    assert exit_code == 1
    assert "--reload cannot be combined with --workers greater than 1." in capsys.readouterr().err


def test_cli_serve_rejects_non_positive_workers(tmp_path: Path, capsys) -> None:
    config_path = _write_config(tmp_path)

    exit_code = main(
        [
            "serve",
            "--config",
            str(config_path),
            "--workers",
            "0",
        ]
    )

    assert exit_code == 1
    assert "workers must be greater than or equal to 1." in capsys.readouterr().err


def test_cli_serve_rejects_odbc_with_multiple_workers(tmp_path: Path, capsys) -> None:
    config_path = _write_config(tmp_path)

    exit_code = main(
        [
            "serve",
            "--config",
            str(config_path),
            "--features",
            "odbc",
            "--workers",
            "2",
        ]
    )

    assert exit_code == 1
    assert "--workers greater than 1 cannot be combined with the odbc feature." in capsys.readouterr().err


def test_runtime_api_uses_factory_import_for_multi_worker_serve(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_config(tmp_path)
    captured: dict[str, object] = {}

    def fake_uvicorn_run(app, **kwargs):
        captured["app"] = app
        captured.update(kwargs)

    monkeypatch.setattr(runtime_server.uvicorn, "run", fake_uvicorn_run)

    runtime_server.run_runtime_api(
        config_path=config_path,
        host="0.0.0.0",
        port=9100,
        features=["ui"],
        debug=True,
        workers=3,
        odbc_host="0.0.0.0",
        odbc_port=15432,
    )

    assert captured["app"] == "langbridge.runtime.hosting.app:create_runtime_api_app_from_env"
    assert captured["factory"] is True
    assert captured["workers"] == 3
    assert captured["reload"] is False
    assert captured["log_level"] == "debug"
    assert os.environ["LANGBRIDGE_RUNTIME_CONFIG_PATH"] == str(config_path.resolve())
    assert os.environ["LANGBRIDGE_RUNTIME_FEATURES"] == "ui"
    assert os.environ["LANGBRIDGE_RUNTIME_DEBUG"] == "true"
    assert os.environ["LANGBRIDGE_RUNTIME_WORKERS"] == "3"
    assert os.environ["LANGBRIDGE_RUNTIME_BACKGROUND_TASKS"] == "auto"
    assert os.environ["LANGBRIDGE_RUNTIME_ODBC_HOST"] == "0.0.0.0"
    assert os.environ["LANGBRIDGE_RUNTIME_ODBC_PORT"] == "15432"


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
