from __future__ import annotations

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from langbridge.packages.runtime.models import (
    CreateDatasetPreviewJobRequest,
    RuntimeMessageRole,
)
from langbridge.packages.runtime.local_config import build_configured_local_runtime
from tests.unit._runtime_host_sync_helpers import (
    mock_stripe_api,
    runtime_storage_dirs,
    write_sync_runtime_config,
)


def _write_config(directory: Path) -> Path:
    config_path = directory / "langbridge_config.yml"
    config_path.write_text(
        """
version: 1

connectors:
  - name: local_demo
    type: sqlite
    connection:
      location: ./example.db

datasets:
  - name: orders
    connector: local_demo
    semantic_model: commerce
    source:
      table: orders

semantic_models:
  - name: commerce
    default: true
    model:
      version: "1"
      name: commerce
      datasets:
        orders:
          relation_name: orders
          dimensions:
            - name: country
              expression: country
              type: string
          measures:
            - name: revenue
              expression: revenue
              type: number
              aggregation: sum

llm_connections:
  - name: local_openai
    provider: openai
    model: gpt-4o-mini
    api_key: test-key
    default: true

agents:
  - name: analyst
    llm_connection: local_openai
    semantic_model: commerce
    dataset: orders
    default: true
    instructions: Answer analytical questions.
""".strip(),
        encoding="utf-8",
    )
    return config_path


def test_build_configured_local_runtime_wires_agent_execution() -> None:
    with TemporaryDirectory() as temp_dir:
        config_path = _write_config(Path(temp_dir))
        runtime = build_configured_local_runtime(config_path=config_path)

    assert runtime.services.agent_execution is not None
    assert runtime.services.dataset_sync is not None


def test_configured_local_runtime_ask_agent_uses_agent_execution() -> None:
    with TemporaryDirectory() as temp_dir:
        config_path = _write_config(Path(temp_dir))
        runtime = build_configured_local_runtime(config_path=config_path)

    captured: dict[str, object] = {}

    async def fake_execute(*, job_id, request, event_emitter=None):
        captured["job_id"] = job_id
        captured["request"] = request
        return SimpleNamespace(
            response={
                "summary": "Handled by agent execution",
                "result": {"rows": [{"value": 1}]},
                "visualization": None,
            }
        )

    runtime.services.agent_execution = SimpleNamespace(execute=fake_execute)
    payload = asyncio.run(runtime.ask_agent(prompt="What is revenue by country?"))

    request = captured["request"]
    assert payload["summary"] == "Handled by agent execution"
    assert payload["thread_id"] == request.thread_id
    assert request.agent_definition_id == next(iter(runtime._agents.values())).id
    assert len(runtime._thread_message_repository.items) == 1
    assert runtime._thread_message_repository.items[0].role == RuntimeMessageRole.user


def test_build_configured_local_runtime_supports_file_backed_datasets() -> None:
    with TemporaryDirectory() as temp_dir:
        directory = Path(temp_dir)
        csv_path = directory / "marketing_campaigns.csv"
        csv_path.write_text(
            "\n".join(
                [
                    "contact_external_id,campaign_name,campaign_channel,engagement_score",
                    "CRM-00000001,Spring Refresh,email,91",
                    "CRM-00000002,VIP Retention,sms,77",
                ]
            ),
            encoding="utf-8",
        )
        config_path = directory / "langbridge_config.yml"
        config_path.write_text(
            """
version: 1

connectors:
  - name: campaign_file
    type: file
    connection: {}

datasets:
  - name: marketing_campaigns
    connector: campaign_file
    source:
      path: ./marketing_campaigns.csv
      format: csv
      header: true

semantic_models:
  - name: marketing
    default: true
    model:
      version: "1"
      name: marketing
      datasets:
        marketing_campaigns:
          relation_name: marketing_campaigns
          dimensions:
            - name: contact_external_id
              expression: contact_external_id
              type: string
              primary_key: true
            - name: campaign_name
              expression: campaign_name
              type: string
          measures:
            - name: influenced_contacts
              expression: contact_external_id
              type: number
              aggregation: count
""".strip(),
            encoding="utf-8",
        )
        runtime = build_configured_local_runtime(config_path=config_path)

        dataset_record = runtime._datasets["marketing_campaigns"]
        dataset_model = asyncio.run(
            runtime.providers.dataset_metadata.get_dataset(
                workspace_id=runtime.context.workspace_id,
                dataset_id=dataset_record.id,
            )
        )
        assert dataset_model.dataset_type == "FILE"
        assert dataset_model.storage_kind == "csv"
        assert dataset_model.storage_uri is not None
        assert dataset_model.file_config == {"format": "csv", "header": True}

        payload = asyncio.run(
            runtime.query_dataset(
                request=CreateDatasetPreviewJobRequest(
                    dataset_id=dataset_record.id,
                    workspace_id=runtime.context.workspace_id,
                    user_id=runtime.context.user_id,
                    requested_limit=5,
                    enforced_limit=5,
                )
            )
        )

        assert payload["rows"] == [
            {
                "contact_external_id": "CRM-00000001",
                "campaign_name": "Spring Refresh",
                "campaign_channel": "email",
                "engagement_score": 91,
            },
            {
                "contact_external_id": "CRM-00000002",
                "campaign_name": "VIP Retention",
                "campaign_channel": "sms",
                "engagement_score": 77,
            },
        ]


def test_configured_local_runtime_syncs_connector_resources(tmp_path: Path) -> None:
    with mock_stripe_api() as api_base_url, runtime_storage_dirs(tmp_path):
        config_path = write_sync_runtime_config(tmp_path, api_base_url=api_base_url)
        runtime = build_configured_local_runtime(config_path=config_path)

        connectors = asyncio.run(runtime.list_connectors())
        assert connectors[0]["name"] == "billing_demo"
        assert connectors[0]["supports_sync"] is True

        resources = asyncio.run(runtime.list_sync_resources(connector_name="billing_demo"))
        assert any(item["name"] == "customers" for item in resources)

        sync_result = asyncio.run(
            runtime.sync_connector_resources(
                connector_name="billing_demo",
                resources=["customers"],
            )
        )
        assert sync_result["status"] == "succeeded"
        assert sync_result["resources"][0]["resource_name"] == "customers"

        datasets = asyncio.run(runtime.list_datasets())
        assert len(datasets) == 1
        synced_dataset_id = datasets[0]["id"]

        preview = asyncio.run(
            runtime.query_dataset(
                request=CreateDatasetPreviewJobRequest(
                    dataset_id=synced_dataset_id,
                    workspace_id=runtime.context.workspace_id,
                    user_id=runtime.context.user_id,
                    requested_limit=10,
                    enforced_limit=10,
                )
            )
        )
        assert preview["row_count_preview"] == 2
        assert preview["rows"][0]["id"] == "cus_001"
