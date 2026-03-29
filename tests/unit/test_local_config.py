
import asyncio
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest

from langbridge.runtime.models import (
    CreateDatasetPreviewJobRequest,
    RuntimeMessageRole,
)
from langbridge.runtime.bootstrap import build_configured_local_runtime
from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHostFactory
from langbridge.runtime.config.models import LocalRuntimeConfig
from langbridge.runtime.models.metadata import ManagementMode
from langbridge.runtime.services.errors import ExecutionValidationError
from tests.unit._runtime_host_sync_helpers import (
    mock_stripe_api,
    runtime_storage_dirs,
    write_sync_runtime_config,
)


def _write_config(directory: Path, *, runtime_block: str | None = None) -> Path:
    config_path = directory / "langbridge_config.yml"
    runtime_section = "\n"
    if runtime_block:
        indented_runtime_block = "\n".join(
            f"  {line}" if line.strip() else line
            for line in runtime_block.strip().splitlines()
        )
        runtime_section = f"\nruntime:\n{indented_runtime_block}\n"
    config_path.write_text(
        f"""
version: 1
{runtime_section}
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
    default: true
    definition:
      prompt:
        system_prompt: You are a local analytics agent.
        user_instructions: Answer analytical questions.
        style_guidance: Keep answers concise and clearly grounded in query results.
      memory:
        strategy: database
      features:
        bi_copilot_enabled: false
        deep_research_enabled: false
        visualization_enabled: true
        mcp_enabled: false
      tools:
        - name: analyst_semantic_sql
          tool_type: sql
          description: Governed semantic analytics access.
          config:
            semantic_model_ids: [commerce]
      access_policy:
        allowed_connectors: [local_demo]
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


def test_build_configured_local_runtime_wires_agent_execution() -> None:
    with TemporaryDirectory() as temp_dir:
        config_path = _write_config(Path(temp_dir))
        runtime = build_configured_local_runtime(config_path=config_path)

    assert runtime.services.agent_execution is not None
    assert runtime.services.dataset_sync is not None


def test_configured_local_runtime_ask_agent_uses_agent_execution() -> None:
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

    with TemporaryDirectory() as temp_dir:
        config_path = _write_config(Path(temp_dir))
        runtime = build_configured_local_runtime(config_path=config_path)
        runtime.services.agent_execution = SimpleNamespace(execute=fake_execute)
        payload = asyncio.run(runtime.ask_agent(prompt="What is revenue by country?"))

        request = captured["request"]
        assert payload["summary"] == "Handled by agent execution"
        assert payload["thread_id"] == request.thread_id
        assert request.agent_definition_id == next(iter(runtime._agents.values())).id
        messages = asyncio.run(runtime.list_thread_messages(thread_id=payload["thread_id"]))
        assert len(messages) == 1
        assert messages[0]["role"] == RuntimeMessageRole.user


def test_configured_local_runtime_defaults_metadata_store_to_sqlite(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)

    runtime = build_configured_local_runtime(config_path=config_path)

    expected_path = (config_path.parent / ".langbridge" / "metadata.db").resolve()
    assert runtime._metadata_store.type == "sqlite"
    assert runtime._metadata_store.path == expected_path
    assert expected_path.exists()
    assert runtime._dataset_repository.__class__.__name__ == "RepositoryDatasetCatalogStore"


def test_configured_local_runtime_supports_explicit_in_memory_metadata_store(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        runtime_block="""
  metadata_store:
    type: in_memory
""",
    )

    runtime = build_configured_local_runtime(config_path=config_path)

    assert runtime._metadata_store.type == "in_memory"
    assert runtime._dataset_repository.__class__.__name__ == "_InMemoryDatasetRepository"
    assert not (config_path.parent / ".langbridge" / "metadata.db").exists()


def test_postgres_metadata_store_config_is_normalized() -> None:
    config = LocalRuntimeConfig.model_validate(
        {
            "version": 1,
            "runtime": {
                "metadata_store": {
                    "type": "postgres",
                    "url": "postgres://langbridge:secret@db.example.com:5432/langbridge",
                    "pool_size": 8,
                }
            },
        }
    )

    resolved = ConfiguredLocalRuntimeHostFactory._resolve_metadata_store_config(
        config_path=Path("/tmp/langbridge_config.yml"),
        config=config,
    )

    assert resolved.type == "postgres"
    assert resolved.url == "postgresql://langbridge:secret@db.example.com:5432/langbridge"
    assert resolved.sync_url == "postgresql+psycopg://langbridge:secret@db.example.com:5432/langbridge"
    assert resolved.async_url == "postgresql+asyncpg://langbridge:secret@db.example.com:5432/langbridge"
    assert resolved.pool_size == 8


def test_local_runtime_config_parses_dataset_materialization_mode_and_connector_capabilities() -> None:
    config = LocalRuntimeConfig.model_validate(
        {
            "version": 1,
            "connectors": [
                {
                    "name": "warehouse",
                    "type": "sqlite",
                    "connection": {"location": "./example.db"},
                    "capabilities": {
                        "supports_live_datasets": True,
                        "supports_synced_datasets": True,
                        "supports_query_pushdown": True,
                    },
                }
            ],
            "datasets": [
                {
                    "name": "orders",
                    "connector": "warehouse",
                    "materialization_mode": "live",
                    "source": {"table": "orders"},
                }
            ],
        }
    )

    assert config.connectors[0].capabilities is not None
    assert config.connectors[0].capabilities.supports_live_datasets is True
    assert config.connectors[0].capabilities.supports_synced_datasets is True
    assert config.datasets[0].materialization_mode.value == "live"


def test_local_runtime_config_uses_explicit_source_resource_for_synced_datasets() -> None:
    config = LocalRuntimeConfig.model_validate(
        {
            "version": 1,
            "connectors": [
                {
                    "name": "billing_demo",
                    "type": "stripe",
                    "connection": {"api_key": "test-key"},
                }
            ],
            "datasets": [
                {
                    "name": "billing_customers",
                    "connector": "billing_demo",
                    "materialization_mode": "synced",
                    "source": {"resource": "customers"},
                }
            ],
        }
    )

    assert config.datasets[0].materialization_mode.value == "synced"
    assert config.datasets[0].source.resource == "customers"
    assert config.datasets[0].source.table is None


def test_local_runtime_config_normalizes_legacy_synced_source_table_to_source_resource() -> None:
    config = LocalRuntimeConfig.model_validate(
        {
            "version": 1,
            "connectors": [
                {
                    "name": "billing_demo",
                    "type": "stripe",
                    "connection": {"api_key": "test-key"},
                }
            ],
            "datasets": [
                {
                    "name": "billing_customers",
                    "connector": "billing_demo",
                    "materialization_mode": "synced",
                    "source": {"table": "customers"},
                }
            ],
        }
    )

    assert config.datasets[0].source.resource == "customers"
    assert config.datasets[0].source.table is None


def test_configured_local_runtime_rejects_live_dataset_for_connector_without_live_support(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "langbridge_config.yml"
    config_path.write_text(
        """
version: 1
connectors:
  - name: billing_demo
    type: stripe
    connection:
      api_key: test-key
datasets:
  - name: billing_customers
    connector: billing_demo
    materialization_mode: live
    source:
      table: customers
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="does not support live datasets"):
        build_configured_local_runtime(config_path=config_path)


def test_configured_local_runtime_supports_config_defined_synced_dataset(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "langbridge_config.yml"
    config_path.write_text(
        """
version: 1
connectors:
  - name: billing_demo
    type: stripe
    connection:
      api_key: test-key
datasets:
  - name: billing_customers
    connector: billing_demo
    materialization_mode: synced
    source:
      resource: customers
""".strip(),
        encoding="utf-8",
    )

    runtime = build_configured_local_runtime(config_path=config_path)

    dataset_record = runtime._datasets["billing_customers"]
    dataset_model = asyncio.run(
        runtime.providers.dataset_metadata.get_dataset(
            workspace_id=runtime.context.workspace_id,
            dataset_id=dataset_record.id,
        )
    )

    assert dataset_model is not None
    assert dataset_model.materialization_mode == "synced"
    assert dataset_model.dataset_type == "FILE"
    assert dataset_model.storage_kind == "parquet"
    assert dataset_model.storage_uri is None
    assert dataset_model.status == "pending_sync"
    assert dataset_model.file_config == {
        "format": "parquet",
        "managed_dataset": True,
            "connector_sync": {
                "connector_id": str(runtime._connectors["billing_demo"].id),
                "connector_type": "STRIPE",
                "connector_family": "api",
                "resource_name": "customers",
                "root_resource_name": "customers",
                "parent_resource_name": None,
        },
    }


def test_configured_local_runtime_supports_legacy_synced_source_table_compatibility(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "langbridge_config.yml"
    config_path.write_text(
        """
version: 1
connectors:
  - name: billing_demo
    type: stripe
    connection:
      api_key: test-key
datasets:
  - name: billing_customers
    connector: billing_demo
    materialization_mode: synced
    source:
      table: customers
""".strip(),
        encoding="utf-8",
    )

    runtime = build_configured_local_runtime(config_path=config_path)

    dataset_record = runtime._datasets["billing_customers"]
    dataset_model = asyncio.run(
        runtime.providers.dataset_metadata.get_dataset(
            workspace_id=runtime.context.workspace_id,
            dataset_id=dataset_record.id,
        )
    )

    assert dataset_model is not None
    assert dataset_model.materialization_mode == "synced"
    assert dataset_model.file_config == {
        "format": "parquet",
        "managed_dataset": True,
            "connector_sync": {
                "connector_id": str(runtime._connectors["billing_demo"].id),
                "connector_type": "STRIPE",
                "connector_family": "api",
                "resource_name": "customers",
                "root_resource_name": "customers",
                "parent_resource_name": None,
        },
    }


def test_configured_local_runtime_rejects_synced_dataset_with_non_resource_source(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "langbridge_config.yml"
    config_path.write_text(
        """
version: 1
connectors:
  - name: billing_demo
    type: stripe
    connection:
      api_key: test-key
datasets:
  - name: billing_customers
    connector: billing_demo
    materialization_mode: synced
    source:
      storage_uri: file:///tmp/customers.parquet
      format: parquet
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must declare source.resource"):
        build_configured_local_runtime(config_path=config_path)


def test_configured_runtime_sqlite_metadata_persists_threads_across_rebuilds(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    runtime = build_configured_local_runtime(config_path=config_path)

    async def fake_execute(*, job_id, request, event_emitter=None):
        return SimpleNamespace(response={"summary": "persisted", "result": None, "visualization": None})

    runtime.services.agent_execution = SimpleNamespace(execute=fake_execute)
    payload = asyncio.run(runtime.ask_agent(prompt="Persist this thread"))

    rebuilt_runtime = build_configured_local_runtime(config_path=config_path)
    threads = asyncio.run(rebuilt_runtime.list_threads())
    messages = asyncio.run(rebuilt_runtime.list_thread_messages(thread_id=payload["thread_id"]))

    assert any(thread["id"] == payload["thread_id"] for thread in threads)
    assert len(messages) == 1
    assert messages[0]["role"] == RuntimeMessageRole.user


def test_configured_runtime_sqlite_uses_fresh_unit_of_work_sessions(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    runtime = build_configured_local_runtime(config_path=config_path)

    async def collect_sessions() -> tuple[object, object]:
        async with runtime._persistence_controller.unit_of_work() as first:  # type: ignore[union-attr]
            first_session = first.session
        async with runtime._persistence_controller.unit_of_work() as second:  # type: ignore[union-attr]
            second_session = second.session
        return first_session, second_session

    first_session, second_session = asyncio.run(collect_sessions())

    assert first_session is not second_session


def test_configured_runtime_sqlite_flushes_thread_messages_before_agent_execution(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    runtime = build_configured_local_runtime(config_path=config_path)
    captured: dict[str, object] = {}

    async def fake_execute(*, job_id, request, event_emitter=None):
        captured["thread"] = await runtime._thread_repository.get_by_id(request.thread_id)
        captured["messages"] = await runtime._thread_message_repository.list_for_thread(
            request.thread_id
        )
        return SimpleNamespace(
            response={
                "summary": "visible within same operation",
                "result": None,
                "visualization": None,
            }
        )

    runtime.services.agent_execution = SimpleNamespace(execute=fake_execute)
    payload = asyncio.run(runtime.ask_agent(prompt="Verify flush visibility"))

    assert payload["summary"] == "visible within same operation"
    thread = captured["thread"]
    messages = captured["messages"]
    assert thread is not None
    assert len(messages) == 1
    assert messages[0].role == RuntimeMessageRole.user
    assert thread.last_message_id == messages[0].id


def test_configured_runtime_sqlite_handles_concurrent_thread_operations(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    runtime = build_configured_local_runtime(config_path=config_path)

    async def create_threads() -> list[dict[str, object]]:
        return await asyncio.gather(
            *[
                runtime.create_thread(title=f"thread-{index}")
                for index in range(5)
            ]
        )

    threads = asyncio.run(create_threads())
    listed_threads = asyncio.run(runtime.list_threads())

    assert len(threads) == 5
    assert {thread["id"] for thread in threads}.issubset(
        {thread["id"] for thread in listed_threads}
    )


def test_configured_runtime_sqlite_rolls_back_failed_agent_operations(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    runtime = build_configured_local_runtime(config_path=config_path)

    async def failing_execute(*, job_id, request, event_emitter=None):
        raise RuntimeError("agent execution failed")

    runtime.services.agent_execution = SimpleNamespace(execute=failing_execute)

    with pytest.raises(RuntimeError, match="agent execution failed"):
        asyncio.run(runtime.ask_agent(prompt="This should roll back"))

    assert asyncio.run(runtime.list_threads()) == []


def test_configured_runtime_close_disposes_persistence_controller(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    runtime = build_configured_local_runtime(config_path=config_path)
    controller = runtime.persistence_controller

    assert controller is not None
    assert controller.closed is False

    runtime.close()
    runtime.close()

    assert controller.closed is True
    with pytest.raises(RuntimeError, match="closed"):
        controller.unit_of_work()


def test_configured_runtime_sqlite_reconciles_config_owned_metadata_at_startup(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    runtime = build_configured_local_runtime(config_path=config_path)

    dataset_record = runtime._datasets["orders"]
    semantic_record = runtime._semantic_models["commerce"]
    agent_record = runtime._agents["analyst"]

    dataset = asyncio.run(
        runtime.providers.dataset_metadata.get_dataset(
            workspace_id=runtime.context.workspace_id,
            dataset_id=dataset_record.id,
        )
    )
    connector = asyncio.run(
        runtime.providers.connector_metadata.get_connector_by_name(
            workspace_id=runtime.context.workspace_id,
            connector_name="local_demo",
        )
    )
    semantic_model = asyncio.run(
        runtime.providers.semantic_models.get_semantic_model(
            workspace_id=runtime.context.workspace_id,
            semantic_model_id=semantic_record.id,
        )
    )
    agent_definition = asyncio.run(
        runtime.services.agent_execution._agent_definition_repository.get_by_id(agent_record.id)  # type: ignore[union-attr]
    )
    llm_connection = asyncio.run(
        runtime.services.agent_execution._llm_repository.get_by_id(  # type: ignore[union-attr]
            agent_record.agent_definition.llm_connection_id
        )
    )

    assert dataset is not None
    assert connector is not None
    assert semantic_model is not None
    assert agent_definition is not None
    assert llm_connection is not None
    assert dataset.management_mode == ManagementMode.CONFIG_MANAGED
    assert connector.management_mode == ManagementMode.CONFIG_MANAGED
    assert semantic_model.management_mode == ManagementMode.CONFIG_MANAGED


def test_configured_runtime_sqlite_direct_dataset_lookup_eager_loads_columns(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    runtime = build_configured_local_runtime(config_path=config_path)
    dataset_record = runtime._datasets["orders"]

    dataset = asyncio.run(runtime._dataset_repository.get_by_id(dataset_record.id))

    assert dataset is not None
    assert dataset.id == dataset_record.id
    assert [column.name for column in dataset.columns] == ["country", "revenue"]


def test_configured_local_runtime_resolves_secret_backed_llm_connection_with_workspace_id(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    with TemporaryDirectory() as temp_dir:
        config_path = Path(temp_dir) / "langbridge_config.yml"
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
    api_key_secret:
      provider_type: env
      identifier: OPENAI_API_KEY
    default: true

agents:
  - name: analyst
    llm_connection: local_openai
    semantic_model: commerce
    default: true
""".strip(),
            encoding="utf-8",
        )
        runtime = build_configured_local_runtime(config_path=config_path)

        agent_record = runtime._resolve_agent(None)
        llm_connection = asyncio.run(
            runtime.services.agent_execution._llm_repository.get_by_id(  # type: ignore[union-attr]
                agent_record.agent_definition.llm_connection_id
            )
        )

        assert llm_connection is not None
        assert llm_connection.api_key == "test-key"
        assert llm_connection.workspace_id == runtime.context.workspace_id


def test_configured_local_runtime_normalizes_canonical_agent_tools() -> None:
    with TemporaryDirectory() as temp_dir:
        config_path = Path(temp_dir) / "langbridge_config.yml"
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

  - name: customers
    connector: local_demo
    semantic_model: customers_model
    source:
      table: customers

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

  - name: customers_model
    model:
      version: "1"
      name: customers_model
      datasets:
        customers:
          relation_name: customers
          dimensions:
            - name: segment
              expression: segment
              type: string
          measures:
            - name: customer_count
              expression: customer_id
              type: number
              aggregation: count

llm_connections:
  - name: local_openai
    provider: openai
    model: gpt-4o-mini
    api_key: test-key
    default: true

agents:
  - name: analyst
    llm_connection: local_openai
    default: true
    definition:
      prompt:
        system_prompt: You are a local analytics agent.
        style_guidance: Keep answers concise and clearly grounded in query results.
      memory:
        strategy: database
      features:
        bi_copilot_enabled: false
        deep_research_enabled: false
        visualization_enabled: true
        mcp_enabled: false
      tools:
        - name: governed_sql
          tool_type: sql
          description: Governed semantic analytics access.
          config:
            semantic_model_ids: [commerce, customers_model]
        - name: dataset_sql
          tool_type: sql
          description: Dataset analytics access.
          config:
            dataset_ids: [orders, customers]
      access_policy:
        allowed_connectors: [local_demo]
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
        runtime = build_configured_local_runtime(config_path=config_path)

    definition = runtime._agents["analyst"].agent_definition.definition
    semantic_models = runtime._semantic_models
    datasets = runtime._datasets
    connector_id = str(runtime._connectors["local_demo"].id)

    assert definition["tools"] == [
        {
            "name": "governed_sql",
            "tool_type": "sql",
            "description": "Governed semantic analytics access.",
            "config": {
                "dataset_ids": [],
                "semantic_model_ids": [
                    str(semantic_models["commerce"].id),
                    str(semantic_models["customers_model"].id),
                ],
            },
        },
        {
            "name": "dataset_sql",
            "tool_type": "sql",
            "description": "Dataset analytics access.",
            "config": {
                "dataset_ids": [
                    str(datasets["orders"].id),
                    str(datasets["customers"].id),
                ],
                "semantic_model_ids": [],
            },
        },
    ]
    assert definition["access_policy"] == {
        "allowed_connectors": [connector_id],
        "denied_connectors": [],
    }


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
        assert dataset_model.materialization_mode == "live"
        assert dataset_model.storage_kind == "csv"
        assert dataset_model.storage_uri is not None
        assert dataset_model.file_config == {"format": "csv", "header": True}

        payload = asyncio.run(
            runtime.query_dataset(
                request=CreateDatasetPreviewJobRequest(
                    dataset_id=dataset_record.id,
                    workspace_id=runtime.context.workspace_id,
                    actor_id=runtime.context.actor_id,
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

        connectors = asyncio.run(runtime.list_connectors())
        assert connectors == [
            {
                "id": runtime._connectors["campaign_file"].id,
                "name": "campaign_file",
                "description": None,
                "connector_type": "FILE",
                "connector_family": "file",
                "supports_sync": False,
                "supported_resources": [],
                "sync_strategy": None,
                "capabilities": {
                    "supports_live_datasets": True,
                    "supports_synced_datasets": False,
                    "supports_incremental_sync": False,
                    "supports_query_pushdown": False,
                    "supports_preview": True,
                    "supports_federated_execution": True,
                },
                "management_mode": "config_managed",
                "managed": True,
            }
        ]


def test_configured_local_runtime_syncs_connector_resources(tmp_path: Path) -> None:
    with mock_stripe_api() as api_base_url, runtime_storage_dirs(tmp_path):
        config_path = write_sync_runtime_config(tmp_path, api_base_url=api_base_url)
        runtime = build_configured_local_runtime(config_path=config_path)

        connectors = asyncio.run(runtime.list_connectors())
        assert connectors[0]["name"] == "billing_demo"
        assert connectors[0]["connector_family"] == "api"
        assert connectors[0]["supports_sync"] is True
        assert connectors[0]["capabilities"]["supports_live_datasets"] is False
        assert connectors[0]["capabilities"]["supports_synced_datasets"] is True
        assert connectors[0]["capabilities"]["supports_incremental_sync"] is True
        assert connectors[0]["management_mode"] == "config_managed"
        assert connectors[0]["managed"] is True

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
        assert datasets[0]["materialization_mode"] == "synced"
        assert datasets[0]["management_mode"] == "runtime_managed"
        assert datasets[0]["managed"] is False
        synced_dataset_id = datasets[0]["id"]

        preview = asyncio.run(
            runtime.query_dataset(
                request=CreateDatasetPreviewJobRequest(
                    dataset_id=synced_dataset_id,
                    workspace_id=runtime.context.workspace_id,
                    actor_id=runtime.context.actor_id,
                    requested_limit=10,
                    enforced_limit=10,
                )
            )
        )
        assert preview["row_count_preview"] == 2
        assert preview["rows"][0]["id"] == "cus_001"


def test_configured_local_runtime_reuses_declared_synced_dataset_for_connector_sync(tmp_path: Path) -> None:
    with mock_stripe_api() as api_base_url, runtime_storage_dirs(tmp_path):
        config_path = write_sync_runtime_config(
            tmp_path,
            api_base_url=api_base_url,
            declared_synced_datasets=[{"name": "billing_customers", "resource": "customers"}],
        )
        runtime = build_configured_local_runtime(config_path=config_path)

        declared_dataset = runtime._datasets["billing_customers"]
        listed_before = asyncio.run(runtime.list_datasets())
        assert listed_before == [
            {
                "id": declared_dataset.id,
                "name": "billing_customers",
                "label": declared_dataset.label,
                "description": "Configured synced dataset awaiting connector sync for resource 'customers'.",
                "connector": "billing_demo",
                "semantic_model": None,
                "materialization_mode": "synced",
                "status": "pending_sync",
                "sync_resource": "customers",
                "sync_status": "never_synced",
                "last_sync_at": None,
                "management_mode": "config_managed",
                "managed": True,
            }
        ]

        detail_before = asyncio.run(runtime.get_dataset(dataset_ref="billing_customers"))
        assert detail_before["status"] == "pending_sync"
        assert detail_before["sync_resource"] == "customers"
        assert detail_before["sync_state"]["status"] == "never_synced"
        assert detail_before["storage_uri"] is None

        with pytest.raises(ExecutionValidationError, match=(
            "Synced dataset 'billing_customers' has not been populated yet. "
            "Run connector sync for stripe resource 'customers' before querying it."
        )):
            asyncio.run(
                runtime.query_dataset(
                    request=CreateDatasetPreviewJobRequest(
                        dataset_id=declared_dataset.id,
                        workspace_id=runtime.context.workspace_id,
                        actor_id=runtime.context.actor_id,
                        requested_limit=5,
                        enforced_limit=5,
                    )
                )
            )

        resources = asyncio.run(runtime.list_sync_resources(connector_name="billing_demo"))
        customers = next(item for item in resources if item["name"] == "customers")
        assert customers["status"] == "never_synced"
        assert customers["dataset_ids"] == [declared_dataset.id]
        assert customers["dataset_names"] == ["billing_customers"]

        sync_result = asyncio.run(
            runtime.sync_connector_resources(
                connector_name="billing_demo",
                resources=["customers"],
            )
        )
        assert sync_result["status"] == "succeeded"
        assert sync_result["resources"][0]["dataset_ids"] == [str(declared_dataset.id)]
        assert sync_result["resources"][0]["dataset_names"] == ["billing_customers"]

        listed_after = asyncio.run(runtime.list_datasets())
        assert listed_after[0]["id"] == declared_dataset.id
        assert listed_after[0]["name"] == "billing_customers"
        assert listed_after[0]["status"] == "published"
        assert listed_after[0]["sync_status"] == "succeeded"
        assert listed_after[0]["management_mode"] == "config_managed"
        assert listed_after[0]["managed"] is True

        detail_after = asyncio.run(runtime.get_dataset(dataset_ref="billing_customers"))
        assert detail_after["status"] == "published"
        assert detail_after["sync_state"]["status"] == "succeeded"
        assert detail_after["storage_uri"] is not None

        preview_after = asyncio.run(
            runtime.query_dataset(
                request=CreateDatasetPreviewJobRequest(
                    dataset_id=declared_dataset.id,
                    workspace_id=runtime.context.workspace_id,
                    actor_id=runtime.context.actor_id,
                    requested_limit=10,
                    enforced_limit=10,
                )
            )
        )
        assert preview_after["row_count_preview"] == 2
        assert preview_after["rows"][0]["id"] == "cus_001"


@pytest.mark.parametrize(
    ("example_name", "connector_name", "env_vars", "expected_resources", "expected_fields"),
    [
        (
            "shopify_sync",
            "shopify_demo",
            {
                "SHOPIFY_SHOP_DOMAIN": "acme.myshopify.com",
                "SHOPIFY_ACCESS_TOKEN": "shpat_test_token",
            },
            ["customers", "draft_orders", "locations"],
            {
                "shop_domain": "acme.myshopify.com",
                "access_token": "shpat_test_token",
            },
        ),
        (
            "hubspot_sync",
            "hubspot_demo",
            {
                "HUBSPOT_ACCESS_TOKEN": "pat_test_token",
            },
            ["contacts", "companies", "deals"],
            {
                "access_token": "pat_test_token",
            },
        ),
    ],
)
def test_saas_connector_example_configs_build_runtime_connectors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    example_name: str,
    connector_name: str,
    env_vars: dict[str, str],
    expected_resources: list[str],
    expected_fields: dict[str, str],
) -> None:
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)

    repo_root = Path(__file__).resolve().parents[2]
    source_config_path = repo_root / "examples" / example_name / "langbridge_config.yml"
    config_dir = tmp_path / example_name
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "langbridge_config.yml"
    config_path.write_text(source_config_path.read_text(encoding="utf-8"), encoding="utf-8")

    runtime = build_configured_local_runtime(config_path=config_path)
    connectors = asyncio.run(runtime.list_connectors())

    assert connectors == [
        {
            "id": runtime._connectors[connector_name].id,
            "name": connector_name,
            "description": runtime._connectors[connector_name].description,
            "connector_type": runtime._connectors[connector_name].connector_type,
            "connector_family": runtime._connectors[connector_name].connector_family,
            "supports_sync": True,
            "supported_resources": expected_resources,
            "sync_strategy": "INCREMENTAL",
            "capabilities": {
                "supports_live_datasets": False,
                "supports_synced_datasets": True,
                "supports_incremental_sync": True,
                "supports_query_pushdown": False,
                "supports_preview": False,
                "supports_federated_execution": False,
            },
            "management_mode": "config_managed",
            "managed": True,
        }
    ]

    api_connector = runtime._build_api_connector(runtime._connectors[connector_name])
    for field_name, expected_value in expected_fields.items():
        assert getattr(api_connector.config, field_name) == expected_value
