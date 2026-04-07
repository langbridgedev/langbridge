
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from langbridge.runtime.persistence.db.dataset import DatasetColumnRecord, DatasetRecord
from langbridge.runtime.persistence.db.workspace import Workspace  # noqa: F401
from langbridge.runtime.models import LifecycleState, ManagementMode, SemanticModelMetadata
from langbridge.orchestrator.definitions.factory import AgentDefinitionFactory
from langbridge.orchestrator.definitions.model import DataAccessPolicy
from langbridge.orchestrator.runtime.access_policy import AnalyticalAccessScope
from langbridge.orchestrator.runtime.agent_orchestrator_factory import (
    AgentOrchestratorFactory,
    AgentToolConfig,
    AnalystBinding,
)
from langbridge.orchestrator.tools.sql_analyst.interfaces import AnalystQueryRequest
from langbridge.semantic.model import Dimension, SemanticModel, Table


class _StaticLLM:
    def __init__(self, sql: str) -> None:
        self._sql = sql

    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        _ = (prompt, temperature, max_tokens)
        return self._sql


class _SemanticModelStore:
    def __init__(self, entries: dict[uuid.UUID, SemanticModelMetadata]) -> None:
        self._entries = entries

    async def get_by_id(self, model_id: uuid.UUID) -> SemanticModelMetadata | None:
        return self._entries.get(model_id)

    async def get_by_ids(self, model_ids: list[uuid.UUID]) -> list[SemanticModelMetadata]:
        return [self._entries[model_id] for model_id in model_ids if model_id in self._entries]


class _DatasetRepository:
    def __init__(self, datasets: dict[uuid.UUID, DatasetRecord]) -> None:
        self._datasets = datasets

    async def get_by_id(self, dataset_id: uuid.UUID) -> DatasetRecord | None:
        return self._datasets.get(dataset_id)

    async def get_by_ids(self, dataset_ids: list[uuid.UUID]) -> list[DatasetRecord]:
        return [self._datasets[dataset_id] for dataset_id in dataset_ids if dataset_id in self._datasets]

    async def get_by_ids_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids: list[uuid.UUID],
    ) -> list[DatasetRecord]:
        return [
            dataset
            for dataset_id in dataset_ids
            if (dataset := self._datasets.get(dataset_id)) is not None and dataset.workspace_id == workspace_id
        ]

    async def get_for_workspace(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> DatasetRecord | None:
        dataset = self._datasets.get(dataset_id)
        if dataset is None or dataset.workspace_id != workspace_id:
            return None
        return dataset


class _DatasetColumnRepository:
    def __init__(self, by_dataset: dict[uuid.UUID, list[DatasetColumnRecord]]) -> None:
        self._by_dataset = by_dataset

    async def list_for_dataset(self, *, dataset_id: uuid.UUID) -> list[DatasetColumnRecord]:
        return list(self._by_dataset.get(dataset_id, []))


class _FederatedQueryTool:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def execute_federated_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(payload)
        return {
            "columns": ["order_id"],
            "rows": [{"order_id": 1}],
            "execution": {"total_runtime_ms": 13},
        }


_UNSET = object()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _build_dataset(
    *,
    dataset_id: uuid.UUID,
    workspace_id: uuid.UUID,
    name: str,
    sql_alias: str,
    connection_id: uuid.UUID | None | object = _UNSET,
    dataset_type: str = "TABLE",
    source_kind: str = "database",
    storage_kind: str = "table",
    materialization_mode: str = "live",
    dialect: str = "postgres",
    table_name: str | None = None,
    sql_text: str | None = None,
    source_json: dict[str, Any] | None = None,
    sync_json: dict[str, Any] | None = None,
) -> DatasetRecord:
    now = datetime.now(timezone.utc)
    resolved_connection_id = uuid.uuid4() if connection_id is _UNSET else connection_id
    return DatasetRecord(
        id=dataset_id,
        workspace_id=workspace_id,
        connection_id=resolved_connection_id,
        created_by=None,
        updated_by=None,
        name=name,
        sql_alias=sql_alias,
        description=f"{name} description",
        tags_json=["analytics"],
        dataset_type=dataset_type,
        materialization_mode=materialization_mode,
        source_json=source_json,
        sync_json=sync_json,
        source_kind=source_kind,
        connector_kind="postgres",
        storage_kind=storage_kind,
        dialect=dialect,
        catalog_name=None,
        schema_name="public",
        table_name=table_name or sql_alias,
        storage_uri=None,
        sql_text=sql_text,
        relation_identity_json={},
        execution_capabilities_json={},
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )


def _build_columns(*, dataset: DatasetRecord) -> list[DatasetColumnRecord]:
    return [
        DatasetColumnRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=dataset.workspace_id,
            name="order_id",
            data_type="integer",
            nullable=False,
            ordinal_position=1,
            description=None,
            is_allowed=True,
            is_computed=False,
            expression=None,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
    ]


def _build_semantic_entry(
    *,
    model_id: uuid.UUID,
    workspace_id: uuid.UUID,
    dataset: DatasetRecord,
    model_name: str = "orders_model",
    table_key: str = "orders",
    connector_id: uuid.UUID | None = None,
) -> SemanticModelMetadata:
    model = SemanticModel(
        version="1.0",
        name=model_name,
        tables={
            table_key: Table(
                dataset_id=str(dataset.id),
                schema="public",
                name=dataset.table_name or dataset.sql_alias,
                dimensions=[Dimension(name="order_id", type="integer", primary_key=True)],
            )
        },
    )
    now = datetime.now(timezone.utc)
    return SemanticModelMetadata(
        id=model_id,
        workspace_id=workspace_id,
        name=model_name,
        description=f"{model_name} governed model",
        content_yaml=model.yml_dump(),
        content_json=None,
        created_at=now,
        updated_at=now,
        connector_id=connector_id if connector_id is not None else dataset.connection_id,
        management_mode=ManagementMode.CONFIG_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )


@pytest.mark.anyio
async def test_agent_orchestrator_factory_builds_dataset_tool_for_federated_analysis() -> None:
    workspace_id = uuid.uuid4()
    dataset = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="orders_dataset",
        sql_alias="orders",
    )
    columns = _build_columns(dataset=dataset)
    federated_tool = _FederatedQueryTool()
    factory = AgentOrchestratorFactory(
        semantic_model_store=_SemanticModelStore(entries={}),
        dataset_repository=_DatasetRepository({dataset.id: dataset}),
        dataset_column_repository=_DatasetColumnRepository({dataset.id: columns}),
        federated_query_tool=federated_tool,
    )

    tool_build = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[AnalystBinding(name="orders", dataset_ids=[dataset.id])],
        ),
        access_policy=DataAccessPolicy(),
        llm_provider=_StaticLLM("SELECT order_id FROM orders"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )
    tools = tool_build.tools

    assert len(tools) == 1

    response = await tools[0].arun(AnalystQueryRequest(question="List orders"))

    assert response.error is None
    assert response.asset_type == "dataset"
    assert response.asset_name == "orders_dataset"
    assert response.result is not None
    assert response.result.rows == [(1,)]
    assert len(federated_tool.calls) == 1


@pytest.mark.anyio
async def test_agent_orchestrator_factory_builds_dataset_backed_semantic_tool() -> None:
    workspace_id = uuid.uuid4()
    dataset = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="orders_dataset",
        sql_alias="orders",
    )
    columns = _build_columns(dataset=dataset)
    model_id = uuid.uuid4()
    semantic_entry = _build_semantic_entry(
        model_id=model_id,
        workspace_id=workspace_id,
        dataset=dataset,
    )
    federated_tool = _FederatedQueryTool()
    factory = AgentOrchestratorFactory(
        semantic_model_store=_SemanticModelStore(entries={model_id: semantic_entry}),
        dataset_repository=_DatasetRepository({dataset.id: dataset}),
        dataset_column_repository=_DatasetColumnRepository({dataset.id: columns}),
        federated_query_tool=federated_tool,
    )

    tool_build = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[AnalystBinding(name="orders_model", semantic_model_ids=[model_id])],
        ),
        access_policy=DataAccessPolicy(),
        llm_provider=_StaticLLM("SELECT orders.order_id FROM orders"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )
    tools = tool_build.tools

    assert len(tools) == 1

    response = await tools[0].arun(AnalystQueryRequest(question="List orders"))

    assert response.error is None
    assert response.asset_type == "semantic_model"
    assert response.asset_name == "orders_model"
    assert response.result is not None
    assert response.result.rows == [(1,)]
    assert len(federated_tool.calls) == 1


@pytest.mark.anyio
async def test_agent_orchestrator_factory_infers_sql_projection_columns_when_dataset_metadata_is_empty() -> None:
    workspace_id = uuid.uuid4()
    sql_text = """
        SELECT
          CAST(customer_id AS TEXT) || '|' || substr(opened_date, 1, 7) AS customer_month_key,
          customer_id,
          date(substr(opened_date, 1, 7) || '-01') AS support_month,
          MIN(ticket_type) AS dominant_ticket_type,
          COUNT(*) AS ticket_count,
          SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_severity_tickets,
          SUM(resolution_hours) AS resolution_hours_total
        FROM support_tickets
        GROUP BY 1, 2, 3
    """
    dataset = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="customer_month_support",
        sql_alias="customer_month_support",
        dataset_type="SQL",
        storage_kind="view",
        dialect="sqlite",
        sql_text=sql_text,
        source_json={"sql": sql_text},
        table_name="customer_month_support",
    )
    factory = AgentOrchestratorFactory(
        semantic_model_store=_SemanticModelStore(entries={}),
        dataset_repository=_DatasetRepository({dataset.id: dataset}),
        dataset_column_repository=_DatasetColumnRepository({}),
        federated_query_tool=_FederatedQueryTool(),
    )

    tool_build = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[AnalystBinding(name="support", dataset_ids=[dataset.id])],
        ),
        access_policy=DataAccessPolicy(),
        llm_provider=_StaticLLM("SELECT ticket_count FROM customer_month_support"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )

    assert len(tool_build.tools) == 1
    dataset_binding = tool_build.tools[0].context.datasets[0]
    assert [column.name for column in dataset_binding.columns] == [
        "customer_month_key",
        "customer_id",
        "support_month",
        "dominant_ticket_type",
        "ticket_count",
        "high_severity_tickets",
        "resolution_hours_total",
    ]
    assert [field.name for field in tool_build.tools[0].context.dimensions] == [
        f"{dataset_binding.sql_alias}.customer_month_key",
        f"{dataset_binding.sql_alias}.customer_id",
        f"{dataset_binding.sql_alias}.support_month",
        f"{dataset_binding.sql_alias}.dominant_ticket_type",
        f"{dataset_binding.sql_alias}.ticket_count",
        f"{dataset_binding.sql_alias}.high_severity_tickets",
        f"{dataset_binding.sql_alias}.resolution_hours_total",
    ]


@pytest.mark.anyio
async def test_agent_orchestrator_factory_builds_multiple_semantic_tools_from_one_binding() -> None:
    workspace_id = uuid.uuid4()
    orders = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="orders_dataset",
        sql_alias="orders",
    )
    customers = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="customers_dataset",
        sql_alias="customers",
    )
    model_a_id = uuid.uuid4()
    model_b_id = uuid.uuid4()
    federated_tool = _FederatedQueryTool()
    factory = AgentOrchestratorFactory(
        semantic_model_store=_SemanticModelStore(
            entries={
                model_a_id: _build_semantic_entry(
                    model_id=model_a_id,
                    workspace_id=workspace_id,
                    dataset=orders,
                    model_name="orders_model",
                    table_key="orders",
                ),
                model_b_id: _build_semantic_entry(
                    model_id=model_b_id,
                    workspace_id=workspace_id,
                    dataset=customers,
                    model_name="customers_model",
                    table_key="customers",
                ),
            }
        ),
        dataset_repository=_DatasetRepository({orders.id: orders, customers.id: customers}),
        dataset_column_repository=_DatasetColumnRepository(
            {
                orders.id: _build_columns(dataset=orders),
                customers.id: _build_columns(dataset=customers),
            }
        ),
        federated_query_tool=federated_tool,
    )

    tool_build = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[
                AnalystBinding(
                    name="governed_sql",
                    semantic_model_ids=[model_a_id, model_b_id],
                )
            ],
        ),
        access_policy=DataAccessPolicy(),
        llm_provider=_StaticLLM("SELECT order_id FROM orders"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )
    tools = tool_build.tools

    assert len(tools) == 2
    assert {tool.context.asset_name for tool in tools} == {"orders_model", "customers_model"}


@pytest.mark.anyio
async def test_agent_orchestrator_factory_filters_dataset_tools_by_allowed_connectors() -> None:
    workspace_id = uuid.uuid4()
    allowed_connector_id = uuid.uuid4()
    denied_connector_id = uuid.uuid4()
    orders = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="orders_dataset",
        sql_alias="orders",
        connection_id=allowed_connector_id,
    )
    customers = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="customers_dataset",
        sql_alias="customers",
        connection_id=denied_connector_id,
    )
    factory = AgentOrchestratorFactory(
        semantic_model_store=_SemanticModelStore(entries={}),
        dataset_repository=_DatasetRepository({orders.id: orders, customers.id: customers}),
        dataset_column_repository=_DatasetColumnRepository(
            {
                orders.id: _build_columns(dataset=orders),
                customers.id: _build_columns(dataset=customers),
            }
        ),
        federated_query_tool=_FederatedQueryTool(),
    )

    tool_build = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[
                AnalystBinding(name="orders", dataset_ids=[orders.id]),
                AnalystBinding(name="customers", dataset_ids=[customers.id]),
            ],
        ),
        access_policy=DataAccessPolicy(allowed_connectors=[allowed_connector_id]),
        llm_provider=_StaticLLM("SELECT order_id FROM orders"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )

    assert [tool.context.asset_name for tool in tool_build.tools] == ["orders_dataset"]
    assert tool_build.access_scope.authorized_asset_count == 1
    assert tool_build.access_scope.denied_asset_count == 1
    denied_asset = tool_build.access_scope.denied_assets[0]
    assert denied_asset.asset_name == "customers_dataset"
    assert denied_asset.policy_rule == "outside_allowed_connectors"


@pytest.mark.anyio
async def test_agent_orchestrator_factory_denied_connectors_override_allowed_scope() -> None:
    workspace_id = uuid.uuid4()
    allowed_connector_id = uuid.uuid4()
    denied_connector_id = uuid.uuid4()
    orders = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="orders_dataset",
        sql_alias="orders",
        connection_id=allowed_connector_id,
    )
    customers = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="customers_dataset",
        sql_alias="customers",
        connection_id=denied_connector_id,
    )
    factory = AgentOrchestratorFactory(
        semantic_model_store=_SemanticModelStore(entries={}),
        dataset_repository=_DatasetRepository({orders.id: orders, customers.id: customers}),
        dataset_column_repository=_DatasetColumnRepository(
            {
                orders.id: _build_columns(dataset=orders),
                customers.id: _build_columns(dataset=customers),
            }
        ),
        federated_query_tool=_FederatedQueryTool(),
    )

    tool_build = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[
                AnalystBinding(name="governed_scope", dataset_ids=[orders.id, customers.id]),
            ],
        ),
        access_policy=DataAccessPolicy(
            allowed_connectors=[allowed_connector_id, denied_connector_id],
            denied_connectors=[denied_connector_id],
        ),
        llm_provider=_StaticLLM("SELECT order_id FROM orders"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )

    assert tool_build.tools == []
    assert tool_build.access_scope.authorized_asset_count == 0
    assert tool_build.access_scope.denied_asset_count == 1
    denied_asset = tool_build.access_scope.denied_assets[0]
    assert denied_asset.policy_rule == "denied_connectors"
    assert [str(item) for item in denied_asset.denied_connector_ids] == [str(denied_connector_id)]


@pytest.mark.anyio
async def test_agent_orchestrator_factory_excludes_semantic_model_when_backing_dataset_connector_is_denied() -> None:
    workspace_id = uuid.uuid4()
    visible_connector_id = uuid.uuid4()
    denied_connector_id = uuid.uuid4()
    dataset = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="orders_dataset",
        sql_alias="orders",
        connection_id=denied_connector_id,
    )
    model_id = uuid.uuid4()
    semantic_entry = _build_semantic_entry(
        model_id=model_id,
        workspace_id=workspace_id,
        dataset=dataset,
        connector_id=visible_connector_id,
    )
    factory = AgentOrchestratorFactory(
        semantic_model_store=_SemanticModelStore(entries={model_id: semantic_entry}),
        dataset_repository=_DatasetRepository({dataset.id: dataset}),
        dataset_column_repository=_DatasetColumnRepository({dataset.id: _build_columns(dataset=dataset)}),
        federated_query_tool=_FederatedQueryTool(),
    )

    tool_build = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[AnalystBinding(name="orders_model", semantic_model_ids=[model_id])],
        ),
        access_policy=DataAccessPolicy(
            allowed_connectors=[visible_connector_id],
            denied_connectors=[denied_connector_id],
        ),
        llm_provider=_StaticLLM("SELECT orders.order_id FROM orders"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )

    assert tool_build.tools == []
    assert tool_build.access_scope.denied_asset_count == 1
    denied_asset = tool_build.access_scope.denied_assets[0]
    assert denied_asset.asset_name == "orders_model"
    assert denied_asset.policy_rule == "denied_connectors"


@pytest.mark.anyio
async def test_agent_orchestrator_factory_blocks_unknown_connector_ownership_conservatively() -> None:
    workspace_id = uuid.uuid4()
    unresolved_dataset = _build_dataset(
        dataset_id=uuid.uuid4(),
        workspace_id=workspace_id,
        name="local_upload",
        sql_alias="local_upload",
        connection_id=None,
    )
    factory = AgentOrchestratorFactory(
        semantic_model_store=_SemanticModelStore(entries={}),
        dataset_repository=_DatasetRepository({unresolved_dataset.id: unresolved_dataset}),
        dataset_column_repository=_DatasetColumnRepository(
            {unresolved_dataset.id: _build_columns(dataset=unresolved_dataset)}
        ),
        federated_query_tool=_FederatedQueryTool(),
    )

    tool_build = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[AnalystBinding(name="uploads", dataset_ids=[unresolved_dataset.id])],
        ),
        access_policy=DataAccessPolicy(allowed_connectors=[uuid.uuid4()]),
        llm_provider=_StaticLLM("SELECT order_id FROM local_upload"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )

    assert tool_build.tools == []
    assert tool_build.access_scope.denied_asset_count == 1
    assert tool_build.access_scope.denied_assets[0].policy_rule == "unknown_connector_ownership"


def test_build_supervisor_orchestrator_wires_response_presentation_from_definition() -> None:
    definition = AgentDefinitionFactory().create_agent_definition(
        {
            "prompt": {"system_prompt": "Factory system prompt"},
            "memory": {"strategy": "none"},
            "features": {
                "bi_copilot_enabled": False,
                "deep_research_enabled": False,
                "visualization_enabled": False,
                "mcp_enabled": False,
            },
            "execution": {
                "mode": "single_step",
                "response_mode": "explainer",
                "max_iterations": 1,
            },
            "output": {"format": "markdown", "markdown_template": "## Reply"},
            "guardrails": {
                "moderation_enabled": True,
                "regex_denylist": ["secret"],
                "escalation_message": "Blocked",
            },
            "observability": {
                "log_level": "info",
                "emit_traces": True,
                "capture_prompts": True,
            },
        }
    )
    factory = AgentOrchestratorFactory(
        semantic_model_store=_SemanticModelStore(entries={}),
        dataset_repository=None,
        dataset_column_repository=None,
        federated_query_tool=None,
    )
    tool_config = AgentToolConfig(
        allow_sql=False,
        allow_web_search=False,
        allow_deep_research=False,
        allow_visualization=False,
    )

    supervisor = factory._build_supervisor_orchestrator(  # noqa: SLF001
        definition=definition,
        llm_provider=_StaticLLM("SELECT 1"),  # type: ignore[arg-type]
        planning_constraints=factory._build_planning_constraints(tool_config, definition),  # noqa: SLF001
        analyst_tools=[],
        analytical_access_scope=AnalyticalAccessScope(),
        event_emitter=None,
    )

    assert supervisor.response_presentation.prompt_contract is not None
    assert supervisor.response_presentation.prompt_contract.system_prompt == "Factory system prompt"
    assert supervisor.response_presentation.output_schema is not None
    assert supervisor.response_presentation.output_schema.markdown_template == "## Reply"
    assert supervisor.response_presentation.guardrails is not None
    assert supervisor.response_presentation.guardrails.escalation_message == "Blocked"
    assert supervisor.response_presentation.response_mode.value == "explainer"
