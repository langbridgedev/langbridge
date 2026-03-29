
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from langbridge.runtime.persistence.db.dataset import DatasetColumnRecord, DatasetRecord
from langbridge.runtime.models import LifecycleState, ManagementMode, SemanticModelMetadata
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


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _build_dataset(
    *,
    dataset_id: uuid.UUID,
    workspace_id: uuid.UUID,
    name: str,
    sql_alias: str,
) -> DatasetRecord:
    now = datetime.now(timezone.utc)
    return DatasetRecord(
        id=dataset_id,
        workspace_id=workspace_id,
        connection_id=uuid.uuid4(),
        created_by=None,
        updated_by=None,
        name=name,
        sql_alias=sql_alias,
        description=f"{name} description",
        tags_json=["analytics"],
        dataset_type="TABLE",
        source_kind="database",
        connector_kind="postgres",
        storage_kind="table",
        dialect="postgres",
        catalog_name=None,
        schema_name="public",
        table_name=sql_alias,
        storage_uri=None,
        sql_text=None,
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
        connector_id=dataset.connection_id,
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

    tools = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[AnalystBinding(name="orders", dataset_ids=[dataset.id])],
        ),
        llm_provider=_StaticLLM("SELECT order_id FROM orders"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )

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

    tools = await factory._build_analyst_tools(  # noqa: SLF001
        tool_config=AgentToolConfig(
            allow_sql=True,
            allow_web_search=False,
            allow_deep_research=False,
            allow_visualization=False,
            analyst_bindings=[AnalystBinding(name="orders_model", semantic_model_ids=[model_id])],
        ),
        llm_provider=_StaticLLM("SELECT orders.order_id FROM orders"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )

    assert len(tools) == 1

    response = await tools[0].arun(AnalystQueryRequest(question="List orders"))

    assert response.error is None
    assert response.asset_type == "semantic_model"
    assert response.asset_name == "orders_model"
    assert response.result is not None
    assert response.result.rows == [(1,)]
    assert len(federated_tool.calls) == 1


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

    tools = await factory._build_analyst_tools(  # noqa: SLF001
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
        llm_provider=_StaticLLM("SELECT order_id FROM orders"),  # type: ignore[arg-type]
        embedding_provider=None,
        event_emitter=None,
    )

    assert len(tools) == 2
    assert {tool.context.asset_name for tool in tools} == {"orders_model", "customers_model"}
