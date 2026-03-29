
import uuid

import pytest

from langbridge.runtime import RuntimeContext
from langbridge.runtime.services.runtime_host import (
    RuntimeHost,
    RuntimeProviders,
    RuntimeServices,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _DatasetQueryService:
    async def query_dataset(self, **kwargs):
        return {"kind": "dataset", **kwargs}


class _SqlQueryService:
    async def execute_sql(self, **kwargs):
        return {"kind": "sql", **kwargs}


class _DatasetSyncService:
    async def sync_resource(self, **kwargs):
        return {"kind": "sync", **kwargs}


class _AgentExecutionService:
    async def execute(self, **kwargs):
        return {"kind": "agent", **kwargs}


class _SemanticQueryService:
    async def execute_standard_query(self, **kwargs):
        return {"kind": "semantic", **kwargs}


def test_runtime_context_build_is_workspace_scoped() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()

    context = RuntimeContext.build(
        workspace_id=workspace_id,
        actor_id=actor_id,
        roles=["runtime:viewer"],
        request_id="req-runtime-context",
    )

    assert context.workspace_id == workspace_id
    assert context.actor_id == actor_id
    assert context.roles == ("runtime:viewer",)
    assert context.request_id == "req-runtime-context"


@pytest.mark.anyio
async def test_runtime_host_delegates_to_runtime_services() -> None:
    context = RuntimeContext.build(
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        roles=["admin"],
        request_id="req-runtime-host",
    )
    host = RuntimeHost(
        context=context,
        providers=RuntimeProviders(),
        services=RuntimeServices(
            dataset_query=_DatasetQueryService(),
            sql_query=_SqlQueryService(),
            dataset_sync=_DatasetSyncService(),
            agent_execution=_AgentExecutionService(),
            semantic_query=_SemanticQueryService(),
        ),
    )

    dataset_result = await host.query_dataset(request="preview")
    sql_result = await host.execute_sql(query="select 1")
    sync_result = await host.sync_dataset(resource="orders")
    agent_result = await host.create_agent(prompt="hello")
    semantic_result = await host.query_semantic(metric="revenue")

    assert dataset_result["kind"] == "dataset"
    assert sql_result["kind"] == "sql"
    assert sync_result["kind"] == "sync"
    assert agent_result["kind"] == "agent"
    assert semantic_result["kind"] == "semantic"
