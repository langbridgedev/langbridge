from __future__ import annotations

import uuid

import pytest

from langbridge.packages.runtime import RuntimeContext
from langbridge.packages.runtime.registry.bootstrap import build_hosted_runtime
from langbridge.packages.runtime.services.runtime_host import (
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


@pytest.mark.anyio
async def test_runtime_host_delegates_to_runtime_services() -> None:
    context = RuntimeContext.build(
        tenant_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
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


def test_build_hosted_runtime_exposes_dataset_and_sql_services() -> None:
    host = build_hosted_runtime(
        context=RuntimeContext.build(tenant_id=uuid.uuid4()),
        control_plane_base_url="https://control-plane.example.com",
        service_token="runtime-service-token",
    )

    assert host.services.dataset_query is not None
    assert host.services.sql_query is not None
    assert host.services.semantic_query is not None
