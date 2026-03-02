from __future__ import annotations

import uuid
from dataclasses import dataclass

import pytest

from langbridge.apps.api.langbridge_api.services.task_dispatch_service import TaskDispatchService
from langbridge.packages.common.langbridge_common.contracts.runtime import ExecutionMode
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.messaging.langbridge_messaging.contracts.base import TestMessagePayload
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.semantic_query import (
    SemanticQueryRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.sql_job import (
    SqlJobRequestMessage,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@dataclass
class _FakeRequestContextProvider:
    correlation_id: str | None = "corr-1"


class _FakeExecutionRoutingService:
    def __init__(self, mode: ExecutionMode) -> None:
        self.mode = mode

    async def get_mode_for_tenant(self, tenant_id):
        return self.mode


class _FakeMessageService:
    def __init__(self) -> None:
        self.outbox_payloads = []

    async def create_outbox_message(self, payload):
        self.outbox_payloads.append(payload)


class _FakeRuntimeRegistryService:
    def __init__(self, runtime_id: uuid.UUID) -> None:
        self._runtime_id = runtime_id

    async def select_runtime_for_dispatch(self, *, tenant_id, message_type, required_tags=None):
        class _Runtime:
            id = self._runtime_id

        return _Runtime()


class _FakeEdgeTaskGatewayService:
    def __init__(self) -> None:
        self.enqueued = []

    async def enqueue_for_runtime(self, *, tenant_id, runtime_id, envelope):
        self.enqueued.append((tenant_id, runtime_id, envelope))


@pytest.mark.anyio
async def test_dispatch_service_routes_hosted_to_outbox() -> None:
    tenant_id = uuid.uuid4()
    message_service = _FakeMessageService()
    edge_gateway = _FakeEdgeTaskGatewayService()
    service = TaskDispatchService(
        execution_routing_service=_FakeExecutionRoutingService(ExecutionMode.hosted),
        message_service=message_service,
        runtime_registry_service=_FakeRuntimeRegistryService(uuid.uuid4()),
        edge_task_gateway_service=edge_gateway,
        request_context_provider=_FakeRequestContextProvider(),
    )

    mode = await service.dispatch_job_message(
        tenant_id=tenant_id,
        payload=TestMessagePayload(message="hello"),
    )
    assert mode == ExecutionMode.hosted
    assert len(message_service.outbox_payloads) == 1
    assert edge_gateway.enqueued == []


@pytest.mark.anyio
async def test_dispatch_service_routes_customer_runtime_to_edge_queue() -> None:
    tenant_id = uuid.uuid4()
    runtime_id = uuid.uuid4()
    message_service = _FakeMessageService()
    edge_gateway = _FakeEdgeTaskGatewayService()
    service = TaskDispatchService(
        execution_routing_service=_FakeExecutionRoutingService(ExecutionMode.customer_runtime),
        message_service=message_service,
        runtime_registry_service=_FakeRuntimeRegistryService(runtime_id),
        edge_task_gateway_service=edge_gateway,
        request_context_provider=_FakeRequestContextProvider(correlation_id="corr-2"),
    )

    mode = await service.dispatch_job_message(
        tenant_id=tenant_id,
        payload=SemanticQueryRequestMessage(
            job_id=uuid.uuid4(),
            job_type=JobType.SEMANTIC_QUERY,
            job_request={"query_scope": "semantic_model"},
            semantic_model_yaml="name: model\nmeasures: []\n",
            connector={"connectorType": "POSTGRES", "config": {"config": {}}},
        ),
    )
    assert mode == ExecutionMode.customer_runtime
    assert message_service.outbox_payloads == []
    assert len(edge_gateway.enqueued) == 1
    queued_tenant, queued_runtime, envelope = edge_gateway.enqueued[0]
    assert queued_tenant == tenant_id
    assert queued_runtime == runtime_id
    assert envelope.headers.correlation_id == "corr-2"


@pytest.mark.anyio
async def test_dispatch_service_routes_sql_jobs_to_edge_queue_for_customer_runtime() -> None:
    tenant_id = uuid.uuid4()
    runtime_id = uuid.uuid4()
    message_service = _FakeMessageService()
    edge_gateway = _FakeEdgeTaskGatewayService()
    service = TaskDispatchService(
        execution_routing_service=_FakeExecutionRoutingService(ExecutionMode.customer_runtime),
        message_service=message_service,
        runtime_registry_service=_FakeRuntimeRegistryService(runtime_id),
        edge_task_gateway_service=edge_gateway,
        request_context_provider=_FakeRequestContextProvider(correlation_id="corr-sql"),
    )

    mode = await service.dispatch_job_message(
        tenant_id=tenant_id,
        payload=SqlJobRequestMessage(
            sql_job_id=uuid.uuid4(),
            job_type=JobType.SQL,
            job_request={
                "sql_job_id": str(uuid.uuid4()),
                "workspace_id": str(tenant_id),
                "user_id": str(uuid.uuid4()),
                "connection_id": str(uuid.uuid4()),
                "query": "SELECT 1",
                "enforced_limit": 1000,
                "enforced_timeout_seconds": 30,
            },
        ),
    )

    assert mode == ExecutionMode.customer_runtime
    assert message_service.outbox_payloads == []
    assert len(edge_gateway.enqueued) == 1
    queued_tenant, queued_runtime, envelope = edge_gateway.enqueued[0]
    assert queued_tenant == tenant_id
    assert queued_runtime == runtime_id
    assert envelope.headers.correlation_id == "corr-sql"
