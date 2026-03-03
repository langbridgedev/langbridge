from __future__ import annotations

import uuid
from datetime import datetime, timezone

from langbridge.apps.api.langbridge_api.services.edge_task_gateway_service import (
    EdgeTaskGatewayService,
)
from langbridge.apps.api.langbridge_api.services.execution_routing_service import (
    ExecutionRoutingService,
)
from langbridge.apps.api.langbridge_api.services.message.message_serivce import MessageService
from langbridge.apps.api.langbridge_api.services.request_context_provider import (
    RequestContextProvider,
)
from langbridge.apps.api.langbridge_api.services.runtime_registry_service import (
    RuntimeRegistryService,
)
from langbridge.packages.common.langbridge_common.contracts.runtime import ExecutionMode
from langbridge.packages.messaging.langbridge_messaging.contracts import (
    BaseMessagePayload,
    MessageHeaders,
)
from langbridge.packages.messaging.langbridge_messaging.contracts.messages import MessageEnvelope


class TaskDispatchService:
    def __init__(
        self,
        execution_routing_service: ExecutionRoutingService,
        message_service: MessageService,
        runtime_registry_service: RuntimeRegistryService,
        edge_task_gateway_service: EdgeTaskGatewayService,
        request_context_provider: RequestContextProvider,
    ) -> None:
        self._execution_routing_service = execution_routing_service
        self._message_service = message_service
        self._runtime_registry_service = runtime_registry_service
        self._edge_task_gateway_service = edge_task_gateway_service
        self._request_context_provider = request_context_provider

    async def dispatch_job_message(
        self,
        *,
        tenant_id: uuid.UUID,
        payload: BaseMessagePayload,
        required_tags: list[str] | None = None,
    ) -> ExecutionMode:
        mode = await self._execution_routing_service.get_mode_for_tenant(tenant_id)
        edge_eligible_types = {
            "semantic_query_request",
            "sql_job_request",
            "agentic_semantic_model_job_request",
        }
        if (
            mode == ExecutionMode.hosted
            or payload.message_type.value not in edge_eligible_types
        ):
            await self._message_service.create_outbox_message(payload=payload)
            return ExecutionMode.hosted if mode != ExecutionMode.hosted else mode

        headers: MessageHeaders = MessageHeaders.default().model_copy(
            update={
                "organisation_id": str(tenant_id),
                "correlation_id": self._request_context_provider.correlation_id,
            }
        )
        envelope = MessageEnvelope(
            message_type=payload.message_type,
            payload=payload,
            headers=headers,
            created_at=datetime.now(tz=timezone.utc),
        )
        runtime = await self._runtime_registry_service.select_runtime_for_dispatch(
            tenant_id=tenant_id,
            message_type=payload.message_type.value,
            required_tags=required_tags,
        )
        await self._edge_task_gateway_service.enqueue_for_runtime(
            tenant_id=tenant_id,
            runtime_id=runtime.id,
            envelope=envelope,
        )
        return mode
