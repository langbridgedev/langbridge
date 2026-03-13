import json
import logging
from datetime import datetime, timezone

from pydantic import ValidationError

from langbridge.apps.worker.langbridge_worker.handlers.jobs.job_event_emitter import (
    BrokerJobEventEmitter,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.agent_job import (
    CreateAgentJobRequest,
)
from langbridge.packages.common.langbridge_common.db.job import JobRecord, JobStatus
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.interfaces.agent_events import (
    AgentEventVisibility,
)
from langbridge.packages.common.langbridge_common.interfaces.semantic_models import (
    ISemanticModelStore,
)
from langbridge.packages.common.langbridge_common.repositories.agent_repository import AgentRepository
from langbridge.packages.common.langbridge_common.repositories.conversation_memory_repository import (
    ConversationMemoryRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetColumnRepository,
    DatasetRepository,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.common.langbridge_common.repositories.llm_connection_repository import (
    LLMConnectionRepository,
)
from langbridge.packages.common.langbridge_common.repositories.thread_message_repository import (
    ThreadMessageRepository,
)
from langbridge.packages.common.langbridge_common.repositories.thread_repository import ThreadRepository
from langbridge.packages.messaging.langbridge_messaging.broker.base import MessageBroker
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.agent_job import (
    AgentJobRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler
from langbridge.packages.runtime.context import RuntimeContext
from langbridge.packages.runtime.execution import FederatedQueryTool
from langbridge.packages.runtime.services.runtime_host import (
    RuntimeHost,
    RuntimeProviders,
    RuntimeServices,
)
from langbridge.packages.runtime.services.agent_execution_service import (
    AgentExecutionService,
)


class AgentJobRequestHandler(BaseMessageHandler):
    message_type: MessageType = MessageType.AGENT_JOB_REQUEST

    def __init__(
        self,
        job_repository: JobRepository,
        agent_definition_repository: AgentRepository,
        llm_repository: LLMConnectionRepository,
        semantic_model_store: ISemanticModelStore,
        dataset_repository: DatasetRepository,
        dataset_column_repository: DatasetColumnRepository,
        thread_repository: ThreadRepository,
        thread_message_repository: ThreadMessageRepository,
        memory_repository: ConversationMemoryRepository,
        message_broker: MessageBroker,
        federated_query_tool: FederatedQueryTool | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._job_repository = job_repository
        self._message_broker = message_broker
        self._agent_execution_service = AgentExecutionService(
            agent_definition_repository=agent_definition_repository,
            llm_repository=llm_repository,
            semantic_model_store=semantic_model_store,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            thread_repository=thread_repository,
            thread_message_repository=thread_message_repository,
            memory_repository=memory_repository,
            federated_query_tool=federated_query_tool,
        )

    async def handle(self, payload: AgentJobRequestMessage) -> None:
        self._logger.info(
            "Received agent job request with ID %s and type %s",
            payload.job_id,
            payload.job_type,
        )

        job_record = await self._job_repository.get_by_id(payload.job_id)
        if job_record is None:
            raise BusinessValidationError(f"Job with ID {payload.job_id} does not exist.")

        if job_record.status in {
            JobStatus.succeeded,
            JobStatus.failed,
            JobStatus.cancelled,
        }:
            self._logger.info(
                "Job %s already in terminal state %s; skipping.",
                job_record.id,
                job_record.status,
            )
            return None

        event_emitter = BrokerJobEventEmitter(
            job_record=job_record,
            broker_client=self._message_broker,
            logger=self._logger,
        )
        job_record.status = JobStatus.running
        if job_record.started_at is None:
            job_record.started_at = datetime.now(timezone.utc)
        await event_emitter.emit(
            event_type="AgentJobStarted",
            message="Agent job started.",
            visibility=AgentEventVisibility.public,
            source="worker",
            details={"job_id": str(job_record.id)},
        )

        request: CreateAgentJobRequest | None = None
        try:
            request = self._parse_job_payload(job_record)
            runtime = RuntimeHost(
                context=RuntimeContext.build(
                    tenant_id=request.organisation_id,
                    workspace_id=request.organisation_id,
                    user_id=request.user_id,
                    request_id=str(job_record.id),
                ),
                providers=RuntimeProviders(),
                services=RuntimeServices(agent_execution=self._agent_execution_service),
            )
            execution = await runtime.create_agent(
                job_id=job_record.id,
                request=request,
                event_emitter=event_emitter,
            )
            job_record.result = execution.response
            job_record.status = JobStatus.succeeded
            job_record.progress = 100
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = None
            await event_emitter.emit(
                event_type="AgentJobCompleted",
                message="Agent job completed.",
                visibility=AgentEventVisibility.public,
                source="worker",
                details={"job_id": str(job_record.id)},
            )
        except Exception as exc:  # pragma: no cover - defensive guard for background jobs
            self._logger.exception("Agent job %s failed: %s", job_record.id, exc)
            job_record.status = JobStatus.failed
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = {"message": str(exc)}
            if request is not None:
                await self._agent_execution_service.reset_thread_after_failure(
                    thread_id=request.thread_id
                )
            await event_emitter.emit(
                event_type="AgentJobFailed",
                message="Agent job failed.",
                visibility=AgentEventVisibility.public,
                source="worker",
                details={"job_id": str(job_record.id), "error": str(exc)},
            )

        return None

    def _parse_job_payload(self, job_record: JobRecord) -> CreateAgentJobRequest:
        raw_payload = job_record.payload

        if isinstance(raw_payload, str):
            try:
                payload_data = json.loads(raw_payload)
            except json.JSONDecodeError as exc:
                raise BusinessValidationError(
                    f"Job payload for {job_record.id} is not valid JSON."
                ) from exc
        elif isinstance(raw_payload, dict):
            payload_data = raw_payload
        else:
            raise BusinessValidationError(
                f"Job payload for {job_record.id} must be an object or JSON string."
            )

        try:
            return CreateAgentJobRequest.model_validate(payload_data)
        except ValidationError as exc:
            raise BusinessValidationError(
                f"Job payload for {job_record.id} is invalid for agent execution."
            ) from exc
