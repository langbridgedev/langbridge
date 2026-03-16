from __future__ import annotations

import logging
from typing import Any

from langbridge.packages.common.langbridge_common.db.job import (
    JobEventRecord,
    JobEventVisibility as JobEventRecordVisibility,
    JobRecord,
)
from langbridge.packages.common.langbridge_common.interfaces.agent_events import (
    AgentEventVisibility,
    IAgentEventEmitter,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.messaging.langbridge_messaging.broker.base import MessageBroker
from langbridge.packages.messaging.langbridge_messaging.broker.redis import RedisStreams
from langbridge.packages.messaging.langbridge_messaging.contracts import MessageHeaders
from langbridge.packages.messaging.langbridge_messaging.contracts.messages import MessageEnvelope
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.event import JobEventMessage

class SqlJobEventEmitter(IAgentEventEmitter):
    """Persists agent/tool events onto a job record for user progress + auditing."""

    def __init__(
        self,
        *,
        job_record: JobRecord,
        job_repository: JobRepository,
        logger: logging.Logger | None = None,
    ) -> None:
        self._job_record = job_record
        self._job_repository = job_repository
        self._logger = logger or logging.getLogger(__name__)

    async def emit(
        self,
        *,
        event_type: str,
        message: str,
        visibility: AgentEventVisibility = AgentEventVisibility.internal,
        source: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "visibility": visibility.value,
            "message": message,
            "source": source or "agent-runtime",
            "details": details or {},
        }
        self._job_repository.add_job_event(
            JobEventRecord(
                job_id=self._job_record.id,
                event_type=event_type,
                details=payload,
                visibility=(
                    JobEventRecordVisibility.public
                    if visibility == AgentEventVisibility.public
                    else JobEventRecordVisibility.internal
                ),
            )
        )

        # Public events are committed immediately so clients can stream progress.
        if visibility == AgentEventVisibility.public:
            try:
                await self._job_repository.flush()
                await self._job_repository.commit()
            except Exception as exc:  # pragma: no cover - defensive guard for event persistence
                self._logger.warning("Failed to persist public job event '%s': %s", event_type, exc)


class BrokerJobEventEmitter(IAgentEventEmitter):
    """Emits job events via a message broker."""
    
    def __init__(
        self,
        *,
        job_record: JobRecord,
        broker_client: MessageBroker,
        logger: logging.Logger | None = None,
    ) -> None:
        self._job_record = job_record
        self._broker_client = broker_client
        self._logger = logger or logging.getLogger(__name__)

    async def emit(
        self,
        *,
        event_type: str,
        message: str,
        visibility: AgentEventVisibility = AgentEventVisibility.internal,
        source: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        payload = JobEventMessage(
            job_id=self._job_record.id,
            event_type=event_type,
            message=message,
            visibility=visibility.value,
            source=source or "agent-runtime",
            details=details or {},
        )
        envelope = MessageEnvelope(
            message_type=payload.message_type,
            payload=payload,
            headers=MessageHeaders.default().model_copy(
                update={"organisation_id": str(self._job_record.organisation_id)}
            ),
        )
        try:
            await self._broker_client.publish(envelope, stream=RedisStreams.API)
        except Exception as exc:  # pragma: no cover - defensive guard for background eventing
            self._logger.warning("Failed to publish job event '%s' for job %s: %s", event_type, self._job_record.id, exc)
