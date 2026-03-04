
from dataclasses import dataclass
from datetime import timezone, datetime
import uuid
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.db.messages import OutboxMessage, MessageStatus
from langbridge.packages.common.langbridge_common.repositories.message_repository import MessageRepository
from langbridge.apps.api.langbridge_api.services.request_context_provider import RequestContextProvider
from langbridge.packages.messaging.langbridge_messaging.contracts import BaseMessagePayload, MessageHeaders
from langbridge.packages.messaging.langbridge_messaging.contracts.messages import MessageEnvelope, MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.stream_mapping import STREAM_MAPPING


@dataclass(frozen=True)
class _MessageRoute:
    stream: str | None
    consumer_group: str | None
    consumer_name: str | None


class MessageService:
    _MESSAGE_ROUTES: dict[MessageType, _MessageRoute] = {
        MessageType.AGENT_JOB_REQUEST: _MessageRoute(
            stream=settings.REDIS_WORKER_STREAM,
            consumer_group=settings.REDIS_WORKER_CONSUMER_GROUP,
            consumer_name=settings.REDIS_CONSUMER_NAME or None,
        ),
        MessageType.SEMANTIC_QUERY_REQUEST: _MessageRoute(
            stream=settings.REDIS_WORKER_STREAM,
            consumer_group=settings.REDIS_WORKER_CONSUMER_GROUP,
            consumer_name=settings.REDIS_CONSUMER_NAME or None,
        ),
        MessageType.AGENTIC_SEMANTIC_MODEL_JOB_REQUEST: _MessageRoute(
            stream=settings.REDIS_WORKER_STREAM,
            consumer_group=settings.REDIS_WORKER_CONSUMER_GROUP,
            consumer_name=settings.REDIS_CONSUMER_NAME or None,
        ),
        MessageType.COPILOT_DASHBOARD_REQUEST: _MessageRoute(
            stream=settings.REDIS_WORKER_STREAM,
            consumer_group=settings.REDIS_WORKER_CONSUMER_GROUP,
            consumer_name=settings.REDIS_CONSUMER_NAME or None,
        ),
        MessageType.SQL_JOB_REQUEST: _MessageRoute(
            stream=settings.REDIS_WORKER_STREAM,
            consumer_group=settings.REDIS_WORKER_CONSUMER_GROUP,
            consumer_name=settings.REDIS_CONSUMER_NAME or None,
        ),
        MessageType.DATASET_JOB_REQUEST: _MessageRoute(
            stream=settings.REDIS_WORKER_STREAM,
            consumer_group=settings.REDIS_WORKER_CONSUMER_GROUP,
            consumer_name=settings.REDIS_CONSUMER_NAME or None,
        ),
    }

    def __init__(
        self,
        message_repository: MessageRepository,
        request_context_provider: RequestContextProvider,
    ):
        self._message_repository = message_repository
        self._request_context_provider = request_context_provider

    async def create_outbox_message(
        self,
        payload: BaseMessagePayload,
        headers: dict | None = None,
        source_timestamp: datetime | None = None,
    ) -> OutboxMessage:
        if source_timestamp is None:
            source_timestamp = datetime.now(tz=timezone.utc)
        if headers is None:
            headers = {}
        message_headers: MessageHeaders = MessageHeaders.default().model_copy(update=headers)
        
        if message_headers.organisation_id is None:
            org_id = self._request_context_provider.current_org_id
            message_headers.organisation_id = str(org_id) if org_id else None
            
        if message_headers.correlation_id is None:
            message_headers.correlation_id = self._request_context_provider.correlation_id
        
        outbox_message_envelope: MessageEnvelope = self._create_message_envelope(
            message_type=payload.message_type,
            payload=payload,
            headers=message_headers,
            timestamp=source_timestamp
        )

        message_route = self._resolve_message_route(payload.message_type)
        
        message_id: uuid.UUID = uuid.uuid4()
        
        outbox_message: OutboxMessage = OutboxMessage(
            id=message_id,
            message_type=payload.message_type.value,
            correlation_id=str(message_headers.correlation_id),
            payload=outbox_message_envelope.payload.model_dump(mode="json"),
            headers=outbox_message_envelope.headers.model_dump(mode="json"),
            status=MessageStatus.not_sent,
            stream=message_route.stream,
            consumer_group=message_route.consumer_group,
            consumer_name=message_route.consumer_name,
        )
        self._message_repository.add(outbox_message)
        self._request_context_provider.mark_outbox_message()
        return outbox_message

    def _create_message_envelope(
        self,
        message_type: MessageType,
        payload: BaseMessagePayload,
        headers: MessageHeaders,
        timestamp: datetime,
    ) -> MessageEnvelope:
        return MessageEnvelope(
            message_type=message_type,
            payload=payload,
            headers=headers,
            created_at=timestamp,
        )

    def _resolve_message_route(self, message_type: MessageType) -> _MessageRoute:
        mapped_stream = STREAM_MAPPING.get(message_type)
        if not mapped_stream:
            raise ValueError(f"No stream mapping found for message type {message_type}")
        return self._MESSAGE_ROUTES.get(
            message_type,
            _MessageRoute(
                stream=mapped_stream,
                consumer_group=settings.REDIS_API_CONSUMER_GROUP,
                consumer_name=settings.REDIS_CONSUMER_NAME or None,
            ),
        )
