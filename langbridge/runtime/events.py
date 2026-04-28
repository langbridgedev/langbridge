from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from enum import Enum
import uuid
from typing import Any, Protocol

from langbridge.runtime.models.streaming import RuntimeRunStreamEvent


class AgentEventVisibility(str, Enum):
    public = "public"
    internal = "internal"


class AgentEventEmitter(Protocol):
    async def emit(
        self,
        *,
        event_type: str,
        message: str,
        visibility: AgentEventVisibility = AgentEventVisibility.internal,
        source: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None: ...


class CollectingAgentEventEmitter:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    async def emit(
        self,
        *,
        event_type: str,
        message: str,
        visibility: AgentEventVisibility = AgentEventVisibility.internal,
        source: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.events.append(
            {
                "event_type": event_type,
                "message": message,
                "visibility": _normalize_visibility(visibility),
                "source": source,
                "details": dict(details or {}),
            }
        )


def _normalize_visibility(value: AgentEventVisibility | str) -> str:
    if isinstance(value, AgentEventVisibility):
        return value.value
    normalized = str(value or "").strip().lower()
    if normalized == AgentEventVisibility.public.value:
        return AgentEventVisibility.public.value
    return AgentEventVisibility.internal.value


def normalize_agent_stream_stage(*, event_type: str, message: str = "", source: str | None = None) -> str:
    raw_type = str(event_type or "").strip()
    normalized = raw_type.lower()
    message_text = str(message or "").lower()
    source_text = str(source or "").lower()

    if "denied" in normalized or "denied" in message_text:
        return "access_denied"
    if "clarification" in normalized:
        return "clarification"
    if "visual" in normalized or "chart" in message_text:
        return "rendering_chart"
    if "charting" in normalized:
        return "rendering_chart"
    if "websearch" in normalized:
        return "searching_web"
    if "semanticsearch" in normalized:
        return "searching_semantic"
    if "deepresearch" in normalized:
        return "researching"
    if "presentation" in normalized:
        return "composing_response"
    if "sqlgeneration" in normalized or "sqlgenerated" in normalized:
        return "generating_sql"
    if "sqlanalysis" in normalized:
        return "generating_sql"
    if "sqlexecution" in normalized or "query" in message_text:
        return "running_query"
    if "retry" in normalized or "retry" in message_text:
        return "retrying"
    if "toolstarted" in normalized or "asset" in message_text or "analyst" in source_text:
        return "selecting_asset"
    if normalized in {"agentruncompleted", "researchcompleted"}:
        return "completed"
    if "failed" in normalized or "error" in normalized:
        return "failed"
    return "planning"


def normalize_agent_stream_status(*, event_type: str, message: str = "") -> str:
    normalized = str(event_type or "").strip().lower()
    message_text = str(message or "").lower()
    if normalized in {"agentruncompleted", "researchcompleted"}:
        return "completed"
    if "failed" in normalized or "error" in normalized or "denied" in normalized:
        return "failed"
    if "failed" in message_text or "denied" in message_text:
        return "failed"
    if "completed" in normalized or "generated" in normalized or "prepared" in normalized:
        return "completed"
    return "in_progress"


class QueueingAgentStreamEmitter:
    def __init__(
        self,
        *,
        thread_id: uuid.UUID,
        job_id: uuid.UUID,
        enqueue: Callable[[RuntimeRunStreamEvent], Awaitable[None]],
        message_id: uuid.UUID | None = None,
        initial_sequence: int = 0,
    ) -> None:
        self._thread_id = thread_id
        self._job_id = job_id
        self._enqueue = enqueue
        self._message_id = message_id
        self._sequence = initial_sequence

    @property
    def sequence(self) -> int:
        return self._sequence

    async def emit(
        self,
        *,
        event_type: str,
        message: str,
        visibility: AgentEventVisibility = AgentEventVisibility.internal,
        source: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self._sequence += 1
        await self._enqueue(
            RuntimeRunStreamEvent(
                sequence=self._sequence,
                event="run.progress",
                status=normalize_agent_stream_status(event_type=event_type, message=message),
                stage=normalize_agent_stream_stage(
                    event_type=event_type,
                    message=message,
                    source=source,
                ),
                message=message,
                timestamp=datetime.now(timezone.utc),
                run_type="agent",
                run_id=self._job_id,
                thread_id=self._thread_id,
                job_id=self._job_id,
                message_id=self._message_id,
                visibility=_normalize_visibility(visibility),
                source=source,
                raw_event_type=event_type,
                details=dict(details or {}),
            )
        )
