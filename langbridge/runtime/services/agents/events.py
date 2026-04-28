from typing import Any

from langbridge.runtime.events import AgentEventEmitter, AgentEventVisibility


class AgentRunEventPublisher:
    async def emit(
        self,
        event_emitter: AgentEventEmitter | None,
        *,
        event_type: str,
        message: str,
        visibility: AgentEventVisibility = AgentEventVisibility.internal,
        source: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if event_emitter is None:
            return
        await event_emitter.emit(
            event_type=event_type,
            message=message,
            visibility=visibility,
            source=source,
            details=details,
        )
