"""Event hooks for Langbridge AI runtime progress."""
from typing import Any, Protocol

class AIEventEmitter(Protocol):
    async def emit(
        self,
        *,
        event_type: str,
        message: str,
        visibility: str = "internal",
        source: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None: ...


class AIEventSource:
    """Small mixin for optional progress events without coupling AI to runtime hosting."""

    def __init__(self, *, event_emitter: AIEventEmitter | None = None) -> None:
        self._event_emitter = event_emitter

    async def _emit_ai_event(
        self,
        *,
        event_type: str,
        message: str,
        visibility: str = "internal",
        source: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if self._event_emitter is None:
            return
        await self._event_emitter.emit(
            event_type=event_type,
            message=message,
            visibility=visibility,
            source=source,
            details=details,
        )


__all__ = ["AIEventEmitter", "AIEventSource"]
