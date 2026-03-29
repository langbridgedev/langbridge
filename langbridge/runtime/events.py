
from enum import Enum
from typing import Any, Protocol


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
