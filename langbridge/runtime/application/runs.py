import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from langbridge.runtime.models import RuntimeRunStreamEvent

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


class RunApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    async def stream_run(
        self,
        *,
        run_id: uuid.UUID,
        after_sequence: int = 0,
        heartbeat_interval: float = 10.0,
    ) -> AsyncIterator[RuntimeRunStreamEvent | None]:
        return await self._host._run_streams.subscribe(
            run_id=run_id,
            after_sequence=after_sequence,
            heartbeat_interval=heartbeat_interval,
        )
