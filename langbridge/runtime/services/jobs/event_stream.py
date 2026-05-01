from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from langbridge.runtime.models import RuntimeJobEvent, RuntimeJobStreamEvent
from langbridge.runtime.services.jobs.mappers import RuntimeJobMapper


class RuntimeJobEventStream:
    def __init__(self, *, repository: Any, mapper: RuntimeJobMapper | None = None) -> None:
        self._repository = repository
        self._mapper = mapper or RuntimeJobMapper()

    async def notify(self) -> None:
        return None

    async def stream(
        self,
        *,
        job_id: uuid.UUID,
        after_sequence: int = 0,
        heartbeat_interval: float = 10.0,
    ) -> AsyncIterator[RuntimeJobStreamEvent | None]:
        if await self._repository.get_by_id(job_id) is None:
            raise KeyError(str(job_id))

        cursor = max(0, int(after_sequence or 0))
        last_heartbeat = asyncio.get_running_loop().time()
        while True:
            events = await self._repository.list_events_after(
                job_id=job_id,
                after_sequence=cursor,
            )
            if events:
                for raw_event in events:
                    event = self._mapper.to_event(raw_event)
                    cursor = max(cursor, int(event.sequence or 0))
                    if self._visibility_value(event.visibility) != "public":
                        continue
                    yield self._to_stream_event(event)
                    if event.terminal:
                        return
                last_heartbeat = asyncio.get_running_loop().time()
                continue

            job = await self._repository.get_by_id(job_id)
            if job is None:
                return
            if getattr(job, "terminal_sequence", None) is not None:
                return

            now = asyncio.get_running_loop().time()
            timeout = max(1.0, float(heartbeat_interval)) - (now - last_heartbeat)
            if timeout <= 0:
                last_heartbeat = now
                yield None
                continue

            await asyncio.sleep(min(timeout, 0.25))

    def _to_stream_event(self, event: RuntimeJobEvent) -> RuntimeJobStreamEvent:
        timestamp = event.created_at or datetime.now(timezone.utc)
        details = dict(event.details or {})
        thread_id = self._uuid_or_none(details.get("thread_id"))
        message_id = self._uuid_or_none(details.get("message_id"))
        return RuntimeJobStreamEvent(
            sequence=event.sequence,
            event=event.event_type,
            status=str(getattr(event.status, "value", event.status)),
            stage=event.stage,
            message=event.message,
            timestamp=timestamp,
            job_type=str(details.get("job_type") or ("agent.run" if thread_id is not None else "job")),
            thread_id=thread_id,
            job_id=event.job_id,
            message_id=message_id,
            visibility=str(getattr(event.visibility, "value", event.visibility)),
            terminal=event.terminal,
            source=event.source,
            raw_event_type=event.raw_event_type,
            details=details,
        )

    def _visibility_value(self, visibility: Any) -> str:
        return str(getattr(visibility, "value", visibility) or "").strip().lower()

    def _uuid_or_none(self, value: Any) -> uuid.UUID | None:
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except (TypeError, ValueError):
            return None
