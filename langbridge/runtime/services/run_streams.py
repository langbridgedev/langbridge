from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import threading
import uuid
from typing import AsyncIterator

from langbridge.runtime.models import RuntimeRunStreamEvent


@dataclass(slots=True)
class _RunStreamState:
    run_id: uuid.UUID
    run_type: str
    thread_id: uuid.UUID | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    closed: bool = False
    terminal_sequence: int | None = None
    events: deque[RuntimeRunStreamEvent] = field(default_factory=deque)


class RuntimeRunStreamRegistry:
    def __init__(
        self,
        *,
        retention_seconds: int = 300,
        max_replay_events: int = 256,
    ) -> None:
        self._retention = timedelta(seconds=max(30, retention_seconds))
        self._max_replay_events = max(32, max_replay_events)
        self._states: dict[uuid.UUID, _RunStreamState] = {}
        self._lock = threading.RLock()

    async def open_run(
        self,
        *,
        run_id: uuid.UUID,
        run_type: str,
        thread_id: uuid.UUID | None = None,
    ) -> None:
        with self._lock:
            self._prune_locked()
            state = self._states.get(run_id)
            if state is None:
                self._states[run_id] = _RunStreamState(
                    run_id=run_id,
                    run_type=str(run_type or "").strip() or "runtime",
                    thread_id=thread_id,
                )
                return
            state.run_type = str(run_type or "").strip() or state.run_type
            if thread_id is not None:
                state.thread_id = thread_id
            state.updated_at = datetime.now(timezone.utc)

    async def ensure_run(self, *, run_id: uuid.UUID) -> None:
        with self._lock:
            self._prune_locked()
            if run_id not in self._states:
                raise KeyError(str(run_id))

    async def publish(self, event: RuntimeRunStreamEvent) -> None:
        run_id = event.run_id or event.job_id
        if run_id is None:
            raise ValueError("Runtime run stream events require a run_id or job_id.")

        with self._lock:
            self._prune_locked()
            state = self._states.get(run_id)
            if state is None:
                state = _RunStreamState(
                    run_id=run_id,
                    run_type=str(event.run_type or "").strip() or "runtime",
                    thread_id=event.thread_id,
                )
                self._states[run_id] = state

        with self._lock:
            state.run_type = str(event.run_type or "").strip() or state.run_type
            if event.thread_id is not None:
                state.thread_id = event.thread_id
            state.updated_at = datetime.now(timezone.utc)
            state.events.append(event)
            while len(state.events) > self._max_replay_events:
                state.events.popleft()
            if event.terminal:
                state.closed = True
                state.terminal_sequence = event.sequence

    async def subscribe(
        self,
        *,
        run_id: uuid.UUID,
        after_sequence: int = 0,
        heartbeat_interval: float | None = None,
    ) -> AsyncIterator[RuntimeRunStreamEvent | None]:
        await self.ensure_run(run_id=run_id)

        async def iterator() -> AsyncIterator[RuntimeRunStreamEvent | None]:
            cursor = max(0, int(after_sequence or 0))
            loop = asyncio.get_running_loop()
            last_heartbeat_at = loop.time()
            while True:
                with self._lock:
                    state = self._states.get(run_id)
                    if state is None:
                        return
                    events = [event for event in state.events if event.sequence > cursor]
                    closed = state.closed

                if events:
                    last_heartbeat_at = loop.time()
                    for event in events:
                        cursor = max(cursor, int(event.sequence or 0))
                        yield event
                    continue

                if closed:
                    return

                if (
                    heartbeat_interval is not None
                    and loop.time() - last_heartbeat_at >= max(1.0, float(heartbeat_interval))
                ):
                    last_heartbeat_at = loop.time()
                    yield None
                    continue

                await asyncio.sleep(0.05)

        return iterator()

    async def aclose(self) -> None:
        with self._lock:
            states = list(self._states.values())
            self._states.clear()

        for state in states:
            state.closed = True

    def _prune_locked(self) -> None:
        cutoff = datetime.now(timezone.utc) - self._retention
        expired = [
            run_id
            for run_id, state in self._states.items()
            if state.closed and state.updated_at < cutoff
        ]
        for run_id in expired:
            self._states.pop(run_id, None)
