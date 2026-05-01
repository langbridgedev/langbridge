from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from langbridge.runtime.services.jobs.context import JobExecutionContext
from langbridge.runtime.services.jobs.handlers import RuntimeJobHandlerRegistry
from langbridge.runtime.services.jobs.service import RuntimeJobService


class RuntimeJobProcessor:
    def __init__(
        self,
        *,
        job_service: RuntimeJobService,
        handlers: RuntimeJobHandlerRegistry,
        worker_id: str | None = None,
        queue_name: str = "default",
        lease_seconds: int = 300,
        poll_interval_seconds: float = 0.5,
        logger: logging.Logger | None = None,
    ) -> None:
        self._job_service = job_service
        self._handlers = handlers
        self._worker_id = worker_id or f"runtime-worker:{uuid.uuid4()}"
        self._queue_name = str(queue_name or "default").strip() or "default"
        self._lease_seconds = max(10, int(lease_seconds))
        self._poll_interval_seconds = max(0.05, float(poll_interval_seconds))
        self._logger = logger or logging.getLogger("langbridge.runtime.jobs.processor")
        self._wake_event = asyncio.Event()
        self._task: asyncio.Task[Any] | None = None
        self._stopping = False

    @property
    def worker_id(self) -> str:
        return self._worker_id

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="langbridge-runtime-job-processor")

    async def stop(self) -> None:
        self._stopping = True
        self._wake_event.set()
        task = self._task
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._task = None

    def wake(self) -> None:
        self._wake_event.set()

    def register_handler(self, handler: Any) -> None:
        self._handlers.register(handler)

    async def process_once(self) -> bool:
        job_types = self._handlers.registered_job_types()
        if not job_types:
            return False
        job = await self._job_service.claim_next(
            worker_id=self._worker_id,
            lease_seconds=self._lease_seconds,
            job_types=job_types,
            queue_name=self._queue_name,
        )
        if job is None:
            return False
        handler = self._handlers.get(job.job_type)
        if handler is None:
            return False
        try:
            context = JobExecutionContext(
                job=job,
                worker_id=self._worker_id,
                _service=self._job_service,
            )
            result = await handler.handle(context)
            await self._job_service.complete_job(
                job_id=job.id,
                result=dict(result or {}),
            )
        except Exception as exc:
            self._logger.exception("Runtime job %s failed.", job.id)
            await self._job_service.fail_job(job_id=job.id, exc=exc)
        return True

    async def _run(self) -> None:
        while not self._stopping:
            processed = await self.process_once()
            if processed:
                continue
            self._wake_event.clear()
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=self._poll_interval_seconds)
            except TimeoutError:
                continue
