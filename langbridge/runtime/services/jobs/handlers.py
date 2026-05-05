from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from langbridge.runtime.services.jobs.context import JobExecutionContext


class RuntimeJobHandler(Protocol):
    @property
    def job_type(self) -> str: ...

    async def handle(self, context: "JobExecutionContext") -> dict[str, Any] | None: ...


class RuntimeJobHandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, RuntimeJobHandler] = {}

    def register(self, handler: RuntimeJobHandler) -> None:
        job_type = str(handler.job_type or "").strip()
        if not job_type:
            raise ValueError("Job handlers must declare a non-empty job_type.")
        self._handlers[job_type] = handler

    def get(self, job_type: str) -> RuntimeJobHandler | None:
        return self._handlers.get(str(job_type or "").strip())

    def registered_job_types(self) -> set[str]:
        return set(self._handlers)
