from __future__ import annotations

import asyncio
import contextvars
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, ParamSpec, TypeVar

from langbridge.runtime.settings import runtime_settings

P = ParamSpec("P")
T = TypeVar("T")


def _run_with_context(
    context: contextvars.Context,
    fn: Callable[[], T],
) -> T:
    return context.run(fn)


class FederationExecutionOffloader:
    """Run blocking federation work on a bounded shared executor."""

    def __init__(
        self,
        *,
        max_workers: int | None = None,
        thread_name_prefix: str = "langbridge-federation",
    ) -> None:
        resolved_max_workers = int(
            max_workers or runtime_settings.FEDERATION_BLOCKING_MAX_WORKERS or 1
        )
        self._max_workers = max(1, resolved_max_workers)
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix=thread_name_prefix,
        )
        self._closed = False

    @property
    def max_workers(self) -> int:
        return self._max_workers

    async def run(
        self,
        fn: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        if self._closed:
            raise RuntimeError("FederationExecutionOffloader is already closed.")
        loop = asyncio.get_running_loop()
        context = contextvars.copy_context()
        bound = functools.partial(fn, *args, **kwargs)
        return await loop.run_in_executor(
            self._executor,
            _run_with_context,
            context,
            bound,
        )

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        shutdown = functools.partial(
            self._executor.shutdown,
            wait=True,
            cancel_futures=False,
        )
        await asyncio.to_thread(shutdown)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._executor.shutdown(wait=True, cancel_futures=False)


async def run_federation_blocking(
    executor: FederationExecutionOffloader | None,
    fn: Callable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    if executor is not None:
        return await executor.run(fn, *args, **kwargs)
    return await asyncio.to_thread(fn, *args, **kwargs)


__all__ = [
    "FederationExecutionOffloader",
    "run_federation_blocking",
]
