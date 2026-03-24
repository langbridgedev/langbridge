from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, replace
from typing import Any, Literal

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from langbridge.runtime.services.runtime_host import RuntimeHost

BackgroundTaskKind = Literal["default", "custom"]


@dataclass(slots=True, frozen=True)
class BackgroundTaskSchedule:
    trigger: Literal["interval", "cron"]
    seconds: float | None = None
    cron_expression: str | None = None
    timezone: str | None = None

    def __post_init__(self) -> None:
        if self.trigger == "interval":
            if self.seconds is None or self.seconds <= 0:
                raise ValueError("Interval background tasks require a positive seconds value.")
            if self.cron_expression is not None:
                raise ValueError("Interval background tasks do not accept a cron expression.")
            return
        if self.trigger == "cron":
            if not str(self.cron_expression or "").strip():
                raise ValueError("Cron background tasks require a cron expression.")
            if self.seconds is not None:
                raise ValueError("Cron background tasks do not accept an interval seconds value.")
            return
        raise ValueError(f"Unsupported background task trigger '{self.trigger}'.")

    @classmethod
    def interval(cls, *, seconds: float, timezone: str | None = None) -> "BackgroundTaskSchedule":
        return cls(trigger="interval", seconds=seconds, timezone=timezone)

    @classmethod
    def cron(
        cls,
        *,
        expression: str,
        timezone: str | None = None,
    ) -> "BackgroundTaskSchedule":
        return cls(trigger="cron", cron_expression=expression, timezone=timezone)


@dataclass(slots=True, frozen=True)
class BackgroundTaskExecutionContext:
    manager: "RuntimeBackgroundTaskManager"
    runtime_host: RuntimeHost
    task_name: str
    kind: BackgroundTaskKind
    definition: "RuntimeBackgroundTaskDefinition | None" = None


BackgroundTaskHandler = Callable[[BackgroundTaskExecutionContext], Awaitable[Any] | Any]


@dataclass(slots=True, frozen=True)
class RuntimeBackgroundTaskDefinition:
    name: str
    handler: BackgroundTaskHandler
    kind: BackgroundTaskKind = "custom"
    schedule: BackgroundTaskSchedule | None = None
    run_on_startup: bool = False
    enabled: bool = True
    description: str | None = None

    def __post_init__(self) -> None:
        if not str(self.name or "").strip():
            raise ValueError("Background task names must not be empty.")
        if self.kind not in ("default", "custom"):
            raise ValueError(f"Unsupported background task kind '{self.kind}'.")

    @classmethod
    def default(
        cls,
        *,
        name: str,
        handler: BackgroundTaskHandler,
        schedule: BackgroundTaskSchedule | None = None,
        run_on_startup: bool = False,
        enabled: bool = True,
        description: str | None = None,
    ) -> "RuntimeBackgroundTaskDefinition":
        return cls(
            name=name,
            handler=handler,
            kind="default",
            schedule=schedule,
            run_on_startup=run_on_startup,
            enabled=enabled,
            description=description,
        )

    @classmethod
    def custom(
        cls,
        *,
        name: str,
        handler: BackgroundTaskHandler,
        schedule: BackgroundTaskSchedule | None = None,
        run_on_startup: bool = False,
        enabled: bool = True,
        description: str | None = None,
    ) -> "RuntimeBackgroundTaskDefinition":
        return cls(
            name=name,
            handler=handler,
            kind="custom",
            schedule=schedule,
            run_on_startup=run_on_startup,
            enabled=enabled,
            description=description,
        )


class RuntimeBackgroundTaskManager:
    def __init__(
        self,
        *,
        runtime_host: RuntimeHost,
        scheduler: AsyncIOScheduler | None = None,
        default_tasks: Iterable[RuntimeBackgroundTaskDefinition] | None = None,
        custom_tasks: Iterable[RuntimeBackgroundTaskDefinition] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.runtime_host = runtime_host
        self._scheduler = scheduler or AsyncIOScheduler()
        self._owns_scheduler = scheduler is None
        self._logger = logger or logging.getLogger("langbridge.runtime.hosting.background")
        self._default_tasks: dict[str, RuntimeBackgroundTaskDefinition] = {}
        self._custom_tasks: dict[str, RuntimeBackgroundTaskDefinition] = {}
        self._active_tasks: dict[str, asyncio.Task[Any]] = {}
        self._task_locks: dict[str, asyncio.Lock] = {}
        self._started = False
        for task in default_tasks or ():
            self.register_default_task(task)
        for task in custom_tasks or ():
            self.register_custom_task(task)

    @property
    def scheduler(self) -> AsyncIOScheduler:
        return self._scheduler

    @property
    def started(self) -> bool:
        return self._started

    @property
    def default_tasks(self) -> tuple[RuntimeBackgroundTaskDefinition, ...]:
        return tuple(self._default_tasks.values())

    @property
    def custom_tasks(self) -> tuple[RuntimeBackgroundTaskDefinition, ...]:
        return tuple(self._custom_tasks.values())

    def list_tasks(
        self,
        *,
        kind: BackgroundTaskKind | None = None,
    ) -> tuple[RuntimeBackgroundTaskDefinition, ...]:
        if kind == "default":
            return self.default_tasks
        if kind == "custom":
            return self.custom_tasks
        return self.default_tasks + self.custom_tasks

    def register(self, task: RuntimeBackgroundTaskDefinition) -> RuntimeBackgroundTaskDefinition:
        if task.kind == "default":
            return self.register_default_task(task)
        return self.register_custom_task(task)

    def register_default_task(
        self,
        task: RuntimeBackgroundTaskDefinition,
    ) -> RuntimeBackgroundTaskDefinition:
        return self._register_definition(task=replace(task, kind="default"), store=self._default_tasks)

    def register_custom_task(
        self,
        task: RuntimeBackgroundTaskDefinition,
    ) -> RuntimeBackgroundTaskDefinition:
        return self._register_definition(task=replace(task, kind="custom"), store=self._custom_tasks)

    def start_task(
        self,
        *,
        name: str,
        handler: BackgroundTaskHandler,
        kind: BackgroundTaskKind = "custom",
    ) -> asyncio.Task[Any]:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("Background task names must not be empty.")
        current = self._active_tasks.get(normalized_name)
        if current is not None and not current.done():
            raise RuntimeError(f"Background task '{normalized_name}' is already running.")

        context = BackgroundTaskExecutionContext(
            manager=self,
            runtime_host=self.runtime_host,
            task_name=normalized_name,
            kind=kind,
        )

        async def _runner() -> Any:
            try:
                return await self._execute_handler(context=context, handler=handler)
            except asyncio.CancelledError:
                self._logger.debug("Background task '%s' was cancelled.", normalized_name)
                raise
            except Exception:
                self._logger.exception("Background task '%s' failed.", normalized_name)
                raise
            finally:
                self._active_tasks.pop(normalized_name, None)

        task_name_label = f"langbridge-background:{kind}:{normalized_name}"
        created_task = asyncio.create_task(_runner(), name=task_name_label)
        self._active_tasks[normalized_name] = created_task
        return created_task

    async def start(self) -> None:
        if self._started:
            return
        for task in self.list_tasks():
            if not task.enabled:
                continue
            if task.schedule is not None:
                self._schedule_definition(task)
        if not self._scheduler.running:
            self._scheduler.start()
        self._started = True
        startup_tasks: list[asyncio.Task[Any]] = []
        for task in self.list_tasks():
            if task.enabled and task.run_on_startup:
                startup_tasks.append(self._run_definition_as_task(task))
        if startup_tasks:
            await asyncio.gather(*startup_tasks)

    async def stop(self) -> None:
        if not self._started:
            return
        active_tasks = list(self._active_tasks.values())
        for task in active_tasks:
            if not task.done():
                task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)
        self._active_tasks.clear()
        self._task_locks.clear()
        if self._scheduler.running:
            self._scheduler.remove_all_jobs()
            self._scheduler.shutdown(wait=False)
            if self._owns_scheduler:
                self._scheduler = AsyncIOScheduler()
        self._started = False

    def _register_definition(
        self,
        *,
        task: RuntimeBackgroundTaskDefinition,
        store: dict[str, RuntimeBackgroundTaskDefinition],
    ) -> RuntimeBackgroundTaskDefinition:
        normalized_name = task.name.strip()
        if normalized_name in self._default_tasks or normalized_name in self._custom_tasks:
            raise ValueError(f"Background task '{normalized_name}' is already registered.")
        normalized_task = replace(task, name=normalized_name)
        store[normalized_name] = normalized_task
        if self._started and normalized_task.enabled:
            if normalized_task.schedule is not None:
                self._schedule_definition(normalized_task)
            if normalized_task.run_on_startup:
                self._run_definition_as_task(normalized_task)
        return normalized_task

    def _run_definition_as_task(
        self,
        definition: RuntimeBackgroundTaskDefinition,
    ) -> asyncio.Task[Any]:
        current = self._active_tasks.get(definition.name)
        if current is not None and not current.done():
            raise RuntimeError(
                f"Background task '{definition.name}' is already running."
            )

        context = BackgroundTaskExecutionContext(
            manager=self,
            runtime_host=self.runtime_host,
            task_name=definition.name,
            kind=definition.kind,
            definition=definition,
        )

        async def _runner() -> Any:
            try:
                return await self._execute_definition(context)
            except asyncio.CancelledError:
                self._logger.debug("Background task '%s' was cancelled.", definition.name)
                raise
            except Exception:
                self._logger.exception("Background task '%s' failed.", definition.name)
                raise
            finally:
                self._active_tasks.pop(definition.name, None)

        task_name_label = f"langbridge-background:{definition.kind}:{definition.name}"
        created_task = asyncio.create_task(_runner(), name=task_name_label)
        self._active_tasks[definition.name] = created_task
        return created_task

    def _schedule_definition(self, definition: RuntimeBackgroundTaskDefinition) -> None:
        job_id = self._job_id(definition.name)
        if self._scheduler.get_job(job_id) is not None:
            self._scheduler.remove_job(job_id)

        add_job_kwargs: dict[str, Any] = {
            "id": job_id,
            "name": definition.name,
            "replace_existing": True,
            "max_instances": 1,
            "coalesce": True,
        }
        if definition.schedule is None:
            return
        if definition.schedule.trigger == "interval":
            if definition.schedule.timezone is not None:
                add_job_kwargs["timezone"] = definition.schedule.timezone
            self._scheduler.add_job(
                self._execute_definition_by_name,
                trigger="interval",
                seconds=definition.schedule.seconds,
                args=[definition.name],
                **add_job_kwargs,
            )
            return
        trigger = CronTrigger.from_crontab(
            definition.schedule.cron_expression or "",
            timezone=definition.schedule.timezone,
        )
        self._scheduler.add_job(
            self._execute_definition_by_name,
            trigger=trigger,
            args=[definition.name],
            **add_job_kwargs,
        )

    async def _execute_definition_by_name(self, name: str) -> Any:
        definition = self._default_tasks.get(name) or self._custom_tasks.get(name)
        if definition is None:
            self._logger.warning("Scheduled background task '%s' no longer exists.", name)
            return None
        context = BackgroundTaskExecutionContext(
            manager=self,
            runtime_host=self.runtime_host,
            task_name=definition.name,
            kind=definition.kind,
            definition=definition,
        )
        return await self._execute_definition(context)

    async def _execute_definition(self, context: BackgroundTaskExecutionContext) -> Any:
        try:
            return await self._execute_handler(context=context, handler=context.definition.handler)  # type: ignore[union-attr]
        except asyncio.CancelledError:
            raise
        except Exception:
            self._logger.exception("Background task '%s' failed.", context.task_name)
            raise

    async def _execute_handler(
        self,
        *,
        context: BackgroundTaskExecutionContext,
        handler: BackgroundTaskHandler,
    ) -> Any:
        task_lock = self._task_locks.setdefault(context.task_name, asyncio.Lock())
        if task_lock.locked():
            self._logger.info(
                "Skipping background task '%s' because a previous execution is still running.",
                context.task_name,
            )
            return None
        async with task_lock:
            result = handler(context)
            if inspect.isawaitable(result):
                return await result
            return result

    @staticmethod
    def _job_id(name: str) -> str:
        return f"runtime-background:{name}"


def build_connector_sync_default_task(
    *,
    connector_name: str,
    resources: Iterable[str],
    schedule: BackgroundTaskSchedule,
    name: str | None = None,
    sync_mode: str = "INCREMENTAL",
    force_full_refresh: bool = False,
    run_on_startup: bool = False,
    description: str | None = None,
) -> RuntimeBackgroundTaskDefinition:
    normalized_resources = [
        str(resource or "").strip()
        for resource in resources
        if str(resource or "").strip()
    ]
    if not normalized_resources:
        raise ValueError("Connector sync background tasks require at least one resource.")
    task_name = name or f"connector-sync:{connector_name}:{','.join(normalized_resources)}"

    async def _handler(context: BackgroundTaskExecutionContext) -> Any:
        sync_method = getattr(context.runtime_host, "sync_connector_resources", None)
        if sync_method is None:
            raise RuntimeError("Runtime host does not expose sync_connector_resources().")
        return await sync_method(
            connector_name=connector_name,
            resources=list(normalized_resources),
            sync_mode=sync_mode,
            force_full_refresh=force_full_refresh,
        )

    return RuntimeBackgroundTaskDefinition.default(
        name=task_name,
        handler=_handler,
        schedule=schedule,
        run_on_startup=run_on_startup,
        description=description or f"Refresh connector resources for '{connector_name}'.",
    )


def build_semantic_vector_refresh_default_task(
    *,
    schedule: BackgroundTaskSchedule,
    name: str = "semantic-vector-refresh",
    run_on_startup: bool = False,
    description: str | None = None,
    refresher: BackgroundTaskHandler | None = None,
) -> RuntimeBackgroundTaskDefinition:
    async def _handler(context: BackgroundTaskExecutionContext) -> Any:
        if refresher is not None:
            result = refresher(context)
            if inspect.isawaitable(result):
                return await result
            return result

        can_refresh = getattr(
            context.runtime_host,
            "can_refresh_semantic_vector_search",
            None,
        )
        if callable(can_refresh) and not bool(can_refresh()):
            reason_getter = getattr(
                context.runtime_host,
                "semantic_vector_refresh_unavailable_reason",
                None,
            )
            reason = (
                reason_getter()
                if callable(reason_getter)
                else "Semantic vector refresh is not configured for this runtime host."
            )
            context.manager._logger.info(
                "Skipping background task '%s': %s",
                context.task_name,
                reason,
            )
            return None

        for method_name in (
            "refresh_semantic_vector_search",
            "refresh_semantic_vectors",
            "refresh_semantic_search_index",
        ):
            method = getattr(context.runtime_host, method_name, None)
            if method is None:
                continue
            result = method()
            if inspect.isawaitable(result):
                return await result
            return result
        raise RuntimeError(
            "Runtime host does not expose a semantic vector refresh method. "
            "Pass a refresher callback when registering this default task."
        )

    return RuntimeBackgroundTaskDefinition.default(
        name=name,
        handler=_handler,
        schedule=schedule,
        run_on_startup=run_on_startup,
        description=description or "Refresh semantic vector search state.",
    )


__all__ = [
    "BackgroundTaskExecutionContext",
    "BackgroundTaskKind",
    "BackgroundTaskSchedule",
    "RuntimeBackgroundTaskDefinition",
    "RuntimeBackgroundTaskManager",
    "build_connector_sync_default_task",
    "build_semantic_vector_refresh_default_task",
]
