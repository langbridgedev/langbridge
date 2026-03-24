from __future__ import annotations

import asyncio
import uuid

import pytest
from fastapi.testclient import TestClient

from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.hosting import (
    BackgroundTaskSchedule,
    RuntimeBackgroundTaskDefinition,
    RuntimeBackgroundTaskManager,
    build_semantic_vector_refresh_default_task,
    create_runtime_api_app,
)
from langbridge.runtime.services.runtime_host import RuntimeHost, RuntimeProviders, RuntimeServices


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class RecordingSemanticVectorSearchService:
    def __init__(self, *, can_refresh: bool = True) -> None:
        self.calls: list[dict[str, object]] = []
        self._can_refresh = can_refresh

    async def refresh_workspace(self, *args, **kwargs):
        _ = args
        self.calls.append(dict(kwargs))
        return []

    def can_refresh(self) -> bool:
        return self._can_refresh

    def refresh_unavailable_reason(self) -> str | None:
        if self._can_refresh:
            return None
        return "Semantic vector refresh requires an embedding provider."


def _build_runtime_host(
    *,
    semantic_vector_search: object | None = None,
) -> RuntimeHost:
    return RuntimeHost(
        context=RuntimeContext.build(
            workspace_id=uuid.uuid4(),
            actor_id=uuid.uuid4(),
            roles=["runtime:operator"],
        ),
        providers=RuntimeProviders(),
        services=RuntimeServices(
            semantic_vector_search=semantic_vector_search,  # type: ignore[arg-type]
        ),
    )


@pytest.mark.anyio
async def test_runtime_background_manager_distinguishes_default_and_custom_tasks() -> None:
    manager = RuntimeBackgroundTaskManager(runtime_host=_build_runtime_host())

    async def _noop(_):
        return None

    default_task = RuntimeBackgroundTaskDefinition.default(
        name="semantic-refresh",
        handler=_noop,
        schedule=BackgroundTaskSchedule.interval(seconds=300),
    )
    custom_task = RuntimeBackgroundTaskDefinition.custom(
        name="tenant-cache-warm",
        handler=_noop,
        run_on_startup=True,
    )

    manager.register_default_task(default_task)
    manager.register_custom_task(custom_task)

    assert [task.name for task in manager.default_tasks] == ["semantic-refresh"]
    assert [task.name for task in manager.custom_tasks] == ["tenant-cache-warm"]

    await manager.start()
    try:
        jobs = manager.scheduler.get_jobs()
        assert len(jobs) == 1
        assert jobs[0].name == "semantic-refresh"
    finally:
        await manager.stop()


@pytest.mark.anyio
async def test_runtime_background_manager_runs_startup_and_manual_tasks() -> None:
    manager = RuntimeBackgroundTaskManager(runtime_host=_build_runtime_host())
    observed: list[tuple[str, str]] = []

    async def _record(context):
        observed.append((context.task_name, context.kind))

    manager.register_default_task(
        RuntimeBackgroundTaskDefinition.default(
            name="semantic-refresh",
            handler=_record,
            run_on_startup=True,
        )
    )

    await manager.start()
    try:
        await asyncio.sleep(0)
        manual_task = manager.start_task(
            name="custom-rebuild",
            handler=_record,
            kind="custom",
        )
        await manual_task
    finally:
        await manager.stop()

    assert ("semantic-refresh", "default") in observed
    assert ("custom-rebuild", "custom") in observed


@pytest.mark.anyio
async def test_semantic_vector_refresh_default_task_uses_runtime_host_refresh() -> None:
    semantic_vector_search = RecordingSemanticVectorSearchService()
    runtime_host = _build_runtime_host(
        semantic_vector_search=semantic_vector_search,
    )
    manager = RuntimeBackgroundTaskManager(runtime_host=runtime_host)
    manager.register_default_task(
        build_semantic_vector_refresh_default_task(
            schedule=BackgroundTaskSchedule.interval(seconds=60),
        )
    )

    await manager._execute_definition_by_name("semantic-vector-refresh")

    assert semantic_vector_search.calls == [
        {"workspace_id": runtime_host.context.workspace_id}
    ]


def test_runtime_api_app_starts_registered_default_background_tasks() -> None:
    runtime_host = _build_runtime_host()
    observed: list[tuple[str, str]] = []

    async def _record(context):
        observed.append((context.task_name, context.kind))

    app = create_runtime_api_app(
        runtime_host=runtime_host,  # type: ignore[arg-type]
        default_background_tasks=[
            RuntimeBackgroundTaskDefinition.default(
                name="semantic-refresh",
                handler=_record,
                run_on_startup=True,
            )
        ],
    )

    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/health")
        assert response.status_code == 200
        manager = client.app.state.runtime_background_tasks
        assert manager.started is True
        assert [task.name for task in manager.default_tasks] == ["semantic-refresh"]

    assert observed == [("semantic-refresh", "default")]
    assert app.state.runtime_background_tasks.started is False


def test_runtime_api_app_registers_semantic_vector_refresh_task_when_service_exists() -> None:
    runtime_host = _build_runtime_host(
        semantic_vector_search=RecordingSemanticVectorSearchService(),
    )

    app = create_runtime_api_app(runtime_host=runtime_host)

    task_names = [task.name for task in app.state.runtime_background_tasks.default_tasks]
    assert "semantic-vector-refresh" in task_names
    refresh_task = next(
        task
        for task in app.state.runtime_background_tasks.default_tasks
        if task.name == "semantic-vector-refresh"
    )
    assert refresh_task.schedule == BackgroundTaskSchedule.interval(seconds=60)


def test_runtime_api_app_skips_semantic_vector_refresh_task_when_service_cannot_refresh() -> None:
    runtime_host = _build_runtime_host(
        semantic_vector_search=RecordingSemanticVectorSearchService(can_refresh=False),
    )

    app = create_runtime_api_app(runtime_host=runtime_host)

    task_names = [task.name for task in app.state.runtime_background_tasks.default_tasks]
    assert "semantic-vector-refresh" not in task_names
