from __future__ import annotations

from typing import Any

__all__ = [
    "BackgroundTaskExecutionContext",
    "BackgroundTaskKind",
    "BackgroundTaskSchedule",
    "RuntimeBackgroundTaskDefinition",
    "RuntimeBackgroundTaskManager",
    "build_connector_sync_default_task",
    "build_semantic_vector_refresh_default_task",
    "create_runtime_api_app",
    "run_runtime_api",
]


def __getattr__(name: str) -> Any:
    if name == "create_runtime_api_app":
        from langbridge.runtime.hosting.app import create_runtime_api_app

        return create_runtime_api_app
    if name == "BackgroundTaskExecutionContext":
        from langbridge.runtime.hosting.background import BackgroundTaskExecutionContext

        return BackgroundTaskExecutionContext
    if name == "BackgroundTaskKind":
        from langbridge.runtime.hosting.background import BackgroundTaskKind

        return BackgroundTaskKind
    if name == "BackgroundTaskSchedule":
        from langbridge.runtime.hosting.background import BackgroundTaskSchedule

        return BackgroundTaskSchedule
    if name == "RuntimeBackgroundTaskDefinition":
        from langbridge.runtime.hosting.background import RuntimeBackgroundTaskDefinition

        return RuntimeBackgroundTaskDefinition
    if name == "RuntimeBackgroundTaskManager":
        from langbridge.runtime.hosting.background import RuntimeBackgroundTaskManager

        return RuntimeBackgroundTaskManager
    if name == "build_connector_sync_default_task":
        from langbridge.runtime.hosting.background import build_connector_sync_default_task

        return build_connector_sync_default_task
    if name == "build_semantic_vector_refresh_default_task":
        from langbridge.runtime.hosting.background import build_semantic_vector_refresh_default_task

        return build_semantic_vector_refresh_default_task
    if name == "run_runtime_api":
        from langbridge.runtime.hosting.server import run_runtime_api

        return run_runtime_api
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
