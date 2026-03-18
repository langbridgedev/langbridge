from typing import Any

__all__ = [
    "RuntimeContext",
    "RuntimeHost",
    "build_local_runtime",
    "build_hosted_runtime",
    "build_configured_local_runtime",
    "create_runtime_api_app",
    "run_runtime_api",
]


def __getattr__(name: str) -> Any:
    if name == "RuntimeContext":
        from langbridge.packages.runtime.context import RuntimeContext

        return RuntimeContext
    if name == "RuntimeHost":
        from langbridge.packages.runtime.services.runtime_host import RuntimeHost

        return RuntimeHost
    if name in {"build_local_runtime", "build_hosted_runtime"}:
        from langbridge.packages.runtime.registry.bootstrap import (
            build_hosted_runtime,
            build_local_runtime,
        )

        return {
            "build_local_runtime": build_local_runtime,
            "build_hosted_runtime": build_hosted_runtime,
        }[name]
    if name == "build_configured_local_runtime":
        from langbridge.packages.runtime.local_config import build_configured_local_runtime

        return build_configured_local_runtime
    if name in {"create_runtime_api_app", "run_runtime_api"}:
        from langbridge.hosting import create_runtime_api_app, run_runtime_api

        return {
            "create_runtime_api_app": create_runtime_api_app,
            "run_runtime_api": run_runtime_api,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
