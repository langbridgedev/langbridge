from __future__ import annotations

from typing import Any

from langbridge.packages.runtime.context import RuntimeContext

__all__ = [
    "RuntimeContext",
    "RuntimeHost",
    "build_local_runtime",
    "build_hosted_runtime",
    "build_configured_local_runtime",
]


def __getattr__(name: str) -> Any:
    if name == "RuntimeHost":
        from langbridge.packages.runtime.services.runtime_host import RuntimeHost

        return RuntimeHost
    if name == "build_local_runtime":
        from langbridge.packages.runtime.registry.bootstrap import build_local_runtime

        return build_local_runtime
    if name == "build_hosted_runtime":
        from langbridge.packages.runtime.registry.bootstrap import build_hosted_runtime

        return build_hosted_runtime
    if name == "build_configured_local_runtime":
        from langbridge.packages.runtime.local_config import build_configured_local_runtime

        return build_configured_local_runtime
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
