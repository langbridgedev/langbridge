
from typing import Any

__all__ = ["register_runtime_ui"]


def __getattr__(name: str) -> Any:
    if name == "register_runtime_ui":
        from langbridge.ui.server import register_runtime_ui

        return register_runtime_ui
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
