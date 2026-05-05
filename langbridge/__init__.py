"""Langbridge application package root."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from langbridge.client import LangbridgeClient

__version__ = "0.1.0a1"
__all__ = ["LangbridgeClient"]

def __getattr__(name: str) -> Any:
    if name == "LangbridgeClient":
        from langbridge.client import LangbridgeClient

        return LangbridgeClient
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
