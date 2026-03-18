"""Canonical runtime contract namespace for the Langbridge monolith."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_CONTRACT_MODULES = (
    "base",
    "agents",
    "auth",
    "connectors",
    "dashboards",
    "datasets",
    "jobs",
    "llm_connections",
    "organizations",
    "query",
    "runtime",
    "semantic",
    "sql",
    "threads",
)

__all__: list[str] = []


def __getattr__(name: str) -> Any:
    for module_name in _CONTRACT_MODULES:
        module = import_module(f"{__name__}.{module_name}")
        if hasattr(module, name):
            value = getattr(module, name)
            globals()[name] = value
            return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
