"""Public connector package for the Langbridge monolith."""


from importlib import import_module
from typing import Any

_CANONICAL_PACKAGES = {
    "base": "langbridge.connectors.base",
    "builtin": "langbridge.connectors.builtin",
    "nosql": "langbridge.connectors.nosql",
    "saas": "langbridge.connectors.saas",
    "sql": "langbridge.connectors.sql",
    "vector": "langbridge.connectors.vector",
}

__all__ = list(_CANONICAL_PACKAGES)


def __getattr__(name: str) -> Any:
    module_name = _CANONICAL_PACKAGES.get(name)
    if module_name is not None:
        module = import_module(module_name)
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
