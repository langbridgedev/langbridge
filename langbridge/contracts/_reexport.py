from __future__ import annotations

from importlib import import_module
from typing import Any


def reexport_public_api(
    legacy_module_name: str,
    target_module_name: str,
    *,
    include_private: tuple[str, ...] = (),
) -> dict[str, Any]:
    legacy_module = import_module(legacy_module_name)
    export_names = getattr(legacy_module, "__all__", None)
    if export_names is None:
        export_names = [
            name
            for name in vars(legacy_module)
            if not name.startswith("_") or name in include_private
        ]

    exported: dict[str, Any] = {}
    for name in export_names:
        if name.startswith("_") and name not in include_private:
            continue

        value = getattr(legacy_module, name)
        module_name = getattr(value, "__module__", "")
        if module_name == legacy_module_name or module_name.startswith(f"{legacy_module_name}."):
            try:
                value.__module__ = target_module_name
            except (AttributeError, TypeError):
                pass
        exported[name] = value

    return exported
