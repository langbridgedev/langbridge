from __future__ import annotations

from dataclasses import dataclass

from langbridge.packages.runtime.local_config import build_configured_local_runtime
from langbridge.packages.runtime.registry.bootstrap import (
    build_hosted_runtime,
    build_local_runtime,
)


@dataclass(slots=True)
class RuntimeBootstrapConfig:
    control_plane_base_url: str | None = None
    service_token: str | None = None


__all__ = [
    "RuntimeBootstrapConfig",
    "build_hosted_runtime",
    "build_local_runtime",
    "build_configured_local_runtime",
]
