from __future__ import annotations

import asyncio
import os
from collections.abc import Iterable
from pathlib import Path

import uvicorn

from langbridge.runtime.hosting.app import (
    _CONFIG_PATH_ENV,
    _DEBUG_ENV,
    _FEATURES_ENV,
    create_runtime_api_app,
)


def run_runtime_api(
    *,
    config_path: str | Path,
    host: str = "127.0.0.1",
    port: int = 8000,
    features: Iterable[str] = (),
    debug: bool = False,
    reload: bool = False,
) -> None:
    _configure_windows_event_loop_policy()
    normalized_features = [str(feature).strip().lower() for feature in features if str(feature).strip()]
    if reload:
        os.environ[_CONFIG_PATH_ENV] = str(Path(config_path).resolve())
        os.environ[_FEATURES_ENV] = ",".join(normalized_features)
        os.environ[_DEBUG_ENV] = "true" if debug else "false"
        uvicorn.run(
            "langbridge.runtime.hosting.app:create_runtime_api_app_from_env",
            host=host,
            port=port,
            reload=True,
            factory=True,
            log_level="debug" if debug else "info",
            timeout_graceful_shutdown=3,
        )
        return

    app = create_runtime_api_app(
        config_path=config_path,
        features=normalized_features,
        debug=debug,
    )
    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=False,
        log_level="debug" if debug else "info",
        timeout_graceful_shutdown=3,
    )


def _configure_windows_event_loop_policy() -> None:
    if os.name != "nt":
        return
    policy_factory = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if policy_factory is None:
        return
    if isinstance(asyncio.get_event_loop_policy(), policy_factory):
        return
    asyncio.set_event_loop_policy(policy_factory())
