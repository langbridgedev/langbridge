
import asyncio
import os
from collections.abc import Iterable
from pathlib import Path

import uvicorn

from langbridge.runtime.hosting.app import (
    _BACKGROUND_TASKS_ENV,
    _CONFIG_PATH_ENV,
    _DEBUG_ENV,
    _FEATURES_ENV,
    _ODBC_HOST_ENV,
    _ODBC_PORT_ENV,
    _WORKERS_ENV,
    create_runtime_api_app,
)

_DEFAULT_GRACEFUL_SHUTDOWN_SECONDS = 30


def run_runtime_api(
    *,
    config_path: str | Path,
    host: str = "127.0.0.1",
    port: int = 8000,
    features: Iterable[str] = (),
    debug: bool = False,
    reload: bool = False,
    workers: int = 1,
    odbc_host: str | None = None,
    odbc_port: int | None = None,
) -> None:
    _configure_windows_event_loop_policy()
    normalized_features = [str(feature).strip().lower() for feature in features if str(feature).strip()]
    workers = int(workers)
    if workers < 1:
        raise ValueError("workers must be greater than or equal to 1.")
    if reload and workers > 1:
        raise ValueError("--reload cannot be combined with --workers greater than 1.")
    if workers > 1 and "odbc" in normalized_features:
        raise ValueError("--workers greater than 1 cannot be combined with the odbc feature.")
    if reload:
        _configure_runtime_app_env(
            config_path=config_path,
            features=normalized_features,
            debug=debug,
            odbc_host=odbc_host,
            odbc_port=odbc_port,
            workers=workers,
        )
        uvicorn.run(
            "langbridge.runtime.hosting.app:create_runtime_api_app_from_env",
            host=host,
            port=port,
            reload=True,
            factory=True,
            log_level="debug" if debug else "info",
            timeout_graceful_shutdown=_DEFAULT_GRACEFUL_SHUTDOWN_SECONDS,
        )
        return

    if workers > 1:
        _configure_runtime_app_env(
            config_path=config_path,
            features=normalized_features,
            debug=debug,
            odbc_host=odbc_host,
            odbc_port=odbc_port,
            workers=workers,
        )
        uvicorn.run(
            "langbridge.runtime.hosting.app:create_runtime_api_app_from_env",
            host=host,
            port=port,
            reload=False,
            factory=True,
            workers=workers,
            log_level="debug" if debug else "info",
            timeout_graceful_shutdown=_DEFAULT_GRACEFUL_SHUTDOWN_SECONDS,
        )
        return

    app = create_runtime_api_app(
        config_path=config_path,
        features=normalized_features,
        debug=debug,
        odbc_host=odbc_host,
        odbc_port=odbc_port,
        workers=workers,
    )
    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=False,
        log_level="debug" if debug else "info",
        timeout_graceful_shutdown=_DEFAULT_GRACEFUL_SHUTDOWN_SECONDS,
    )


def _configure_runtime_app_env(
    *,
    config_path: str | Path,
    features: Iterable[str],
    debug: bool,
    odbc_host: str | None,
    odbc_port: int | None,
    workers: int,
) -> None:
    os.environ[_CONFIG_PATH_ENV] = str(Path(config_path).resolve())
    os.environ[_FEATURES_ENV] = ",".join(features)
    os.environ[_DEBUG_ENV] = "true" if debug else "false"
    os.environ[_WORKERS_ENV] = str(max(1, int(workers or 1)))
    os.environ[_BACKGROUND_TASKS_ENV] = "auto"
    if odbc_host is not None:
        os.environ[_ODBC_HOST_ENV] = str(odbc_host)
    else:
        os.environ.pop(_ODBC_HOST_ENV, None)
    if odbc_port is not None:
        os.environ[_ODBC_PORT_ENV] = str(odbc_port)
    else:
        os.environ.pop(_ODBC_PORT_ENV, None)


def _configure_windows_event_loop_policy() -> None:
    if os.name != "nt":
        return
    policy_factory = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if policy_factory is None:
        return
    if isinstance(asyncio.get_event_loop_policy(), policy_factory):
        return
    asyncio.set_event_loop_policy(policy_factory())
