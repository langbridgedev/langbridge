import logging
import os
import tempfile
from logging.handlers import RotatingFileHandler
from typing import Optional

DEFAULT_LOG_DIR = "./"
DEFAULT_LOG_FILE = "app.log"
DEFAULT_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

def get_root_logger() -> logging.Logger:
    """Return the process-wide root logger."""
    return logging.getLogger("")


def _build_formatter() -> logging.Formatter:
    return logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _build_file_handler(
    log_dir: str,
    log_file: str,
    *,
    truncate: bool,
    formatter: logging.Formatter,
) -> RotatingFileHandler:
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, log_file)

    if truncate and os.path.exists(path):
        with open(path, "w", encoding="utf-8"):
            pass

    try:
        file_handler = RotatingFileHandler(
            path,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
    except OSError:
        fallback_dir = os.path.join(tempfile.gettempdir(), "langbridge-logs")
        os.makedirs(fallback_dir, exist_ok=True)
        fallback_path = os.path.join(fallback_dir, log_file)
        file_handler = RotatingFileHandler(
            fallback_path,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
    file_handler.setFormatter(formatter)
    return file_handler


def _ensure_handlers(logger: logging.Logger, handlers: list[logging.Handler]) -> None:
    # Avoid duplicate handlers when setup is called multiple times.
    existing = set(logger.handlers)
    for handler in handlers:
        if handler not in existing:
            logger.addHandler(handler)


def setup_logging(
    *,
    service_name: Optional[str] = None,
    level: str | int = DEFAULT_LOG_LEVEL,
    log_dir: str = DEFAULT_LOG_DIR,
    log_file: str = DEFAULT_LOG_FILE,
    truncate: bool = False,
    with_console: bool = True,
) -> logging.Logger:
    """
    Configure application logging with OpenTelemetry OTLP exporters.
    Safe to call multiple times; handlers are only added once.
    """
    root = get_root_logger()
    root.setLevel(level)
    formatter = _build_formatter()
    handlers: list[logging.Handler] = []
    if with_console:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        handlers.append(console)

    handlers.insert(
        0,
        _build_file_handler(
            log_dir,
            log_file,
            truncate=truncate,
            formatter=formatter,
        ),
    )
    _ensure_handlers(root, handlers)

    root.propagate = True
    return root