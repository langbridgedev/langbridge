"""
Logging utilities with OpenTelemetry defaults for logs and traces.
"""
import logging
import os
import tempfile
from logging.handlers import RotatingFileHandler
from typing import Optional

try:
    from opentelemetry import _logs, trace
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
        OTLPLogExporter as GrpcOTLPLogExporter,
    )
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter as GrpcOTLPSpanExporter,
    )
    from opentelemetry.exporter.otlp.proto.http._log_exporter import (
        OTLPLogExporter as HttpOTLPLogExporter,
    )
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter as HttpOTLPSpanExporter,
    )
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    OTEL_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - optional in local/dev test environments
    OTEL_AVAILABLE = False
    _logs = None  # type: ignore[assignment]
    trace = None  # type: ignore[assignment]
    GrpcOTLPLogExporter = None  # type: ignore[assignment]
    GrpcOTLPSpanExporter = None  # type: ignore[assignment]
    HttpOTLPLogExporter = None  # type: ignore[assignment]
    HttpOTLPSpanExporter = None  # type: ignore[assignment]
    LoggingInstrumentor = None  # type: ignore[assignment]
    LoggerProvider = None  # type: ignore[assignment]
    LoggingHandler = None  # type: ignore[assignment]
    BatchLogRecordProcessor = None  # type: ignore[assignment]
    Resource = None  # type: ignore[assignment]
    TracerProvider = None  # type: ignore[assignment]
    BatchSpanProcessor = None  # type: ignore[assignment]

DEFAULT_LOG_DIR = "./"
DEFAULT_LOG_FILE = "app.log"
DEFAULT_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DEFAULT_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "langbridge")

_otel_initialized = False


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

def _exporter_enabled(env_key: str, default: str = "otlp") -> bool:
    return os.getenv(env_key, default).strip().lower() not in {"none", "disabled"}


def _get_protocol(env_key: str) -> str:
    value = os.getenv(env_key)
    if value:
        return value.strip().lower()
    return os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc").strip().lower()


def _build_log_exporter() -> Optional[object]:
    if not _exporter_enabled("OTEL_LOGS_EXPORTER"):
        return None
    protocol = _get_protocol("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL")
    if protocol in {"http/protobuf", "http"}:
        return HttpOTLPLogExporter()
    return GrpcOTLPLogExporter()


def _build_span_exporter() -> Optional[object]:
    if not _exporter_enabled("OTEL_TRACES_EXPORTER"):
        return None
    protocol = _get_protocol("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
    if protocol in {"http/protobuf", "http"}:
        return HttpOTLPSpanExporter()
    return GrpcOTLPSpanExporter()


def _build_resource(service_name: Optional[str]) -> Resource: # type: ignore
    if Resource is None:  # pragma: no cover - guarded by _otel_disabled
        raise RuntimeError("OpenTelemetry resource support is unavailable")
    name = service_name or DEFAULT_SERVICE_NAME
    return Resource.create({"service.name": name})


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
    global _otel_initialized

    root = get_root_logger()
    root.setLevel(level)

    if _otel_initialized:
        return root

    _otel_initialized = True

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
    return root

    resource = _build_resource(service_name)

    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)

    span_exporter = _build_span_exporter()
    if span_exporter is not None:
        tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))

    logger_provider = LoggerProvider(resource=resource)
    _logs.set_logger_provider(logger_provider)

    log_exporter = _build_log_exporter()
    if log_exporter is not None:
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
        otel_handler = LoggingHandler(level=level, logger_provider=logger_provider)
        handlers.insert(0, otel_handler)

    LoggingInstrumentor().instrument(set_logging_format=False)

    _ensure_handlers(root, handlers)

    # Propagate to child loggers automatically.
    root.propagate = True
    return root


# Backwards compatibility shim for existing imports.
def setup_file_logging() -> logging.Logger:
    return setup_logging()
