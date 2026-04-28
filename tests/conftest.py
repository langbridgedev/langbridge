import logging
import sys
import types

import pytest


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _ensure_opentelemetry_stub() -> None:
    if "opentelemetry" in sys.modules:
        return

    class _NoopExporter:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            pass

    class _NoopProcessor:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            pass

    class _NoopInstrumentor:
        def instrument(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            return None

    class _LoggerProvider:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            pass

        def add_log_record_processor(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            return None

    class _LoggingHandler(logging.Handler):
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            super().__init__()

    class _Resource:
        @classmethod
        def create(cls, *_args, **_kwargs):  # noqa: ANN002, ANN003
            return cls()

    class _TracerProvider:
        def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            pass

        def add_span_processor(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
            return None

    opentelemetry = types.ModuleType("opentelemetry")
    opentelemetry._logs = types.SimpleNamespace(set_logger_provider=lambda *_args, **_kwargs: None)
    opentelemetry.trace = types.SimpleNamespace(set_tracer_provider=lambda *_args, **_kwargs: None)

    modules = {
        "opentelemetry": opentelemetry,
        "opentelemetry.exporter": types.ModuleType("opentelemetry.exporter"),
        "opentelemetry.exporter.otlp": types.ModuleType("opentelemetry.exporter.otlp"),
        "opentelemetry.exporter.otlp.proto": types.ModuleType("opentelemetry.exporter.otlp.proto"),
        "opentelemetry.exporter.otlp.proto.grpc": types.ModuleType("opentelemetry.exporter.otlp.proto.grpc"),
        "opentelemetry.exporter.otlp.proto.grpc._log_exporter": types.ModuleType("opentelemetry.exporter.otlp.proto.grpc._log_exporter"),
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": types.ModuleType("opentelemetry.exporter.otlp.proto.grpc.trace_exporter"),
        "opentelemetry.exporter.otlp.proto.http": types.ModuleType("opentelemetry.exporter.otlp.proto.http"),
        "opentelemetry.exporter.otlp.proto.http._log_exporter": types.ModuleType("opentelemetry.exporter.otlp.proto.http._log_exporter"),
        "opentelemetry.exporter.otlp.proto.http.trace_exporter": types.ModuleType("opentelemetry.exporter.otlp.proto.http.trace_exporter"),
        "opentelemetry.instrumentation": types.ModuleType("opentelemetry.instrumentation"),
        "opentelemetry.instrumentation.logging": types.ModuleType("opentelemetry.instrumentation.logging"),
        "opentelemetry.sdk": types.ModuleType("opentelemetry.sdk"),
        "opentelemetry.sdk._logs": types.ModuleType("opentelemetry.sdk._logs"),
        "opentelemetry.sdk._logs.export": types.ModuleType("opentelemetry.sdk._logs.export"),
        "opentelemetry.sdk.resources": types.ModuleType("opentelemetry.sdk.resources"),
        "opentelemetry.sdk.trace": types.ModuleType("opentelemetry.sdk.trace"),
        "opentelemetry.sdk.trace.export": types.ModuleType("opentelemetry.sdk.trace.export"),
    }

    modules["opentelemetry.exporter.otlp.proto.grpc._log_exporter"].OTLPLogExporter = _NoopExporter
    modules["opentelemetry.exporter.otlp.proto.grpc.trace_exporter"].OTLPSpanExporter = _NoopExporter
    modules["opentelemetry.exporter.otlp.proto.http._log_exporter"].OTLPLogExporter = _NoopExporter
    modules["opentelemetry.exporter.otlp.proto.http.trace_exporter"].OTLPSpanExporter = _NoopExporter
    modules["opentelemetry.instrumentation.logging"].LoggingInstrumentor = _NoopInstrumentor
    modules["opentelemetry.sdk._logs"].LoggerProvider = _LoggerProvider
    modules["opentelemetry.sdk._logs"].LoggingHandler = _LoggingHandler
    modules["opentelemetry.sdk._logs.export"].BatchLogRecordProcessor = _NoopProcessor
    modules["opentelemetry.sdk.resources"].Resource = _Resource
    modules["opentelemetry.sdk.trace"].TracerProvider = _TracerProvider
    modules["opentelemetry.sdk.trace.export"].BatchSpanProcessor = _NoopProcessor

    sys.modules.update(modules)


_ensure_opentelemetry_stub()
