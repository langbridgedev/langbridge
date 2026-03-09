import asyncio
from contextlib import asynccontextmanager, suppress
import inspect
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import Response

from alembic import command
from alembic.config import Config

from langbridge.apps.api.langbridge_api.error_responses import (
    build_error_response,
    build_validation_error_response,
)
from langbridge.apps.api.langbridge_api.routers import api_router_v1
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.apps.api.langbridge_api.ioc import build_container
from langbridge.apps.api.langbridge_api.ioc.wiring import wire_packages
from langbridge.packages.common.langbridge_common.logging.logger import setup_logging
from langbridge.packages.common.langbridge_common.monitoring import (
    PrometheusMiddleware,
    metrics_response,
)

from langbridge.apps.api.langbridge_api.middleware import (
    MessageFlusherMiddleware,
    UnitOfWorkMiddleware,
    ErrorMiddleware,
    AuthMiddleware,
    RequestContextMiddleware,
    CorrelationIdMiddleware,
)
from dotenv import load_dotenv

try:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test envs
    FastAPIInstrumentor = None  # type: ignore[assignment]

load_dotenv()
logger = logging.getLogger(__name__)
ROOT_DIR = Path(__file__).resolve().parents[4]
ALEMBIC_CONFIG_PATH = ROOT_DIR / "alembic.ini"


def _resolve_sqlite_path(sqlite_url: str) -> Path | None:
    if sqlite_url.startswith("sqlite:///"):
        path = sqlite_url.removeprefix("sqlite:///")
    elif sqlite_url.startswith("sqlite://"):
        path = sqlite_url.removeprefix("sqlite://")
    else:
        return None
    if not path or path == ":memory:":
        return None
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = ROOT_DIR / resolved
    return resolved


def _should_apply_local_sqlite_schema() -> bool:
    return (
        settings.ENVIRONMENT == "local"
        and settings.SQLALCHEMY_DATABASE_URI.startswith("sqlite")
    )


def _ensure_local_sqlite_schema() -> None:
    if not _should_apply_local_sqlite_schema():
        return
    sqlite_path = _resolve_sqlite_path(settings.SQLALCHEMY_DATABASE_URI)
    if sqlite_path:
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Ensuring local SQLite database directory exists at %s", sqlite_path.parent)
    if not ALEMBIC_CONFIG_PATH.exists():
        logger.warning("Skipping local migration: %s missing", ALEMBIC_CONFIG_PATH)
        return
    config = Config(str(ALEMBIC_CONFIG_PATH))
    config.set_main_option("sqlalchemy.url", settings.SQLALCHEMY_DATABASE_URI)
    logger.info("Running Alembic upgrade for local SQLite (%s)", settings.SQLALCHEMY_DATABASE_URI)
    try:
        command.upgrade(config, "head")
        logger.info("Alembic upgrade successful for local SQLite")
    except Exception as e:
        logger.error("Alembic upgrade failed for local SQLite: %s", e)
        raise

def custom_generate_unique_id(route: APIRoute) -> str:
    if len(route.tags) == 0:
        return route.name
    return f"{route.tags[0]}-{route.name}"

container = build_container(settings)
wire_packages(
    container,
    package_names=[
        "langbridge.apps.api.langbridge_api.routers",
        "langbridge.apps.api.langbridge_api.services",
        "langbridge.packages.common.langbridge_common.repositories",
        "langbridge.apps.api.langbridge_api.repositories",
        "langbridge.apps.api.langbridge_api.auth",
        "langbridge.apps.api.langbridge_api.middleware",
    ],
    extra_modules=["langbridge.apps.api.langbridge_api.main"],
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager to handle startup and shutdown events."""
    try:
        _ensure_local_sqlite_schema()
        logger.info("Local SQLite schema ensured successfully.")
    except Exception as e:
        logger.error("Error during local SQLite schema ensure: %s", e)
        raise
    init_result = container.init_resources()
    if inspect.isawaitable(init_result):
        await init_result
    app.state.container = container
    job_event_consumer = container.job_event_consumer()
    consumer_task = asyncio.create_task(
        job_event_consumer.run(),
        name="job-event-consumer",
    )
    try:
        yield
    finally:
        await job_event_consumer.stop()
        consumer_task.cancel()
        with suppress(asyncio.CancelledError):
            await consumer_task
        shutdown_result = container.shutdown_resources()
        if inspect.isawaitable(shutdown_result):
            await shutdown_result

setup_logging(service_name=settings.PROJECT_NAME)

app = FastAPI(
    title=settings.PROJECT_NAME,
    generate_unique_id_function=custom_generate_unique_id,
    lifespan=lifespan,
)


# Middleware
# Starlette executes middleware in reverse order of addition (last added runs first).
# Add middleware from innermost to outermost to preserve the intended execution order.
app.add_middleware(PrometheusMiddleware, service_name="langbridge_api")
app.add_middleware(AuthMiddleware)
# Unit of Work should run before auth so DB access works during authentication.
app.add_middleware(UnitOfWorkMiddleware)
app.add_middleware(CorrelationIdMiddleware)
app.add_middleware(RequestContextMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.SESSION_SECRET,
    same_site="lax",
    https_only=False,
)
app.add_middleware(MessageFlusherMiddleware)
app.add_middleware(ErrorMiddleware)
# Flush messages after the request commits.

if settings.CORS_ENABLED:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["Authorization", "Content-Type"],
    )

app.include_router(
    api_router_v1,
    prefix=settings.API_V1_STR,
)


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> Response:
    detail = exc.detail
    if isinstance(detail, dict) and "error" in detail:
        return build_error_response(
            status_code=exc.status_code,
            details=detail["error"].get("details") if isinstance(detail.get("error"), dict) else detail,
            code=detail["error"].get("code") if isinstance(detail.get("error"), dict) else None,
            message=detail["error"].get("message") if isinstance(detail.get("error"), dict) else None,
            suggestions=detail["error"].get("suggestions") if isinstance(detail.get("error"), dict) else None,
            field_errors=detail["error"].get("fieldErrors") if isinstance(detail.get("error"), dict) else None,
        )

    return build_error_response(
        status_code=exc.status_code,
        details=detail,
        message=str(detail) if detail else None,
    )


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(_: Request, exc: RequestValidationError) -> Response:
    return build_validation_error_response(exc)


@app.get("/metrics")
def metrics() -> Response:
    return metrics_response()

if FastAPIInstrumentor is not None:
    FastAPIInstrumentor.instrument_app(app)
else:
    logger.warning("OpenTelemetry FastAPI instrumentation is unavailable; skipping instrumentation.")

if __name__ == "__main__":
    import uvicorn

    host = getattr(settings, "HOST", "0.0.0.0")
    port = int(getattr(settings, "PORT", 8000))
    reload = bool(getattr(settings, "UVICORN_RELOAD", settings.IS_LOCAL))
    log_level = getattr(settings, "UVICORN_LOG_LEVEL", "info")

    # You can also set workers via env/CLI in production (e.g., `--workers 4`)

    uvicorn.run(
        "langbridge.apps.api.langbridge_api.main:app",
        host=host,
        port=port,
        reload=reload,
        log_level=log_level,
        factory=False,
    )
