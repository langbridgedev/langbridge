import logging
from typing import Final

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from langbridge.apps.api.langbridge_api.ioc import Container
from langbridge.packages.common.langbridge_common.db.session_context import (
    reset_session,
    set_session,
)

READ_ONLY_METHODS: Final[set[str]] = {"GET", "HEAD", "OPTIONS"}
CONNECTOR_SCHEMA_ROUTE_SEGMENTS: Final[tuple[str, ...]] = (
    "/source/",
    "/catalog",
)


class UnitOfWorkMiddleware(BaseHTTPMiddleware):
    """
    Request-scoped SQLAlchemy session middleware.

    Behaviour:
    - Creates one async session per request.
    - Exposes it through the current session context.
    - For read-only methods (GET/HEAD/OPTIONS), does not commit.
    - For write methods, commits only if the response completed successfully.
    - Rolls back on exceptions or failed write responses.
    - Always closes the session.
    """

    def __init__(self, app):
        super().__init__(app)
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _should_skip_request_session(request: Request) -> bool:
        path = request.url.path
        if not path.startswith("/api/v1/connectors/"):
            return False
        return any(segment in path for segment in CONNECTOR_SCHEMA_ROUTE_SEGMENTS)

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        if self._should_skip_request_session(request):
            self.logger.debug(
                "UoW: skipping request-scoped session for connector schema route %s %s",
                request.method,
                request.url.path,
            )
            return await call_next(request)

        container: Container = request.app.state.container  # type: ignore[attr-defined]

        session_factory = container.async_session_factory()
        session = session_factory()
        token = set_session(session)

        is_read_only = request.method.upper() in READ_ONLY_METHODS

        try:
            self.logger.debug(
                "UoW: opened async DB session for %s %s (read_only=%s)",
                request.method,
                request.url.path,
                is_read_only,
            )

            response = await call_next(request)

            if is_read_only:
                # No commit for GET/HEAD/OPTIONS.
                # If SQLAlchemy started a transaction implicitly due to reads,
                # close/rollback in finally will clean it up.
                self.logger.debug(
                    "UoW: skipping commit for read-only request %s %s",
                    request.method,
                    request.url.path,
                )
                return response

            if response.status_code >= 400:
                self.logger.debug(
                    "UoW: rolling back write request due to status code %s",
                    response.status_code,
                )
                await session.rollback()
            else:
                self.logger.debug("UoW: committing write request")
                await session.commit()

            return response

        except BaseException as exc:
            self.logger.exception("UoW: exception during request, rolling back: %s", exc)
            await session.rollback()
            raise

        finally:
            reset_session(token)

            # Defensive cleanup. A session may still have an open transaction
            # because SQLAlchemy can autobegin on first use.
            if session.in_transaction():
                self.logger.debug("UoW: cleaning up open transaction with rollback")
                await session.rollback()
                if is_read_only:
                    self.logger.warning(
                        "UoW: read-only request unexpectedly started a transaction, rolled back"
                    ) # this is a sign of some code doing writes during a read-only request, which should be investigated

            await session.close()
            self.logger.debug("UoW: session closed")
