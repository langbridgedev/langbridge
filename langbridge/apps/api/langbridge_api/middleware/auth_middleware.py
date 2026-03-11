
import logging
import secrets
import uuid
from typing import Optional, Any

from fastapi import status
from jose import JWTError
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from dependency_injector.wiring import Provide, inject

from langbridge.apps.api.langbridge_api.ioc import Container
from langbridge.apps.api.langbridge_api.services.auth.token_service import TokenService
from langbridge.apps.api.langbridge_api.services.auth.token_service import TokenService
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.apps.api.langbridge_api.auth.jwt import verify_jwt
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.apps.api.langbridge_api.services.auth.auth_service import AuthService
from langbridge.apps.api.langbridge_api.services.service_utils import (
    reset_internal_service_call,
    set_internal_service_call,
)

PATHS_TO_EXCLUDE = [
    "/api/v1/auth/health",
    "/api/v1/auth/login",
    "/api/v1/auth/register",
    "/api/v1/auth/github/callback",
    "/api/v1/auth/google/callback",
    "/api/v1/auth/logout",
    "/api/v1/auth/me",
    "/metrics",
    "/docs",
    "/openapi.json",
    "/api/v1/runtimes/register",
    "/api/v1/runtimes/heartbeat",
    "/api/v1/runtimes/capabilities",
    "/api/v1/edge/tasks/pull",
    "/api/v1/edge/tasks/ack",
    "/api/v1/edge/tasks/result",
    "/api/v1/edge/tasks/fail",
]

INTERNAL_SERVICE_HEADER = "x-langbridge-service-token"
INTERNAL_SERVICE_USERNAME = "internal-service"
INTERNAL_SERVICE_USER_ID = uuid.UUID(int=0)

AUTHORIZATION_HEADER_PREFIX = "Bearer "

class AuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle JWT authentication via cookies.
    """
    def __init__(self, app):
        super().__init__(app)
        self.logger = logging.getLogger(__name__)

    @inject
    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
        auth_service: AuthService = Provide[Container.auth_service]
    ) -> Response:
        self.logger.debug(f"AuthMiddleware: Processing request {request.method} {request.url.path}")

        internal_token = request.headers.get(INTERNAL_SERVICE_HEADER)
        if internal_token:
            if settings.SERVICE_USER_SECRET and secrets.compare_digest(
                internal_token, settings.SERVICE_USER_SECRET
            ):
                request.state.is_internal = True
                request.state.username = INTERNAL_SERVICE_USERNAME
                request.state.user = UserResponse(
                    id=INTERNAL_SERVICE_USER_ID,
                    username=INTERNAL_SERVICE_USERNAME,
                    email=None,
                    is_active=True,
                    available_organizations=[],
                    available_projects=[],
                )
                if hasattr(request.state, "request_context"):
                    request.state.request_context.user = request.state.user
                ctx_token = set_internal_service_call(True)
                try:
                    return await call_next(request)
                finally:
                    reset_internal_service_call(ctx_token)
            else:
                self.logger.warning(
                    "AuthMiddleware: invalid internal service token for %s",
                    request.url.path,
                )

        if any(request.url.path.startswith(path) for path in PATHS_TO_EXCLUDE):
            return await call_next(request)

        token = request.cookies.get(settings.COOKIE_NAME)
        if not token:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"error": "Unauthenticated", "message": "Missing authentication cookie"}
            )

        try:
            claims = verify_jwt(token)
        except JWTError as exc:
            self.logger.warning(f"Invalid JWT: {exc}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"error": "InvalidSession", "message": "Invalid authentication session"}
            )
        
        if not claims:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"error": "InvalidSession", "message": "Invalid authentication session"}
            )

        username = claims.get("username") if isinstance(claims, dict) else None
        if not username:
             return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"error": "InvalidToken", "message": "Token payload is missing username"}
            )

        # Set user context
        try:
            request.state.username = username
            user: UserResponse = await auth_service.get_user_by_username(username)
            request.state.user = user
            if hasattr(request.state, "request_context"):
                request.state.request_context.user = user
        except Exception as e:
            self.logger.error(f"Failed to load user '{username}': {e}")
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"error": "UserNotFound", "message": "Authenticated user could not be found"}
            )

        return await call_next(request)
