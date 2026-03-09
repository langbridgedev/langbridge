import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from langbridge.apps.api.langbridge_api.error_responses import build_error_response
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    AuthenticationError,
    AuthorizationError,
    PermissionDeniedBusinessValidationError,
    ResourceAlreadyExists, 
    ResourceNotFound, 
    InvalidRequest, 
    ApplicationError,
    BusinessValidationError
)

class ErrorMiddleware(BaseHTTPMiddleware):
    """
    Middleware to catch application-specific exceptions and return structured JSON responses.
    """
    def __init__(self, app):
        super().__init__(app)
        self.logger = logging.getLogger(__name__)
        # Set level to INFO by default, can be overridden by config
        self.logger.setLevel(logging.INFO)

    async def dispatch(self, request: Request, call_next) -> Response:
        try:
            response = await call_next(request)
            return response
        except ResourceAlreadyExists as e:
            self.logger.warning(f"Resource already exists: {e}")
            return build_error_response(
                status_code=409,
                code="RESOURCE_ALREADY_EXISTS",
                message=str(e),
                details=str(e),
            )
        except ResourceNotFound as e:
            self.logger.warning(f"Resource not found: {e}")
            return build_error_response(
                status_code=404,
                code="RESOURCE_NOT_FOUND",
                message=str(e),
                details=str(e),
            )
        except InvalidRequest as e:
            self.logger.warning(f"Invalid request: {e}")
            return build_error_response(
                status_code=400,
                code="INVALID_REQUEST",
                message=str(e),
                details=str(e),
            )
        except BusinessValidationError as e:
            self.logger.warning(f"Business validation error: {e}")
            return build_error_response(
                status_code=400,
                code="BUSINESS_VALIDATION_ERROR",
                message=e.message,
                details=str(e),
                field_errors=e.errors,
            )
        except PermissionDeniedBusinessValidationError as e:
            self.logger.warning(f"Permission denied business validation error: {e}")
            return build_error_response(
                status_code=403,
                code="PERMISSION_DENIED",
                message=e.message,
                details=str(e),
                field_errors=e.errors,
            )
        except AuthenticationError as e:
            self.logger.warning(f"Authentication error: {e}")
            return build_error_response(
                status_code=401,
                code="AUTHENTICATION_REQUIRED",
                message=str(e),
                details=str(e),
            )
        except AuthorizationError as e:
            self.logger.warning(f"Authorization error: {e}")
            return build_error_response(
                status_code=403,
                code="AUTHORIZATION_ERROR",
                message=str(e),
                details=str(e),
            )
        except ApplicationError as e:
            self.logger.error("Application error", exc_info=True)
            return build_error_response(
                status_code=500,
                code="APPLICATION_ERROR",
                message="An internal application error occurred.",
            )
        except ValueError as e:
            self.logger.warning(f"Value error: {e}")
            return build_error_response(
                status_code=400,
                code="INVALID_VALUE",
                message=str(e),
                details=str(e),
            )
        except Exception as e:
            self.logger.error("Unhandled exception", exc_info=True)
            return build_error_response(
                status_code=500,
                code="INTERNAL_ERROR",
                message="An internal server error occurred.",
            )
