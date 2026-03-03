import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    AuthenticationError,
    AuthorizationError,
    ResourceAlreadyExists, 
    ResourceNotFound, 
    InvalidRequest, 
    ApplicationError,
    BusinessValidationError
)
from langbridge.packages.common.langbridge_common.config import settings

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
            return JSONResponse(
                status_code=409,
                content={"error": "ResourceAlreadyExists", "message": str(e)}
            )
        except ResourceNotFound as e:
            self.logger.warning(f"Resource not found: {e}")
            return JSONResponse(
                status_code=404,
                content={"error": "ResourceNotFound", "message": str(e)}
            )
        except InvalidRequest as e:
            self.logger.warning(f"Invalid request: {e}")
            return JSONResponse(
                status_code=400,
                content={"error": "InvalidRequest", "message": str(e)}
            )
        except BusinessValidationError as e:
            self.logger.warning(f"Business validation error: {e}")
            return JSONResponse(
                status_code=400,
                content={"error": "BusinessValidationError", "message": str(e)}
            )
        except AuthenticationError as e:
            self.logger.warning(f"Authentication error: {e}")
            return JSONResponse(
                status_code=401,
                content={"error": "AuthenticationError", "message": str(e)}
            )
        except AuthorizationError as e:
            self.logger.warning(f"Authorization error: {e}")
            return JSONResponse(
                status_code=403,
                content={"error": "AuthorizationError", "message": str(e)}
            )
        except ApplicationError as e:
            self.logger.error("Application error", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={"error": "ApplicationError", "message": "An internal application error occurred."}
            )
        except ValueError as e:
            self.logger.warning(f"Value error: {e}")
            return JSONResponse(
                status_code=400,
                content={"error": "ValueError", "message": str(e)}
            )
        except Exception as e:
            self.logger.error("Unhandled exception", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={"error": "InternalServerError", "message": "An internal server error occurred."}
            )