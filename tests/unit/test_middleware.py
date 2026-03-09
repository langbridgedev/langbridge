import pytest
from unittest.mock import AsyncMock, MagicMock
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import Scope, Receive, Send

from langbridge.apps.api.langbridge_api.middleware.error_middleware import ErrorMiddleware
from langbridge.packages.common.langbridge_common.errors.application_errors import ResourceNotFound, ApplicationError

@pytest.fixture
def anyio_backend():
    return 'asyncio'

async def mock_app(scope: Scope, receive: Receive, send: Send):
    pass

@pytest.mark.anyio
async def test_error_middleware_catches_resource_not_found():
    """Test that ResourceNotFound is caught and returns 404 JSON."""
    
    async def next_mock(request):
        raise ResourceNotFound("Item missing")

    middleware = ErrorMiddleware(mock_app)
    
    # Create a dummy request
    scope = {"type": "http", "method": "GET", "path": "/"}
    request = Request(scope)
    
    response = await middleware.dispatch(request, next_mock)
    
    assert response.status_code == 404
    import json
    body = json.loads(response.body.decode())
    assert body["error"]["code"] == "RESOURCE_NOT_FOUND"
    assert body["error"]["message"] == "Item missing"

@pytest.mark.anyio
async def test_error_middleware_catches_generic_exception():
    """Test that generic exceptions are caught and masked as 500."""
    
    async def next_mock(request):
        raise Exception("Secret database failure")

    middleware = ErrorMiddleware(mock_app)
    
    scope = {"type": "http", "method": "GET", "path": "/"}
    request = Request(scope)
    
    response = await middleware.dispatch(request, next_mock)
    
    assert response.status_code == 500
    import json
    body = json.loads(response.body.decode())
    assert body["error"]["code"] == "INTERNAL_ERROR"
    # Ensure strict message is not leaked
    assert body["error"]["message"] == "An internal server error occurred."
