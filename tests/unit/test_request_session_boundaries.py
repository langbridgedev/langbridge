from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import uuid

import pytest
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import Receive, Scope, Send

from langbridge.apps.api.langbridge_api.middleware.auth_middleware import AuthMiddleware
from langbridge.apps.api.langbridge_api.middleware.uow_middleware import UnitOfWorkMiddleware
from langbridge.apps.api.langbridge_api.services.connector_schema_service import (
    ConnectorSchemaService,
)
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _mock_app(scope: Scope, receive: Receive, send: Send):
    return


class _FakeSession:
    def __init__(self) -> None:
        self.commit = AsyncMock()
        self.rollback = AsyncMock()
        self.close = AsyncMock()
        self._in_transaction = False

    def in_transaction(self) -> bool:
        return self._in_transaction


def _build_request(path: str, *, cookie: str | None = None, container=None) -> Request:
    headers: list[tuple[bytes, bytes]] = []
    if cookie is not None:
        headers.append((b"cookie", cookie.encode("utf-8")))
    app = SimpleNamespace(state=SimpleNamespace(container=container))
    scope: Scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": headers,
        "app": app,
    }
    return Request(scope)


@pytest.mark.anyio
async def test_uow_skips_request_scoped_session_for_connector_schema_routes() -> None:
    container = SimpleNamespace(async_session_factory=MagicMock())
    middleware = UnitOfWorkMiddleware(_mock_app)
    request = _build_request(
        "/api/v1/connectors/org-id/connector-id/source/schemas",
        container=container,
    )

    async def next_mock(_: Request) -> Response:
        return Response(status_code=200)

    response = await middleware.dispatch(request, next_mock)

    assert response.status_code == 200
    container.async_session_factory.assert_not_called()


@pytest.mark.anyio
async def test_auth_uses_temporary_session_when_request_session_is_absent(monkeypatch) -> None:
    fake_session = _FakeSession()
    session_factory = MagicMock(return_value=fake_session)
    auth_service = SimpleNamespace(
        get_user_by_username=AsyncMock(
            return_value=UserResponse(
                id=uuid.uuid4(),
                username="alice",
                email="alice@example.com",
                is_active=True,
                available_organizations=[uuid.uuid4()],
                available_projects=[],
            )
        )
    )
    container = SimpleNamespace(
        async_session_factory=MagicMock(return_value=session_factory),
        auth_service=MagicMock(return_value=auth_service),
    )
    middleware = AuthMiddleware(_mock_app)
    request = _build_request(
        "/api/v1/connectors/org-id/connector-id/source/schemas",
        cookie="langbridge_token=test-token",
        container=container,
    )

    monkeypatch.setattr(
        "langbridge.apps.api.langbridge_api.middleware.auth_middleware.verify_jwt",
        lambda _: {"username": "alice"},
    )

    async def next_mock(_: Request) -> Response:
        return Response(status_code=200)

    response = await middleware.dispatch(request, next_mock)

    assert response.status_code == 200
    container.async_session_factory.assert_called_once()
    container.auth_service.assert_called_once()
    session_factory.assert_called_once()
    auth_service.get_user_by_username.assert_awaited_once_with("alice")
    fake_session.close.assert_awaited_once()


@pytest.mark.anyio
async def test_connector_schema_service_uses_short_lived_session(monkeypatch) -> None:
    fake_session = _FakeSession()
    session_factory = MagicMock(return_value=fake_session)
    service = ConnectorSchemaService(async_session_factory=session_factory)
    connector = object()
    get_by_id = AsyncMock(return_value=connector)

    monkeypatch.setattr(
        "langbridge.apps.api.langbridge_api.services.connector_schema_service.ConnectorRepository.get_by_id",
        get_by_id,
    )

    result = await service._get_connector(uuid.uuid4())

    assert result is connector
    session_factory.assert_called_once()
    get_by_id.assert_awaited_once()
    fake_session.close.assert_awaited_once()
