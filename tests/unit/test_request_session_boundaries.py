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
