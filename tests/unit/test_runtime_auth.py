from __future__ import annotations

import uuid

import pytest
from jose import jwt
from starlette.requests import Request

from langbridge.runtime import RuntimeContext
from langbridge.runtime.hosting.auth import (
    RuntimeAuthConfig,
    RuntimeAuthMode,
    RuntimeAuthResolver,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _request(*, headers: dict[str, str] | None = None) -> Request:
    encoded_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": encoded_headers,
    }
    return Request(scope)


def _default_context() -> RuntimeContext:
    return RuntimeContext.build(
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        roles=["runtime:admin"],
        request_id="req-default",
    )


@pytest.mark.anyio
async def test_runtime_auth_none_uses_default_context_identity() -> None:
    default_context = _default_context()
    resolver = RuntimeAuthResolver(
        config=RuntimeAuthConfig(mode=RuntimeAuthMode.none),
        default_context=default_context,
    )
    request = _request(headers={"X-Request-Id": "req-none"})

    principal = await resolver.authenticate(request)
    context = resolver.build_context(request=request, principal=principal)

    assert principal.workspace_id == default_context.workspace_id
    assert principal.actor_id == default_context.actor_id
    assert principal.roles == default_context.roles
    assert context.workspace_id == default_context.workspace_id
    assert context.actor_id == default_context.actor_id
    assert context.roles == default_context.roles
    assert context.request_id == "req-none"


@pytest.mark.anyio
async def test_runtime_auth_static_token_maps_workspace_actor_and_roles() -> None:
    default_context = _default_context()
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    resolver = RuntimeAuthResolver(
        config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.static_token,
            static_token="runtime-token",
            static_workspace_id=workspace_id,
            static_actor_id=actor_id,
            static_roles=("runtime:viewer", "dataset:preview"),
        ),
        default_context=default_context,
    )
    request = _request(
        headers={
            "Authorization": "Bearer runtime-token",
            "X-Request-Id": "req-static",
        }
    )

    principal = await resolver.authenticate(request)
    context = resolver.build_context(request=request, principal=principal)

    assert principal.workspace_id == workspace_id
    assert principal.actor_id == actor_id
    assert principal.roles == ("runtime:viewer", "dataset:preview")
    assert context.workspace_id == workspace_id
    assert context.actor_id == actor_id
    assert context.roles == ("runtime:viewer", "dataset:preview")
    assert context.request_id == "req-static"


@pytest.mark.anyio
async def test_runtime_auth_jwt_maps_claims_into_runtime_principal() -> None:
    default_context = _default_context()
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    token = jwt.encode(
        {
            "workspace_id": str(workspace_id),
            "actor_id": str(actor_id),
            "roles": ["runtime:editor", "semantic:query"],
            "sub": str(actor_id),
        },
        "jwt-secret",
        algorithm="HS256",
    )
    resolver = RuntimeAuthResolver(
        config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.jwt,
            jwt_secret="jwt-secret",
        ),
        default_context=default_context,
    )
    request = _request(
        headers={
            "Authorization": f"Bearer {token}",
            "X-Correlation-Id": "req-jwt",
        }
    )

    principal = await resolver.authenticate(request)
    context = resolver.build_context(request=request, principal=principal)

    assert principal.workspace_id == workspace_id
    assert principal.actor_id == actor_id
    assert principal.subject == str(actor_id)
    assert principal.roles == ("runtime:editor", "semantic:query")
    assert context.workspace_id == workspace_id
    assert context.actor_id == actor_id
    assert context.roles == ("runtime:editor", "semantic:query")
    assert context.request_id == "req-jwt"
