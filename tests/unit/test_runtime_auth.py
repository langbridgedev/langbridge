
import uuid
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from jose import jwt
from starlette.requests import Request

from langbridge.runtime import RuntimeContext
from langbridge.runtime.hosting.auth import (
    RuntimeAuthConfig,
    RuntimeAuthMode,
    RuntimeAuthResolver,
)
from langbridge.runtime.config.models import ResolvedLocalRuntimeMetadataStoreConfig


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _request(
    *,
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> Request:
    request_headers = dict(headers or {})
    if cookies:
        request_headers["Cookie"] = "; ".join(
            f"{key}={value}"
            for key, value in cookies.items()
        )
    encoded_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in request_headers.items()
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


def _runtime_host_stub(
    *,
    persistence_mode: str,
    persistence_controller=None,
):
    return SimpleNamespace(
        metadata_store=ResolvedLocalRuntimeMetadataStoreConfig(type=persistence_mode),
        persistence_controller=persistence_controller,
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
    assert principal.provider == "runtime_none"
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
            local_auth_enabled=False,
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
    assert principal.provider == "static_token"
    assert context.workspace_id == workspace_id
    assert context.actor_id == actor_id
    assert context.roles == ("runtime:viewer", "dataset:preview")
    assert context.request_id == "req-static"


@pytest.mark.anyio
async def test_runtime_auth_static_token_rejects_invalid_token() -> None:
    resolver = RuntimeAuthResolver(
        config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.static_token,
            static_token="runtime-token",
            local_auth_enabled=False,
        ),
        default_context=_default_context(),
    )

    with pytest.raises(HTTPException) as exc_info:
        await resolver.authenticate(
            _request(headers={"Authorization": "Bearer not-the-runtime-token"})
        )

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid runtime token."


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
            local_auth_enabled=False,
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
    assert principal.provider == "jwt"
    assert context.workspace_id == workspace_id
    assert context.actor_id == actor_id
    assert context.roles == ("runtime:editor", "semantic:query")
    assert context.request_id == "req-jwt"


@pytest.mark.anyio
async def test_runtime_auth_jwt_rejects_invalid_workspace_claim() -> None:
    token = jwt.encode(
        {
            "workspace_id": "not-a-uuid",
            "roles": ["runtime:viewer"],
            "sub": "operator@example.com",
        },
        "jwt-secret",
        algorithm="HS256",
    )
    resolver = RuntimeAuthResolver(
        config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.jwt,
            jwt_secret="jwt-secret",
            local_auth_enabled=False,
        ),
        default_context=_default_context(),
    )

    with pytest.raises(HTTPException) as exc_info:
        await resolver.authenticate(_request(headers={"Authorization": f"Bearer {token}"}))

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Runtime auth claim 'workspace_id' must be a UUID."


@pytest.mark.anyio
async def test_runtime_auth_local_operator_session_builds_runtime_context() -> None:
    default_context = _default_context()
    resolver = RuntimeAuthResolver(
        config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.static_token,
            static_token="runtime-token",
            local_auth_enabled=True,
            local_session_secret="runtime-local-auth-secret",
        ),
        default_context=default_context,
        runtime_host=_runtime_host_stub(persistence_mode="in_memory"),
    )
    assert resolver.local_auth is not None

    session = await resolver.local_auth.bootstrap_admin(
        username="runtime-admin",
        email="admin@example.com",
        password="Password123!",
    )
    token = await resolver.local_auth.issue_session_token(session)
    request = _request(
        headers={"X-Request-Id": "req-local"},
        cookies={resolver.local_auth.cookie_name: token},
    )

    principal = await resolver.authenticate(request)
    context = resolver.build_context(request=request, principal=principal)

    assert principal.workspace_id == default_context.workspace_id
    assert principal.actor_id == session.id
    assert principal.subject == "runtime-admin"
    assert principal.email == "admin@example.com"
    assert principal.roles == ("runtime:admin",)
    assert principal.provider == "runtime_local_session"
    assert context.workspace_id == default_context.workspace_id
    assert context.actor_id == session.id
    assert context.roles == ("runtime:admin",)
    assert context.request_id == "req-local"


def test_runtime_auth_uses_persisted_store_for_postgres_local_operator_sessions() -> None:
    resolver = RuntimeAuthResolver(
        config=RuntimeAuthConfig(
            mode=RuntimeAuthMode.static_token,
            static_token="runtime-token",
            local_auth_enabled=True,
        ),
        default_context=_default_context(),
        runtime_host=_runtime_host_stub(persistence_mode="postgres", persistence_controller=object()),
    )

    assert resolver.local_auth is not None
    assert resolver.local_auth.persistence_mode == "postgres"
