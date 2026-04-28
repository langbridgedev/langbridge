from __future__ import annotations
import os
import time
import uuid
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from fastapi import HTTPException, Request, status
from jose import JWTError, jwt

from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.hosting.local_auth import (
    RuntimeLocalAuthBootstrapRequiredError,
    RuntimeLocalAuthError,
    RuntimeLocalAuthManager,
    RuntimeLocalSession,
)
from langbridge.runtime.utils.util import _coerce_uuid   

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


_REQUEST_ID_HEADERS = ("x-request-id", "x-correlation-id")


class RuntimeAuthMode(str, Enum):
    none = "none"
    static_token = "static_token"
    jwt = "jwt"


@dataclass(slots=True, frozen=True)
class RuntimeAuthPrincipal:
    workspace_id: uuid.UUID
    actor_id: uuid.UUID | None
    roles: tuple[str, ...] = field(default_factory=tuple)
    subject: str | None = None
    username: str | None = None
    email: str | None = None
    display_name: str | None = None
    provider: str = "runtime_none"


@dataclass(slots=True, frozen=True)
class RuntimeAuthConfig:
    mode: RuntimeAuthMode = RuntimeAuthMode.none
    local_auth_enabled: bool = False
    local_cookie_name: str = "langbridge_runtime_session"
    local_session_max_age_seconds: int = 60 * 60 * 24 * 14
    local_session_secret: str | None = None
    static_token: str | None = None
    static_workspace_id: uuid.UUID | None = None
    static_actor_id: uuid.UUID | None = None
    static_roles: tuple[str, ...] = field(default_factory=tuple)
    jwt_secret: str | None = None
    jwt_jwks_url: str | None = None
    jwt_algorithms: tuple[str, ...] = ("HS256",)
    jwt_issuer: str | None = None
    jwt_audience: str | None = None
    jwt_workspace_claim: str = "workspace_id"
    jwt_actor_claim: str = "actor_id"
    jwt_roles_claim: str = "roles"
    jwt_subject_claim: str = "sub"

    def validate(self) -> None:
        if self.mode == RuntimeAuthMode.none:
            if self.local_auth_enabled:
                raise ValueError("local operator auth requires a secured runtime auth mode.")
            return

        if self.mode == RuntimeAuthMode.static_token and not self.static_token:
            raise ValueError("static_token auth mode requires LANGBRIDGE_RUNTIME_AUTH_STATIC_TOKEN.")
        if self.mode == RuntimeAuthMode.jwt and not self.jwt_secret and not self.jwt_jwks_url:
            raise ValueError(
                "jwt auth mode requires LANGBRIDGE_RUNTIME_AUTH_JWT_SECRET or LANGBRIDGE_RUNTIME_AUTH_JWT_JWKS_URL."
            )

    @classmethod
    def from_env(cls, *, config_path: str | Path | None = None) -> "RuntimeAuthConfig":
        removed_store_path = _optional_env("LANGBRIDGE_RUNTIME_AUTH_LOCAL_STORE_PATH")
        if removed_store_path is not None:
            raise ValueError(
                "LANGBRIDGE_RUNTIME_AUTH_LOCAL_STORE_PATH is no longer supported. "
                "Local operator auth now uses the configured runtime.metadata_store."
            )
        mode_raw = str(os.getenv("LANGBRIDGE_RUNTIME_AUTH_MODE", "none")).strip().lower() or "none"
        mode = RuntimeAuthMode(mode_raw)
        config = cls(
            mode=mode,
            local_auth_enabled=_bool_env(
                "LANGBRIDGE_RUNTIME_AUTH_LOCAL_ENABLED",
                default=mode != RuntimeAuthMode.none,
            ),
            local_cookie_name=_optional_env("LANGBRIDGE_RUNTIME_AUTH_LOCAL_COOKIE_NAME") or "langbridge_runtime_session",
            local_session_max_age_seconds=_int_env(
                "LANGBRIDGE_RUNTIME_AUTH_LOCAL_SESSION_MAX_AGE_SECONDS",
                default=60 * 60 * 24 * 14,
            ),
            local_session_secret=_optional_env("LANGBRIDGE_RUNTIME_AUTH_LOCAL_SESSION_SECRET"),
            static_token=_optional_env("LANGBRIDGE_RUNTIME_AUTH_STATIC_TOKEN"),
            static_workspace_id=_uuid_env("LANGBRIDGE_RUNTIME_AUTH_STATIC_WORKSPACE_ID"),
            static_actor_id=_uuid_env("LANGBRIDGE_RUNTIME_AUTH_STATIC_ACTOR_ID"),
            static_roles=_split_csv_env("LANGBRIDGE_RUNTIME_AUTH_STATIC_ROLES"),
            jwt_secret=_optional_env("LANGBRIDGE_RUNTIME_AUTH_JWT_SECRET"),
            jwt_jwks_url=_optional_env("LANGBRIDGE_RUNTIME_AUTH_JWT_JWKS_URL"),
            jwt_algorithms=_split_csv_env("LANGBRIDGE_RUNTIME_AUTH_JWT_ALGORITHMS", default=("HS256",)),
            jwt_issuer=_optional_env("LANGBRIDGE_RUNTIME_AUTH_JWT_ISSUER"),
            jwt_audience=_optional_env("LANGBRIDGE_RUNTIME_AUTH_JWT_AUDIENCE"),
            jwt_workspace_claim=_optional_env("LANGBRIDGE_RUNTIME_AUTH_JWT_WORKSPACE_CLAIM") or "workspace_id",
            jwt_actor_claim=_optional_env("LANGBRIDGE_RUNTIME_AUTH_JWT_ACTOR_CLAIM") or "actor_id",
            jwt_roles_claim=_optional_env("LANGBRIDGE_RUNTIME_AUTH_JWT_ROLES_CLAIM") or "roles",
            jwt_subject_claim=_optional_env("LANGBRIDGE_RUNTIME_AUTH_JWT_SUBJECT_CLAIM") or "sub",
        )
        config.validate()
        return config


class RuntimeAuthResolver:
    def __init__(
        self,
        *,
        config: RuntimeAuthConfig | None = None,
        default_context: RuntimeContext,
        runtime_host: ConfiguredLocalRuntimeHost | None = None,
        jwks_cache_ttl_seconds: int = 300,
    ) -> None:
        self._config = config or RuntimeAuthConfig()
        self._config.validate()
        self._default_context = default_context
        self._runtime_host = runtime_host
        self._jwks_cache_ttl_seconds = max(1, int(jwks_cache_ttl_seconds))
        self._jwks_cache: dict[str, Any] | None = None
        self._jwks_cache_expires_at = 0.0
        if self._config.local_auth_enabled and self._runtime_host is None:
            raise ValueError("local operator auth requires a configured local runtime host.")
        self._local_auth = (
            RuntimeLocalAuthManager(
                workspace_id=self._default_context.workspace_id,
                persistence_mode=self._resolve_local_auth_persistence_mode(),
                cookie_name=self._config.local_cookie_name,
                session_max_age_seconds=self._config.local_session_max_age_seconds,
                session_secret=self._config.local_session_secret,
                persistence_controller=getattr(self._runtime_host, "persistence_controller", None),
            )
            if self._config.local_auth_enabled
            else None
        )

    @property
    def mode(self) -> RuntimeAuthMode:
        return self._config.mode

    @property
    def config(self) -> RuntimeAuthConfig:
        return self._config

    @property
    def local_auth(self) -> RuntimeLocalAuthManager | None:
        return self._local_auth

    @property
    def local_auth_enabled(self) -> bool:
        return self._local_auth is not None

    async def authenticate(self, request: Request) -> RuntimeAuthPrincipal:
        if self._config.mode == RuntimeAuthMode.none:
            return await self._finalize_principal(
                RuntimeAuthPrincipal(
                    workspace_id=self._default_context.workspace_id,
                    actor_id=self._default_context.actor_id,
                    roles=tuple(self._default_context.roles),
                    provider="runtime_none",
                    display_name="runtime",
                )
            )

        token = self._maybe_extract_bearer_token(request)
        if token is not None:
            if self._config.mode == RuntimeAuthMode.static_token:
                if token != self._config.static_token:
                    raise self._unauthorized("Invalid runtime token.")
                return await self._finalize_principal(
                    RuntimeAuthPrincipal(
                        workspace_id=self._config.static_workspace_id or self._default_context.workspace_id,
                        actor_id=self._config.static_actor_id or self._default_context.actor_id,
                        roles=tuple(self._config.static_roles or self._default_context.roles),
                        provider=self._config.mode.value,
                        display_name="runtime",
                    )
                )

            claims = await self._decode_jwt(token)
            return await self._finalize_principal(self._principal_from_claims(claims))

        if self._local_auth is not None:
            return await self.authenticate_local_session(request)

        raise self._unauthorized("Authorization header is required.")

    async def authenticate_local_session(self, request: Request) -> RuntimeAuthPrincipal:
        local_auth = self._require_local_auth()
        try:
            session = await local_auth.authenticate_session_request(request)
        except RuntimeLocalAuthBootstrapRequiredError as exc:
            raise self._unauthorized(str(exc)) from exc
        except RuntimeLocalAuthError as exc:
            raise self._unauthorized(str(exc)) from exc
        return await self.sync_local_session(session)

    async def sync_local_session(self, session: RuntimeLocalSession) -> RuntimeAuthPrincipal:
        return await self._finalize_principal(
            RuntimeAuthPrincipal(
                workspace_id=self._default_context.workspace_id,
                actor_id=session.id,
                roles=tuple(session.roles),
                subject=session.subject,
                username=session.username,
                email=session.email,
                display_name=session.display_name,
                provider=session.provider,
            ),
            persist_local_actor=True,
        )

    def build_context(self, *, request: Request, principal: RuntimeAuthPrincipal) -> RuntimeContext:
        request_id = self._resolve_request_id(request)
        actor_id = (
            principal.actor_id
            or self._default_context.actor_id
            or uuid.uuid5(uuid.NAMESPACE_URL, f"langbridge-runtime-anonymous:{principal.workspace_id}")
        )
        return RuntimeContext.build(
            workspace_id=principal.workspace_id,
            actor_id=actor_id,
            roles=principal.roles,
            request_id=request_id,
        )

    @staticmethod
    def _maybe_extract_bearer_token(request: Request) -> str | None:
        header_value = str(request.headers.get("authorization") or "").strip()
        if not header_value:
            return None
        scheme, _, token = header_value.partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            raise RuntimeAuthResolver._unauthorized("Authorization header must use Bearer authentication.")
        return token.strip()

    async def _decode_jwt(self, token: str) -> dict[str, Any]:
        options = {"verify_aud": self._config.jwt_audience is not None}
        try:
            if self._config.jwt_jwks_url:
                key = await self._resolve_jwks_key(token)
            else:
                key = self._config.jwt_secret
            claims = jwt.decode(
                token,
                key,
                algorithms=list(self._config.jwt_algorithms),
                issuer=self._config.jwt_issuer,
                audience=self._config.jwt_audience,
                options=options,
            )
        except (JWTError, ValueError) as exc:
            raise self._unauthorized(f"Invalid runtime JWT: {exc}") from exc
        if not isinstance(claims, dict):
            raise self._unauthorized("Invalid runtime JWT payload.")
        return claims

    async def _resolve_jwks_key(self, token: str) -> dict[str, Any]:
        header = jwt.get_unverified_header(token)
        kid = str(header.get("kid") or "").strip() or None
        jwks = await self._get_jwks()
        keys = jwks.get("keys")
        if not isinstance(keys, list) or not keys:
            raise self._unauthorized("Runtime JWKS endpoint did not return any signing keys.")
        if kid is None and len(keys) == 1 and isinstance(keys[0], dict):
            return dict(keys[0])
        for item in keys:
            if isinstance(item, dict) and str(item.get("kid") or "").strip() == kid:
                return dict(item)
        raise self._unauthorized("Runtime JWT key id was not found in the configured JWKS.")

    async def _get_jwks(self) -> dict[str, Any]:
        now = time.time()
        if self._jwks_cache is not None and now < self._jwks_cache_expires_at:
            return self._jwks_cache
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(str(self._config.jwt_jwks_url))
            response.raise_for_status()
            payload = response.json()
        if not isinstance(payload, dict):
            raise self._unauthorized("Runtime JWKS endpoint returned an invalid payload.")
        self._jwks_cache = payload
        self._jwks_cache_expires_at = now + float(self._jwks_cache_ttl_seconds)
        return payload

    def _principal_from_claims(self, claims: dict[str, Any]) -> RuntimeAuthPrincipal:
        workspace_id = _coerce_uuid(
            claims.get(self._config.jwt_workspace_claim),
            field_name=self._config.jwt_workspace_claim,
        )
        if workspace_id is None:
            raise RuntimeAuthResolver._unauthorized(
                f"Runtime auth claim '{self._config.jwt_workspace_claim}' must be a UUID."
            )
        
        subject = _coerce_optional_text(claims.get(self._config.jwt_subject_claim))
        actor_id = _coerce_optional_uuid(
            claims.get(self._config.jwt_actor_claim),
            field_name=self._config.jwt_actor_claim,
        )
        if actor_id is None and subject:
            try:
                actor_id = uuid.UUID(subject)
            except ValueError:
                actor_id = uuid.uuid5(uuid.NAMESPACE_URL, f"langbridge-runtime-subject:{subject}")
        roles = _normalize_roles(claims.get(self._config.jwt_roles_claim))
        return RuntimeAuthPrincipal(
            workspace_id=workspace_id,
            actor_id=actor_id,
            roles=roles,
            subject=subject,
            display_name=subject,
            provider=self._config.mode.value,
        )

    def _require_local_auth(self) -> RuntimeLocalAuthManager:
        if self._local_auth is None:
            raise RuntimeError("Runtime local operator auth is not configured.")
        return self._local_auth

    def _resolve_local_auth_persistence_mode(self) -> str:
        if self._runtime_host is None:
            raise ValueError("local operator auth requires a configured runtime host.")
        metadata_store = getattr(self._runtime_host, "metadata_store", None)
        if metadata_store is None:
            metadata_store = getattr(self._runtime_host, "_metadata_store", None)
        if metadata_store is None:
            raise ValueError("local operator auth requires a configured runtime metadata store.")
        return str(metadata_store.type)

    async def _finalize_principal(
        self,
        principal: RuntimeAuthPrincipal,
        *,
        persist_local_actor: bool = False,
    ) -> RuntimeAuthPrincipal:
        resolved_actor_id = (
            principal.actor_id
            or self._default_context.actor_id
            or uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"langbridge-runtime-actor:{principal.workspace_id}:{principal.subject or principal.provider}",
            )
        )
        finalized = replace(
            principal,
            actor_id=resolved_actor_id,
            roles=tuple(principal.roles),
        )
        if not persist_local_actor:
            return finalized
        persisted_actor_id = await self._ensure_runtime_actor(finalized)
        return replace(finalized, actor_id=persisted_actor_id)

    async def _ensure_runtime_actor(self, principal: RuntimeAuthPrincipal) -> uuid.UUID:
        if self._runtime_host is None:
            return principal.actor_id or uuid.uuid4()

        controller = getattr(self._runtime_host, "persistence_controller", None)
        if controller is None:
            return principal.actor_id or uuid.uuid4()

        from langbridge.runtime.persistence.db.workspace import RuntimeActor

        async with controller.unit_of_work() as uow:
            workspace_repository = uow.repository("workspace_repository")
            actor_repository = uow.repository("actor_repository")
            await workspace_repository.ensure_configured(
                workspace_id=principal.workspace_id,
                name=f"local-runtime-{principal.workspace_id}",
            )

            actor = await actor_repository.get_by_id(principal.actor_id)
            if actor is None and principal.subject:
                actor = await actor_repository.get_by_subject(
                    workspace_id=principal.workspace_id,
                    subject=principal.subject,
                )

            if actor is None:
                actor = RuntimeActor(
                    id=principal.actor_id or uuid.uuid4(),
                    workspace_id=principal.workspace_id,
                    subject=principal.subject,
                    username=principal.username or principal.subject,
                    actor_type="human",
                    status="active",
                    email=principal.email,
                    display_name=principal.display_name or principal.subject or principal.email or "Runtime Operator",
                    roles_json=list(principal.roles),
                    is_active=True,
                    metadata_json={
                        "provider": principal.provider,
                        "runtime_operator": True,
                    },
                )
                actor_repository.add(actor)
            else:
                if actor.workspace_id != principal.workspace_id:
                    raise self._unauthorized("Runtime actor does not belong to the authenticated workspace.")
                actor.subject = principal.subject
                actor.username = principal.username or principal.subject or actor.username
                actor.actor_type = "human"
                actor.status = "active"
                actor.email = principal.email
                actor.display_name = principal.display_name or principal.subject or principal.email or actor.display_name
                actor.roles_json = list(principal.roles)
                actor.is_active = True
                actor.metadata_json = {
                    **dict(actor.metadata_json or {}),
                    "provider": principal.provider,
                    "runtime_operator": True,
                }

            await uow.flush()
            await uow.commit()
            return actor.id

    def _resolve_request_id(self, request: Request) -> str:
        for header_name in _REQUEST_ID_HEADERS:
            value = str(request.headers.get(header_name) or "").strip()
            if value:
                return value
        return self._default_context.request_id or f"runtime:{uuid.uuid4()}"

    @staticmethod
    def _unauthorized(detail: str) -> HTTPException:
        return HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)


def _optional_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _uuid_env(name: str) -> uuid.UUID | None:
    value = _optional_env(name)
    if value is None:
        return None
    return uuid.UUID(value)


def _split_csv_env(name: str, *, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    value = _optional_env(name)
    if value is None:
        return default
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _int_env(name: str, *, default: int) -> int:
    value = _optional_env(name)
    if value is None:
        return int(default)
    return int(value)


def _bool_env(name: str, *, default: bool) -> bool:
    value = _optional_env(name)
    if value is None:
        return bool(default)
    return value.lower() in {"1", "true", "yes", "on"}


def _coerce_optional_uuid(value: Any, *, field_name: str) -> uuid.UUID | None:
    if value is None or value == "":
        return None
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise RuntimeAuthResolver._unauthorized(
            f"Runtime auth claim '{field_name}' must be a UUID when provided."
        ) from exc


def _coerce_optional_text(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _normalize_roles(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    if isinstance(value, (list, tuple, set)):
        return tuple(str(part).strip() for part in value if str(part).strip())
    return ()


__all__ = [
    "RuntimeAuthConfig",
    "RuntimeAuthMode",
    "RuntimeAuthPrincipal",
    "RuntimeAuthResolver",
]
