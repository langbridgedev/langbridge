from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import httpx
from fastapi import HTTPException, Request, status
from jose import JWTError, jwt

from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.hosting.local_auth import (
    RuntimeLocalAuthBootstrapRequiredError,
    RuntimeLocalAuthError,
    RuntimeLocalAuthManager,
)


_REQUEST_ID_HEADERS = ("x-request-id", "x-correlation-id")


class RuntimeAuthMode(str, Enum):
    none = "none"
    local = "local"
    static_token = "static_token"
    jwt = "jwt"


@dataclass(slots=True, frozen=True)
class RuntimeAuthPrincipal:
    workspace_id: uuid.UUID
    actor_id: uuid.UUID | None
    roles: tuple[str, ...] = field(default_factory=tuple)
    subject: str | None = None


@dataclass(slots=True, frozen=True)
class RuntimeAuthConfig:
    mode: RuntimeAuthMode = RuntimeAuthMode.none
    local_store_path: str | None = None
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
        if self.mode == RuntimeAuthMode.local and not self.local_store_path:
            raise ValueError("local auth mode requires a resolved runtime auth store path.")
        if self.mode == RuntimeAuthMode.static_token and not self.static_token:
            raise ValueError("static_token auth mode requires LANGBRIDGE_RUNTIME_AUTH_STATIC_TOKEN.")
        if self.mode == RuntimeAuthMode.jwt and not self.jwt_secret and not self.jwt_jwks_url:
            raise ValueError("jwt auth mode requires LANGBRIDGE_RUNTIME_AUTH_JWT_SECRET or LANGBRIDGE_RUNTIME_AUTH_JWT_JWKS_URL.")

    @classmethod
    def from_env(cls, *, config_path: str | Path | None = None) -> "RuntimeAuthConfig":
        mode_raw = str(os.getenv("LANGBRIDGE_RUNTIME_AUTH_MODE", "none")).strip().lower() or "none"
        config = cls(
            mode=RuntimeAuthMode(mode_raw),
            local_store_path=_resolve_local_store_path(
                explicit_path=_optional_env("LANGBRIDGE_RUNTIME_AUTH_LOCAL_STORE_PATH"),
                config_path=config_path,
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
        jwks_cache_ttl_seconds: int = 300,
    ) -> None:
        self._config = config or RuntimeAuthConfig()
        self._config.validate()
        self._default_context = default_context
        self._jwks_cache_ttl_seconds = max(1, int(jwks_cache_ttl_seconds))
        self._jwks_cache: dict[str, Any] | None = None
        self._jwks_cache_expires_at = 0.0
        self._local_auth = (
            RuntimeLocalAuthManager(
                workspace_id=self._default_context.workspace_id,
                store_path=Path(str(self._config.local_store_path)),
                cookie_name=self._config.local_cookie_name,
                session_max_age_seconds=self._config.local_session_max_age_seconds,
                session_secret=self._config.local_session_secret,
            )
            if self._config.mode == RuntimeAuthMode.local
            else None
        )

    @property
    def mode(self) -> RuntimeAuthMode:
        return self._config.mode

    @property
    def local_auth(self) -> RuntimeLocalAuthManager | None:
        return self._local_auth

    async def authenticate(self, request: Request) -> RuntimeAuthPrincipal:
        if self._config.mode == RuntimeAuthMode.none:
            return RuntimeAuthPrincipal(
                workspace_id=self._default_context.workspace_id,
                actor_id=self._default_context.actor_id,
                roles=tuple(self._default_context.roles),
            )

        if self._config.mode == RuntimeAuthMode.local:
            local_auth = self._require_local_auth()
            try:
                session = local_auth.authenticate_request(request)
            except RuntimeLocalAuthBootstrapRequiredError as exc:
                raise self._unauthorized(str(exc)) from exc
            except RuntimeLocalAuthError as exc:
                raise self._unauthorized(str(exc)) from exc
            return RuntimeAuthPrincipal(
                workspace_id=self._default_context.workspace_id,
                actor_id=session.id,
                roles=tuple(session.roles),
                subject=session.username,
            )

        token = self._extract_bearer_token(request)
        if self._config.mode == RuntimeAuthMode.static_token:
            if token != self._config.static_token:
                raise self._unauthorized("Invalid runtime token.")
            return RuntimeAuthPrincipal(
                workspace_id=self._config.static_workspace_id or self._default_context.workspace_id,
                actor_id=self._config.static_actor_id or self._default_context.actor_id,
                roles=tuple(self._config.static_roles or self._default_context.roles),
            )

        claims = await self._decode_jwt(token)
        return self._principal_from_claims(claims)

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
    def _extract_bearer_token(request: Request) -> str:
        header_value = str(request.headers.get("authorization") or "").strip()
        if not header_value:
            raise RuntimeAuthResolver._unauthorized("Authorization header is required.")
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
        )

    def _require_local_auth(self) -> RuntimeLocalAuthManager:
        if self._local_auth is None:
            raise RuntimeError("Runtime local auth is not configured.")
        return self._local_auth

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


def _resolve_local_store_path(
    *,
    explicit_path: str | None,
    config_path: str | Path | None,
) -> str | None:
    if explicit_path:
        return str(Path(explicit_path).expanduser().resolve())
    if config_path is None:
        return str((Path.cwd() / ".langbridge" / "auth.json").resolve())
    base_path = Path(config_path).expanduser().resolve()
    return str((base_path.parent / ".langbridge" / "auth.json").resolve())


def _coerce_uuid(value: Any, *, field_name: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise RuntimeAuthResolver._unauthorized(f"Runtime auth claim '{field_name}' must be a UUID.") from exc


def _coerce_optional_uuid(value: Any, *, field_name: str) -> uuid.UUID | None:
    if value is None or value == "":
        return None
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise RuntimeAuthResolver._unauthorized(f"Runtime auth claim '{field_name}' must be a UUID when provided.") from exc


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
