from __future__ import annotations

import json
import re
import secrets
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Request
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from pydantic import Field

from langbridge.runtime.models.base import RuntimeModel
from langbridge.runtime.hosting.passwords import hash_password, verify_password


_USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]{3,64}$")


class RuntimeLocalAuthError(ValueError):
    pass


class RuntimeLocalAuthBootstrapRequiredError(RuntimeLocalAuthError):
    pass


class RuntimeLocalAccount(RuntimeModel):
    id: uuid.UUID
    username: str
    email: str
    password_hash: str
    roles: list[str] = Field(default_factory=lambda: ["runtime:admin"])
    created_at: datetime
    updated_at: datetime


class RuntimeLocalAuthState(RuntimeModel):
    version: int = 1
    session_secret: str | None = None
    accounts: list[RuntimeLocalAccount] = Field(default_factory=list)


class RuntimeLocalSession(RuntimeModel):
    id: uuid.UUID
    username: str
    email: str
    roles: list[str] = Field(default_factory=list)
    provider: str = "runtime_local"


class RuntimeLocalAuthManager:
    def __init__(
        self,
        *,
        workspace_id: uuid.UUID,
        store_path: Path,
        cookie_name: str,
        session_max_age_seconds: int,
        session_secret: str | None = None,
    ) -> None:
        self._workspace_id = workspace_id
        self._store_path = Path(store_path).resolve()
        self._cookie_name = str(cookie_name or "").strip() or "langbridge_runtime_session"
        self._session_max_age_seconds = max(60, int(session_max_age_seconds))
        self._session_secret = str(session_secret or "").strip() or None

    @property
    def cookie_name(self) -> str:
        return self._cookie_name

    @property
    def session_max_age_seconds(self) -> int:
        return self._session_max_age_seconds

    def auth_status(self) -> dict[str, bool]:
        state = self._load_state()
        has_admin = bool(state.accounts)
        return {
            "has_admin": has_admin,
            "bootstrap_required": not has_admin,
        }

    def bootstrap_admin(
        self,
        *,
        username: str,
        email: str,
        password: str,
    ) -> RuntimeLocalSession:
        normalized_username = _normalize_username(username)
        normalized_email = _normalize_email(email)
        _validate_password(password)

        state = self._load_state()
        if state.accounts:
            raise RuntimeLocalAuthError("Runtime bootstrap has already been completed.")

        timestamp = datetime.now(timezone.utc)
        account = RuntimeLocalAccount(
            id=uuid.uuid4(),
            username=normalized_username,
            email=normalized_email,
            password_hash=hash_password(password),
            roles=["runtime:admin"],
            created_at=timestamp,
            updated_at=timestamp,
        )
        state.accounts.append(account)
        self._save_state(state)
        return self._to_session(account)

    def authenticate(
        self,
        *,
        identifier: str,
        password: str,
    ) -> RuntimeLocalSession:
        normalized_identifier = _normalize_identifier(identifier)
        if not normalized_identifier:
            raise RuntimeLocalAuthError("Username or email is required.")

        state = self._load_state()
        if not state.accounts:
            raise RuntimeLocalAuthBootstrapRequiredError("Runtime bootstrap setup is required.")

        account = next(
            (
                candidate
                for candidate in state.accounts
                if candidate.username.casefold() == normalized_identifier
                or candidate.email.casefold() == normalized_identifier
            ),
            None,
        )
        if account is None or not verify_password(password, account.password_hash):
            raise RuntimeLocalAuthError("Invalid username, email, or password.")

        return self._to_session(account)

    def issue_session_token(self, session: RuntimeLocalSession) -> str:
        serializer = self._serializer()
        return serializer.dumps(
            {
                "workspace_id": str(self._workspace_id),
                "actor_id": str(session.id),
                "username": session.username,
                "email": session.email,
                "roles": list(session.roles),
                "provider": session.provider,
                "version": 1,
            }
        )

    def authenticate_request(self, request: Request) -> RuntimeLocalSession:
        token = self._extract_token(request)
        serializer = self._serializer()
        try:
            payload = serializer.loads(token, max_age=self._session_max_age_seconds)
        except SignatureExpired as exc:
            raise RuntimeLocalAuthError("Runtime session has expired.") from exc
        except BadSignature as exc:
            raise RuntimeLocalAuthError("Runtime session is invalid.") from exc

        actor_id_raw = str(payload.get("actor_id") or "").strip()
        if not actor_id_raw:
            raise RuntimeLocalAuthError("Runtime session is invalid.")
        try:
            actor_id = uuid.UUID(actor_id_raw)
        except ValueError as exc:
            raise RuntimeLocalAuthError("Runtime session is invalid.") from exc

        state = self._load_state()
        account = next((candidate for candidate in state.accounts if candidate.id == actor_id), None)
        if account is None:
            raise RuntimeLocalAuthError("Runtime session is no longer valid.")
        return self._to_session(account)

    def delete_session_cookie(self, response) -> None:
        response.delete_cookie(self._cookie_name, path="/")

    def _extract_token(self, request: Request) -> str:
        authorization = str(request.headers.get("authorization") or "").strip()
        if authorization:
            scheme, _, token = authorization.partition(" ")
            if scheme.lower() == "bearer" and token.strip():
                return token.strip()
        token = str(request.cookies.get(self._cookie_name) or "").strip()
        if token:
            return token
        raise RuntimeLocalAuthError("Runtime session is required.")

    def _serializer(self) -> URLSafeTimedSerializer:
        state = self._load_state()
        secret = str(self._session_secret or state.session_secret or "").strip()
        if not secret:
            secret = secrets.token_urlsafe(32)
            state.session_secret = secret
            self._save_state(state)
        return URLSafeTimedSerializer(secret_key=secret, salt="langbridge-runtime-session")

    def _load_state(self) -> RuntimeLocalAuthState:
        self._store_path.parent.mkdir(parents=True, exist_ok=True)
        if not self._store_path.exists():
            state = RuntimeLocalAuthState(session_secret=self._session_secret or secrets.token_urlsafe(32))
            self._save_state(state)
            return state

        payload = json.loads(self._store_path.read_text(encoding="utf-8"))
        state = RuntimeLocalAuthState.model_validate(payload)
        if self._session_secret and state.session_secret != self._session_secret:
            state.session_secret = self._session_secret
            self._save_state(state)
        return state

    def _save_state(self, state: RuntimeLocalAuthState) -> None:
        self._store_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._store_path.with_suffix(f"{self._store_path.suffix}.tmp")
        temp_path.write_text(
            json.dumps(state.model_dump(mode="json"), indent=2),
            encoding="utf-8",
        )
        temp_path.replace(self._store_path)

    @staticmethod
    def _to_session(account: RuntimeLocalAccount) -> RuntimeLocalSession:
        return RuntimeLocalSession(
            id=account.id,
            username=account.username,
            email=account.email,
            roles=list(account.roles),
        )


def _normalize_username(value: str) -> str:
    normalized = str(value or "").strip()
    if not _USERNAME_PATTERN.fullmatch(normalized):
        raise RuntimeLocalAuthError(
            "Username must be 3-64 characters and use letters, numbers, dots, underscores, or hyphens."
        )
    return normalized


def _normalize_email(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if "@" not in normalized or normalized.startswith("@") or normalized.endswith("@"):
        raise RuntimeLocalAuthError("A valid email address is required.")
    return normalized


def _normalize_identifier(value: str) -> str:
    return str(value or "").strip().casefold()


def _validate_password(value: str) -> None:
    if len(str(value or "")) < 8:
        raise RuntimeLocalAuthError("Password must be at least 8 characters.")
