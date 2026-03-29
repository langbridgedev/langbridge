
import re
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Protocol

from fastapi import Request
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from pydantic import Field

from langbridge.runtime.hosting.passwords import hash_password, verify_password
from langbridge.runtime.persistence.uow import _ConfiguredRuntimePersistenceController
from langbridge.runtime.models.base import RuntimeModel


_USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9._-]{3,64}$")
_RUNTIME_ADMIN_ROLE = "runtime:admin"


class RuntimeLocalAuthError(ValueError):
    pass


class RuntimeLocalAuthBootstrapRequiredError(RuntimeLocalAuthError):
    pass


class RuntimeLocalAuthAccount(RuntimeModel):
    id: uuid.UUID
    username: str
    email: str
    password_hash: str
    roles: list[str] = Field(default_factory=lambda: [_RUNTIME_ADMIN_ROLE])
    is_active: bool = True
    created_at: datetime
    updated_at: datetime


class RuntimeLocalSession(RuntimeModel):
    id: uuid.UUID
    username: str
    email: str
    roles: list[str] = Field(default_factory=list)
    provider: str = "runtime_local_session"


class _RuntimeLocalAuthStore(Protocol):
    persistence_mode: Literal["in_memory", "sqlite", "postgres"]

    async def has_admin_account(self) -> bool:
        ...

    async def create_admin_account(
        self,
        *,
        username: str,
        email: str,
        password_hash: str,
    ) -> RuntimeLocalAuthAccount:
        ...

    async def get_account_by_identifier(self, *, identifier: str) -> RuntimeLocalAuthAccount | None:
        ...

    async def get_account_by_id(self, *, actor_id: uuid.UUID) -> RuntimeLocalAuthAccount | None:
        ...

    async def get_session_secret(self) -> str | None:
        ...

    async def save_session_secret(self, *, secret: str) -> str:
        ...


class _InMemoryRuntimeLocalAuthStore:
    persistence_mode: Literal["in_memory"] = "in_memory"

    def __init__(self) -> None:
        self._accounts: dict[uuid.UUID, RuntimeLocalAuthAccount] = {}
        self._session_secret: str | None = None

    async def has_admin_account(self) -> bool:
        return any(account.is_active and _RUNTIME_ADMIN_ROLE in account.roles for account in self._accounts.values())

    async def create_admin_account(
        self,
        *,
        username: str,
        email: str,
        password_hash: str,
    ) -> RuntimeLocalAuthAccount:
        if await self.has_admin_account():
            raise RuntimeLocalAuthError("Runtime bootstrap has already been completed.")

        normalized_identifier = _normalize_identifier(username)
        normalized_email = _normalize_identifier(email)
        for account in self._accounts.values():
            if _normalize_identifier(account.username) == normalized_identifier:
                raise RuntimeLocalAuthError("A runtime local auth account already uses that username.")
            if _normalize_identifier(account.email) == normalized_email:
                raise RuntimeLocalAuthError("A runtime local auth account already uses that email.")

        timestamp = datetime.now(timezone.utc)
        account = RuntimeLocalAuthAccount(
            id=uuid.uuid4(),
            username=username,
            email=email,
            password_hash=password_hash,
            roles=[_RUNTIME_ADMIN_ROLE],
            is_active=True,
            created_at=timestamp,
            updated_at=timestamp,
        )
        self._accounts[account.id] = account
        return account

    async def get_account_by_identifier(self, *, identifier: str) -> RuntimeLocalAuthAccount | None:
        normalized_identifier = _normalize_identifier(identifier)
        for account in self._accounts.values():
            if not account.is_active:
                continue
            if (
                _normalize_identifier(account.username) == normalized_identifier
                or _normalize_identifier(account.email) == normalized_identifier
            ):
                return account
        return None

    async def get_account_by_id(self, *, actor_id: uuid.UUID) -> RuntimeLocalAuthAccount | None:
        account = self._accounts.get(actor_id)
        if account is None or not account.is_active:
            return None
        return account

    async def get_session_secret(self) -> str | None:
        return self._session_secret

    async def save_session_secret(self, *, secret: str) -> str:
        self._session_secret = secret
        return secret


class _PersistedRuntimeLocalAuthStore:
    def __init__(
        self,
        *,
        workspace_id: uuid.UUID,
        persistence_mode: Literal["sqlite", "postgres"],
        controller: _ConfiguredRuntimePersistenceController,
    ) -> None:
        self.persistence_mode = persistence_mode
        self._workspace_id = workspace_id
        self._controller = controller

    async def has_admin_account(self) -> bool:
        async with self._controller.unit_of_work() as uow:
            repository = uow.repository("local_auth_repository")
            accounts = await repository.list_for_workspace(workspace_id=self._workspace_id)
            return any(
                account.actor is not None
                and bool(account.actor.is_active)
                and _RUNTIME_ADMIN_ROLE in list(account.actor.roles_json or [])
                for account in accounts
            )

    async def create_admin_account(
        self,
        *,
        username: str,
        email: str,
        password_hash: str,
    ) -> RuntimeLocalAuthAccount:
        from langbridge.runtime.persistence.db.auth import RuntimeLocalAuthCredential
        from langbridge.runtime.persistence.db.workspace import RuntimeActor

        async with self._controller.unit_of_work() as uow:
            workspace_repository = uow.repository("workspace_repository")
            actor_repository = uow.repository("actor_repository")
            auth_repository = uow.repository("local_auth_repository")

            await workspace_repository.ensure_configured(
                workspace_id=self._workspace_id,
                name=f"local-runtime-{self._workspace_id}",
            )

            accounts = await auth_repository.list_for_workspace(workspace_id=self._workspace_id)
            if any(
                account.actor is not None
                and bool(account.actor.is_active)
                and _RUNTIME_ADMIN_ROLE in list(account.actor.roles_json or [])
                for account in accounts
            ):
                raise RuntimeLocalAuthError("Runtime bootstrap has already been completed.")

            existing_actor = await actor_repository.get_by_subject(
                workspace_id=self._workspace_id,
                subject=username,
            )
            if existing_actor is not None:
                raise RuntimeLocalAuthError("A runtime actor already uses that username.")

            existing_email_actor = await actor_repository.get_by_email(
                workspace_id=self._workspace_id,
                email=email,
            )
            if existing_email_actor is not None:
                raise RuntimeLocalAuthError("A runtime actor already uses that email.")

            actor = RuntimeActor(
                id=uuid.uuid4(),
                workspace_id=self._workspace_id,
                subject=username,
                actor_type="operator",
                email=email,
                display_name=username,
                roles_json=[_RUNTIME_ADMIN_ROLE],
                is_active=True,
                metadata_json={
                    "provider": "runtime_local_session",
                    "runtime_operator": True,
                    "local_auth": True,
                },
            )
            actor_repository.add(actor)
            credential = RuntimeLocalAuthCredential(
                actor_id=actor.id,
                workspace_id=self._workspace_id,
                password_hash=password_hash,
            )
            credential.actor = actor
            auth_repository.add(credential)
            await uow.flush()
            await uow.commit()
            return _credential_to_account(credential)

    async def get_account_by_identifier(self, *, identifier: str) -> RuntimeLocalAuthAccount | None:
        async with self._controller.unit_of_work() as uow:
            repository = uow.repository("local_auth_repository")
            credential = await repository.get_by_identifier(
                workspace_id=self._workspace_id,
                identifier=identifier,
            )
            if credential is None:
                return None
            account = _credential_to_account(credential)
            if not account.is_active:
                return None
            return account

    async def get_account_by_id(self, *, actor_id: uuid.UUID) -> RuntimeLocalAuthAccount | None:
        async with self._controller.unit_of_work() as uow:
            repository = uow.repository("local_auth_repository")
            credential = await repository.get_by_actor_id(actor_id=actor_id)
            if credential is None or credential.workspace_id != self._workspace_id:
                return None
            account = _credential_to_account(credential)
            if not account.is_active:
                return None
            return account

    async def get_session_secret(self) -> str | None:
        async with self._controller.unit_of_work() as uow:
            repository = uow.repository("local_auth_state_repository")
            state = await repository.get_for_workspace(workspace_id=self._workspace_id)
            if state is None:
                return None
            return str(state.session_secret or "").strip() or None

    async def save_session_secret(self, *, secret: str) -> str:
        from langbridge.runtime.persistence.db.auth import RuntimeLocalAuthState

        async with self._controller.unit_of_work() as uow:
            workspace_repository = uow.repository("workspace_repository")
            state_repository = uow.repository("local_auth_state_repository")

            await workspace_repository.ensure_configured(
                workspace_id=self._workspace_id,
                name=f"local-runtime-{self._workspace_id}",
            )
            state = await state_repository.get_for_workspace(workspace_id=self._workspace_id)
            if state is None:
                state = RuntimeLocalAuthState(
                    workspace_id=self._workspace_id,
                    session_secret=secret,
                )
                state_repository.add(state)
            else:
                state.session_secret = secret
            await uow.flush()
            await uow.commit()
            return state.session_secret


class RuntimeLocalAuthManager:
    def __init__(
        self,
        *,
        workspace_id: uuid.UUID,
        persistence_mode: Literal["in_memory", "sqlite", "postgres"],
        cookie_name: str,
        session_max_age_seconds: int,
        session_secret: str | None = None,
        persistence_controller: _ConfiguredRuntimePersistenceController | None = None,
    ) -> None:
        self._workspace_id = workspace_id
        self._cookie_name = str(cookie_name or "").strip() or "langbridge_runtime_session"
        self._session_max_age_seconds = max(60, int(session_max_age_seconds))
        self._session_secret = str(session_secret or "").strip() or None
        if persistence_mode == "in_memory":
            self._store: _RuntimeLocalAuthStore = _InMemoryRuntimeLocalAuthStore()
        else:
            if persistence_controller is None:
                raise ValueError("Persisted local auth requires a runtime persistence controller.")
            self._store = _PersistedRuntimeLocalAuthStore(
                workspace_id=workspace_id,
                persistence_mode=persistence_mode,
                controller=persistence_controller,
            )

    @property
    def cookie_name(self) -> str:
        return self._cookie_name

    @property
    def session_max_age_seconds(self) -> int:
        return self._session_max_age_seconds

    @property
    def persistence_mode(self) -> Literal["in_memory", "sqlite", "postgres"]:
        return self._store.persistence_mode

    async def auth_status(self) -> dict[str, bool]:
        has_admin = await self._store.has_admin_account()
        return {
            "has_admin": has_admin,
            "bootstrap_required": not has_admin,
        }

    async def bootstrap_admin(
        self,
        *,
        username: str,
        email: str,
        password: str,
    ) -> RuntimeLocalSession:
        normalized_username = _normalize_username(username)
        normalized_email = _normalize_email(email)
        _validate_password(password)

        account = await self._store.create_admin_account(
            username=normalized_username,
            email=normalized_email,
            password_hash=hash_password(password),
        )
        return self._to_session(account)

    async def authenticate(
        self,
        *,
        identifier: str,
        password: str,
    ) -> RuntimeLocalSession:
        normalized_identifier = _normalize_identifier(identifier)
        if not normalized_identifier:
            raise RuntimeLocalAuthError("Username or email is required.")

        if not await self._store.has_admin_account():
            raise RuntimeLocalAuthBootstrapRequiredError("Runtime bootstrap setup is required.")

        account = await self._store.get_account_by_identifier(identifier=normalized_identifier)
        if account is None or not verify_password(password, account.password_hash):
            raise RuntimeLocalAuthError("Invalid username, email, or password.")

        return self._to_session(account)

    async def issue_session_token(self, session: RuntimeLocalSession) -> str:
        serializer = URLSafeTimedSerializer(
            secret_key=await self._resolve_session_secret(),
            salt="langbridge-runtime-session",
        )
        return serializer.dumps(
            {
                "workspace_id": str(self._workspace_id),
                "actor_id": str(session.id),
                "provider": session.provider,
                "version": 2,
            }
        )

    async def authenticate_session_request(self, request: Request) -> RuntimeLocalSession:
        if not await self._store.has_admin_account():
            raise RuntimeLocalAuthBootstrapRequiredError("Runtime bootstrap setup is required.")

        token = self._extract_cookie_token(request)
        serializer = URLSafeTimedSerializer(
            secret_key=await self._resolve_session_secret(),
            salt="langbridge-runtime-session",
        )
        try:
            payload = serializer.loads(token, max_age=self._session_max_age_seconds)
        except SignatureExpired as exc:
            raise RuntimeLocalAuthError("Runtime session has expired.") from exc
        except BadSignature as exc:
            raise RuntimeLocalAuthError("Runtime session is invalid.") from exc

        workspace_id_raw = str(payload.get("workspace_id") or "").strip()
        if workspace_id_raw != str(self._workspace_id):
            raise RuntimeLocalAuthError("Runtime session is invalid.")

        actor_id_raw = str(payload.get("actor_id") or "").strip()
        if not actor_id_raw:
            raise RuntimeLocalAuthError("Runtime session is invalid.")
        try:
            actor_id = uuid.UUID(actor_id_raw)
        except ValueError as exc:
            raise RuntimeLocalAuthError("Runtime session is invalid.") from exc

        account = await self._store.get_account_by_id(actor_id=actor_id)
        if account is None:
            raise RuntimeLocalAuthError("Runtime session is no longer valid.")
        return self._to_session(account)

    async def authenticate_request(self, request: Request) -> RuntimeLocalSession:
        return await self.authenticate_session_request(request)

    def delete_session_cookie(self, response: Any) -> None:
        response.delete_cookie(self._cookie_name, path="/")

    def _extract_cookie_token(self, request: Request) -> str:
        token = str(request.cookies.get(self._cookie_name) or "").strip()
        if token:
            return token
        raise RuntimeLocalAuthError("Runtime session is required.")

    async def _resolve_session_secret(self) -> str:
        if self._session_secret:
            persisted_secret = await self._store.get_session_secret()
            if persisted_secret != self._session_secret:
                await self._store.save_session_secret(secret=self._session_secret)
            return self._session_secret

        persisted_secret = await self._store.get_session_secret()
        if persisted_secret:
            return persisted_secret

        generated_secret = secrets.token_urlsafe(32)
        await self._store.save_session_secret(secret=generated_secret)
        return generated_secret

    @staticmethod
    def _to_session(account: RuntimeLocalAuthAccount) -> RuntimeLocalSession:
        return RuntimeLocalSession(
            id=account.id,
            username=account.username,
            email=account.email,
            roles=list(account.roles),
        )


def _credential_to_account(credential: Any) -> RuntimeLocalAuthAccount:
    actor = credential.actor
    if actor is None:
        raise RuntimeLocalAuthError("Runtime local auth credential is missing its actor.")
    return RuntimeLocalAuthAccount(
        id=actor.id,
        username=str(actor.subject or ""),
        email=str(actor.email or ""),
        password_hash=credential.password_hash,
        roles=list(actor.roles_json or []),
        is_active=bool(actor.is_active),
        created_at=credential.created_at or actor.created_at or datetime.now(timezone.utc),
        updated_at=credential.updated_at or actor.updated_at or datetime.now(timezone.utc),
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
