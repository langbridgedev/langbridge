import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Mapping

from pydantic import BaseModel

from langbridge.ai.llm import LLMMessage, LLMRequest, create_provider
from langbridge.runtime.application.errors import ApplicationError, BusinessValidationError
from langbridge.runtime.models.llm import LLMConnectionSecret, LLMProvider
from langbridge.runtime.models.metadata import ManagementMode
from langbridge.runtime.persistence.mappers.llm_connections import (
    from_llm_connection_record,
    to_llm_connection_record,
)

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import (
        ConfiguredLocalRuntimeHost,
        LocalRuntimeLLMConnectionRecord,
    )


_SECRET_CONFIG_KEYS = {"api_key", "apikey", "secret", "token", "password", "client_secret"}


class _LLMConnectionSmokeResponse(BaseModel):
    status: str
    provider: str


def _redact_configuration(value: Mapping[str, Any] | None) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, raw_value in dict(value or {}).items():
        normalized_key = str(key).strip().lower()
        if normalized_key in _SECRET_CONFIG_KEYS or any(marker in normalized_key for marker in _SECRET_CONFIG_KEYS):
            redacted[str(key)] = "********"
        elif isinstance(raw_value, Mapping):
            redacted[str(key)] = _redact_configuration(raw_value)
        else:
            redacted[str(key)] = raw_value
    return redacted


def _provider_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _normalize_provider(value: Any) -> LLMProvider:
    try:
        return LLMProvider(_provider_value(value))
    except ValueError as exc:
        supported = ", ".join(item.value for item in LLMProvider)
        raise BusinessValidationError(f"Unsupported LLM provider '{value}'. Supported providers: {supported}.") from exc


def _now() -> datetime:
    return datetime.now(timezone.utc)


class LLMConnectionApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    @staticmethod
    def _management_mode_value(value: ManagementMode | str) -> str:
        return str(getattr(value, "value", value)).strip().lower()

    @staticmethod
    def _require_runtime_managed(record: "LocalRuntimeLLMConnectionRecord") -> None:
        management_mode = LLMConnectionApplication._management_mode_value(record.management_mode)
        if management_mode != ManagementMode.RUNTIME_MANAGED.value:
            raise BusinessValidationError(
                f"LLM connection '{record.name}' is config_managed and read-only in the runtime UI."
            )

    def _agent_usage(self, connection_id: uuid.UUID) -> list[dict[str, Any]]:
        agents: list[dict[str, Any]] = []
        for name, record in self._host._agents.items():
            if record.agent_definition.llm_connection_id != connection_id:
                continue
            agents.append(
                {
                    "id": record.id,
                    "name": name,
                    "description": record.config.description or record.agent_definition.description,
                    "default": (
                        self._host._default_agent is not None
                        and self._host._default_agent.id == record.id
                    ),
                }
            )
        agents.sort(key=lambda item: (not bool(item["default"]), str(item["name"]).lower()))
        return agents

    def _credential_state(self, record: "LocalRuntimeLLMConnectionRecord") -> str:
        if _provider_value(record.connection.provider) == LLMProvider.OLLAMA.value:
            return "not_required"
        if record.api_key_secret is not None or str(record.connection.api_key or "").strip():
            return "configured"
        return "missing"

    def _serialize_connection(self, record: "LocalRuntimeLLMConnectionRecord") -> dict[str, Any]:
        connection = record.connection
        management_mode = self._management_mode_value(record.management_mode)
        agent_usage = self._agent_usage(connection.id)
        configuration = _redact_configuration(connection.configuration)
        structured_outputs = str(configuration.get("structured_outputs") or "auto").strip().lower() or "auto"
        return {
            "id": connection.id,
            "name": connection.name,
            "description": connection.description,
            "provider": _provider_value(connection.provider),
            "model": connection.model,
            "configuration": configuration,
            "structured_outputs": structured_outputs,
            "base_url": configuration.get("base_url"),
            "is_active": bool(connection.is_active),
            "default": bool(connection.default),
            "credential_state": self._credential_state(record),
            "management_mode": management_mode,
            "managed": management_mode == ManagementMode.CONFIG_MANAGED.value,
            "agent_count": len(agent_usage),
            "agents": agent_usage,
            "created_at": connection.created_at,
            "updated_at": connection.updated_at,
        }

    async def list_llm_connections(self) -> list[dict[str, Any]]:
        items = [
            self._serialize_connection(record)
            for record in self._host._llm_connections.values()
        ]
        items.sort(
            key=lambda item: (
                not bool(item.get("default")),
                str(item.get("provider") or ""),
                str(item.get("name") or "").lower(),
            )
        )
        return items

    async def get_llm_connection(self, *, connection_ref: str) -> dict[str, Any]:
        record = self._host._resolve_llm_connection_record(connection_ref)
        return self._serialize_connection(record)

    async def create_llm_connection(self, *, request) -> dict[str, Any]:
        provider = _normalize_provider(request.provider)
        name = str(request.name or "").strip()
        model = str(request.model or "").strip()
        if not name:
            raise BusinessValidationError("LLM connection name is required.")
        if not model:
            raise BusinessValidationError("LLM model is required.")
        if self._host._find_llm_connection_record(name) is not None:
            raise BusinessValidationError(f"LLM connection '{name}' already exists.")

        api_key = str(request.api_key or "").strip()
        if provider != LLMProvider.OLLAMA and not api_key:
            raise BusinessValidationError("LLM API key is required for non-Ollama providers.")

        timestamp = _now()
        connection = LLMConnectionSecret(
            id=uuid.uuid4(),
            name=name,
            description=(str(request.description).strip() or None) if request.description is not None else None,
            provider=provider,
            api_key=api_key,
            model=model,
            configuration=dict(request.configuration or {}),
            is_active=bool(request.is_active),
            default=bool(request.default),
            workspace_id=self._host.context.workspace_id,
            created_at=timestamp,
            updated_at=timestamp,
        )

        async with self._host._runtime_operation_scope() as uow:
            repository = uow.repository("llm_repository") if uow is not None else self._host._llm_repository
            if connection.default:
                await self._clear_default_connections(repository=repository, except_id=connection.id)
            repository.add(to_llm_connection_record(connection))
            if uow is not None:
                await uow.commit()

        self._host._upsert_runtime_llm_connection(
            connection,
            management_mode=ManagementMode.RUNTIME_MANAGED,
            clear_default=connection.default,
        )
        return self._serialize_connection(self._host._resolve_llm_connection_record(str(connection.id)))

    async def update_llm_connection(self, *, connection_ref: str, request) -> dict[str, Any]:
        record = self._host._resolve_llm_connection_record(connection_ref)
        self._require_runtime_managed(record)

        fields_set = set(getattr(request, "model_fields_set", set()))
        current = record.connection
        api_key = str(request.api_key or "").strip() if "api_key" in fields_set else current.api_key
        provider = _normalize_provider(current.provider)
        if provider != LLMProvider.OLLAMA and not str(api_key or "").strip():
            raise BusinessValidationError("LLM API key is required for non-Ollama providers.")

        updated = current.model_copy(
            update={
                "description": (
                    (str(request.description).strip() or None)
                    if "description" in fields_set
                    else current.description
                ),
                "model": (
                    str(request.model or "").strip()
                    if "model" in fields_set
                    else current.model
                ),
                "configuration": (
                    dict(request.configuration or {})
                    if "configuration" in fields_set
                    else dict(current.configuration or {})
                ),
                "api_key": api_key,
                "is_active": bool(request.is_active) if "is_active" in fields_set else current.is_active,
                "default": bool(request.default) if "default" in fields_set else current.default,
                "updated_at": _now(),
            }
        )
        if not str(updated.model or "").strip():
            raise BusinessValidationError("LLM model is required.")

        async with self._host._runtime_operation_scope() as uow:
            repository = uow.repository("llm_repository") if uow is not None else self._host._llm_repository
            if updated.default:
                await self._clear_default_connections(repository=repository, except_id=updated.id)
            existing = await repository.get_by_id(updated.id)
            if existing is None:
                raise ValueError(f"LLM connection '{record.name}' was not found.")
            await repository.save(to_llm_connection_record(updated))
            if uow is not None:
                await uow.commit()

        self._host._upsert_runtime_llm_connection(
            updated,
            management_mode=ManagementMode.RUNTIME_MANAGED,
            clear_default=updated.default,
        )
        return self._serialize_connection(self._host._resolve_llm_connection_record(str(updated.id)))

    async def delete_llm_connection(self, *, connection_ref: str) -> dict[str, Any]:
        record = self._host._resolve_llm_connection_record(connection_ref)
        self._require_runtime_managed(record)
        usage = self._agent_usage(record.connection.id)
        if usage:
            agent_names = ", ".join(str(item["name"]) for item in usage)
            raise BusinessValidationError(
                f"LLM connection '{record.name}' cannot be deleted while agents reference it: {agent_names}."
            )

        async with self._host._runtime_operation_scope() as uow:
            repository = uow.repository("llm_repository") if uow is not None else self._host._llm_repository
            existing = await repository.get_by_id(record.connection.id)
            if existing is None:
                raise ValueError(f"LLM connection '{record.name}' was not found.")
            await repository.delete(existing)
            if uow is not None:
                await uow.commit()

        self._host._remove_runtime_llm_connection(
            connection_name=record.name,
            connection_id=record.connection.id,
        )
        return {"ok": True, "deleted": True, "id": record.connection.id, "name": record.name}

    async def test_llm_connection(self, *, connection_ref: str) -> dict[str, Any]:
        record = self._host._resolve_llm_connection_record(connection_ref)
        started = time.perf_counter()
        connection = await self._load_connection_secret(record.connection.id)
        try:
            provider = create_provider(connection)
            invocation = await provider.ainvoke(
                LLMRequest[_LLMConnectionSmokeResponse](
                    purpose="runtime.llm_connection_smoke_test",
                    messages=[
                        LLMMessage(
                            role="system",
                            content=(
                                "Return a minimal structured health response for this Langbridge "
                                "LLM connection smoke test."
                            ),
                            kind="instruction",
                            trusted=True,
                        ),
                        LLMMessage(
                            role="user",
                            content=(
                                "Respond with status='ok' and provider="
                                f"'{_provider_value(connection.provider)}'."
                            ),
                            kind="user_input",
                        ),
                    ],
                    response_model=_LLMConnectionSmokeResponse,
                    temperature=0.0,
                    max_tokens=80,
                )
            )
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            parsed = invocation.response.parsed
            return {
                "status": "success",
                "ok": True,
                "connection_id": connection.id,
                "connection_name": connection.name,
                "provider": _provider_value(connection.provider),
                "model": connection.model,
                "extract_mode": invocation.response.extract_mode,
                "response_model": invocation.response.response_model_name,
                "latency_ms": elapsed_ms,
                "parsed": parsed.model_dump(mode="json") if parsed is not None else None,
                "error": None,
            }
        except Exception as exc:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return {
                "status": "failed",
                "ok": False,
                "connection_id": record.connection.id,
                "connection_name": record.name,
                "provider": _provider_value(record.connection.provider),
                "model": record.connection.model,
                "extract_mode": None,
                "response_model": _LLMConnectionSmokeResponse.__name__,
                "latency_ms": elapsed_ms,
                "parsed": None,
                "error": self._format_test_error(exc),
            }

    async def _load_connection_secret(self, connection_id: uuid.UUID) -> LLMConnectionSecret:
        value = await self._host._llm_repository.get_by_id(connection_id)
        connection = from_llm_connection_record(value)
        if connection is None:
            raise ValueError(f"LLM connection '{connection_id}' was not found.")
        return connection

    async def _clear_default_connections(self, *, repository: Any, except_id: uuid.UUID) -> None:
        items = await repository.list_llm_connections(workspace_id=self._host.context.workspace_id)
        for raw_item in items:
            connection = from_llm_connection_record(raw_item)
            if connection is None or connection.id == except_id or not connection.default:
                continue
            cleared = connection.model_copy(update={"default": False, "updated_at": _now()})
            await repository.save(to_llm_connection_record(cleared))

    @staticmethod
    def _format_test_error(exc: Exception) -> dict[str, str]:
        message = str(exc).strip()
        if len(message) > 500:
            message = f"{message[:497]}..."
        return {
            "type": exc.__class__.__name__,
            "message": message or "LLM connection smoke test failed.",
        }
