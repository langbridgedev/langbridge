
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any

from langbridge.runtime.config.models import ResolvedLocalRuntimeMetadataStoreConfig


@dataclass(slots=True)
class _ConfiguredRuntimePersistenceController:
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig
    async_engine: Any | None = None
    async_session_factory: Any | None = None
    _closed: bool = False

    def current_uow(self) -> "_ConfiguredRuntimeUnitOfWork | None":
        return _CONFIGURED_RUNTIME_UOW.get()

    def unit_of_work(self) -> "_ConfiguredRuntimeUnitOfWork":
        if self._closed:
            raise RuntimeError("Persisted runtime resources have been closed.")
        if self.async_session_factory is None:
            raise RuntimeError("Persisted runtime session factory is not configured.")
        return _ConfiguredRuntimeUnitOfWork(controller=self)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self.async_engine is not None:
            await self.async_engine.dispose()

    @property
    def closed(self) -> bool:
        return self._closed

    def _bind_session(self, session: Any) -> dict[str, Any]:
        from langbridge.runtime.persistence.repositories.agent_repository import AgentRepository
        from langbridge.runtime.persistence.repositories.auth_repository import (
            RuntimeLocalAuthCredentialRepository,
            RuntimeLocalAuthStateRepository,
        )
        from langbridge.runtime.persistence.repositories.connector_repository import ConnectorRepository
        from langbridge.runtime.persistence.repositories.connector_sync_repository import (
            ConnectorSyncStateRepository,
        )
        from langbridge.runtime.persistence.repositories.conversation_memory_repository import (
            ConversationMemoryRepository,
        )
        from langbridge.runtime.persistence.repositories.dataset_repository import (
            DatasetColumnRepository,
            DatasetPolicyRepository,
            DatasetRepository,
            DatasetRevisionRepository,
        )
        from langbridge.runtime.persistence.repositories.lineage_repository import LineageEdgeRepository
        from langbridge.runtime.persistence.repositories.llm_connection_repository import (
            LLMConnectionRepository,
        )
        from langbridge.runtime.persistence.repositories.semantic_model_repository import (
            SemanticModelRepository,
        )
        from langbridge.runtime.persistence.repositories.semantic_search_repository import (
            SemanticVectorIndexRepository,
        )
        from langbridge.runtime.persistence.repositories.thread_message_repository import (
            ThreadMessageRepository,
        )
        from langbridge.runtime.persistence.repositories.thread_repository import ThreadRepository
        from langbridge.runtime.persistence.repositories.workspace_repository import (
            RuntimeActorRepository,
            WorkspaceRepository,
        )

        return {
            "dataset_repository": DatasetRepository(session),
            "dataset_column_repository": DatasetColumnRepository(session),
            "dataset_policy_repository": DatasetPolicyRepository(session),
            "connector_repository": ConnectorRepository(session),
            "semantic_model_repository": SemanticModelRepository(session),
            "semantic_vector_index_repository": SemanticVectorIndexRepository(session),
            "connector_sync_state_repository": ConnectorSyncStateRepository(session),
            "dataset_revision_repository": DatasetRevisionRepository(session),
            "lineage_edge_repository": LineageEdgeRepository(session),
            "agent_repository": AgentRepository(session),
            "llm_repository": LLMConnectionRepository(session),
            "thread_repository": ThreadRepository(session),
            "thread_message_repository": ThreadMessageRepository(session),
            "conversation_memory_repository": ConversationMemoryRepository(session),
            "workspace_repository": WorkspaceRepository(session),
            "actor_repository": RuntimeActorRepository(session),
            "local_auth_state_repository": RuntimeLocalAuthStateRepository(session),
            "local_auth_repository": RuntimeLocalAuthCredentialRepository(session),
        }


_CONFIGURED_RUNTIME_UOW: ContextVar["_ConfiguredRuntimeUnitOfWork | None"] = ContextVar(
    "configured_runtime_uow",
    default=None,
)


class _NullTrackedSession:
    new: tuple[()] = ()
    identity_map: dict[object, object] = {}


_NULL_TRACKED_SESSION = _NullTrackedSession()


@dataclass(slots=True)
class _ConfiguredRuntimeUnitOfWork:
    controller: _ConfiguredRuntimePersistenceController
    session: Any | None = None
    bindings: dict[str, Any] = field(default_factory=dict)
    _token: Token | None = None

    async def __aenter__(self) -> "_ConfiguredRuntimeUnitOfWork":
        if self.controller.async_session_factory is None:
            raise RuntimeError("Persisted runtime session factory is not configured.")
        self.session = self.controller.async_session_factory()
        self.bindings = self.controller._bind_session(self.session)
        self._token = _CONFIGURED_RUNTIME_UOW.set(self)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None:
                await self.rollback()
        finally:
            if self._token is not None:
                _CONFIGURED_RUNTIME_UOW.reset(self._token)
                self._token = None
            if self.session is not None:
                await self.session.close()
                self.session = None
            self.bindings = {}

    def repository(self, name: str) -> Any:
        return self.bindings[name]

    async def flush(self) -> None:
        if self.session is None:
            return
        await self.session.flush()

    async def commit(self) -> None:
        if self.session is None:
            return
        await self.session.commit()

    async def rollback(self) -> None:
        if self.session is None:
            return
        await self.session.rollback()


class _RuntimeSessionRepositoryProxy:
    def __init__(
        self,
        *,
        controller: _ConfiguredRuntimePersistenceController,
        repository_attr: str,
        sync_methods: set[str] | None = None,
        write_methods: set[str] | None = None,
    ) -> None:
        self._controller = controller
        self._repository_attr = repository_attr
        self._sync_methods = set(sync_methods or set())
        self._write_methods = set(write_methods or set())

    @property
    def _session(self) -> Any:
        uow = self._controller.current_uow()
        if uow is None:
            return _NULL_TRACKED_SESSION
        repository = uow.repository(self._repository_attr)
        return getattr(repository, "_session", _NULL_TRACKED_SESSION)

    def _require_current_repository(self) -> Any:
        uow = self._controller.current_uow()
        if uow is None:
            raise RuntimeError(
                f"Repository method on '{self._repository_attr}' requires an active Unit of Work."
            )
        return uow.repository(self._repository_attr)

    async def _run_async(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        uow = self._controller.current_uow()
        if uow is not None:
            method = getattr(uow.repository(self._repository_attr), method_name)
            return await method(*args, **kwargs)

        async with self._controller.unit_of_work() as owned_uow:
            method = getattr(owned_uow.repository(self._repository_attr), method_name)
            result = await method(*args, **kwargs)
            if method_name in self._write_methods:
                await owned_uow.commit()
            return result

    def __getattr__(self, name: str) -> Any:
        if name in self._sync_methods:
            def _sync_call(*args: Any, **kwargs: Any) -> Any:
                method = getattr(self._require_current_repository(), name)
                return method(*args, **kwargs)

            return _sync_call

        async def _async_call(*args: Any, **kwargs: Any) -> Any:
            return await self._run_async(name, *args, **kwargs)

        return _async_call


__all__ = [
    "_ConfiguredRuntimePersistenceController",
    "_ConfiguredRuntimeUnitOfWork",
    "_RuntimeSessionRepositoryProxy",
]
