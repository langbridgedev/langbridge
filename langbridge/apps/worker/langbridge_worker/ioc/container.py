from __future__ import annotations

from typing import Any

from dependency_injector import containers, providers

from langbridge.packages.common.langbridge_common.db import (
    create_async_engine_for_url,
    create_async_session_factory,
)
from langbridge.packages.common.langbridge_common.db.session_context import get_session
from langbridge.packages.common.langbridge_common.repositories.agent_repository import AgentRepository
from langbridge.packages.common.langbridge_common.repositories.connector_repository import ConnectorRepository, ConnectorStore
from langbridge.packages.common.langbridge_common.repositories.connector_sync_repository import (
    ConnectorSyncStateRepository,
)
from langbridge.packages.common.langbridge_common.repositories.conversation_memory_repository import (
    ConversationMemoryRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetColumnRepository,
    DatasetPolicyRepository,
    DatasetRepository,
    DatasetRevisionRepository,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.common.langbridge_common.repositories.lineage_repository import (
    LineageEdgeRepository,
)
from langbridge.packages.common.langbridge_common.repositories.llm_connection_repository import (
    LLMConnectionRepository,
)
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlJobRepository,
    SqlJobResultArtifactRepository,
)
from langbridge.packages.common.langbridge_common.config import Settings, settings
from langbridge.packages.common.langbridge_common.repositories.message_repository import MessageRepository
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import (
    SemanticModelRepository,
    SemanticModelStore,
)
from langbridge.packages.common.langbridge_common.repositories.thread_message_repository import (
    ThreadMessageRepository,
)
from langbridge.packages.common.langbridge_common.repositories.thread_repository import ThreadRepository
from langbridge.packages.messaging.langbridge_messaging.broker.redis import RedisBroker
from langbridge.packages.messaging.langbridge_messaging.flusher.flusher import MessageFlusher
from langbridge.apps.worker.langbridge_worker.secrets import SecretProviderRegistry
from langbridge.apps.worker.langbridge_worker.tools import FederatedQueryTool


class WorkerContainer(containers.DeclarativeContainer):
    """Worker dependency injection container."""

    wiring_config = containers.WiringConfiguration()

    config = providers.Configuration()

    async_engine = providers.Singleton(
        create_async_engine_for_url,
        database_url=config.database.async_url,
        echo=config.database.echo,
        pool_size=config.database.pool_size,
        max_overflow=config.database.max_overflow,
        pool_timeout=config.database.pool_timeout,
    )

    async_session_factory = providers.Singleton(
        create_async_session_factory,
        engine=async_engine,
    )

    async_session = providers.Factory(get_session)

    job_repository = providers.Factory(JobRepository, session=async_session)
    sql_job_repository = providers.Factory(SqlJobRepository, session=async_session)
    sql_job_result_artifact_repository = providers.Factory(
        SqlJobResultArtifactRepository,
        session=async_session,
    )
    agent_definition_repository = providers.Factory(AgentRepository, session=async_session)
    semantic_model_repository = providers.Factory(SemanticModelRepository, session=async_session)
    llm_repository = providers.Factory(LLMConnectionRepository, session=async_session)
    connector_repository = providers.Factory(ConnectorRepository, session=async_session)
    connector_sync_state_repository = providers.Factory(
        ConnectorSyncStateRepository,
        session=async_session,
    )
    dataset_repository = providers.Factory(DatasetRepository, session=async_session)
    dataset_column_repository = providers.Factory(DatasetColumnRepository, session=async_session)
    dataset_policy_repository = providers.Factory(DatasetPolicyRepository, session=async_session)
    dataset_revision_repository = providers.Factory(DatasetRevisionRepository, session=async_session)
    lineage_edge_repository = providers.Factory(LineageEdgeRepository, session=async_session)
    thread_repository = providers.Factory(ThreadRepository, session=async_session)
    thread_message_repository = providers.Factory(ThreadMessageRepository, session=async_session)
    memory_repository = providers.Factory(ConversationMemoryRepository, session=async_session)

    # stores
    connector_store = providers.Factory(
        ConnectorStore,
        repository=connector_repository,
    )
    semantic_model_store = providers.Factory(
        SemanticModelStore,
        repository=semantic_model_repository,
    )

    message_repository = providers.Factory(MessageRepository, session=async_session)
    message_broker = providers.Singleton(
        RedisBroker,
        stream=settings.REDIS_WORKER_STREAM,
        group=settings.REDIS_WORKER_CONSUMER_GROUP,
    )
    message_flusher = providers.Factory(
        MessageFlusher,
        message_repository=message_repository,
        message_bus=message_broker,
    )
    secret_provider_registry = providers.Singleton(SecretProviderRegistry)
    federated_query_tool = providers.Factory(
        FederatedQueryTool,
        connector_repository=connector_repository,
        secret_provider_registry=secret_provider_registry,
    )


def build_config(settings_obj: Settings) -> dict[str, Any]:
    return {
        "database": {
            "async_url": settings_obj.SQLALCHEMY_ASYNC_DATABASE_URI,
            "echo": settings_obj.IS_LOCAL,
            "pool_size": settings_obj.SQLALCHEMY_POOL_SIZE,
            "max_overflow": settings_obj.SQLALCHEMY_MAX_OVERFLOW,
            "pool_timeout": settings_obj.SQLALCHEMY_POOL_TIMEOUT,
        }
    }


def create_container() -> WorkerContainer:
    container = WorkerContainer()
    container.config.from_dict(build_config(settings))
    return container
