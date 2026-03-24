from __future__ import annotations

import logging

from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.execution import FederatedQueryTool
from langbridge.runtime.local_config import build_configured_local_runtime
from langbridge.runtime.persistence import (
    RepositoryAgentDefinitionStore,
    RepositoryConnectorSyncStateStore,
    RepositoryConversationMemoryStore,
    RepositoryDatasetCatalogStore,
    RepositoryDatasetColumnStore,
    RepositoryDatasetPolicyStore,
    RepositoryDatasetRevisionStore,
    RepositoryLLMConnectionStore,
    RepositoryLineageEdgeStore,
    RepositorySemanticModelStore,
    RepositorySemanticVectorIndexStore,
    RepositorySqlJobArtifactStore,
    RepositoryThreadMessageStore,
    RepositoryThreadStore,
)
from langbridge.runtime.persistence.repositories.agent_repository import (
    AgentRepository,
)
from langbridge.runtime.persistence.repositories.connector_repository import (
    ConnectorRepository,
)
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
from langbridge.runtime.persistence.repositories.lineage_repository import (
    LineageEdgeRepository,
)
from langbridge.runtime.persistence.repositories.llm_connection_repository import (
    LLMConnectionRepository,
)
from langbridge.runtime.persistence.repositories.semantic_model_repository import (
    SemanticModelRepository,
)
from langbridge.runtime.persistence.repositories.sql_repository import (
    SqlJobResultArtifactRepository,
)
from langbridge.runtime.persistence.repositories.semantic_search_repository import (
    SemanticVectorIndexRepository,
)
from langbridge.runtime.persistence.repositories.thread_message_repository import (
    ThreadMessageRepository,
)
from langbridge.runtime.persistence.repositories.thread_repository import (
    ThreadRepository,
)
from langbridge.runtime.ports import (
    ConversationMemoryStore,
    SemanticModelStore,
)
from langbridge.runtime.providers import (
    CachedConnectorMetadataProvider,
    CachedDatasetMetadataProvider,
    CachedSemanticModelMetadataProvider,
    RepositoryConnectorMetadataProvider,
    RepositoryDatasetMetadataProvider,
    RepositorySemanticModelMetadataProvider,
    RepositorySemanticVectorIndexMetadataProvider,
    RepositorySyncStateProvider,
    SecretRegistryCredentialProvider,
)
from langbridge.runtime.security import SecretProviderRegistry
from langbridge.runtime.services.agent_execution_service import (
    AgentExecutionService,
)
from langbridge.runtime.services.dataset_query_service import DatasetQueryService
from langbridge.runtime.services.dataset_sync_service import ConnectorSyncRuntime
from langbridge.runtime.services.runtime_host import (
    RuntimeHost,
    RuntimeProviders,
    RuntimeServices,
)
from langbridge.runtime.services.semantic_query_execution_service import (
    SemanticQueryExecutionService,
)
from langbridge.runtime.services.semantic_vector_search_service import (
    SemanticVectorSearchService,
)
from langbridge.runtime.services.sql_query_service import SqlQueryService


def build_local_runtime(
    *,
    context: RuntimeContext,
    dataset_repository: DatasetRepository,
    dataset_column_repository: DatasetColumnRepository | None,
    dataset_policy_repository: DatasetPolicyRepository | None,
    connector_repository: ConnectorRepository,
    semantic_model_repository: SemanticModelRepository | None,
    semantic_vector_index_repository: SemanticVectorIndexRepository | None = None,
    connector_sync_state_repository: ConnectorSyncStateRepository | None = None,
    dataset_revision_repository: DatasetRevisionRepository | None = None,
    lineage_edge_repository: LineageEdgeRepository | None = None,
    sql_job_result_artifact_repository: SqlJobResultArtifactRepository | None = None,
    agent_definition_repository: AgentRepository | None = None,
    llm_repository: LLMConnectionRepository | None = None,
    thread_repository: ThreadRepository | None = None,
    thread_message_repository: ThreadMessageRepository | None = None,
    memory_repository: ConversationMemoryStore | ConversationMemoryRepository | None = None,
    semantic_model_store: SemanticModelStore | None = None,
    secret_provider_registry: SecretProviderRegistry | None = None,
    logger: logging.Logger | None = None,
) -> RuntimeHost:
    dataset_store = RepositoryDatasetCatalogStore(repository=dataset_repository)
    dataset_column_store = (
        RepositoryDatasetColumnStore(repository=dataset_column_repository)
        if dataset_column_repository is not None
        else None
    )
    dataset_policy_store = (
        RepositoryDatasetPolicyStore(repository=dataset_policy_repository)
        if dataset_policy_repository is not None
        else None
    )
    dataset_revision_store = (
        RepositoryDatasetRevisionStore(repository=dataset_revision_repository)
        if dataset_revision_repository is not None
        else None
    )
    lineage_edge_store = (
        RepositoryLineageEdgeStore(repository=lineage_edge_repository)
        if lineage_edge_repository is not None
        else None
    )
    connector_sync_state_store = (
        RepositoryConnectorSyncStateStore(repository=connector_sync_state_repository)
        if connector_sync_state_repository is not None
        else None
    )
    agent_definition_store = (
        RepositoryAgentDefinitionStore(repository=agent_definition_repository)
        if agent_definition_repository is not None
        else None
    )
    llm_connection_store = (
        RepositoryLLMConnectionStore(repository=llm_repository)
        if llm_repository is not None
        else None
    )
    thread_store = (
        RepositoryThreadStore(repository=thread_repository)
        if thread_repository is not None
        else None
    )
    thread_message_store = (
        RepositoryThreadMessageStore(repository=thread_message_repository)
        if thread_message_repository is not None
        else None
    )
    runtime_semantic_model_store = semantic_model_store or (
        RepositorySemanticModelStore(repository=semantic_model_repository)
        if semantic_model_repository is not None
        else None
    )
    runtime_memory_store = (
        RepositoryConversationMemoryStore(repository=memory_repository)
        if isinstance(memory_repository, ConversationMemoryRepository)
        else memory_repository
    )
    runtime_semantic_vector_index_store = (
        RepositorySemanticVectorIndexStore(repository=semantic_vector_index_repository)
        if semantic_vector_index_repository is not None
        else None
    )
    credential_provider = SecretRegistryCredentialProvider(registry=secret_provider_registry)
    dataset_provider = CachedDatasetMetadataProvider(
        RepositoryDatasetMetadataProvider(
            dataset_repository=dataset_store,
            dataset_column_repository=dataset_column_store,
            dataset_policy_repository=dataset_policy_store,
        )
    )
    connector_provider = CachedConnectorMetadataProvider(
        RepositoryConnectorMetadataProvider(connector_repository=connector_repository)
    )
    semantic_provider = (
        CachedSemanticModelMetadataProvider(
            RepositorySemanticModelMetadataProvider(
                semantic_model_repository=semantic_model_repository
            )
        )
        if semantic_model_repository is not None
        else None
    )
    semantic_vector_index_provider = (
        RepositorySemanticVectorIndexMetadataProvider(
            semantic_vector_index_repository=semantic_vector_index_repository
        )
        if semantic_vector_index_repository is not None
        else None
    )
    sync_state_provider = (
        RepositorySyncStateProvider(
            connector_sync_state_repository=connector_sync_state_store
        )
        if connector_sync_state_store is not None
        else None
    )
    federated_query_tool = FederatedQueryTool(
        connector_provider=connector_provider,
        credential_provider=credential_provider,
        secret_provider_registry=secret_provider_registry,
    )
    semantic_query_service = (
        SemanticQueryExecutionService(
            dataset_repository=dataset_store,
            dataset_provider=dataset_provider,
            semantic_model_provider=semantic_provider,
            federated_query_tool=federated_query_tool,
            logger=logger or logging.getLogger("langbridge.runtime.semantic"),
        )
        if semantic_model_repository is not None
        else None
    )
    semantic_vector_search_service = (
        SemanticVectorSearchService(
            dataset_repository=dataset_store,
            dataset_provider=dataset_provider,
            semantic_model_provider=semantic_provider,
            semantic_vector_index_store=runtime_semantic_vector_index_store,
            connector_provider=connector_provider,
            credential_provider=credential_provider,
            federated_query_tool=federated_query_tool,
            logger=logger or logging.getLogger("langbridge.runtime.semantic.vector"),
        )
        if (
            semantic_model_repository is not None
            and runtime_semantic_vector_index_store is not None
        )
        else None
    )
    dataset_query_service = DatasetQueryService(
        dataset_repository=dataset_store,
        dataset_column_repository=dataset_column_store,
        dataset_policy_repository=dataset_policy_store,
        dataset_revision_repository=dataset_revision_store,
        lineage_edge_repository=lineage_edge_store,
        federated_query_tool=federated_query_tool,
        dataset_provider=dataset_provider,
    )
    sql_query_service = SqlQueryService(
        sql_job_result_artifact_store=(
            RepositorySqlJobArtifactStore(repository=sql_job_result_artifact_repository)
            if sql_job_result_artifact_repository is not None
            else None
        ),
        connector_provider=connector_provider,
        dataset_repository=dataset_store,
        dataset_provider=dataset_provider,
        credential_provider=credential_provider,
        secret_provider_registry=secret_provider_registry,
        federated_query_tool=federated_query_tool,
    )
    dataset_sync_service = (
        ConnectorSyncRuntime(
            connector_sync_state_repository=connector_sync_state_store,
            dataset_repository=dataset_store,
            dataset_column_repository=dataset_column_store,
            dataset_policy_repository=dataset_policy_store,
            dataset_revision_repository=dataset_revision_store,
            lineage_edge_repository=lineage_edge_store,
        )
        if connector_sync_state_store is not None
        else None
    )
    agent_execution_service = (
        AgentExecutionService(
            agent_definition_repository=agent_definition_store,
            llm_repository=llm_connection_store,
            semantic_model_store=runtime_semantic_model_store,
            dataset_repository=dataset_store,
            dataset_column_repository=dataset_column_store,
            thread_repository=thread_store,
            thread_message_repository=thread_message_store,
            memory_repository=runtime_memory_store,
            federated_query_tool=federated_query_tool,
            semantic_vector_search_service=semantic_vector_search_service,
        )
        if (
            agent_definition_store is not None
            and llm_connection_store is not None
            and dataset_column_store is not None
            and thread_store is not None
            and thread_message_store is not None
            and runtime_memory_store is not None
            and runtime_semantic_model_store is not None
        )
        else None
    )
    return RuntimeHost(
        context=context,
        providers=RuntimeProviders(
            dataset_metadata=dataset_provider,
            connector_metadata=connector_provider,
            semantic_models=semantic_provider,
            semantic_vector_indexes=semantic_vector_index_provider,
            sync_state=sync_state_provider,
            credentials=credential_provider,
        ),
        services=RuntimeServices(
            dataset_query=dataset_query_service,
            federated_query_tool=federated_query_tool,
            semantic_query=semantic_query_service,
            semantic_vector_search=semantic_vector_search_service,
            sql_query=sql_query_service,
            dataset_sync=dataset_sync_service,
            agent_execution=agent_execution_service,
        ),
    )


__all__ = [
    "build_local_runtime",
    "build_configured_local_runtime",
]
