from __future__ import annotations

import logging

from langbridge.packages.common.langbridge_common.interfaces.semantic_models import (
    ISemanticModelStore,
)
from langbridge.packages.common.langbridge_common.repositories.agent_repository import (
    AgentRepository,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
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
from langbridge.packages.common.langbridge_common.repositories.lineage_repository import (
    LineageEdgeRepository,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import (
    SemanticModelRepository,
)
from langbridge.packages.common.langbridge_common.repositories.llm_connection_repository import (
    LLMConnectionRepository,
)
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlJobResultArtifactRepository,
)
from langbridge.packages.common.langbridge_common.repositories.thread_message_repository import (
    ThreadMessageRepository,
)
from langbridge.packages.common.langbridge_common.repositories.thread_repository import (
    ThreadRepository,
)
from langbridge.packages.runtime.context import RuntimeContext
from langbridge.packages.runtime.execution import FederatedQueryTool
from langbridge.packages.runtime.providers import (
    CachedConnectorMetadataProvider,
    CachedDatasetMetadataProvider,
    CachedSemanticModelMetadataProvider,
    ControlPlaneApiClient,
    ControlPlaneApiConnectorProvider,
    ControlPlaneApiDatasetProvider,
    ControlPlaneApiSemanticModelProvider,
    ControlPlaneApiSyncStateProvider,
    RepositoryConnectorMetadataProvider,
    RepositoryDatasetMetadataProvider,
    RepositorySemanticModelMetadataProvider,
    RepositorySyncStateProvider,
    SecretRegistryCredentialProvider,
)
from langbridge.packages.runtime.security import SecretProviderRegistry
from langbridge.packages.runtime.services.agent_execution_service import (
    AgentExecutionService,
)
from langbridge.packages.runtime.services.dataset_sync_service import ConnectorSyncRuntime
from langbridge.packages.runtime.services.runtime_host import (
    RuntimeHost,
    RuntimeProviders,
    RuntimeServices,
)
from langbridge.packages.runtime.services.semantic_query_execution_service import (
    SemanticQueryExecutionService,
)
from langbridge.packages.runtime.services.dataset_query_service import DatasetQueryService
from langbridge.packages.runtime.services.sql_query_service import SqlQueryService


def build_local_runtime(
    *,
    context: RuntimeContext,
    dataset_repository: DatasetRepository,
    dataset_column_repository: DatasetColumnRepository | None,
    dataset_policy_repository: DatasetPolicyRepository | None,
    connector_repository: ConnectorRepository,
    semantic_model_repository: SemanticModelRepository | None,
    connector_sync_state_repository: ConnectorSyncStateRepository | None = None,
    dataset_revision_repository: DatasetRevisionRepository | None = None,
    lineage_edge_repository: LineageEdgeRepository | None = None,
    sql_job_result_artifact_repository: SqlJobResultArtifactRepository | None = None,
    agent_definition_repository: AgentRepository | None = None,
    llm_repository: LLMConnectionRepository | None = None,
    thread_repository: ThreadRepository | None = None,
    thread_message_repository: ThreadMessageRepository | None = None,
    memory_repository: ConversationMemoryRepository | None = None,
    semantic_model_store: ISemanticModelStore | None = None,
    secret_provider_registry: SecretProviderRegistry | None = None,
    logger: logging.Logger | None = None,
) -> RuntimeHost:
    credential_provider = SecretRegistryCredentialProvider(registry=secret_provider_registry)
    dataset_provider = CachedDatasetMetadataProvider(
        RepositoryDatasetMetadataProvider(
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            dataset_policy_repository=dataset_policy_repository,
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
    sync_state_provider = (
        RepositorySyncStateProvider(
            connector_sync_state_repository=connector_sync_state_repository
        )
        if connector_sync_state_repository is not None
        else None
    )
    federated_query_tool = FederatedQueryTool(
        connector_provider=connector_provider,
        credential_provider=credential_provider,
        secret_provider_registry=secret_provider_registry,
    )
    semantic_query_service = (
        SemanticQueryExecutionService(
            semantic_model_repository=semantic_model_repository,
            dataset_repository=dataset_repository,
            dataset_provider=dataset_provider,
            semantic_model_provider=semantic_provider,
            federated_query_tool=federated_query_tool,
            logger=logger or logging.getLogger("langbridge.runtime.semantic"),
        )
        if semantic_model_repository is not None
        else None
    )
    dataset_query_service = DatasetQueryService(
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        lineage_edge_repository=lineage_edge_repository,
        federated_query_tool=federated_query_tool,
        dataset_provider=dataset_provider,
    )
    sql_query_service = SqlQueryService(
        sql_job_result_artifact_repository=sql_job_result_artifact_repository,
        connector_repository=connector_repository,
        connector_provider=connector_provider,
        dataset_repository=dataset_repository,
        dataset_provider=dataset_provider,
        credential_provider=credential_provider,
        secret_provider_registry=secret_provider_registry,
        federated_query_tool=federated_query_tool,
    )
    dataset_sync_service = (
        ConnectorSyncRuntime(
            connector_sync_state_repository=connector_sync_state_repository,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            dataset_policy_repository=dataset_policy_repository,
            dataset_revision_repository=dataset_revision_repository,
            lineage_edge_repository=lineage_edge_repository,
        )
        if connector_sync_state_repository is not None
        else None
    )
    agent_execution_service = (
        AgentExecutionService(
            agent_definition_repository=agent_definition_repository,
            llm_repository=llm_repository,
            semantic_model_store=semantic_model_store,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            thread_repository=thread_repository,
            thread_message_repository=thread_message_repository,
            memory_repository=memory_repository,
            federated_query_tool=federated_query_tool,
            logger=logger or logging.getLogger("langbridge.runtime.agent"),
        )
        if (
            agent_definition_repository is not None
            and llm_repository is not None
            and dataset_column_repository is not None
            and thread_repository is not None
            and thread_message_repository is not None
            and memory_repository is not None
            and semantic_model_store is not None
        )
        else None
    )
    return RuntimeHost(
        context=context,
        providers=RuntimeProviders(
            dataset_metadata=dataset_provider,
            connector_metadata=connector_provider,
            semantic_models=semantic_provider,
            sync_state=sync_state_provider,
            credentials=credential_provider,
        ),
        services=RuntimeServices(
            dataset_query=dataset_query_service,
            federated_query_tool=federated_query_tool,
            semantic_query=semantic_query_service,
            sql_query=sql_query_service,
            dataset_sync=dataset_sync_service,
            agent_execution=agent_execution_service,
        ),
    )


def build_hosted_runtime(
    *,
    context: RuntimeContext,
    control_plane_base_url: str,
    service_token: str,
    secret_provider_registry: SecretProviderRegistry | None = None,
    logger: logging.Logger | None = None,
) -> RuntimeHost:
    client = ControlPlaneApiClient(
        base_url=control_plane_base_url,
        service_token=service_token,
    )
    credential_provider = SecretRegistryCredentialProvider(registry=secret_provider_registry)
    dataset_provider = CachedDatasetMetadataProvider(
        ControlPlaneApiDatasetProvider(client=client)
    )
    connector_provider = CachedConnectorMetadataProvider(
        ControlPlaneApiConnectorProvider(client=client)
    )
    semantic_provider = CachedSemanticModelMetadataProvider(
        ControlPlaneApiSemanticModelProvider(client=client)
    )
    sync_state_provider = ControlPlaneApiSyncStateProvider(client=client)
    federated_query_tool = FederatedQueryTool(
        connector_provider=connector_provider,
        credential_provider=credential_provider,
        secret_provider_registry=secret_provider_registry,
    )
    semantic_query_service = SemanticQueryExecutionService(
        semantic_model_repository=None,
        dataset_repository=None,
        dataset_provider=dataset_provider,
        semantic_model_provider=semantic_provider,
        federated_query_tool=federated_query_tool,
        logger=logger or logging.getLogger("langbridge.runtime.semantic"),
    )
    dataset_query_service = DatasetQueryService(
        dataset_repository=None,
        dataset_column_repository=None,
        dataset_policy_repository=None,
        federated_query_tool=federated_query_tool,
        dataset_provider=dataset_provider,
    )
    sql_query_service = SqlQueryService(
        sql_job_result_artifact_repository=None,
        connector_repository=None,
        connector_provider=connector_provider,
        dataset_repository=None,
        dataset_provider=dataset_provider,
        credential_provider=credential_provider,
        secret_provider_registry=secret_provider_registry,
        federated_query_tool=federated_query_tool,
    )
    return RuntimeHost(
        context=context,
        providers=RuntimeProviders(
            dataset_metadata=dataset_provider,
            connector_metadata=connector_provider,
            semantic_models=semantic_provider,
            sync_state=sync_state_provider,
            credentials=credential_provider,
        ),
        services=RuntimeServices(
            federated_query_tool=federated_query_tool,
            dataset_query=dataset_query_service,
            semantic_query=semantic_query_service,
            sql_query=sql_query_service,
        ),
    )
