from __future__ import annotations

from typing import Any

from dependency_injector import containers, providers

from langbridge.apps.api.langbridge_api.auth.register import create_oauth_client
from langbridge.apps.api.langbridge_api.repositories.token_repository import UserPATRepository
from langbridge.apps.api.langbridge_api.services.jobs.agent_job_request_service import AgentJobRequestService
from langbridge.apps.api.langbridge_api.services.jobs.copilot_dashboard_job_request_service import (
    CopilotDashboardJobRequestService,
)
from langbridge.apps.api.langbridge_api.services.jobs.job_service import JobService
from langbridge.apps.api.langbridge_api.services.jobs.semantic_query_job_request_service import (
    SemanticQueryJobRequestService,
)
from langbridge.apps.api.langbridge_api.services.jobs.sql_job_request_service import (
    SqlJobRequestService,
)
from langbridge.packages.common.langbridge_common.config import Settings, settings
from langbridge.packages.common.langbridge_common.db import (
    create_async_engine_for_url,
    create_async_session_factory,
    create_engine_for_url,
    create_session_factory,
    session_scope,
)
from langbridge.packages.common.langbridge_common.db.session_context import get_session
from langbridge.packages.common.langbridge_common.repositories.agent_repository import AgentRepository
from langbridge.packages.common.langbridge_common.repositories.connector_repository import ConnectorRepository
from langbridge.packages.common.langbridge_common.repositories.conversation_memory_repository import (
    ConversationMemoryRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dashboard_repository import DashboardRepository
from langbridge.packages.common.langbridge_common.repositories.edge_task_repository import (
    EdgeResultReceiptRepository,
    EdgeTaskRepository,
)
from langbridge.packages.common.langbridge_common.repositories.environment_repository import OrganizationEnvironmentSettingRepository
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlJobRepository,
    SqlJobResultArtifactRepository,
    SqlSavedQueryRepository,
    SqlWorkspacePolicyRepository,
)
from langbridge.packages.common.langbridge_common.repositories.llm_connection_repository import LLMConnectionRepository
from langbridge.packages.common.langbridge_common.repositories.organization_repository import (
    OrganizationInviteRepository,
    OrganizationRepository,
    ProjectInviteRepository,
    ProjectRepository,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import SemanticModelRepository
from langbridge.packages.common.langbridge_common.repositories.semantic_search_repository import SemanticVectorStoreEntryRepository
from langbridge.packages.common.langbridge_common.repositories.thread_message_repository import ThreadMessageRepository
from langbridge.packages.common.langbridge_common.repositories.thread_repository import ThreadRepository
from langbridge.packages.common.langbridge_common.repositories.tool_call_repository import ToolCallRepository
from langbridge.packages.common.langbridge_common.repositories.runtime_repository import (
    RuntimeInstanceRepository,
    RuntimeRegistrationTokenRepository,
)
from langbridge.packages.common.langbridge_common.repositories.user_repository import OAuthAccountRepository, UserRepository
from langbridge.packages.common.langbridge_common.repositories.message_repository import MessageRepository
from langbridge.packages.semantic.langbridge_semantic.semantic_model_builder import SemanticModelBuilder
from langbridge.apps.api.langbridge_api.services.agent_service import AgentService
from langbridge.apps.api.langbridge_api.services.auth.auth_service import AuthService
from langbridge.apps.api.langbridge_api.services.auth.token_service import TokenService
from langbridge.apps.api.langbridge_api.services.connector_schema_service import ConnectorSchemaService
from langbridge.apps.api.langbridge_api.services.connector_service import ConnectorService
from langbridge.apps.api.langbridge_api.services.dashboard_service import DashboardService
from langbridge.apps.api.langbridge_api.services.environment_service import EnvironmentService
from langbridge.apps.api.langbridge_api.services.internal_api_client import InternalApiClient
from langbridge.apps.api.langbridge_api.services.execution_routing_service import (
    ExecutionRoutingService,
)
from langbridge.apps.api.langbridge_api.services.edge_task_gateway_service import (
    EdgeTaskGatewayService,
)
from langbridge.apps.api.langbridge_api.services.organization_service import OrganizationService
from langbridge.apps.api.langbridge_api.services.orchestrator_service import OrchestratorService
from langbridge.apps.api.langbridge_api.services.message.message_serivce import MessageService
from langbridge.apps.api.langbridge_api.services.message.job_event_consumer import (
    JobEventConsumer,
)
from langbridge.apps.api.langbridge_api.services.runtime_auth_service import RuntimeAuthService
from langbridge.apps.api.langbridge_api.services.runtime_registry_service import (
    RuntimeRegistryService,
)
from langbridge.apps.api.langbridge_api.services.request_context_provider import RequestContextProvider
from langbridge.apps.api.langbridge_api.services.semantic import (
    SemanticModelService,
    SemanticQueryService,
    SemanticSearchService,
)
from langbridge.apps.api.langbridge_api.services.task_dispatch_service import (
    TaskDispatchService,
)
from langbridge.apps.api.langbridge_api.services.thread_service import ThreadService
from langbridge.apps.api.langbridge_api.services.sql_service import SqlService
from langbridge.apps.api.langbridge_api.services.storage import create_dashboard_snapshot_storage
from langbridge.packages.messaging.langbridge_messaging.broker.redis import RedisBroker
from langbridge.packages.messaging.langbridge_messaging.flusher.flusher import MessageFlusher
from langbridge.apps.api.langbridge_api.request_context import get_request_context


class Container(containers.DeclarativeContainer):
    """Application dependency injection container."""

    wiring_config = containers.WiringConfiguration()

    config = providers.Configuration()

    engine = providers.Singleton(
        create_engine_for_url,
        database_url=config.database.url,
        echo=config.database.echo,
        pool_size=config.database.pool_size,
        max_overflow=config.database.max_overflow,
        pool_timeout=config.database.pool_timeout,
    )

    async_engine = providers.Singleton(
        create_async_engine_for_url,
        database_url=config.database.async_url,
        echo=config.database.echo,
        pool_size=config.database.pool_size,
        max_overflow=config.database.max_overflow,
        pool_timeout=config.database.pool_timeout,
    )

    oauth = providers.Singleton(create_oauth_client)

    internal_api_client = providers.Factory(
        InternalApiClient,
        base_url=settings.BACKEND_URL,
        service_token=settings.SERVICE_USER_SECRET,
    )
    request_context = providers.Factory(get_request_context)
    request_context_provider = providers.Factory(
        RequestContextProvider,
        request_context=request_context,
    )

    session_factory = providers.Singleton(create_session_factory, engine=engine)
    async_session_factory = providers.Singleton(
        create_async_session_factory,
        engine=async_engine,
    )

    session = providers.Resource(session_scope, session_factory=session_factory)
    async_session = providers.Factory(get_session)

    user_repository = providers.Factory(UserRepository, session=async_session)

    oauth_account_repository = providers.Factory(OAuthAccountRepository, session=async_session)
    organization_repository = providers.Factory(OrganizationRepository, session=async_session)
    project_repository = providers.Factory(ProjectRepository, session=async_session)
    organization_invite_repository = providers.Factory(OrganizationInviteRepository, session=async_session)
    project_invite_repository = providers.Factory(ProjectInviteRepository, session=async_session)
    connector_repository = providers.Factory(ConnectorRepository, session=async_session)
    dashboard_repository = providers.Factory(DashboardRepository, session=async_session)
    environment_repository = providers.Factory(OrganizationEnvironmentSettingRepository, session=async_session)
    llm_connection_repository = providers.Factory(LLMConnectionRepository, session=async_session)
    semantic_model_repository = providers.Factory(SemanticModelRepository, session=async_session)
    thread_repository = providers.Factory(ThreadRepository, session=async_session)
    thread_message_repository = providers.Factory(ThreadMessageRepository, session=async_session)
    memory_repository = providers.Factory(ConversationMemoryRepository, session=async_session)
    tool_call_repository = providers.Factory(ToolCallRepository, session=async_session)
    agent_definition_repository = providers.Factory(AgentRepository, session=async_session)
    semantic_vector_store_repository = providers.Factory(SemanticVectorStoreEntryRepository, session=async_session)
    message_repository = providers.Factory(MessageRepository, session=async_session)
    job_repository = providers.Factory(JobRepository, session=async_session)
    sql_job_repository = providers.Factory(SqlJobRepository, session=async_session)
    sql_job_result_artifact_repository = providers.Factory(
        SqlJobResultArtifactRepository,
        session=async_session,
    )
    sql_saved_query_repository = providers.Factory(SqlSavedQueryRepository, session=async_session)
    sql_workspace_policy_repository = providers.Factory(
        SqlWorkspacePolicyRepository,
        session=async_session,
    )
    user_pat_repository = providers.Factory(UserPATRepository, session=async_session)
    runtime_repository = providers.Factory(RuntimeInstanceRepository, session=async_session)
    runtime_registration_token_repository = providers.Factory(
        RuntimeRegistrationTokenRepository,
        session=async_session,
    )
    edge_task_repository = providers.Factory(EdgeTaskRepository, session=async_session)
    edge_result_receipt_repository = providers.Factory(
        EdgeResultReceiptRepository,
        session=async_session,
    )
    message_broker = providers.Singleton(RedisBroker)
    api_message_broker = providers.Singleton(
        RedisBroker,
        stream=settings.REDIS_API_STREAM,
        group=settings.REDIS_API_CONSUMER_GROUP,
    )

    environment_service = providers.Factory(
        EnvironmentService,
        repository=environment_repository,
    )

    organization_service = providers.Factory(
        OrganizationService,
        organization_repository=organization_repository,
        project_repository=project_repository,
        organization_invite_repository=organization_invite_repository,
        project_invite_repository=project_invite_repository,
        user_repository=user_repository,
        environment_service=environment_service,
    )

    auth_service = providers.Factory(
        AuthService,
        user_repository=user_repository,
        oauth_account_repository=oauth_account_repository,
        oauth=oauth,
        organization_service=organization_service,
    )

    token_service = providers.Factory(
        TokenService,
        auth_service=auth_service,
        user_pat_repository=user_pat_repository,
    )

    connector_service = providers.Factory(
        ConnectorService,
        connector_repository=connector_repository,
        organization_repository=organization_repository,
        project_repository=project_repository
    )

    connector_schema_service = providers.Factory(
        ConnectorSchemaService,
        connector_repository=connector_repository
    )

    agent_service = providers.Factory(
        AgentService,
        agent_definition_repository=agent_definition_repository,
        llm_repository=llm_connection_repository,
        organization_repository=organization_repository,
        project_repository=project_repository
    )

    semantic_model_builder = providers.Factory(
        SemanticModelBuilder,
        connector_service=connector_service,
    )

    semantic_search_service = providers.Factory(
        SemanticSearchService,
        vector_store_entry_repository=semantic_vector_store_repository,
    )

    runtime_auth_service = providers.Singleton(RuntimeAuthService)
    runtime_registry_service = providers.Factory(
        RuntimeRegistryService,
        runtime_repository=runtime_repository,
        runtime_registration_token_repository=runtime_registration_token_repository,
        runtime_auth_service=runtime_auth_service,
    )
    edge_task_gateway_service = providers.Factory(
        EdgeTaskGatewayService,
        edge_task_repository=edge_task_repository,
        edge_result_receipt_repository=edge_result_receipt_repository,
    )
    execution_routing_service = providers.Factory(
        ExecutionRoutingService,
        environment_service=environment_service,
    )
    
    message_service = providers.Factory(
        MessageService,
        message_repository=message_repository,
        request_context_provider=request_context_provider,
    )

    task_dispatch_service = providers.Factory(
        TaskDispatchService,
        execution_routing_service=execution_routing_service,
        message_service=message_service,
        runtime_registry_service=runtime_registry_service,
        edge_task_gateway_service=edge_task_gateway_service,
        request_context_provider=request_context_provider,
    )

    semantic_model_service = providers.Factory(
        SemanticModelService,
        repository=semantic_model_repository,
        builder=semantic_model_builder,
        organization_repository=organization_repository,
        project_repository=project_repository,
        connector_service=connector_service,
        agent_service=agent_service,
        semantic_search_service=semantic_search_service,
        emvironment_service=environment_service
    )

    semantic_query_service = providers.Factory(
        SemanticQueryService,
        semantic_model_service=semantic_model_service,
        connector_service=connector_service,
    )

    dashboard_snapshot_storage = providers.Singleton(create_dashboard_snapshot_storage)

    dashboard_service = providers.Factory(
        DashboardService,
        repository=dashboard_repository,
        organization_repository=organization_repository,
        project_repository=project_repository,
        semantic_model_service=semantic_model_service,
        snapshot_storage=dashboard_snapshot_storage,
    )
    
    agent_job_request_service = providers.Factory(
        AgentJobRequestService,
        job_repository=job_repository,
        agent_repository=agent_definition_repository,
        task_dispatch_service=task_dispatch_service,
    )
    semantic_query_job_request_service = providers.Factory(
        SemanticQueryJobRequestService,
        job_repository=job_repository,
        task_dispatch_service=task_dispatch_service,
        semantic_model_repository=semantic_model_repository,
        connector_repository=connector_repository,
    )
    copilot_dashboard_job_request_service = providers.Factory(
        CopilotDashboardJobRequestService,
        job_repository=job_repository,
        agent_definition_repository=agent_definition_repository,
        semantic_model_repository=semantic_model_repository,
        task_dispatch_service=task_dispatch_service,
    )
    sql_job_request_service = providers.Factory(
        SqlJobRequestService,
        task_dispatch_service=task_dispatch_service,
    )
    job_service = providers.Factory(
        JobService,
        job_repository=job_repository,
    )
    sql_service = providers.Factory(
        SqlService,
        sql_job_repository=sql_job_repository,
        sql_job_result_artifact_repository=sql_job_result_artifact_repository,
        sql_saved_query_repository=sql_saved_query_repository,
        sql_workspace_policy_repository=sql_workspace_policy_repository,
        connector_repository=connector_repository,
        organization_repository=organization_repository,
        user_repository=user_repository,
        sql_job_request_service=sql_job_request_service,
        request_context_provider=request_context_provider,
    )

    thread_service = providers.Factory(
        ThreadService,
        thread_repository=thread_repository,
        thread_message_repository=thread_message_repository,
        tool_call_repository=tool_call_repository,
        project_repository=project_repository,
        organization_service=organization_service,
        agent_job_request=agent_job_request_service,
    )

    orchestrator_service = providers.Factory(
        OrchestratorService,
        organization_service=organization_service,
        semantic_model_service=semantic_model_service,
        connector_service=connector_service,
        agent_service=agent_service,
        thread_service=thread_service,
        message_service=message_service,
    )

    message_flusher = providers.Factory(
        MessageFlusher,
        message_repository=message_repository,
        message_bus=message_broker,
    )

    job_event_consumer = providers.Singleton(
        JobEventConsumer,
        broker_client=api_message_broker,
        async_session_factory=async_session_factory,
    )


def _build_config(settings_obj: Settings) -> dict[str, Any]:
    return {
        "database": {
            "url": settings_obj.SQLALCHEMY_DATABASE_URI,
            "async_url": settings_obj.SQLALCHEMY_ASYNC_DATABASE_URI,
            "echo": settings_obj.IS_LOCAL,
            "pool_size": settings_obj.SQLALCHEMY_POOL_SIZE,
            "max_overflow": settings_obj.SQLALCHEMY_MAX_OVERFLOW,
            "pool_timeout": settings_obj.SQLALCHEMY_POOL_TIMEOUT,
        },
    }


def build_container(settings_obj: Settings = settings) -> Container:
    """Build a Container with settings bound to configuration providers."""
    container = Container()
    container.config.from_dict(_build_config(settings_obj))
    return container
