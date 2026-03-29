
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from langbridge.runtime.config.models import ResolvedLocalRuntimeMetadataStoreConfig
from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.models import (
    ConnectorMetadata,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    LLMConnectionSecret,
)
from langbridge.runtime.persistence.uow import (
    _ConfiguredRuntimePersistenceController,
    _RuntimeSessionRepositoryProxy,
)
from langbridge.runtime.security import SecretProviderRegistry


def _resolve_llm_connection_secret(
    *,
    connection: Any,
    secret_provider_registry: SecretProviderRegistry,
) -> LLMConnectionSecret:
    if connection.api_key_secret is None:
        return connection.connection
    return connection.connection.model_copy(
        update={"api_key": secret_provider_registry.resolve(connection.api_key_secret)}
    )


def _reconcile_sql_runtime_metadata(
    *,
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
    context: RuntimeContext,
    connectors: dict[str, ConnectorMetadata],
    semantic_models: dict[str, Any],
    llm_connections: dict[str, Any],
    agents: dict[str, Any],
    dataset_repository_rows: dict[uuid.UUID, DatasetMetadata],
    dataset_columns: dict[uuid.UUID, list[DatasetColumnMetadata]],
    dataset_policies: dict[uuid.UUID, DatasetPolicyMetadata],
    secret_provider_registry: SecretProviderRegistry,
) -> None:
    from sqlalchemy import delete

    from langbridge.runtime.persistence.db import agent as _db_agent  # noqa: F401
    from langbridge.runtime.persistence.db import auth as _db_auth  # noqa: F401
    from langbridge.runtime.persistence.db import connector as _db_connector  # noqa: F401
    from langbridge.runtime.persistence.db import connector_sync as _db_connector_sync  # noqa: F401
    from langbridge.runtime.persistence.db import dataset as _db_dataset  # noqa: F401
    from langbridge.runtime.persistence.db import job as _db_job  # noqa: F401
    from langbridge.runtime.persistence.db import lineage as _db_lineage  # noqa: F401
    from langbridge.runtime.persistence.db import runtime as _db_runtime  # noqa: F401
    from langbridge.runtime.persistence.db import semantic as _db_semantic  # noqa: F401
    from langbridge.runtime.persistence.db import sql as _db_sql  # noqa: F401
    from langbridge.runtime.persistence.db import threads as _db_threads  # noqa: F401
    from langbridge.runtime.persistence.db import workspace as _db_workspace  # noqa: F401
    from langbridge.runtime.persistence.db import create_engine_for_url, create_session_factory
    from langbridge.runtime.persistence.db.dataset import DatasetColumnRecord, DatasetPolicyRecord
    from langbridge.runtime.persistence.db.semantic import SemanticModelEntry
    from langbridge.runtime.persistence.db.workspace import Workspace
    from langbridge.runtime.persistence.mappers.agents import to_agent_definition_record
    from langbridge.runtime.persistence.mappers.connectors import to_connector_record
    from langbridge.runtime.persistence.mappers.datasets import (
        to_dataset_column_record,
        to_dataset_policy_record,
        to_dataset_record,
    )
    from langbridge.runtime.persistence.mappers.llm_connections import to_llm_connection_record

    sync_engine = create_engine_for_url(
        metadata_store.sync_url or "",
        metadata_store.echo,
        pool_size=metadata_store.pool_size,
        max_overflow=metadata_store.max_overflow,
        pool_timeout=metadata_store.pool_timeout,
    )
    session_factory = create_session_factory(sync_engine)
    session = session_factory()
    try:
        session.merge(
            Workspace(
                id=context.workspace_id,
                name=f"local-runtime-{context.workspace_id}",
            )
        )
        for connector in connectors.values():
            session.merge(to_connector_record(connector))
        for connection in llm_connections.values():
            session.merge(
                to_llm_connection_record(
                    _resolve_llm_connection_secret(
                        connection=connection,
                        secret_provider_registry=secret_provider_registry,
                    )
                )
            )
        for agent in agents.values():
            session.merge(to_agent_definition_record(agent.agent_definition))
        for semantic_model in semantic_models.values():
            session.merge(
                SemanticModelEntry(
                    id=semantic_model.id,
                    connector_id=None,
                    workspace_id=context.workspace_id,
                    name=semantic_model.name,
                    description=(
                        semantic_model.semantic_model.description
                        if semantic_model.semantic_model is not None
                        else semantic_model.content_json.get("description")
                    ),
                    content_yaml=semantic_model.content_yaml,
                    content_json=json.dumps(semantic_model.content_json),
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                    management_mode="config_managed",
                    lifecycle_state="active",
                )
            )
        configured_dataset_ids = list(dataset_repository_rows.keys())
        for dataset in dataset_repository_rows.values():
            session.merge(to_dataset_record(dataset))
        if configured_dataset_ids:
            session.execute(delete(DatasetColumnRecord).where(DatasetColumnRecord.dataset_id.in_(configured_dataset_ids)))
            session.execute(delete(DatasetPolicyRecord).where(DatasetPolicyRecord.dataset_id.in_(configured_dataset_ids)))
        for columns in dataset_columns.values():
            for column in columns:
                session.merge(to_dataset_column_record(column))
        for policy in dataset_policies.values():
            session.merge(to_dataset_policy_record(policy))
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        sync_engine.dispose()


def build_sql_runtime_resources(
    *,
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
    context: RuntimeContext,
    connectors: dict[str, ConnectorMetadata],
    semantic_models: dict[str, Any],
    llm_connections: dict[str, Any],
    agents: dict[str, Any],
    dataset_repository_rows: dict[uuid.UUID, DatasetMetadata],
    dataset_columns: dict[uuid.UUID, list[DatasetColumnMetadata]],
    dataset_policies: dict[uuid.UUID, DatasetPolicyMetadata],
    secret_provider_registry: SecretProviderRegistry,
) -> tuple[Any, Any, Any, Any, Any, Any, Any, Any, Any, _ConfiguredRuntimePersistenceController]:
    from langbridge.runtime.bootstrap.runtime_factory import build_local_runtime
    from langbridge.runtime.persistence import (
        RepositoryConversationMemoryStore,
        RepositoryConnectorSyncStateStore,
        RepositoryDatasetCatalogStore,
        RepositoryDatasetColumnStore,
        RepositoryDatasetPolicyStore,
        RepositoryThreadMessageStore,
        RepositoryThreadStore,
    )
    from langbridge.runtime.persistence.db import create_async_engine_for_url, create_async_session_factory

    if metadata_store.path is not None:
        metadata_store.path.parent.mkdir(parents=True, exist_ok=True)

    _reconcile_sql_runtime_metadata(
        metadata_store=metadata_store,
        context=context,
        connectors=connectors,
        semantic_models=semantic_models,
        llm_connections=llm_connections,
        agents=agents,
        dataset_repository_rows=dataset_repository_rows,
        dataset_columns=dataset_columns,
        dataset_policies=dataset_policies,
        secret_provider_registry=secret_provider_registry,
    )

    async_engine = create_async_engine_for_url(
        metadata_store.async_url or "",
        metadata_store.echo,
        pool_size=metadata_store.pool_size,
        max_overflow=metadata_store.max_overflow,
        pool_timeout=metadata_store.pool_timeout,
    )
    async_session_factory = create_async_session_factory(async_engine)
    controller = _ConfiguredRuntimePersistenceController(
        metadata_store=metadata_store,
        async_engine=async_engine,
        async_session_factory=async_session_factory,
    )
    raw_dataset_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="dataset_repository",
        sync_methods={"add"},
        write_methods={"save", "delete"},
    )
    raw_dataset_column_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="dataset_column_repository",
        sync_methods={"add"},
        write_methods={"delete_for_dataset"},
    )
    raw_dataset_policy_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="dataset_policy_repository",
        sync_methods={"add"},
        write_methods={"save"},
    )
    raw_connector_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="connector_repository",
    )
    raw_semantic_model_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="semantic_model_repository",
    )
    raw_semantic_vector_index_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="semantic_vector_index_repository",
        write_methods={"save", "delete_for_workspace", "delete"},
    )
    raw_connector_sync_state_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="connector_sync_state_repository",
        sync_methods={"add"},
        write_methods={"save"},
    )
    raw_dataset_revision_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="dataset_revision_repository",
        sync_methods={"add"},
    )
    raw_lineage_edge_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="lineage_edge_repository",
        sync_methods={"add"},
        write_methods={"delete_for_target", "delete_for_node"},
    )
    raw_agent_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="agent_repository",
    )
    raw_llm_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="llm_repository",
    )
    raw_thread_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="thread_repository",
        sync_methods={"add"},
        write_methods={"save", "delete"},
    )
    raw_thread_message_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="thread_message_repository",
        sync_methods={"add"},
        write_methods={"delete"},
    )
    raw_memory_repository = _RuntimeSessionRepositoryProxy(
        controller=controller,
        repository_attr="conversation_memory_repository",
        sync_methods={"add", "create_item"},
        write_methods={"touch_items"},
    )
    runtime_host = build_local_runtime(
        context=context,
        dataset_repository=raw_dataset_repository,
        dataset_column_repository=raw_dataset_column_repository,
        dataset_policy_repository=raw_dataset_policy_repository,
        connector_repository=raw_connector_repository,
        semantic_model_repository=raw_semantic_model_repository,
        semantic_vector_index_repository=raw_semantic_vector_index_repository,
        connector_sync_state_repository=raw_connector_sync_state_repository,
        dataset_revision_repository=raw_dataset_revision_repository,
        lineage_edge_repository=raw_lineage_edge_repository,
        agent_definition_repository=raw_agent_repository,
        llm_repository=raw_llm_repository,
        thread_repository=raw_thread_repository,
        thread_message_repository=raw_thread_message_repository,
        memory_repository=RepositoryConversationMemoryStore(repository=raw_memory_repository),
        secret_provider_registry=secret_provider_registry,
        cache_metadata=False,
    )
    return (
        runtime_host,
        RepositoryDatasetCatalogStore(repository=raw_dataset_repository),
        RepositoryDatasetColumnStore(repository=raw_dataset_column_repository),
        RepositoryDatasetPolicyStore(repository=raw_dataset_policy_repository),
        raw_dataset_revision_repository,
        raw_lineage_edge_repository,
        RepositoryConnectorSyncStateStore(repository=raw_connector_sync_state_repository),
        RepositoryThreadStore(repository=raw_thread_repository),
        RepositoryThreadMessageStore(repository=raw_thread_message_repository),
        controller,
    )


__all__ = [
    "build_sql_runtime_resources",
]
