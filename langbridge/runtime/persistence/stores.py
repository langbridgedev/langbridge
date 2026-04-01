import uuid

from langbridge.runtime.models import (
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    LLMConnectionSecret,
    LineageEdge,
    RuntimeAgentDefinition,
    RuntimeConversationMemoryItem,
    RuntimeThread,
    RuntimeThreadMessage,
    SemanticModelMetadata,
    SemanticVectorIndexMetadata,
    SqlJob,
    SqlJobResultArtifact,
)
from langbridge.runtime.persistence.mappers import (
    from_agent_definition_record,
    from_connector_sync_state_record,
    from_conversation_memory_record,
    from_dataset_column_record,
    from_dataset_policy_record,
    from_dataset_record,
    from_dataset_revision_record,
    from_lineage_edge_record,
    from_llm_connection_record,
    from_semantic_model_record,
    from_semantic_vector_index_record,
    from_sql_job_record,
    from_sql_job_result_artifact_record,
    from_thread_message_record,
    from_thread_record,
    to_connector_sync_state_record,
    to_dataset_column_record,
    to_dataset_policy_record,
    to_dataset_record,
    to_dataset_revision_record,
    to_lineage_edge_record,
    to_semantic_vector_index_record,
    to_sql_job_record,
    to_sql_job_result_artifact_record,
    to_thread_message_record,
    to_thread_record,
)
from langbridge.runtime.persistence.repositories.agent_repository import (
    AgentRepository,
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
from langbridge.runtime.persistence.repositories.semantic_search_repository import (
    SemanticVectorIndexRepository,
)
from langbridge.runtime.persistence.repositories.sql_repository import (
    SqlJobRepository,
    SqlJobResultArtifactRepository,
)
from langbridge.runtime.persistence.repositories.thread_message_repository import (
    ThreadMessageRepository,
)
from langbridge.runtime.persistence.repositories.thread_repository import (
    ThreadRepository,
)
from langbridge.runtime.ports import (
    AgentDefinitionStore,
    ConnectorSyncStateStore,
    ConversationMemoryStore,
    DatasetCatalogStore,
    DatasetColumnStore,
    DatasetPolicyStore,
    DatasetRevisionStore,
    LLMConnectionStore,
    LineageEdgeStore,
    SemanticModelStore,
    SemanticVectorIndexStore,
    SqlJobArtifactStore,
    SqlJobStore,
    ThreadMessageStore,
    ThreadStore,
)


def _find_tracked_record(session, record):
    sync_session = getattr(session, "sync_session", session)
    record_id = getattr(record, "id", None)
    if record_id is None:
        return None
    for candidate in list(getattr(sync_session, "new", [])):
        if type(candidate) is type(record) and getattr(candidate, "id", None) == record_id:
            return candidate
    for candidate in list(getattr(sync_session, "identity_map", {}).values()):
        if type(candidate) is type(record) and getattr(candidate, "id", None) == record_id:
            return candidate
    return None


def _copy_record_columns(target, source) -> None:
    table = getattr(source, "__table__", None)
    if table is None:
        return
    for column in table.columns:
        setattr(target, column.key, getattr(source, column.key))


class RepositoryAgentDefinitionStore(AgentDefinitionStore):
    def __init__(self, *, repository: AgentRepository) -> None:
        self._repository = repository

    async def get_by_id(self, id_: object) -> RuntimeAgentDefinition | None:
        return from_agent_definition_record(await self._repository.get_by_id(id_))


class RepositoryLLMConnectionStore(LLMConnectionStore):
    def __init__(self, *, repository: LLMConnectionRepository) -> None:
        self._repository = repository

    async def get_by_id(self, id_: object) -> LLMConnectionSecret | None:
        return from_llm_connection_record(await self._repository.get_by_id(id_))


class RepositoryThreadStore(ThreadStore):
    def __init__(self, *, repository: ThreadRepository) -> None:
        self._repository = repository

    def add(self, instance: RuntimeThread) -> RuntimeThread:
        record = self._repository.add(to_thread_record(instance))
        return from_thread_record(record) or instance

    async def save(self, instance: RuntimeThread) -> RuntimeThread:
        next_record = to_thread_record(instance)
        tracked = _find_tracked_record(self._repository._session, next_record)
        if tracked is not None:
            _copy_record_columns(tracked, next_record)
            return from_thread_record(tracked) or instance
        record = await self._repository.save(next_record)
        return from_thread_record(record) or instance

    async def get_by_id(self, id_: object) -> RuntimeThread | None:
        return from_thread_record(await self._repository.get_by_id(id_))

    async def delete(self, id_: object) -> None:
        record = await self._repository.get_by_id(id_)
        if record is None:
            return
        await self._repository.delete(record)

    async def list_for_actor(self, actor_id: uuid.UUID | None = None) -> list[RuntimeThread]:
        if actor_id is None:
            return []
        return [
            runtime_thread
            for item in await self._repository.list_for_actor(actor_id)
            if (runtime_thread := from_thread_record(item)) is not None
        ]

    async def list_for_workspace(self, workspace_id: uuid.UUID) -> list[RuntimeThread]:
        return [
            runtime_thread
            for item in await self._repository.list_for_workspace(workspace_id)
            if (runtime_thread := from_thread_record(item)) is not None
        ]

    async def flush(self) -> None:
        await self._repository.flush()


class RepositoryThreadMessageStore(ThreadMessageStore):
    def __init__(self, *, repository: ThreadMessageRepository) -> None:
        self._repository = repository

    def add(self, instance: RuntimeThreadMessage) -> RuntimeThreadMessage:
        record = self._repository.add(to_thread_message_record(instance))
        return from_thread_message_record(record) or instance

    async def list_for_thread(self, thread_id) -> list[RuntimeThreadMessage]:
        return [
            runtime_message
            for item in await self._repository.list_for_thread(thread_id)
            if (runtime_message := from_thread_message_record(item)) is not None
        ]

    async def delete_for_thread(self, thread_id) -> None:
        for item in await self._repository.list_for_thread(thread_id):
            await self._repository.delete(item)

    async def flush(self) -> None:
        await self._repository.flush()


class RepositorySemanticModelStore(SemanticModelStore):
    def __init__(self, *, repository: SemanticModelRepository) -> None:
        self._repository = repository

    async def get_by_id(self, model_id) -> SemanticModelMetadata | None:
        return from_semantic_model_record(await self._repository.get_by_id(model_id))

    async def get_by_ids(self, model_ids) -> list[SemanticModelMetadata]:
        return [
            runtime_model
            for item in await self._repository.get_by_ids(model_ids)
            if (runtime_model := from_semantic_model_record(item)) is not None
        ]


class RepositorySemanticVectorIndexStore(SemanticVectorIndexStore):
    def __init__(self, *, repository: SemanticVectorIndexRepository) -> None:
        self._repository = repository

    async def get_by_id(self, *, workspace_id, semantic_vector_index_id) -> SemanticVectorIndexMetadata | None:
        return from_semantic_vector_index_record(
            await self._repository.get_for_workspace(
                vector_index_id=semantic_vector_index_id,
                workspace_id=workspace_id,
            )
        )

    async def get_for_dimension(
        self,
        *,
        workspace_id,
        semantic_model_id,
        dataset_key,
        dimension_name,
    ) -> SemanticVectorIndexMetadata | None:
        return from_semantic_vector_index_record(
            await self._repository.get_for_dimension(
                workspace_id=workspace_id,
                semantic_model_id=semantic_model_id,
                dataset_key=dataset_key,
                dimension_name=dimension_name,
            )
        )

    async def list_for_workspace(
        self,
        *,
        workspace_id,
        semantic_model_id=None,
    ) -> list[SemanticVectorIndexMetadata]:
        return [
            runtime_index
            for item in await self._repository.list_for_workspace(
                workspace_id=workspace_id,
                semantic_model_id=semantic_model_id,
            )
            if (runtime_index := from_semantic_vector_index_record(item)) is not None
        ]

    async def save(self, instance: SemanticVectorIndexMetadata) -> SemanticVectorIndexMetadata:
        next_record = to_semantic_vector_index_record(instance)
        tracked = _find_tracked_record(self._repository._session, next_record)
        if tracked is not None:
            _copy_record_columns(tracked, next_record)
            return from_semantic_vector_index_record(tracked) or instance
        record = await self._repository.save(next_record)
        return from_semantic_vector_index_record(record) or instance

    async def delete(self, *, workspace_id, semantic_vector_index_id) -> None:
        await self._repository.delete_for_workspace(
            workspace_id=workspace_id,
            vector_index_id=semantic_vector_index_id,
        )


class RepositoryConversationMemoryStore(ConversationMemoryStore):
    def __init__(self, *, repository: ConversationMemoryRepository) -> None:
        self._repository = repository

    async def list_for_thread(
        self,
        thread_id,
        *,
        limit: int = 200,
    ) -> list[RuntimeConversationMemoryItem]:
        return [
            runtime_item
            for item in await self._repository.list_for_thread(thread_id, limit=limit)
            if (runtime_item := from_conversation_memory_record(item)) is not None
        ]

    def create_item(
        self,
        *,
        thread_id,
        category: str,
        content: str,
        metadata_json=None,
        actor_id=None,
    ) -> RuntimeConversationMemoryItem | None:
        return from_conversation_memory_record(
            self._repository.create_item(
                thread_id=thread_id,
                actor_id=actor_id,
                category=category,
                content=content,
                metadata_json=metadata_json,
            )
        )

    async def touch_items(self, item_ids) -> None:
        await self._repository.touch_items(item_ids)

    async def flush(self) -> None:
        await self._repository.flush()


class RepositorySqlJobStore(SqlJobStore):
    def __init__(self, *, repository: SqlJobRepository) -> None:
        self._repository = repository

    async def get_by_id_for_workspace(
        self,
        *,
        sql_job_id,
        workspace_id,
    ) -> SqlJob | None:
        return from_sql_job_record(
            await self._repository.get_by_id_for_workspace(
                sql_job_id=sql_job_id,
                workspace_id=workspace_id,
            )
        )

    async def save(self, instance: SqlJob) -> SqlJob:
        next_record = to_sql_job_record(instance)
        tracked = _find_tracked_record(self._repository._session, next_record)
        if tracked is not None:
            _copy_record_columns(tracked, next_record)
            return from_sql_job_record(tracked) or instance
        record = await self._repository.save(next_record)
        return from_sql_job_record(record) or instance


class RepositorySqlJobArtifactStore(SqlJobArtifactStore):
    def __init__(self, *, repository: SqlJobResultArtifactRepository) -> None:
        self._repository = repository

    def add(self, instance: SqlJobResultArtifact) -> SqlJobResultArtifact:
        record = self._repository.add(to_sql_job_result_artifact_record(instance))
        return from_sql_job_result_artifact_record(record) or instance

    async def list_for_job(self, *, sql_job_id) -> list[SqlJobResultArtifact]:
        return [
            runtime_artifact
            for item in await self._repository.list_for_job(sql_job_id=sql_job_id)
            if (runtime_artifact := from_sql_job_result_artifact_record(item)) is not None
        ]


class RepositoryDatasetCatalogStore(DatasetCatalogStore):
    def __init__(self, *, repository: DatasetRepository) -> None:
        self._repository = repository

    def add(self, instance: DatasetMetadata) -> DatasetMetadata:
        record = self._repository.add(to_dataset_record(instance))
        return from_dataset_record(record) or instance

    async def save(self, instance: DatasetMetadata) -> DatasetMetadata:
        next_record = to_dataset_record(instance)
        tracked = _find_tracked_record(self._repository._session, next_record)
        if tracked is not None:
            _copy_record_columns(tracked, next_record)
            return from_dataset_record(tracked) or instance
        record = await self._repository.save(next_record)
        reloaded = record
        workspace_id = getattr(record, "workspace_id", None)
        record_id = getattr(record, "id", None)
        if workspace_id is not None and record_id is not None:
            reloaded = await self._repository.get_for_workspace(
                dataset_id=record_id,
                workspace_id=workspace_id,
            )
        return from_dataset_record(reloaded) or instance

    async def delete(self, instance: DatasetMetadata) -> None:
        record = await self._repository.get_for_workspace(
            dataset_id=instance.id,
            workspace_id=instance.workspace_id,
        )
        if record is not None:
            await self._repository.delete(record)

    async def get_by_id(self, id_: object) -> DatasetMetadata | None:
        return from_dataset_record(await self._repository.get_by_id(id_))

    async def get_for_workspace(self, *, dataset_id, workspace_id) -> DatasetMetadata | None:
        return from_dataset_record(
            await self._repository.get_for_workspace(
                dataset_id=dataset_id,
                workspace_id=workspace_id,
            )
        )

    async def get_for_workspace_by_sql_alias(
        self,
        *,
        workspace_id,
        sql_alias,
    ) -> DatasetMetadata | None:
        return from_dataset_record(
            await self._repository.get_for_workspace_by_sql_alias(
                workspace_id=workspace_id,
                sql_alias=sql_alias,
            )
        )

    async def get_by_ids(self, dataset_ids) -> list[DatasetMetadata]:
        return [
            runtime_dataset
            for item in await self._repository.get_by_ids(dataset_ids)
            if (runtime_dataset := from_dataset_record(item)) is not None
        ]

    async def get_by_ids_for_workspace(
        self,
        *,
        workspace_id,
        dataset_ids,
    ) -> list[DatasetMetadata]:
        return [
            runtime_dataset
            for item in await self._repository.get_by_ids_for_workspace(
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )
            if (runtime_dataset := from_dataset_record(item)) is not None
        ]

    async def list_for_workspace(
        self,
        *,
        workspace_id,
        search=None,
        tags=None,
        dataset_types=None,
        limit=200,
        offset=0,
    ) -> list[DatasetMetadata]:
        return [
            runtime_dataset
            for item in await self._repository.list_for_workspace(
                workspace_id=workspace_id,
                search=search,
                tags=tags,
                dataset_types=dataset_types,
                limit=limit,
                offset=offset,
            )
            if (runtime_dataset := from_dataset_record(item)) is not None
        ]

    async def find_file_dataset_for_connection(
        self,
        *,
        workspace_id,
        connection_id,
        table_name,
    ) -> DatasetMetadata | None:
        return from_dataset_record(
            await self._repository.find_file_dataset_for_connection(
                workspace_id=workspace_id,
                connection_id=connection_id,
                table_name=table_name,
            )
        )

    async def list_for_connection(
        self,
        *,
        workspace_id,
        connection_id,
        dataset_types=None,
        limit=500,
    ) -> list[DatasetMetadata]:
        return [
            runtime_dataset
            for item in await self._repository.list_for_connection(
                workspace_id=workspace_id,
                connection_id=connection_id,
                dataset_types=dataset_types,
                limit=limit,
            )
            if (runtime_dataset := from_dataset_record(item)) is not None
        ]

    async def flush(self) -> None:
        await self._repository.flush()


class RepositoryDatasetColumnStore(DatasetColumnStore):
    def __init__(self, *, repository: DatasetColumnRepository) -> None:
        self._repository = repository

    def add(self, instance: DatasetColumnMetadata) -> DatasetColumnMetadata:
        record = self._repository.add(to_dataset_column_record(instance))
        return from_dataset_column_record(record)

    async def list_for_dataset(self, *, dataset_id) -> list[DatasetColumnMetadata]:
        return [
            from_dataset_column_record(item)
            for item in await self._repository.list_for_dataset(dataset_id=dataset_id)
        ]

    async def delete_for_dataset(self, *, dataset_id) -> None:
        await self._repository.delete_for_dataset(dataset_id=dataset_id)

    async def flush(self) -> None:
        await self._repository.flush()


class RepositoryDatasetPolicyStore(DatasetPolicyStore):
    def __init__(self, *, repository: DatasetPolicyRepository) -> None:
        self._repository = repository

    def add(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata:
        record = self._repository.add(to_dataset_policy_record(instance))
        return from_dataset_policy_record(record) or instance

    async def save(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata:
        next_record = to_dataset_policy_record(instance)
        tracked = _find_tracked_record(self._repository._session, next_record)
        if tracked is not None:
            _copy_record_columns(tracked, next_record)
            return from_dataset_policy_record(tracked) or instance
        record = await self._repository.save(next_record)
        return from_dataset_policy_record(record) or instance

    async def get_for_dataset(self, *, dataset_id) -> DatasetPolicyMetadata | None:
        return from_dataset_policy_record(
            await self._repository.get_for_dataset(dataset_id=dataset_id)
        )

    async def flush(self) -> None:
        await self._repository.flush()


class RepositoryDatasetRevisionStore(DatasetRevisionStore):
    def __init__(self, *, repository: DatasetRevisionRepository) -> None:
        self._repository = repository

    def add(self, instance: DatasetRevision) -> DatasetRevision:
        record = self._repository.add(to_dataset_revision_record(instance))
        return from_dataset_revision_record(record) or instance

    async def next_revision_number(self, *, dataset_id) -> int:
        return await self._repository.next_revision_number(dataset_id=dataset_id)

    async def flush(self) -> None:
        await self._repository.flush()


class RepositoryLineageEdgeStore(LineageEdgeStore):
    def __init__(self, *, repository: LineageEdgeRepository) -> None:
        self._repository = repository

    def add(self, instance: LineageEdge) -> LineageEdge:
        record = self._repository.add(to_lineage_edge_record(instance))
        return from_lineage_edge_record(record) or instance

    async def delete_for_target(self, *, workspace_id, target_type, target_id) -> None:
        await self._repository.delete_for_target(
            workspace_id=workspace_id,
            target_type=target_type,
            target_id=target_id,
        )

    async def flush(self) -> None:
        await self._repository.flush()


class RepositoryConnectorSyncStateStore(ConnectorSyncStateStore):
    def __init__(self, *, repository: ConnectorSyncStateRepository) -> None:
        self._repository = repository

    def add(self, instance: ConnectorSyncState) -> ConnectorSyncState:
        record = self._repository.add(to_connector_sync_state_record(instance))
        return from_connector_sync_state_record(record) or instance

    async def save(self, instance: ConnectorSyncState) -> ConnectorSyncState:
        next_record = to_connector_sync_state_record(instance)
        tracked = _find_tracked_record(self._repository._session, next_record)
        if tracked is not None:
            _copy_record_columns(tracked, next_record)
            return from_connector_sync_state_record(tracked) or instance
        record = await self._repository.save(next_record)
        return from_connector_sync_state_record(record) or instance

    async def list_for_connection(
        self,
        *,
        workspace_id,
        connection_id,
    ) -> list[ConnectorSyncState]:
        return [
            runtime_state
            for item in await self._repository.list_for_connection(
                workspace_id=workspace_id,
                connection_id=connection_id,
            )
            if (runtime_state := from_connector_sync_state_record(item)) is not None
        ]

    async def get_for_resource(
        self,
        *,
        workspace_id,
        connection_id,
        resource_name,
    ) -> ConnectorSyncState | None:
        return from_connector_sync_state_record(
            await self._repository.get_for_resource(
                workspace_id=workspace_id,
                connection_id=connection_id,
                resource_name=resource_name,
            )
        )

    async def flush(self) -> None:
        await self._repository.flush()


__all__ = [
    "RepositoryAgentDefinitionStore",
    "RepositoryConnectorSyncStateStore",
    "RepositoryConversationMemoryStore",
    "RepositoryDatasetCatalogStore",
    "RepositoryDatasetColumnStore",
    "RepositoryDatasetPolicyStore",
    "RepositoryDatasetRevisionStore",
    "RepositoryLLMConnectionStore",
    "RepositoryLineageEdgeStore",
    "RepositorySemanticModelStore",
    "RepositorySemanticVectorIndexStore",
    "RepositorySqlJobArtifactStore",
    "RepositorySqlJobStore",
    "RepositoryThreadMessageStore",
    "RepositoryThreadStore",
]
