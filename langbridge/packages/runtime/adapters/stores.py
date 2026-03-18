from __future__ import annotations

from langbridge.packages.common.langbridge_common.repositories.agent_repository import (
    AgentRepository,
)
from langbridge.packages.common.langbridge_common.repositories.connector_sync_repository import (
    ConnectorSyncStateRepository,
)
from langbridge.packages.common.langbridge_common.repositories.conversation_memory_repository import (
    ConversationMemoryRepository,
)
from langbridge.packages.common.langbridge_common.repositories.llm_connection_repository import (
    LLMConnectionRepository,
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
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlJobRepository,
    SqlJobResultArtifactRepository,
)
from langbridge.packages.common.langbridge_common.repositories.thread_message_repository import (
    ThreadMessageRepository,
)
from langbridge.packages.common.langbridge_common.repositories.thread_repository import (
    ThreadRepository,
)
from langbridge.packages.runtime.adapters.legacy import (
    to_runtime_conversation_memory_item,
    to_legacy_connector_sync_state,
    to_legacy_dataset,
    to_legacy_dataset_column,
    to_legacy_dataset_policy,
    to_legacy_dataset_revision,
    to_legacy_lineage_edge,
    to_legacy_sql_job,
    to_legacy_sql_job_result_artifact,
    to_legacy_thread,
    to_legacy_thread_message,
    to_runtime_agent_definition,
    to_runtime_dataset,
    to_runtime_dataset_column,
    to_runtime_dataset_policy,
    to_runtime_llm_connection,
    to_runtime_semantic_model,
    to_runtime_sql_job,
    to_runtime_sql_job_result_artifact,
    to_runtime_sync_state,
    to_runtime_thread,
    to_runtime_thread_message,
)
from langbridge.packages.runtime.models import (
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    RuntimeConversationMemoryItem,
    LLMConnectionSecret,
    LineageEdge,
    RuntimeAgentDefinition,
    SemanticModelMetadata,
    SqlJob,
    SqlJobResultArtifact,
    RuntimeThread,
    RuntimeThreadMessage,
)
from langbridge.packages.runtime.ports import (
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
    SqlJobArtifactStore,
    SqlJobStore,
    ThreadMessageStore,
    ThreadStore,
)


class RepositoryAgentDefinitionStore(AgentDefinitionStore):
    def __init__(self, *, repository: AgentRepository) -> None:
        self._repository = repository

    async def get_by_id(self, id_: object) -> RuntimeAgentDefinition | None:
        return to_runtime_agent_definition(await self._repository.get_by_id(id_))


class RepositoryLLMConnectionStore(LLMConnectionStore):
    def __init__(self, *, repository: LLMConnectionRepository) -> None:
        self._repository = repository

    async def get_by_id(self, id_: object) -> LLMConnectionSecret | None:
        return to_runtime_llm_connection(await self._repository.get_by_id(id_))


class RepositoryThreadStore(ThreadStore):
    def __init__(self, *, repository: ThreadRepository) -> None:
        self._repository = repository

    def add(self, instance: RuntimeThread) -> RuntimeThread:
        record = self._repository.add(to_legacy_thread(instance))
        return to_runtime_thread(record) or instance

    async def save(self, instance: RuntimeThread) -> RuntimeThread:
        record = await self._repository.save(to_legacy_thread(instance))
        return to_runtime_thread(record) or instance

    async def get_by_id(self, id_: object) -> RuntimeThread | None:
        return to_runtime_thread(await self._repository.get_by_id(id_))


class RepositoryThreadMessageStore(ThreadMessageStore):
    def __init__(self, *, repository: ThreadMessageRepository) -> None:
        self._repository = repository

    def add(self, instance: RuntimeThreadMessage) -> RuntimeThreadMessage:
        record = self._repository.add(to_legacy_thread_message(instance))
        return to_runtime_thread_message(record) or instance

    async def list_for_thread(self, thread_id) -> list[RuntimeThreadMessage]:
        return [
            runtime_message
            for item in await self._repository.list_for_thread(thread_id)
            if (runtime_message := to_runtime_thread_message(item)) is not None
        ]


class RepositorySemanticModelStore(SemanticModelStore):
    def __init__(self, *, repository: SemanticModelRepository) -> None:
        self._repository = repository

    async def get_by_id(self, model_id) -> SemanticModelMetadata | None:
        return to_runtime_semantic_model(await self._repository.get_by_id(model_id))

    async def get_by_ids(self, model_ids) -> list[SemanticModelMetadata]:
        return [
            runtime_model
            for item in await self._repository.get_by_ids(model_ids)
            if (runtime_model := to_runtime_semantic_model(item)) is not None
        ]


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
            if (runtime_item := to_runtime_conversation_memory_item(item)) is not None
        ]

    def create_item(
        self,
        *,
        thread_id,
        user_id,
        category: str,
        content: str,
        metadata_json=None,
    ) -> RuntimeConversationMemoryItem | None:
        return to_runtime_conversation_memory_item(
            self._repository.create_item(
                thread_id=thread_id,
                user_id=user_id,
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
        return to_runtime_sql_job(
            await self._repository.get_by_id_for_workspace(
                sql_job_id=sql_job_id,
                workspace_id=workspace_id,
            )
        )

    async def save(self, instance: SqlJob) -> SqlJob:
        record = await self._repository.save(to_legacy_sql_job(instance))
        return to_runtime_sql_job(record) or instance


class RepositorySqlJobArtifactStore(SqlJobArtifactStore):
    def __init__(self, *, repository: SqlJobResultArtifactRepository) -> None:
        self._repository = repository

    def add(self, instance: SqlJobResultArtifact) -> SqlJobResultArtifact:
        record = self._repository.add(to_legacy_sql_job_result_artifact(instance))
        return to_runtime_sql_job_result_artifact(record) or instance

    async def list_for_job(self, *, sql_job_id) -> list[SqlJobResultArtifact]:
        return [
            runtime_artifact
            for item in await self._repository.list_for_job(sql_job_id=sql_job_id)
            if (runtime_artifact := to_runtime_sql_job_result_artifact(item)) is not None
        ]


class RepositoryDatasetCatalogStore(DatasetCatalogStore):
    def __init__(self, *, repository: DatasetRepository) -> None:
        self._repository = repository

    def add(self, instance: DatasetMetadata) -> DatasetMetadata:
        record = self._repository.add(to_legacy_dataset(instance))
        return to_runtime_dataset(record) or instance

    async def save(self, instance: DatasetMetadata) -> DatasetMetadata:
        record = await self._repository.save(to_legacy_dataset(instance))
        return to_runtime_dataset(record) or instance

    async def get_by_id(self, id_: object) -> DatasetMetadata | None:
        return to_runtime_dataset(await self._repository.get_by_id(id_))

    async def get_for_workspace(self, *, dataset_id, workspace_id) -> DatasetMetadata | None:
        return to_runtime_dataset(
            await self._repository.get_for_workspace(
                dataset_id=dataset_id,
                workspace_id=workspace_id,
            )
        )

    async def get_for_workspace_by_sql_alias(self, *, workspace_id, sql_alias) -> DatasetMetadata | None:
        return to_runtime_dataset(
            await self._repository.get_for_workspace_by_sql_alias(
                workspace_id=workspace_id,
                sql_alias=sql_alias,
            )
        )

    async def get_by_ids(self, dataset_ids) -> list[DatasetMetadata]:
        return [
            runtime_dataset
            for item in await self._repository.get_by_ids(dataset_ids)
            if (runtime_dataset := to_runtime_dataset(item)) is not None
        ]

    async def get_by_ids_for_workspace(self, *, workspace_id, dataset_ids) -> list[DatasetMetadata]:
        return [
            runtime_dataset
            for item in await self._repository.get_by_ids_for_workspace(
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )
            if (runtime_dataset := to_runtime_dataset(item)) is not None
        ]

    async def list_for_workspace(
        self,
        *,
        workspace_id,
        project_id=None,
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
                project_id=project_id,
                search=search,
                tags=tags,
                dataset_types=dataset_types,
                limit=limit,
                offset=offset,
            )
            if (runtime_dataset := to_runtime_dataset(item)) is not None
        ]

    async def find_file_dataset_for_connection(
        self,
        *,
        workspace_id,
        connection_id,
        table_name,
    ) -> DatasetMetadata | None:
        return to_runtime_dataset(
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
            if (runtime_dataset := to_runtime_dataset(item)) is not None
        ]


class RepositoryDatasetColumnStore(DatasetColumnStore):
    def __init__(self, *, repository: DatasetColumnRepository) -> None:
        self._repository = repository

    def add(self, instance: DatasetColumnMetadata) -> DatasetColumnMetadata:
        record = self._repository.add(to_legacy_dataset_column(instance))
        return to_runtime_dataset_column(record)

    async def list_for_dataset(self, *, dataset_id) -> list[DatasetColumnMetadata]:
        return [
            to_runtime_dataset_column(item)
            for item in await self._repository.list_for_dataset(dataset_id=dataset_id)
        ]

    async def delete_for_dataset(self, *, dataset_id) -> None:
        await self._repository.delete_for_dataset(dataset_id=dataset_id)


class RepositoryDatasetPolicyStore(DatasetPolicyStore):
    def __init__(self, *, repository: DatasetPolicyRepository) -> None:
        self._repository = repository

    def add(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata:
        record = self._repository.add(to_legacy_dataset_policy(instance))
        return to_runtime_dataset_policy(record) or instance

    async def save(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata:
        record = await self._repository.save(to_legacy_dataset_policy(instance))
        return to_runtime_dataset_policy(record) or instance

    async def get_for_dataset(self, *, dataset_id) -> DatasetPolicyMetadata | None:
        return to_runtime_dataset_policy(
            await self._repository.get_for_dataset(dataset_id=dataset_id)
        )


class RepositoryDatasetRevisionStore(DatasetRevisionStore):
    def __init__(self, *, repository: DatasetRevisionRepository) -> None:
        self._repository = repository

    def add(self, instance: DatasetRevision) -> DatasetRevision:
        record = self._repository.add(to_legacy_dataset_revision(instance))
        return DatasetRevision.model_validate(record)

    async def next_revision_number(self, *, dataset_id) -> int:
        return await self._repository.next_revision_number(dataset_id=dataset_id)


class RepositoryLineageEdgeStore(LineageEdgeStore):
    def __init__(self, *, repository: LineageEdgeRepository) -> None:
        self._repository = repository

    def add(self, instance: LineageEdge) -> LineageEdge:
        record = self._repository.add(to_legacy_lineage_edge(instance))
        return LineageEdge.model_validate(record)

    async def delete_for_target(self, *, workspace_id, target_type, target_id) -> None:
        await self._repository.delete_for_target(
            workspace_id=workspace_id,
            target_type=target_type,
            target_id=target_id,
        )


class RepositoryConnectorSyncStateStore(ConnectorSyncStateStore):
    def __init__(self, *, repository: ConnectorSyncStateRepository) -> None:
        self._repository = repository

    def add(self, instance: ConnectorSyncState) -> ConnectorSyncState:
        record = self._repository.add(to_legacy_connector_sync_state(instance))
        return to_runtime_sync_state(record) or instance

    async def save(self, instance: ConnectorSyncState) -> ConnectorSyncState:
        record = await self._repository.save(to_legacy_connector_sync_state(instance))
        return to_runtime_sync_state(record) or instance

    async def list_for_connection(self, *, workspace_id, connection_id) -> list[ConnectorSyncState]:
        return [
            runtime_state
            for item in await self._repository.list_for_connection(
                workspace_id=workspace_id,
                connection_id=connection_id,
            )
            if (runtime_state := to_runtime_sync_state(item)) is not None
        ]

    async def get_for_resource(
        self,
        *,
        workspace_id,
        connection_id,
        resource_name,
    ) -> ConnectorSyncState | None:
        return to_runtime_sync_state(
            await self._repository.get_for_resource(
                workspace_id=workspace_id,
                connection_id=connection_id,
                resource_name=resource_name,
            )
        )
