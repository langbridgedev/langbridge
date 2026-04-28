import uuid
from typing import Any, Protocol

from langbridge.runtime.models import (
    ConnectorMetadata,
    ConnectorSyncState,
    RuntimeConversationMemoryItem,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    LineageEdge,
    LLMConnectionSecret,
    RuntimeAgentDefinition,
    RuntimeJobStatus,
    RuntimeThread,
    RuntimeThreadMessage,
    SecretReference,
    SemanticModelMetadata,
    SemanticVectorIndexMetadata,
    SqlJob,
    SqlJobResultArtifact,
)


class IConnectorStore(Protocol):
    async def get_by_name(self, name: str) -> ConnectorMetadata | None: ...

    async def get_by_id(self, connector_id: uuid.UUID) -> ConnectorMetadata | None: ...

    async def get_by_ids(
        self,
        connector_ids: list[uuid.UUID],
    ) -> list[ConnectorMetadata]: ...


class ISemanticModelStore(Protocol):
    async def get_by_id(
        self,
        model_id: uuid.UUID,
    ) -> SemanticModelMetadata | None: ...

    async def get_by_ids(
        self,
        model_ids: list[uuid.UUID],
    ) -> list[SemanticModelMetadata]: ...

class DatasetMetadataProvider(Protocol):
    async def get_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_id: uuid.UUID,
    ) -> DatasetMetadata | None: ...

    async def get_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids: list[uuid.UUID],
    ) -> list[DatasetMetadata]: ...

    async def get_dataset_columns(
        self,
        *,
        dataset_id: uuid.UUID,
    ) -> list[DatasetColumnMetadata]: ...

    async def get_dataset_policy(
        self,
        *,
        dataset_id: uuid.UUID,
    ) -> DatasetPolicyMetadata | None: ...


class SemanticModelMetadataProvider(Protocol):
    async def get_semantic_model(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
    ) -> SemanticModelMetadata | None: ...
    
    async def get_semantic_models(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_ids: list[uuid.UUID] | None = None,
    ) -> list[SemanticModelMetadata]: ...

class SemanticVectorIndexMetadataProvider(Protocol):
    async def get_semantic_vector_index(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_vector_index_id: uuid.UUID,
    ) -> SemanticVectorIndexMetadata | None: ...

    async def get_semantic_vector_index_for_dimension(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
        dataset_key: str,
        dimension_name: str,
    ) -> SemanticVectorIndexMetadata | None: ...

    async def list_semantic_vector_indexes(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID | None = None,
    ) -> list[SemanticVectorIndexMetadata]: ...

class ConnectorMetadataProvider(Protocol):
    async def get_connector(
        self,
        *,
        workspace_id: uuid.UUID,
        connector_id: uuid.UUID,
    ) -> ConnectorMetadata | None: ...

    async def get_connector_by_name(
        self,
        *,
        workspace_id: uuid.UUID,
        connector_name: str,
    ) -> ConnectorMetadata | None: ...


class SyncStateProvider(Protocol):
    async def get_or_create_state(self, **kwargs: object) -> ConnectorSyncState: ...

    async def mark_failed(self, **kwargs: object) -> None: ...


class SqlJobResultArtifactProvider(Protocol):
    async def create_sql_job_result_artifact(self, **kwargs: object) -> SqlJobResultArtifact: ...

    async def list_sql_job_result_artifacts(self, **kwargs: object) -> list[SqlJobResultArtifact]: ...


class SqlJobStore(Protocol):
    async def get_by_id_for_workspace(
        self,
        *,
        sql_job_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> SqlJob | None: ...

    async def save(self, instance: SqlJob) -> SqlJob: ...


class SqlJobArtifactStore(Protocol):
    def add(self, instance: SqlJobResultArtifact) -> SqlJobResultArtifact: ...

    async def list_for_job(self, *, sql_job_id: uuid.UUID) -> list[SqlJobResultArtifact]: ...


class CredentialProvider(Protocol):
    def resolve_secret(self, reference: SecretReference) -> str: ...


class AgentDefinitionStore(Protocol):
    async def get_by_id(self, id_: object) -> RuntimeAgentDefinition | None: ...


class LLMConnectionStore(Protocol):
    async def get_by_id(self, id_: object) -> LLMConnectionSecret | None: ...
    async def list_llm_connections(self) -> list[LLMConnectionSecret]: ...


class ThreadStore(Protocol):
    def add(self, instance: RuntimeThread) -> RuntimeThread: ...

    async def save(self, instance: RuntimeThread) -> RuntimeThread: ...

    async def get_by_id(self, id_: object) -> RuntimeThread | None: ...

    async def delete(self, id_: object) -> None: ...

    async def list_for_actor(self, actor_id: uuid.UUID | None = None) -> list[RuntimeThread]: ...

    async def list_for_workspace(self, workspace_id: uuid.UUID) -> list[RuntimeThread]: ...


class ThreadMessageStore(Protocol):
    def add(self, instance: RuntimeThreadMessage) -> RuntimeThreadMessage: ...

    async def list_for_thread(self, thread_id: uuid.UUID) -> list[RuntimeThreadMessage]: ...

    async def delete_for_thread(self, thread_id: uuid.UUID) -> None: ...


class SemanticModelStore(Protocol):
    async def get_by_id(self, model_id: uuid.UUID) -> SemanticModelMetadata | None: ...

    async def get_by_ids(self, model_ids: list[uuid.UUID]) -> list[SemanticModelMetadata]: ...

class SemanticVectorIndexStore(Protocol):
    async def get_by_id(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_vector_index_id: uuid.UUID,
    ) -> SemanticVectorIndexMetadata | None: ...

    async def get_for_dimension(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
        dataset_key: str,
        dimension_name: str,
    ) -> SemanticVectorIndexMetadata | None: ...

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID | None = None,
    ) -> list[SemanticVectorIndexMetadata]: ...

    async def save(
        self,
        instance: SemanticVectorIndexMetadata,
    ) -> SemanticVectorIndexMetadata: ...

    async def delete(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_vector_index_id: uuid.UUID,
    ) -> None: ...

class ConversationMemoryStore(Protocol):
    async def list_for_thread(
        self,
        thread_id: uuid.UUID,
        *,
        limit: int = 200,
    ) -> list[RuntimeConversationMemoryItem]: ...

    def create_item(
        self,
        *,
        thread_id: uuid.UUID,
        actor_id: uuid.UUID | None,
        category: str,
        content: str,
        metadata_json: dict[str, Any] | None = None,
    ) -> RuntimeConversationMemoryItem | None: ...

    async def touch_items(self, item_ids: list[uuid.UUID]) -> None: ...

    async def flush(self) -> None: ...


class DatasetCatalogStore(Protocol):
    def add(self, instance: DatasetMetadata) -> DatasetMetadata: ...

    async def save(self, instance: DatasetMetadata) -> DatasetMetadata: ...

    async def delete(self, instance: DatasetMetadata) -> None: ...

    async def get_by_id(self, id_: object) -> DatasetMetadata | None: ...

    async def get_for_workspace(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> DatasetMetadata | None: ...

    async def get_for_workspace_by_sql_alias(
        self,
        *,
        workspace_id: uuid.UUID,
        sql_alias: str,
    ) -> DatasetMetadata | None: ...

    async def get_by_ids(self, dataset_ids: list[uuid.UUID]) -> list[DatasetMetadata]: ...

    async def get_by_ids_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids: list[uuid.UUID],
    ) -> list[DatasetMetadata]: ...

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        search: str | None = None,
        tags: list[str] | None = None,
        dataset_types: list[str] | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[DatasetMetadata]: ...

    async def find_file_dataset_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        table_name: str,
    ) -> DatasetMetadata | None: ...

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        dataset_types: list[str] | None = None,
        limit: int = 500,
    ) -> list[DatasetMetadata]: ...


class DatasetColumnStore(Protocol):
    def add(self, instance: DatasetColumnMetadata) -> DatasetColumnMetadata: ...

    async def list_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
    ) -> list[DatasetColumnMetadata]: ...

    async def delete_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
    ) -> None: ...


class DatasetPolicyStore(Protocol):
    def add(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata: ...

    async def save(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata: ...

    async def get_for_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
    ) -> DatasetPolicyMetadata | None: ...


class DatasetRevisionStore(Protocol):
    def add(self, instance: DatasetRevision) -> DatasetRevision: ...

    async def next_revision_number(
        self,
        *,
        dataset_id: uuid.UUID,
    ) -> int: ...


class LineageEdgeStore(Protocol):
    def add(self, instance: LineageEdge) -> LineageEdge: ...

    async def delete_for_target(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> None: ...


class ConnectorSyncStateStore(Protocol):
    def add(self, instance: ConnectorSyncState) -> ConnectorSyncState: ...

    async def save(self, instance: ConnectorSyncState) -> ConnectorSyncState: ...

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
    ) -> list[ConnectorSyncState]: ...

    async def get_for_resource(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        resource_name: str,
    ) -> ConnectorSyncState | None: ...


class MutableJobHandle(Protocol):
    id: uuid.UUID
    status: RuntimeJobStatus | str
    progress: int
    status_message: str | None
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    started_at: Any
    finished_at: Any
    updated_at: Any


__all__ = [
    "AgentDefinitionStore",
    "ConnectorMetadataProvider",
    "IConnectorStore",
    "ISemanticModelStore",
    "ConnectorSyncStateStore",
    "ConversationMemoryStore",
    "CredentialProvider",
    "LLMConnectionStore",
    "DatasetCatalogStore",
    "DatasetColumnStore",
    "DatasetMetadataProvider",
    "DatasetPolicyStore",
    "DatasetRevisionStore",
    "LineageEdgeStore",
    "MutableJobHandle",
    "SemanticModelStore",
    "SemanticModelMetadataProvider",
    "SemanticVectorIndexMetadataProvider",
    "SemanticVectorIndexStore",
    "SqlJobArtifactStore",
    "SqlJobStore",
    "SqlJobResultArtifactProvider",
    "SyncStateProvider",
    "ThreadMessageStore",
    "ThreadStore",
]
