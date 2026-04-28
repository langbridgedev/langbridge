
import uuid
from datetime import datetime, timezone
from typing import Any

from langbridge.runtime.models import (
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    LLMConnectionSecret,
    LineageEdge,
    RuntimeAgentDefinition,
    RuntimeConversationMemoryCategory,
    RuntimeConversationMemoryItem,
    RuntimeThread,
    RuntimeThreadMessage,
    SemanticModelMetadata,
)
from langbridge.runtime.models.metadata import DatasetMaterializationMode, DatasetType
from langbridge.runtime.security import SecretProviderRegistry


class _InMemoryDatasetRepository:
    def __init__(self, datasets: dict[uuid.UUID, DatasetMetadata]) -> None:
        self._datasets = dict(datasets)

    def add(self, instance: DatasetMetadata) -> DatasetMetadata:
        self._datasets[instance.id] = instance
        return instance

    async def save(self, instance: DatasetMetadata) -> DatasetMetadata:
        self._datasets[instance.id] = instance
        return instance

    async def get_by_id(self, id_: object) -> DatasetMetadata | None:
        return self._datasets.get(id_)

    async def get_for_workspace(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> DatasetMetadata | None:
        dataset = self._datasets.get(dataset_id)
        if dataset is None or dataset.workspace_id != workspace_id:
            return None
        return dataset

    async def get_for_workspace_by_sql_alias(
        self,
        *,
        workspace_id: uuid.UUID,
        sql_alias: str,
    ) -> DatasetMetadata | None:
        normalized_alias = str(sql_alias or "").strip().lower()
        if not normalized_alias:
            return None
        for dataset in self._datasets.values():
            if dataset.workspace_id == workspace_id and str(dataset.sql_alias or "").strip().lower() == normalized_alias:
                return dataset
        return None

    async def get_by_ids(self, dataset_ids) -> list[DatasetMetadata]:
        return [self._datasets[dataset_id] for dataset_id in dataset_ids if dataset_id in self._datasets]

    async def get_by_ids_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids,
    ) -> list[DatasetMetadata]:
        items: list[DatasetMetadata] = []
        for dataset_id in dataset_ids:
            dataset = self._datasets.get(dataset_id)
            if dataset is not None and dataset.workspace_id == workspace_id:
                items.append(dataset)
        return items

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        search: str | None = None,
        tags=None,
        dataset_types=None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[DatasetMetadata]:
        normalized_search = str(search or "").strip().lower()
        normalized_tags = {
            str(tag).strip().lower()
            for tag in (tags or [])
            if str(tag).strip()
        }
        normalized_types = {
            str(getattr(dataset_type, "value", dataset_type)).strip().upper()
            for dataset_type in (dataset_types or [])
            if str(dataset_type).strip()
        }

        items: list[DatasetMetadata] = []
        for dataset in self._datasets.values():
            if dataset.workspace_id != workspace_id:
                continue
            if normalized_search:
                haystacks = [
                    str(dataset.name or "").lower(),
                    str(dataset.description or "").lower(),
                ]
                if not any(normalized_search in haystack for haystack in haystacks):
                    continue
            if normalized_tags:
                dataset_tags = {str(tag).strip().lower() for tag in (dataset.tags_json or []) if str(tag).strip()}
                if not normalized_tags.issubset(dataset_tags):
                    continue
            if normalized_types and dataset.dataset_type_value not in normalized_types:
                continue
            items.append(dataset)

        items.sort(
            key=lambda dataset: dataset.updated_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        return items[start:end]

    async def find_file_dataset_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        table_name: str,
    ) -> DatasetMetadata | None:
        normalized_table = str(table_name or "").strip().lower()
        for dataset in self._datasets.values():
            if dataset.workspace_id != workspace_id:
                continue
            if dataset.connection_id != connection_id:
                continue
            if dataset.dataset_type != DatasetType.FILE:
                continue
            mode = dataset.materialization_mode
            if mode != DatasetMaterializationMode.SYNCED:
                continue
            if str(dataset.table_name or "").strip().lower() == normalized_table:
                return dataset
        return None

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        dataset_types=None,
        limit: int = 500,
    ) -> list[DatasetMetadata]:
        normalized_types = {
            str(getattr(dataset_type, "value", dataset_type)).strip().upper()
            for dataset_type in (dataset_types or [])
            if str(dataset_type).strip()
        }
        items = [
            dataset
            for dataset in self._datasets.values()
            if dataset.workspace_id == workspace_id
            and dataset.connection_id == connection_id
            and (
                not normalized_types
                or dataset.dataset_type_value in normalized_types
            )
        ]
        items.sort(
            key=lambda dataset: dataset.updated_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return items[: max(1, int(limit))]

    async def flush(self) -> None:
        return None


class _InMemoryDatasetColumnRepository:
    def __init__(self, columns_by_dataset: dict[uuid.UUID, list[DatasetColumnMetadata]]) -> None:
        self._columns_by_dataset = {
            dataset_id: list(columns)
            for dataset_id, columns in columns_by_dataset.items()
        }

    async def list_for_dataset(self, *, dataset_id: uuid.UUID) -> list[DatasetColumnMetadata]:
        return list(self._columns_by_dataset.get(dataset_id, []))

    async def delete_for_dataset(self, *, dataset_id: uuid.UUID) -> None:
        self._columns_by_dataset[dataset_id] = []

    def add(self, instance: DatasetColumnMetadata) -> DatasetColumnMetadata:
        self._columns_by_dataset.setdefault(instance.dataset_id, []).append(instance)
        self._columns_by_dataset[instance.dataset_id].sort(
            key=lambda column: int(column.ordinal_position or 0)
        )
        return instance

    async def flush(self) -> None:
        return None


class _InMemoryDatasetPolicyRepository:
    def __init__(self, policies: dict[uuid.UUID, DatasetPolicyMetadata] | None = None) -> None:
        self._policies_by_dataset = dict(policies or {})

    async def get_for_dataset(self, *, dataset_id: uuid.UUID) -> DatasetPolicyMetadata | None:
        return self._policies_by_dataset.get(dataset_id)

    def add(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata:
        self._policies_by_dataset[instance.dataset_id] = instance
        return instance

    async def save(self, instance: DatasetPolicyMetadata) -> DatasetPolicyMetadata:
        self._policies_by_dataset[instance.dataset_id] = instance
        return instance

    async def flush(self) -> None:
        return None


class _InMemoryConnectorSyncStateRepository:
    def __init__(
        self,
        states: dict[tuple[uuid.UUID, uuid.UUID, str], ConnectorSyncState] | None = None,
    ) -> None:
        self._states = dict(states or {})

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
    ) -> list[ConnectorSyncState]:
        items = [
            state
            for (state_workspace_id, state_connection_id, _), state in self._states.items()
            if state_workspace_id == workspace_id and state_connection_id == connection_id
        ]
        items.sort(
            key=lambda state: (
                str(state.source_key or "").lower(),
                state.updated_at or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=False,
        )
        return items

    async def get_for_resource(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        resource_name: str,
    ) -> ConnectorSyncState | None:
        return self._states.get((workspace_id, connection_id, str(resource_name or "").strip()))

    def add(self, instance: ConnectorSyncState) -> ConnectorSyncState:
        key = (
            instance.workspace_id,
            instance.connection_id,
            str(instance.source_key or "").strip(),
        )
        self._states[key] = instance
        return instance

    async def save(self, instance: ConnectorSyncState) -> ConnectorSyncState:
        return self.add(instance)

    async def flush(self) -> None:
        return None


class _InMemoryDatasetRevisionRepository:
    def __init__(self) -> None:
        self._revisions_by_dataset: dict[uuid.UUID, list[DatasetRevision]] = {}

    def add(self, instance: DatasetRevision) -> DatasetRevision:
        self._revisions_by_dataset.setdefault(instance.dataset_id, []).append(instance)
        self._revisions_by_dataset[instance.dataset_id].sort(
            key=lambda revision: int(revision.revision_number)
        )
        return instance

    async def next_revision_number(self, *, dataset_id: uuid.UUID) -> int:
        revisions = self._revisions_by_dataset.get(dataset_id, [])
        if not revisions:
            return 1
        return max(int(revision.revision_number) for revision in revisions) + 1

    async def flush(self) -> None:
        return None


class _InMemoryLineageEdgeRepository:
    def __init__(self) -> None:
        self._edges: list[LineageEdge] = []

    def add(self, instance: LineageEdge) -> LineageEdge:
        self._edges.append(instance)
        return instance

    async def delete_for_target(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> None:
        self._edges = [
            edge
            for edge in self._edges
            if not (
                edge.workspace_id == workspace_id
                and edge.target_type == target_type
                and edge.target_id == target_id
            )
        ]

    async def flush(self) -> None:
        return None


class _InMemoryAgentRepository:
    def __init__(self, agents: dict[uuid.UUID, RuntimeAgentDefinition]) -> None:
        self._agents = dict(agents)

    async def get_by_id(self, id_: object) -> RuntimeAgentDefinition | None:
        return self._agents.get(id_)


class _InMemoryLLMConnectionRepository:
    def __init__(
        self,
        connections: dict[uuid.UUID, Any],
        *,
        registry: SecretProviderRegistry,
    ) -> None:
        self._connections = dict(connections)
        self._registry = registry

    async def get_by_id(self, id_: object) -> LLMConnectionSecret | None:
        record = self._connections.get(id_)
        if record is None:
            return None
        if record.api_key_secret is None:
            return record.connection
        return LLMConnectionSecret(
            id=record.connection.id,
            name=record.connection.name,
            description=record.connection.description,
            provider=record.connection.provider,
            api_key=self._registry.resolve(record.api_key_secret),
            model=record.connection.model,
            configuration=dict(record.connection.configuration or {}),
            is_active=record.connection.is_active,
            default=record.connection.default,
            workspace_id=record.connection.workspace_id,
            created_at=record.connection.created_at,
            updated_at=record.connection.updated_at,
        )


class _InMemoryThreadRepository:
    def __init__(self) -> None:
        self._threads: dict[uuid.UUID, RuntimeThread] = {}

    def add(self, instance: RuntimeThread) -> RuntimeThread:
        self._threads[instance.id] = instance
        return instance

    async def save(self, instance: RuntimeThread) -> RuntimeThread:
        self._threads[instance.id] = instance
        return instance

    async def get_by_id(self, id_: object) -> RuntimeThread | None:
        return self._threads.get(id_)

    async def delete(self, id_: object) -> None:
        self._threads.pop(id_, None)

    async def list_for_actor(self, actor_id: uuid.UUID | None = None) -> list[RuntimeThread]:
        if actor_id is None:
            return []
        items = [
            thread
            for thread in self._threads.values()
            if thread.created_by == actor_id
        ]
        items.sort(key=lambda thread: thread.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return items

    async def list_for_workspace(self, workspace_id: uuid.UUID) -> list[RuntimeThread]:
        items = [
            thread
            for thread in self._threads.values()
            if thread.workspace_id == workspace_id
        ]
        items.sort(key=lambda thread: thread.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return items


class _InMemoryThreadMessageRepository:
    def __init__(self) -> None:
        self.items: list[RuntimeThreadMessage] = []

    def add(self, instance: RuntimeThreadMessage) -> RuntimeThreadMessage:
        self.items.append(instance)
        return instance

    async def list_for_thread(self, thread_id) -> list[RuntimeThreadMessage]:
        return [item for item in self.items if item.thread_id == thread_id]

    async def delete_for_thread(self, thread_id) -> None:
        self.items = [item for item in self.items if item.thread_id != thread_id]


class _InMemoryConversationMemoryRepository:
    def __init__(self) -> None:
        self._items: list[RuntimeConversationMemoryItem] = []

    async def list_for_thread(
        self,
        thread_id: uuid.UUID,
        *,
        limit: int = 200,
    ) -> list[RuntimeConversationMemoryItem]:
        items = [item for item in self._items if item.thread_id == thread_id]
        items.sort(key=lambda item: item.created_at or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
        return items[: max(1, int(limit))]

    def create_item(
        self,
        *,
        thread_id: uuid.UUID,
        category: str,
        content: str,
        metadata_json: dict[str, Any] | None = None,
        actor_id: uuid.UUID | None = None,
    ) -> RuntimeConversationMemoryItem | None:
        clean_content = str(content or "").strip()
        if not clean_content:
            return None

        try:
            category_enum = RuntimeConversationMemoryCategory(str(category))
        except ValueError:
            category_enum = RuntimeConversationMemoryCategory.fact

        timestamp = datetime.now(timezone.utc)
        item = RuntimeConversationMemoryItem(
            id=uuid.uuid4(),
            thread_id=thread_id,
            actor_id=actor_id,
            category=category_enum,
            content=clean_content,
            metadata=dict(metadata_json or {}),
            created_at=timestamp,
            updated_at=timestamp,
            last_accessed_at=None,
        )
        self._items.append(item)
        return item

    async def touch_items(self, item_ids) -> None:
        timestamp = datetime.now(timezone.utc)
        target_ids = {item_id for item_id in item_ids if isinstance(item_id, uuid.UUID)}
        for item in self._items:
            if item.id in target_ids:
                item.last_accessed_at = timestamp

    async def flush(self) -> None:
        return None


class _InMemorySemanticModelStore:
    def __init__(self, models: dict[uuid.UUID, SemanticModelMetadata]) -> None:
        self._models = dict(models)

    async def get_by_id(self, model_id: uuid.UUID) -> SemanticModelMetadata | None:
        return self._models.get(model_id)

    async def get_by_ids(self, model_ids: list[uuid.UUID]) -> list[SemanticModelMetadata]:
        return [self._models[model_id] for model_id in model_ids if model_id in self._models]

    def upsert(self, model: SemanticModelMetadata) -> None:
        self._models[model.id] = model


__all__ = [
    "_InMemoryAgentRepository",
    "_InMemoryConnectorSyncStateRepository",
    "_InMemoryConversationMemoryRepository",
    "_InMemoryDatasetColumnRepository",
    "_InMemoryDatasetPolicyRepository",
    "_InMemoryDatasetRepository",
    "_InMemoryDatasetRevisionRepository",
    "_InMemoryLLMConnectionRepository",
    "_InMemoryLineageEdgeRepository",
    "_InMemorySemanticModelStore",
    "_InMemoryThreadMessageRepository",
    "_InMemoryThreadRepository",
]
