import copy
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from langbridge.packages.runtime.utils.connector_runtime import (
    build_connector_runtime_payload,
)
from langbridge.packages.runtime.context import RuntimeContext
from langbridge.packages.runtime.execution import FederatedQueryTool
from langbridge.packages.runtime.models import (
    ConnectionMetadata,
    ConnectionPolicy,
    ConnectorMetadata,
    ConnectorSyncState,
    CreateAgentJobRequest,
    CreateSqlJobRequest,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    JobType,
    LLMConnectionSecret,
    LineageEdge,
    RuntimeAgentDefinition,
    RuntimeConversationMemoryCategory,
    RuntimeConversationMemoryItem,
    RuntimeMessageRole,
    RuntimeThread,
    RuntimeThreadMessage,
    RuntimeThreadState,
    SemanticModelMetadata,
    SecretReference,
    SqlWorkbenchMode,
)
from langbridge.packages.runtime.providers import (
    MemoryConnectorProvider,
    MemorySemanticModelProvider,
    RepositoryDatasetMetadataProvider,
    RepositorySyncStateProvider,
    SecretRegistryCredentialProvider,
)
from langbridge.packages.runtime.security import SecretProviderRegistry
from langbridge.packages.runtime.services.agent_execution_service import AgentExecutionService
from langbridge.packages.runtime.services.dataset_query_service import DatasetQueryService
from langbridge.packages.runtime.services.dataset_sync_service import ConnectorSyncRuntime
from langbridge.packages.runtime.services.runtime_host import (
    RuntimeHost,
    RuntimeProviders,
    RuntimeServices,
)
from langbridge.packages.connectors.langbridge_connectors.api import (
    ApiConnectorFactory,
    ApiResource,
    ConnectorFamily,
    ConnectorRuntimeType,
    get_connector_config_factory,
    get_connector_plugin,
)
from langbridge.packages.runtime.services.semantic_query_execution_service import (
    SemanticQueryExecutionService,
)
from langbridge.packages.runtime.services.sql_query_service import SqlQueryService
from langbridge.packages.runtime.settings import runtime_settings as settings
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery


class LocalRuntimeConnectorConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    type: str
    description: str | None = None
    connection: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    secrets: dict[str, Any] = Field(default_factory=dict)
    policy: dict[str, Any] | None = None
    managed: bool = False


class LocalRuntimeDatasetSourceConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    table: str | None = None
    sql: str | None = None
    path: str | None = None
    storage_uri: str | None = None
    format: str | None = None
    file_format: str | None = None
    header: bool | None = None
    delimiter: str | None = None
    quote: str | None = None

    @model_validator(mode="after")
    def _validate_source(self) -> "LocalRuntimeDatasetSourceConfig":
        has_table = bool(str(self.table or "").strip())
        has_sql = bool(str(self.sql or "").strip())
        has_file = bool(
            str(self.path or "").strip()
            or str(self.storage_uri or "").strip()
        )
        configured_modes = sum((has_table, has_sql, has_file))
        if configured_modes != 1:
            raise ValueError("Dataset source must define exactly one of table, sql, or path/storage_uri.")
        return self


class LocalRuntimeDatasetPolicyConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    max_rows_preview: int | None = Field(default=None, ge=1)
    max_export_rows: int | None = Field(default=None, ge=1)
    redaction_rules: dict[str, str] = Field(default_factory=dict)
    row_filters: list[str] = Field(default_factory=list)
    allow_dml: bool = False


class LocalRuntimeDatasetConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    label: str | None = None
    description: str | None = None
    connector: str
    source: LocalRuntimeDatasetSourceConfig
    semantic_model: str | None = None
    default_time_dimension: str | None = None
    tags: list[str] = Field(default_factory=list)
    policy: LocalRuntimeDatasetPolicyConfig | None = None


class LocalRuntimeSemanticModelConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    description: str | None = None
    default: bool = False
    model: dict[str, Any] | None = None
    datasets: list[str] = Field(default_factory=list)


class LocalRuntimeLLMConnectionConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    provider: str
    model: str
    description: str | None = None
    api_key: str | None = None
    api_key_secret: SecretReference | None = None
    configuration: dict[str, Any] = Field(default_factory=dict)
    default: bool = False

    @model_validator(mode="after")
    def _validate_credentials(self) -> "LocalRuntimeLLMConnectionConfig":
        if not str(self.api_key or "").strip() and self.api_key_secret is None:
            raise ValueError("LLM connection must define api_key or api_key_secret.")
        return self


class LocalRuntimeAgentConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    description: str | None = None
    llm_connection: str | None = None
    semantic_model: str
    dataset: str
    default: bool = False
    instructions: str | None = None
    definition: dict[str, Any] | None = None


class LocalRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    version: int | str = 1
    runtime: dict[str, Any] = Field(default_factory=dict)
    connectors: list[LocalRuntimeConnectorConfig] = Field(default_factory=list)
    datasets: list[LocalRuntimeDatasetConfig] = Field(default_factory=list)
    semantic_models: list[LocalRuntimeSemanticModelConfig] = Field(default_factory=list)
    llm_connections: list[LocalRuntimeLLMConnectionConfig] = Field(default_factory=list)
    agents: list[LocalRuntimeAgentConfig] = Field(default_factory=list)


@dataclass(slots=True, frozen=True)
class LocalRuntimeDatasetRecord:
    id: uuid.UUID
    name: str
    label: str
    description: str | None
    connector_name: str
    relation_name: str
    semantic_model_name: str | None
    default_time_dimension: str | None


@dataclass(slots=True, frozen=True)
class LocalRuntimeSemanticModelRecord:
    id: uuid.UUID
    name: str
    semantic_model: SemanticModel


@dataclass(slots=True, frozen=True)
class LocalRuntimeLLMConnectionRecord:
    id: uuid.UUID
    name: str
    connection: LLMConnectionSecret
    api_key_secret: SecretReference | None = None


@dataclass(slots=True, frozen=True)
class LocalRuntimeAgentRecord:
    id: uuid.UUID
    config: LocalRuntimeAgentConfig
    agent_definition: RuntimeAgentDefinition


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
        project_id: uuid.UUID | None = None,
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
            str(dataset_type).strip().upper()
            for dataset_type in (dataset_types or [])
            if str(dataset_type).strip()
        }

        items: list[DatasetMetadata] = []
        for dataset in self._datasets.values():
            if dataset.workspace_id != workspace_id:
                continue
            if project_id is not None and dataset.project_id not in {None, project_id}:
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
            if normalized_types and str(dataset.dataset_type or "").upper() not in normalized_types:
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
            if str(dataset.dataset_type or "").upper() != "FILE":
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
            str(dataset_type).strip().upper()
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
                or str(dataset.dataset_type or "").upper() in normalized_types
            )
        ]
        items.sort(
            key=lambda dataset: dataset.updated_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return items[: max(1, int(limit))]


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
                str(state.resource_name or "").lower(),
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
            str(instance.resource_name or "").strip(),
        )
        self._states[key] = instance
        return instance

    async def save(self, instance: ConnectorSyncState) -> ConnectorSyncState:
        return self.add(instance)


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


class _InMemoryAgentRepository:
    def __init__(self, agents: dict[uuid.UUID, RuntimeAgentDefinition]) -> None:
        self._agents = dict(agents)

    async def get_by_id(self, id_: object) -> RuntimeAgentDefinition | None:
        return self._agents.get(id_)


class _InMemoryLLMConnectionRepository:
    def __init__(
        self,
        connections: dict[uuid.UUID, LocalRuntimeLLMConnectionRecord],
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


class _InMemoryThreadMessageRepository:
    def __init__(self) -> None:
        self.items: list[RuntimeThreadMessage] = []

    def add(self, instance: RuntimeThreadMessage) -> RuntimeThreadMessage:
        self.items.append(instance)
        return instance

    async def list_for_thread(self, thread_id: uuid.UUID) -> list[RuntimeThreadMessage]:
        return sorted(
            [message for message in self.items if message.thread_id == thread_id],
            key=lambda message: message.created_at or datetime.min.replace(tzinfo=timezone.utc),
        )


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
        user_id: uuid.UUID | None,
        category: str,
        content: str,
        metadata_json: dict[str, Any] | None = None,
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
            user_id=user_id,
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

    async def get_by_ids(
        self,
        model_ids: list[uuid.UUID],
    ) -> list[SemanticModelMetadata]:
        return [self._models[model_id] for model_id in model_ids if model_id in self._models]


@dataclass(slots=True)
class _ConfiguredLocalRuntimeResources:
    runtime_host: RuntimeHost
    datasets: dict[str, LocalRuntimeDatasetRecord]
    datasets_by_id: dict[uuid.UUID, LocalRuntimeDatasetRecord]
    connectors: dict[str, ConnectorMetadata]
    semantic_models: dict[str, LocalRuntimeSemanticModelRecord]
    agents: dict[str, LocalRuntimeAgentRecord]
    default_agent: LocalRuntimeAgentRecord | None
    default_semantic_model_name: str | None
    dataset_repository: _InMemoryDatasetRepository
    dataset_column_repository: _InMemoryDatasetColumnRepository
    dataset_policy_repository: _InMemoryDatasetPolicyRepository
    dataset_revision_repository: _InMemoryDatasetRevisionRepository
    lineage_edge_repository: _InMemoryLineageEdgeRepository
    connector_sync_state_repository: _InMemoryConnectorSyncStateRepository
    secret_provider_registry: SecretProviderRegistry
    thread_repository: _InMemoryThreadRepository
    thread_message_repository: _InMemoryThreadMessageRepository


def _stable_uuid(namespace: str, value: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"langbridge:{namespace}:{value}")


def _resolve_relative_path(base_dir: Path, value: str | None) -> str | None:
    normalized = str(value or "").strip()
    if not normalized or normalized == ":memory:":
        return normalized or None
    candidate = Path(normalized)
    if candidate.is_absolute():
        return str(candidate)
    return str((base_dir / candidate).resolve())


def _resolve_storage_uri(base_dir: Path, value: str | None) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if "://" in normalized:
        return normalized
    resolved_path = _resolve_relative_path(base_dir, normalized)
    if not resolved_path:
        return None
    return Path(resolved_path).resolve().as_uri()


def _dataset_sql_alias(name: str) -> str:
    alias = re.sub(r"[^a-z0-9_]+", "_", str(name or "").strip().lower())
    alias = re.sub(r"_+", "_", alias).strip("_")
    if not alias:
        return "dataset"
    if alias[0].isdigit():
        return f"dataset_{alias}"
    return alias


def _relation_parts(relation_name: str) -> tuple[str | None, str | None, str]:
    parts = [part.strip() for part in str(relation_name or "").split(".") if part.strip()]
    if not parts:
        raise ValueError("Dataset table source must not be empty.")
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return parts[0], parts[1], parts[2]


def _connector_runtime_type(connector_type: str) -> str:
    return str(connector_type or "").strip().upper()


def _connector_dialect(connector_type: str) -> str:
    normalized = _connector_runtime_type(connector_type)
    dialect_map = {
        "POSTGRES": "postgres",
        "MYSQL": "mysql",
        "MARIADB": "mysql",
        "SNOWFLAKE": "snowflake",
        "REDSHIFT": "redshift",
        "BIGQUERY": "bigquery",
        "SQLSERVER": "tsql",
        "ORACLE": "oracle",
        "SQLITE": "sqlite",
    }
    return dialect_map.get(normalized, normalized.lower() or "tsql")


def _extract_connection_metadata(payload: Mapping[str, Any]) -> ConnectionMetadata | None:
    known_keys = {"host", "port", "database", "schema", "warehouse", "role", "account", "user"}
    metadata_payload: dict[str, Any] = {}
    extra_payload: dict[str, Any] = {}
    for key, value in payload.items():
        if key in known_keys:
            metadata_payload[key] = value
        else:
            extra_payload[key] = value
    if not metadata_payload and not extra_payload:
        return None
    metadata_payload["extra"] = extra_payload
    return ConnectionMetadata.model_validate(metadata_payload)


class ConfiguredLocalRuntimeHost(RuntimeHost):
    def __init__(
        self,
        *,
        config_path: Path,
        context: RuntimeContext,
        runtime_host: RuntimeHost,
        datasets: dict[str, LocalRuntimeDatasetRecord],
        datasets_by_id: dict[uuid.UUID, LocalRuntimeDatasetRecord],
        connectors: dict[str, ConnectorMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
        agents: dict[str, LocalRuntimeAgentRecord],
        default_agent: LocalRuntimeAgentRecord | None,
        default_semantic_model_name: str | None,
        dataset_repository: _InMemoryDatasetRepository,
        dataset_column_repository: _InMemoryDatasetColumnRepository,
        dataset_policy_repository: _InMemoryDatasetPolicyRepository,
        connector_sync_state_repository: _InMemoryConnectorSyncStateRepository,
        secret_provider_registry: SecretProviderRegistry,
        thread_repository: _InMemoryThreadRepository,
        thread_message_repository: _InMemoryThreadMessageRepository,
    ) -> None:
        self._config_path = config_path
        self._runtime_host = runtime_host
        self._datasets = datasets
        self._datasets_by_id = datasets_by_id
        self._connectors = connectors
        self._semantic_models = semantic_models
        self._agents = agents
        self._default_agent = default_agent
        self._default_semantic_model_name = default_semantic_model_name
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._dataset_policy_repository = dataset_policy_repository
        self._connector_sync_state_repository = connector_sync_state_repository
        self._secret_provider_registry = secret_provider_registry
        self._api_connector_factory = ApiConnectorFactory()
        self._thread_repository = thread_repository
        self._thread_message_repository = thread_message_repository
        self.context = context

    @property
    def providers(self):
        return self._runtime_host.providers

    @property
    def services(self):
        return self._runtime_host.services

    def __getattr__(self, name: str) -> Any:
        return getattr(self._runtime_host, name)

    async def list_datasets(self) -> list[dict[str, Any]]:
        records = await self._dataset_repository.list_for_workspace(
            workspace_id=self.context.workspace_id,
            project_id=None,
            limit=1000,
            offset=0,
        )
        items: list[dict[str, Any]] = []
        for dataset in records:
            configured_record = self._datasets_by_id.get(dataset.id)
            connector_name = None
            if dataset.connection_id is not None:
                connector = next(
                    (candidate for candidate in self._connectors.values() if candidate.id == dataset.connection_id),
                    None,
                )
                connector_name = connector.name if connector is not None else None
            items.append(
                {
                    "id": dataset.id,
                    "name": dataset.name,
                    "label": configured_record.label if configured_record is not None else dataset.name,
                    "description": dataset.description,
                    "connector": connector_name,
                    "semantic_model": configured_record.semantic_model_name if configured_record is not None else None,
                    "managed": "managed" in {str(tag).strip().lower() for tag in (dataset.tags_json or [])},
                }
            )
        return items

    async def query_dataset(self, *, request) -> dict[str, Any]:
        payload = await self._runtime_host.query_dataset(request=request)
        return self._normalize_dataset_query_payload(payload)

    async def query_semantic(self, *args: Any, **kwargs: Any) -> Any:
        return await self._runtime_host.query_semantic(*args, **kwargs)

    async def execute_sql(self, *, request) -> dict[str, Any]:
        payload = await self._runtime_host.execute_sql(request=request)
        normalized = dict(payload or {})
        stats = normalized.get("stats")
        if isinstance(stats, dict):
            normalized.setdefault("generated_sql", stats.get("query_sql"))
        return normalized

    async def create_agent(self, *args: Any, **kwargs: Any) -> Any:
        return await self._runtime_host.create_agent(*args, **kwargs)

    async def query_semantic_models(
        self,
        *,
        semantic_models: list[str] | None = None,
        measures: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
        time_dimensions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        semantic_model_records = self._resolve_semantic_models(semantic_models)
        semantic_query = SemanticQuery(
            measures=self._normalize_semantic_members(
                members=measures,
                semantic_models=semantic_model_records,
            ),
            dimensions=self._normalize_semantic_members(
                members=dimensions,
                semantic_models=semantic_model_records,
            ),
            filters=self._normalize_semantic_filters_for_models(
                semantic_models=semantic_model_records,
                filters=filters,
            ),
            timeDimensions=self._normalize_time_dimensions_for_models(
                semantic_models=semantic_model_records,
                time_dimensions=time_dimensions,
            ),
            order=self._normalize_order_for_models(
                semantic_models=semantic_model_records,
                order=order,
            ),
            limit=int(limit) if limit else None,
        )
        if len(semantic_model_records) == 1:
            semantic_model_record = semantic_model_records[0]
            result = await self._runtime_host.query_semantic(
                organization_id=self.context.workspace_id,
                project_id=None,
                semantic_model_id=semantic_model_record.id,
                semantic_query=semantic_query,
            )
            semantic_model_ids = [semantic_model_record.id]
            semantic_model_id = semantic_model_record.id
            connector_id = None
        else:
            result = await self._runtime_host.query_unified_semantic(
                organization_id=self.context.workspace_id,
                project_id=None,
                semantic_model_ids=[record.id for record in semantic_model_records],
                semantic_query=semantic_query,
            )
            semantic_model_ids = list(result.response.semantic_model_ids)
            semantic_model_id = None
            connector_id = result.response.connector_id

        rows = self._normalize_semantic_rows(
            rows=result.response.data,
            semantic_models=semantic_model_records,
        )
        output_fields = [
            *self._display_semantic_members(dimensions or []),
            *[self._display_time_dimension(item) for item in (time_dimensions or [])],
            *self._display_semantic_members(measures or []),
        ]
        response_payload = {
            "rows": rows,
            "columns": self._columns_from_rows(rows, fallback_names=output_fields),
            "row_count": len(rows),
            "annotations": list(result.response.annotations or []),
            "metadata": list(result.response.metadata or []),
            "generated_sql": result.compiled_sql,
            "semantic_model_ids": semantic_model_ids,
        }
        if semantic_model_id is not None:
            response_payload["semantic_model_id"] = semantic_model_id
        if connector_id is not None:
            response_payload["connector_id"] = connector_id
        return response_payload

    async def execute_sql_text(
        self,
        *,
        query: str,
        connection_name: str | None = None,
        requested_limit: int | None = None,
    ) -> dict[str, Any]:
        connector = self._resolve_connector(connection_name)
        request = CreateSqlJobRequest(
            sql_job_id=uuid.uuid4(),
            workspace_id=self.context.workspace_id,
            project_id=None,
            user_id=self.context.user_id,
            workbench_mode=SqlWorkbenchMode.direct_sql,
            connection_id=connector.id,
            execution_mode="single",
            query=str(query or "").strip(),
            query_dialect=_connector_dialect(connector.connector_type or ""),
            params={},
            requested_limit=requested_limit,
            requested_timeout_seconds=None,
            enforced_limit=int(requested_limit or 100),
            enforced_timeout_seconds=30,
            allow_dml=False,
            allow_federation=False,
            selected_datasets=[],
            federated_datasets=[],
            explain=False,
            correlation_id=self.context.request_id,
        )
        payload = await self.execute_sql(request=request)
        payload.setdefault("generated_sql", None)
        return payload

    async def ask_agent(
        self,
        *,
        prompt: str,
        agent_name: str | None = None,
    ) -> dict[str, Any]:
        agent = self._resolve_agent(agent_name)
        user_id = self.context.user_id or _stable_uuid("local-runtime-user", str(self._config_path))
        thread_id = uuid.uuid4()
        timestamp = datetime.now(timezone.utc)
        thread = RuntimeThread(
            id=thread_id,
            organization_id=self.context.workspace_id,
            project_id=self.context.workspace_id,
            title=agent.config.name,
            created_by=user_id,
            state=RuntimeThreadState.processing,
            metadata={"runtime_mode": "local_config"},
            created_at=timestamp,
            updated_at=timestamp,
        )
        self._thread_repository.add(thread)

        user_message = RuntimeThreadMessage(
            id=uuid.uuid4(),
            thread_id=thread_id,
            role=RuntimeMessageRole.user,
            content={"text": str(prompt or "").strip()},
            created_at=timestamp,
        )
        self._thread_message_repository.add(user_message)
        thread.last_message_id = user_message.id

        job_id = uuid.uuid4()
        execution = await self._runtime_host.create_agent(
            job_id=job_id,
            request=CreateAgentJobRequest(
                job_type=JobType.AGENT,
                agent_definition_id=agent.id,
                organisation_id=self.context.workspace_id,
                project_id=self.context.workspace_id,
                user_id=user_id,
                thread_id=thread_id,
            ),
            event_emitter=None,
        )
        response = getattr(execution, "response", {}) or {}
        return {
            "thread_id": thread_id,
            "job_id": job_id,
            "summary": response.get("summary"),
            "result": response.get("result"),
            "visualization": response.get("visualization"),
            "error": response.get("error"),
        }

    async def list_connectors(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for connector in self._connectors.values():
            plugin = self._get_connector_plugin(connector)
            items.append(
                {
                    "id": connector.id,
                    "name": connector.name,
                    "description": connector.description,
                    "connector_type": connector.connector_type,
                    "supports_sync": self._connector_supports_sync(connector),
                    "supported_resources": list(plugin.supported_resources) if plugin is not None else [],
                    "sync_strategy": (
                        plugin.sync_strategy.value
                        if plugin is not None and plugin.sync_strategy is not None
                        else None
                    ),
                    "managed": bool(connector.is_managed),
                }
            )
        return items

    async def list_sync_resources(
        self,
        *,
        connector_name: str,
    ) -> list[dict[str, Any]]:
        connector = self._resolve_connector(connector_name)
        api_connector = self._build_api_connector(connector)
        await api_connector.test_connection()

        states = await self._connector_sync_state_repository.list_for_connection(
            workspace_id=self.context.workspace_id,
            connection_id=connector.id,
        )
        states_by_resource = {
            str(state.resource_name or "").strip(): state
            for state in states
        }
        datasets = await self._dataset_repository.list_for_connection(
            workspace_id=self.context.workspace_id,
            connection_id=connector.id,
            limit=1000,
        )
        dataset_bindings = self._datasets_for_resources(datasets)

        items: list[dict[str, Any]] = []
        for resource in await api_connector.discover_resources():
            state = states_by_resource.get(resource.name)
            bound_datasets = dataset_bindings.get(resource.name, [])
            items.append(
                {
                    "name": resource.name,
                    "label": resource.label,
                    "primary_key": resource.primary_key,
                    "parent_resource": resource.parent_resource,
                    "cursor_field": resource.cursor_field,
                    "incremental_cursor_field": resource.incremental_cursor_field,
                    "supports_incremental": bool(resource.supports_incremental),
                    "default_sync_mode": str(resource.default_sync_mode or "FULL_REFRESH"),
                    "status": str(state.status) if state is not None else "never_synced",
                    "last_cursor": state.last_cursor if state is not None else None,
                    "last_sync_at": state.last_sync_at if state is not None else None,
                    "dataset_ids": [dataset.id for dataset in bound_datasets],
                    "dataset_names": [dataset.name for dataset in bound_datasets],
                    "records_synced": int(state.records_synced or 0) if state is not None else 0,
                    "bytes_synced": state.bytes_synced if state is not None else None,
                }
            )
        return items

    async def list_sync_states(
        self,
        *,
        connector_name: str,
    ) -> list[dict[str, Any]]:
        connector = self._resolve_connector(connector_name)
        states = await self._connector_sync_state_repository.list_for_connection(
            workspace_id=self.context.workspace_id,
            connection_id=connector.id,
        )
        datasets = await self._dataset_repository.list_for_connection(
            workspace_id=self.context.workspace_id,
            connection_id=connector.id,
            limit=1000,
        )
        dataset_bindings = self._datasets_for_resources(datasets)
        return [
            {
                "id": state.id,
                "workspace_id": state.workspace_id,
                "connection_id": state.connection_id,
                "connector_name": connector.name,
                "connector_type": state.connector_type,
                "resource_name": state.resource_name,
                "sync_mode": state.sync_mode,
                "last_cursor": state.last_cursor,
                "last_sync_at": state.last_sync_at,
                "state": dict(state.state_json or {}),
                "status": state.status,
                "error_message": state.error_message,
                "records_synced": int(state.records_synced or 0),
                "bytes_synced": state.bytes_synced,
                "dataset_ids": [dataset.id for dataset in dataset_bindings.get(state.resource_name, [])],
                "dataset_names": [dataset.name for dataset in dataset_bindings.get(state.resource_name, [])],
                "created_at": state.created_at,
                "updated_at": state.updated_at,
            }
            for state in states
        ]

    async def sync_connector_resources(
        self,
        *,
        connector_name: str,
        resources: list[str],
        sync_mode: str = "INCREMENTAL",
        force_full_refresh: bool = False,
    ) -> dict[str, Any]:
        if self.services.dataset_sync is None:
            raise RuntimeError("Dataset sync is not configured for this runtime host.")

        connector = self._resolve_connector(connector_name)
        connector_type = self._resolve_connector_runtime_type(connector)
        api_connector = self._build_api_connector(connector)
        await api_connector.test_connection()

        discovered_resources = {
            resource.name: resource
            for resource in await api_connector.discover_resources()
        }
        normalized_resources = [
            str(resource or "").strip()
            for resource in (resources or [])
            if str(resource or "").strip()
        ]
        if not normalized_resources:
            raise ValueError("At least one resource must be supplied for connector sync.")
        unknown_resources = [
            resource_name
            for resource_name in normalized_resources
            if resource_name not in discovered_resources
        ]
        if unknown_resources:
            raise ValueError(
                f"Unsupported resource(s) requested for sync: {', '.join(sorted(unknown_resources))}."
            )

        normalized_sync_mode = self._normalize_sync_mode(sync_mode)
        summaries: list[dict[str, Any]] = []
        active_state: ConnectorSyncState | None = None
        try:
            for resource_name in normalized_resources:
                active_state = await self.services.dataset_sync.get_or_create_state(
                    workspace_id=self.context.workspace_id,
                    connection_id=connector.id,
                    connector_type=connector_type,
                    resource_name=resource_name,
                    sync_mode=normalized_sync_mode,
                )
                active_state.status = "running"
                active_state.sync_mode = normalized_sync_mode
                active_state.error_message = None
                active_state.updated_at = datetime.now(timezone.utc)
                summary = await self._runtime_host.sync_dataset(
                    workspace_id=self.context.workspace_id,
                    project_id=None,
                    user_id=self.context.user_id,
                    connection_id=connector.id,
                    connector_record=connector,
                    connector_type=connector_type,
                    resource=discovered_resources[resource_name],
                    api_connector=api_connector,
                    state=active_state,
                    sync_mode=("FULL_REFRESH" if force_full_refresh else normalized_sync_mode),
                )
                summaries.append(summary)
        except Exception as exc:
            if active_state is not None:
                await self.services.dataset_sync.mark_failed(
                    state=active_state,
                    error_message=str(exc),
                )
            raise

        return {
            "status": "succeeded",
            "connector_id": connector.id,
            "connector_name": connector.name,
            "sync_mode": "FULL_REFRESH" if force_full_refresh else normalized_sync_mode,
            "resources": summaries,
            "summary": f"Connector sync completed for {len(summaries)} resource(s).",
        }

    def _resolve_connector(self, connection_name: str | None) -> ConnectorMetadata:
        if connection_name:
            connector = self._connectors.get(connection_name)
            if connector is None:
                raise ValueError(f"Unknown connector '{connection_name}'.")
            return connector
        if self._connectors:
            return next(iter(self._connectors.values()))
        raise ValueError("No connectors are configured for the local runtime.")

    def _resolve_agent(self, agent_name: str | None) -> LocalRuntimeAgentRecord:
        if agent_name:
            agent = self._agents.get(agent_name)
            if agent is None:
                raise ValueError(f"Unknown agent '{agent_name}'.")
            return agent
        if self._default_agent is not None:
            return self._default_agent
        if self._agents:
            return next(iter(self._agents.values()))
        raise ValueError("No agents are configured for this local runtime.")

    def _resolve_semantic_models(
        self,
        semantic_model_names: list[str] | None,
    ) -> list[LocalRuntimeSemanticModelRecord]:
        requested_names = [str(name).strip() for name in (semantic_model_names or []) if str(name).strip()]
        if not requested_names:
            if self._default_semantic_model_name:
                requested_names = [self._default_semantic_model_name]
            elif len(self._semantic_models) == 1:
                requested_names = [next(iter(self._semantic_models))]
            else:
                raise ValueError("semantic_models is required when multiple semantic models are configured.")

        resolved: list[LocalRuntimeSemanticModelRecord] = []
        seen_ids: set[uuid.UUID] = set()
        for name in requested_names:
            semantic_model = self._semantic_models.get(name)
            if semantic_model is None:
                raise ValueError(f"Unknown semantic model '{name}'.")
            if semantic_model.id in seen_ids:
                continue
            resolved.append(semantic_model)
            seen_ids.add(semantic_model.id)
        return resolved

    def _normalize_semantic_members(
        self,
        *,
        members: list[str] | None,
        semantic_models: list[LocalRuntimeSemanticModelRecord],
    ) -> list[str]:
        return [
            self._qualify_semantic_member(
                member=str(member),
                semantic_models=semantic_models,
            )
            for member in (members or [])
            if str(member or "").strip()
        ]

    def _qualify_semantic_member(
        self,
        *,
        member: str,
        semantic_models: list[LocalRuntimeSemanticModelRecord],
    ) -> str:
        value = str(member or "").strip()
        if not value:
            raise ValueError("semantic member names must not be empty.")
        if "." in value:
            return value

        matches: list[str] = []
        for semantic_model in semantic_models:
            for dataset_name, dataset in semantic_model.semantic_model.datasets.items():
                dimension_names = {dimension.name for dimension in (dataset.dimensions or [])}
                measure_names = {measure.name for measure in (dataset.measures or [])}
                if value in dimension_names or value in measure_names:
                    matches.append(f"{dataset_name}.{value}")

        unique_matches = list(dict.fromkeys(matches))
        if len(unique_matches) == 1:
            return unique_matches[0]
        if not unique_matches:
            raise ValueError(
                f"Semantic member '{value}' was not found in the selected semantic models."
            )
        raise ValueError(
            f"Semantic member '{value}' is ambiguous across datasets; use a qualified name like 'dataset.{value}'."
        )

    def _normalize_semantic_filters_for_models(
        self,
        *,
        semantic_models: list[LocalRuntimeSemanticModelRecord],
        filters: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for filter_entry in filters or []:
            entry = dict(filter_entry or {})
            if entry.get("member"):
                entry["member"] = self._qualify_semantic_member(
                    member=str(entry["member"]),
                    semantic_models=semantic_models,
                )
            if entry.get("dimension"):
                entry["dimension"] = self._qualify_semantic_member(
                    member=str(entry["dimension"]),
                    semantic_models=semantic_models,
                )
            if entry.get("measure"):
                entry["measure"] = self._qualify_semantic_member(
                    member=str(entry["measure"]),
                    semantic_models=semantic_models,
                )
            if entry.get("timeDimension"):
                entry["timeDimension"] = self._qualify_semantic_member(
                    member=str(entry["timeDimension"]),
                    semantic_models=semantic_models,
                )
            if entry.get("time_dimension"):
                entry["time_dimension"] = self._qualify_semantic_member(
                    member=str(entry["time_dimension"]),
                    semantic_models=semantic_models,
                )
            payload.append(entry)
        return payload

    def _normalize_time_dimensions_for_models(
        self,
        *,
        semantic_models: list[LocalRuntimeSemanticModelRecord],
        time_dimensions: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for item in time_dimensions or []:
            entry = dict(item or {})
            dimension = str(entry.get("dimension") or "").strip()
            if not dimension:
                continue
            entry["dimension"] = self._qualify_semantic_member(
                member=dimension,
                semantic_models=semantic_models,
            )
            payload.append(entry)
        return payload

    def _normalize_order_for_models(
        self,
        *,
        semantic_models: list[LocalRuntimeSemanticModelRecord],
        order: dict[str, str] | list[dict[str, str]] | None,
    ) -> dict[str, str] | list[dict[str, str]] | None:
        if order is None:
            return None
        entries = [order] if isinstance(order, dict) else list(order)
        normalized: list[dict[str, str]] = []
        for entry in entries:
            item_payload: dict[str, str] = {}
            for key, value in entry.items():
                item_payload[
                    self._qualify_semantic_member(
                        member=str(key),
                        semantic_models=semantic_models,
                    )
                ] = str(value)
            if item_payload:
                normalized.append(item_payload)
        if isinstance(order, dict):
            return normalized[0] if normalized else None
        return normalized

    @staticmethod
    def _display_semantic_members(members: list[str]) -> list[str]:
        return [
            str(member)
            for member in members
            if str(member or "").strip()
        ]

    @staticmethod
    def _columns_from_rows(
        rows: list[dict[str, Any]],
        *,
        fallback_names: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if rows:
            return [{"name": str(name), "data_type": None} for name in rows[0].keys()]
        return [
            {"name": str(name), "data_type": None}
            for name in (fallback_names or [])
            if str(name or "").strip()
        ]

    def _display_time_dimension(self, item: dict[str, Any]) -> str:
        dimension = str(item.get("dimension") or "").strip().split(".")[-1]
        granularity = str(item.get("granularity") or "").strip().lower()
        if dimension and granularity:
            return f"{dimension}_{granularity}"
        return dimension

    @staticmethod
    def _normalize_semantic_rows(
        *,
        rows: list[dict[str, Any]],
        semantic_models: list[LocalRuntimeSemanticModelRecord],
    ) -> list[dict[str, Any]]:
        dataset_names = {
            dataset_name
            for semantic_model in semantic_models
            for dataset_name in semantic_model.semantic_model.datasets.keys()
        }
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            normalized_row: dict[str, Any] = {}
            for key, value in row.items():
                normalized_key = str(key)
                if "__" in normalized_key:
                    dataset_name, suffix = normalized_key.split("__", 1)
                    if dataset_name in dataset_names:
                        normalized_key = f"{dataset_name}.{suffix}"
                normalized_row[normalized_key] = value
            normalized_rows.append(normalized_row)
        return normalized_rows

    def _normalize_dataset_query_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        normalized.setdefault("generated_sql", normalized.get("query_sql"))
        dataset_id = normalized.get("dataset_id")
        try:
            dataset_uuid = uuid.UUID(str(dataset_id))
        except (TypeError, ValueError):
            dataset_uuid = None
        if dataset_uuid is not None and "dataset_name" not in normalized:
            record = self._datasets_by_id.get(dataset_uuid)
            if record is not None:
                normalized["dataset_name"] = record.name
            else:
                dataset = self._dataset_repository._datasets.get(dataset_uuid)
                if dataset is not None:
                    normalized["dataset_name"] = dataset.name
        return normalized

    def _get_connector_plugin(self, connector: ConnectorMetadata):
        connector_type = self._resolve_connector_runtime_type(connector)
        return get_connector_plugin(connector_type)

    def _connector_supports_sync(self, connector: ConnectorMetadata) -> bool:
        plugin = self._get_connector_plugin(connector)
        return bool(
            plugin is not None
            and plugin.connector_family == ConnectorFamily.API
            and plugin.api_connector_class is not None
        )

    def _resolve_connector_runtime_type(self, connector: ConnectorMetadata) -> ConnectorRuntimeType:
        raw_type = str(connector.connector_type or "").strip().upper()
        if not raw_type:
            raise ValueError(f"Connector '{connector.name}' does not define a connector_type.")
        return ConnectorRuntimeType(raw_type)

    def _build_api_connector(self, connector: ConnectorMetadata):
        if not self._connector_supports_sync(connector):
            raise ValueError(f"Connector '{connector.name}' does not support runtime sync.")
        connector_type = self._resolve_connector_runtime_type(connector)
        runtime_payload = build_connector_runtime_payload(
            config_json=connector.config,
            connection_metadata=(
                connector.connection_metadata.model_dump(mode="json")
                if connector.connection_metadata is not None
                else None
            ),
            secret_references={
                key: value.model_dump(mode="json")
                for key, value in (connector.secret_references or {}).items()
            },
            secret_resolver=self._secret_provider_registry.resolve,
        )
        config_factory = get_connector_config_factory(connector_type)
        return self._api_connector_factory.create_api_connector(
            connector_type,
            config_factory.create(runtime_payload.get("config") or {}),
            logger=logging.getLogger("langbridge.runtime.sync.local"),
        )

    @staticmethod
    def _normalize_sync_mode(value: str | None) -> str:
        normalized = str(value or "INCREMENTAL").strip().upper()
        if normalized not in {"INCREMENTAL", "FULL_REFRESH"}:
            raise ValueError("sync_mode must be either INCREMENTAL or FULL_REFRESH.")
        return normalized

    @staticmethod
    def _datasets_for_resources(
        datasets: list[DatasetMetadata],
    ) -> dict[str, list[DatasetMetadata]]:
        bindings: dict[str, list[DatasetMetadata]] = {}
        for dataset in datasets:
            file_config = dict(dataset.file_config_json or {})
            sync_meta = file_config.get("connector_sync")
            if not isinstance(sync_meta, Mapping):
                continue
            resource_name = str(sync_meta.get("resource_name") or "").strip()
            if not resource_name:
                continue
            bindings.setdefault(resource_name, []).append(dataset)
        return bindings

    async def _infer_time_dimensions(
        self,
        *,
        prompt: str,
        dataset_record: LocalRuntimeDatasetRecord,
        semantic_model: SemanticModel,
    ) -> list[dict[str, Any]]:
        if "quarter" not in prompt.lower():
            return []
        time_dimension = dataset_record.default_time_dimension
        if not time_dimension:
            dataset = semantic_model.datasets[dataset_record.name]
            time_dimension = next(
                (dimension.name for dimension in (dataset.dimensions or []) if dimension.type == "time"),
                None,
            )
        if not time_dimension:
            return []
        max_date = await self._get_max_date(
            connector_name=dataset_record.connector_name,
            relation_name=dataset_record.relation_name,
            column_name=time_dimension,
        )
        if max_date is None:
            return []
        quarter_start_month = ((max_date.month - 1) // 3) * 3 + 1
        quarter_start = date(max_date.year, quarter_start_month, 1)
        if quarter_start_month == 10:
            quarter_end = date(max_date.year, 12, 31)
        else:
            next_quarter = date(max_date.year, quarter_start_month + 3, 1)
            quarter_end = date.fromordinal(next_quarter.toordinal() - 1)
        return [{"dimension": time_dimension, "dateRange": [quarter_start.isoformat(), quarter_end.isoformat()]}]

    async def _get_max_date(
        self,
        *,
        connector_name: str,
        relation_name: str,
        column_name: str,
    ) -> date | None:
        result = await self.execute_sql_text(
            query=f"SELECT MAX({column_name}) AS max_value FROM {relation_name}",
            connection_name=connector_name,
            requested_limit=1,
        )
        rows = result.get("rows") if isinstance(result, dict) else []
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[0] if isinstance(rows[0], dict) else {}
        raw_value = row.get("max_value")
        if raw_value is None:
            return None
        return datetime.fromisoformat(str(raw_value)).date()

    def _infer_dimension(
        self,
        *,
        prompt: str,
        semantic_model: SemanticModel,
        dataset_name: str,
    ) -> str | None:
        lowered = prompt.lower()
        dataset = semantic_model.datasets[dataset_name]
        for dimension in dataset.dimensions or []:
            candidates = {dimension.name.lower(), *(item.lower() for item in (dimension.synonyms or []))}
            if any(candidate in lowered for candidate in candidates):
                return dimension.name
            if dimension.name.endswith("y") and dimension.name[:-1] + "ies" in lowered:
                return dimension.name
        return next((dimension.name for dimension in (dataset.dimensions or []) if dimension.type != "time"), None)

    def _infer_metric(
        self,
        *,
        prompt: str,
        semantic_model: SemanticModel,
        dataset_name: str,
    ) -> str | None:
        lowered = prompt.lower()
        dataset = semantic_model.datasets[dataset_name]
        for measure in dataset.measures or []:
            candidates = {measure.name.lower(), *(item.lower() for item in (measure.synonyms or []))}
            if any(candidate in lowered for candidate in candidates):
                return measure.name
        return next((measure.name for measure in (dataset.measures or [])), None)

    @staticmethod
    def _summarize_agent_response(
        *,
        prompt: str,
        metric: str | None,
        dimension: str | None,
        rows: list[dict[str, Any]],
    ) -> str:
        if not rows:
            return "No matching rows were found for the question."
        first_row = rows[0]
        if metric and dimension and dimension in first_row and metric in first_row:
            return (
                f"{first_row[dimension]} is leading for {metric.replace('_', ' ')} "
                f"at {first_row[metric]}."
            )
        return f"Answered question: {prompt}"


class ConfiguredLocalRuntimeHostFactory:
    @staticmethod
    def build(
        *,
        config_path: str | Path,
        context: RuntimeContext,
    ) -> ConfiguredLocalRuntimeHost:
        resolved_config_path = Path(config_path).resolve()
        local_runtime_config = ConfiguredLocalRuntimeHostFactory._load_config(resolved_config_path)
        resources = ConfiguredLocalRuntimeHostFactory._build_resources(
            config_path=resolved_config_path,
            config=local_runtime_config,
            context=context,
        )
        return ConfiguredLocalRuntimeHost(
            config_path=resolved_config_path,
            context=context,
            runtime_host=resources.runtime_host,
            datasets=resources.datasets,
            datasets_by_id=resources.datasets_by_id,
            connectors=resources.connectors,
            semantic_models=resources.semantic_models,
            agents=resources.agents,
            default_agent=resources.default_agent,
            default_semantic_model_name=resources.default_semantic_model_name,
            dataset_repository=resources.dataset_repository,
            dataset_column_repository=resources.dataset_column_repository,
            dataset_policy_repository=resources.dataset_policy_repository,
            connector_sync_state_repository=resources.connector_sync_state_repository,
            secret_provider_registry=resources.secret_provider_registry,
            thread_repository=resources.thread_repository,
            thread_message_repository=resources.thread_message_repository,
        )

    @staticmethod
    def _load_config(path: Path) -> LocalRuntimeConfig:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return LocalRuntimeConfig.model_validate(payload)

    @staticmethod
    def _build_resources(
        *,
        config_path: Path,
        config: LocalRuntimeConfig,
        context: RuntimeContext,
    ) -> _ConfiguredLocalRuntimeResources:
        connector_models = ConfiguredLocalRuntimeHostFactory._build_connector_models(
            config_path=config_path,
            config=config,
            context=context,
        )
        dataset_models, dataset_records = ConfiguredLocalRuntimeHostFactory._build_dataset_models(
            config_path=config_path,
            config=config,
            context=context,
            connectors=connector_models,
        )
        semantic_models = ConfiguredLocalRuntimeHostFactory._build_semantic_model_records(
            config=config,
            context=context,
            datasets=dataset_models,
        )
        llm_connections = ConfiguredLocalRuntimeHostFactory._build_llm_connection_records(
            config_path=config_path,
            config=config,
        )
        agents = ConfiguredLocalRuntimeHostFactory._build_agent_records(
            config_path=config_path,
            config=config,
            context=context,
            datasets=dataset_models,
            connectors=connector_models,
            semantic_models=semantic_models,
            llm_connections=llm_connections,
        )
        (
            dataset_repository_rows,
            dataset_columns,
            dataset_policies,
        ) = ConfiguredLocalRuntimeHostFactory._build_dataset_repository_records(
            datasets=dataset_models,
            semantic_models=semantic_models,
        )
        dataset_repository = _InMemoryDatasetRepository(dataset_repository_rows)
        dataset_column_repository = _InMemoryDatasetColumnRepository(dataset_columns)
        dataset_policy_repository = _InMemoryDatasetPolicyRepository(dataset_policies)
        dataset_revision_repository = _InMemoryDatasetRevisionRepository()
        lineage_edge_repository = _InMemoryLineageEdgeRepository()
        connector_sync_state_repository = _InMemoryConnectorSyncStateRepository()
        secret_provider_registry = SecretProviderRegistry()
        runtime_host, thread_repository, thread_message_repository = ConfiguredLocalRuntimeHostFactory._build_runtime_host(
            context=context,
            connectors=connector_models,
            datasets=dataset_models,
            semantic_models=semantic_models,
            llm_connections=llm_connections,
            agents=agents,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            dataset_policy_repository=dataset_policy_repository,
            dataset_revision_repository=dataset_revision_repository,
            lineage_edge_repository=lineage_edge_repository,
            connector_sync_state_repository=connector_sync_state_repository,
            secret_provider_registry=secret_provider_registry,
        )
        default_agent = next((agent for agent in agents.values() if agent.config.default), None)
        default_semantic_model_name = next(
            (item.name for item in config.semantic_models if item.default),
            config.semantic_models[0].name if config.semantic_models else None,
        )
        datasets_by_id = {record.id: record for record in dataset_records.values()}
        return _ConfiguredLocalRuntimeResources(
            runtime_host=runtime_host,
            datasets=dataset_records,
            datasets_by_id=datasets_by_id,
            connectors={connector.name: connector for connector in connector_models.values()},
            semantic_models=semantic_models,
            agents=agents,
            default_agent=default_agent,
            default_semantic_model_name=default_semantic_model_name,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            dataset_policy_repository=dataset_policy_repository,
            dataset_revision_repository=dataset_revision_repository,
            lineage_edge_repository=lineage_edge_repository,
            connector_sync_state_repository=connector_sync_state_repository,
            secret_provider_registry=secret_provider_registry,
            thread_repository=thread_repository,
            thread_message_repository=thread_message_repository,
        )

    @staticmethod
    def _build_connector_models(
        *,
        config_path: Path,
        config: LocalRuntimeConfig,
        context: RuntimeContext,
    ) -> dict[str, ConnectorMetadata]:
        connectors: dict[str, ConnectorMetadata] = {}
        for connector in config.connectors:
            connection_payload = dict(connector.connection or {})
            connector_type = _connector_runtime_type(connector.type)
            if "path" in connection_payload:
                resolved_path = _resolve_relative_path(config_path.parent, str(connection_payload.get("path")))
                if resolved_path:
                    if connector_type == "SQLITE":
                        connection_payload["location"] = resolved_path
                        connection_payload.pop("path", None)
                    else:
                        connection_payload["path"] = resolved_path
            metadata_payload = dict(connector.metadata or {})
            merged_connection = {**connection_payload, **metadata_payload}
            connector_id = _stable_uuid("connector", f"{config_path}:{connector.name}")
            connectors[connector.name] = ConnectorMetadata(
                id=connector_id,
                name=connector.name,
                description=connector.description,
                connector_type=connector_type,
                organization_id=context.workspace_id,
                project_id=None,
                config={"config": connection_payload},
                connection_metadata=_extract_connection_metadata(merged_connection),
                secret_references=dict(connector.secrets or {}),
                connection_policy=(
                    ConnectionPolicy.model_validate(connector.policy)
                    if isinstance(connector.policy, Mapping)
                    else None
                ),
                is_managed=connector.managed,
            )
        return connectors

    @staticmethod
    def _build_dataset_models(
        *,
        config_path: Path,
        config: LocalRuntimeConfig,
        context: RuntimeContext,
        connectors: dict[str, ConnectorMetadata],
    ) -> tuple[dict[str, DatasetMetadata], dict[str, LocalRuntimeDatasetRecord]]:
        datasets: dict[str, DatasetMetadata] = {}
        dataset_records: dict[str, LocalRuntimeDatasetRecord] = {}
        now = datetime.utcnow()
        for dataset in config.datasets:
            connector = connectors.get(dataset.connector)
            if connector is None:
                raise ValueError(f"Dataset '{dataset.name}' references unknown connector '{dataset.connector}'.")

            source_table = str(dataset.source.table or "").strip()
            source_sql = str(dataset.source.sql or "").strip()
            source_storage_uri = _resolve_storage_uri(
                config_path.parent,
                dataset.source.storage_uri or dataset.source.path,
            )
            dataset_id = _stable_uuid("dataset", f"{config_path}:{dataset.name}")
            if source_table:
                catalog_name, schema_name, table_name = _relation_parts(source_table)
                relation_name = source_table
                dataset_type = "TABLE"
                sql_text = None
                storage_kind = "table"
                source_kind = "database"
                dialect = _connector_dialect(connector.connector_type or "")
                storage_uri = None
                file_config = None
            else:
                catalog_name = None
                schema_name = None
                table_name = _dataset_sql_alias(dataset.name)
                relation_name = table_name
                if source_sql:
                    dataset_type = "SQL"
                    sql_text = source_sql
                    storage_kind = "view"
                    source_kind = "database"
                    dialect = _connector_dialect(connector.connector_type or "")
                    storage_uri = None
                    file_config = None
                else:
                    dataset_type = "FILE"
                    sql_text = None
                    storage_uri = source_storage_uri
                    if not storage_uri:
                        raise ValueError(
                            f"Dataset '{dataset.name}' must define source.path or source.storage_uri for file-backed datasets."
                        )
                    file_format = str(
                        dataset.source.format
                        or dataset.source.file_format
                        or (((connector.config or {}).get("config") or {}).get("format"))
                        or (((connector.config or {}).get("config") or {}).get("file_format"))
                        or ""
                    ).strip().lower()
                    if file_format not in {"csv", "parquet"}:
                        raise ValueError(
                            f"Dataset '{dataset.name}' must declare a supported file format (csv or parquet)."
                        )
                    source_kind = "file"
                    storage_kind = file_format
                    dialect = "duckdb"
                    file_config = {
                        "format": file_format,
                    }
                    if dataset.source.header is not None:
                        file_config["header"] = dataset.source.header
                    if dataset.source.delimiter is not None:
                        file_config["delimiter"] = dataset.source.delimiter
                    if dataset.source.quote is not None:
                        file_config["quote"] = dataset.source.quote

            policy = dataset.policy or LocalRuntimeDatasetPolicyConfig()
            datasets[dataset.name] = DatasetMetadata(
                id=dataset_id,
                workspace_id=context.workspace_id,
                project_id=None,
                connection_id=connector.id,
                owner_id=context.user_id,
                created_by=context.user_id,
                updated_by=context.user_id,
                name=dataset.name,
                sql_alias=_dataset_sql_alias(dataset.name),
                description=dataset.description,
                tags=list(dataset.tags),
                dataset_type=dataset_type,
                source_kind=source_kind,
                connector_kind=(connector.connector_type or "").lower() or None,
                storage_kind=storage_kind,
                dialect=dialect,
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=table_name,
                storage_uri=storage_uri,
                sql_text=sql_text,
                relation_identity={
                    "dataset_id": str(dataset_id),
                    "connector_id": str(connector.id),
                    "canonical_reference": relation_name,
                    "catalog_name": catalog_name,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "storage_uri": storage_uri,
                },
                execution_capabilities={
                    "supports_structured_scan": True,
                    "supports_sql_federation": True,
                },
                referenced_dataset_ids=[],
                federated_plan=None,
                file_config=file_config,
                status="published",
                revision_id=None,
                row_count_estimate=None,
                bytes_estimate=None,
                last_profiled_at=None,
                columns=[],
                policy=DatasetPolicyMetadata(
                    dataset_id=dataset_id,
                    workspace_id=context.workspace_id,
                    max_rows_preview=policy.max_rows_preview or settings.SQL_DEFAULT_MAX_PREVIEW_ROWS,
                    max_export_rows=policy.max_export_rows or settings.SQL_DEFAULT_MAX_EXPORT_ROWS,
                    redaction_rules=dict(policy.redaction_rules or {}),
                    row_filters=list(policy.row_filters or []),
                    allow_dml=bool(policy.allow_dml),
                ),
                created_at=now,
                updated_at=now,
            )
            dataset_records[dataset.name] = LocalRuntimeDatasetRecord(
                id=dataset_id,
                name=dataset.name,
                label=dataset.label or dataset.name.replace("_", " ").title(),
                description=dataset.description,
                connector_name=dataset.connector,
                relation_name=relation_name,
                semantic_model_name=dataset.semantic_model,
                default_time_dimension=dataset.default_time_dimension,
            )
        return datasets, dataset_records

    @staticmethod
    def _build_semantic_model_records(
        *,
        config: LocalRuntimeConfig,
        context: RuntimeContext,
        datasets: dict[str, DatasetMetadata],
    ) -> dict[str, LocalRuntimeSemanticModelRecord]:
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord] = {}
        for item in config.semantic_models:
            payload = ConfiguredLocalRuntimeHostFactory._materialize_semantic_model_payload(
                semantic_model=item,
                datasets=datasets,
            )
            semantic_model = SemanticModel.model_validate(payload)
            semantic_model_id = _stable_uuid("semantic-model", f"{context.workspace_id}:{item.name}")
            semantic_models[item.name] = LocalRuntimeSemanticModelRecord(
                id=semantic_model_id,
                name=item.name,
                semantic_model=semantic_model,
            )
        return semantic_models

    @staticmethod
    def _build_llm_connection_records(
        *,
        config_path: Path,
        config: LocalRuntimeConfig,
    ) -> dict[str, LocalRuntimeLLMConnectionRecord]:
        records: dict[str, LocalRuntimeLLMConnectionRecord] = {}
        now = datetime.now(timezone.utc)
        for llm_connection in config.llm_connections:
            connection_id = _stable_uuid("llm-connection", f"{config_path}:{llm_connection.name}")
            api_key = str(llm_connection.api_key or "").strip()
            records[llm_connection.name] = LocalRuntimeLLMConnectionRecord(
                id=connection_id,
                name=llm_connection.name,
                connection=LLMConnectionSecret(
                    id=connection_id,
                    name=llm_connection.name,
                    description=llm_connection.description,
                    provider=str(llm_connection.provider).strip().lower(),
                    api_key=api_key,
                    model=llm_connection.model,
                    configuration=dict(llm_connection.configuration or {}),
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                ),
                api_key_secret=llm_connection.api_key_secret,
            )
        return records

    @staticmethod
    def _build_agent_records(
        *,
        config_path: Path,
        config: LocalRuntimeConfig,
        context: RuntimeContext,
        datasets: dict[str, DatasetMetadata],
        connectors: dict[str, ConnectorMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
        llm_connections: dict[str, LocalRuntimeLLMConnectionRecord],
    ) -> dict[str, LocalRuntimeAgentRecord]:
        records: dict[str, LocalRuntimeAgentRecord] = {}
        now = datetime.now(timezone.utc)
        default_llm_connection_name = next(
            (item.name for item in config.llm_connections if item.default),
            config.llm_connections[0].name if config.llm_connections else None,
        )
        for agent in config.agents:
            dataset = datasets.get(agent.dataset)
            if dataset is None:
                raise ValueError(f"Agent '{agent.name}' references unknown dataset '{agent.dataset}'.")
            semantic_model = semantic_models.get(agent.semantic_model)
            if semantic_model is None:
                raise ValueError(
                    f"Agent '{agent.name}' references unknown semantic model '{agent.semantic_model}'."
                )

            llm_connection_name = str(agent.llm_connection or default_llm_connection_name or "").strip()
            if not llm_connection_name:
                raise ValueError(
                    f"Agent '{agent.name}' requires an llm_connection or a default llm_connections entry."
                )
            llm_connection = llm_connections.get(llm_connection_name)
            if llm_connection is None:
                raise ValueError(
                    f"Agent '{agent.name}' references unknown llm connection '{llm_connection_name}'."
                )

            connector_id = dataset.connection_id
            agent_id = _stable_uuid("agent", f"{config_path}:{agent.name}")
            definition = ConfiguredLocalRuntimeHostFactory._build_agent_definition_payload(
                agent=agent,
                semantic_model_id=semantic_model.id,
                connector_id=connector_id,
                connector_name=next(
                    (name for name, item in connectors.items() if item.id == connector_id),
                    None,
                ),
            )
            records[agent.name] = LocalRuntimeAgentRecord(
                id=agent_id,
                config=agent,
                agent_definition=RuntimeAgentDefinition(
                    id=agent_id,
                    name=agent.name,
                    description=agent.description,
                    llm_connection_id=llm_connection.id,
                    definition=definition,
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                ),
            )
        return records

    @staticmethod
    def _build_agent_definition_payload(
        *,
        agent: LocalRuntimeAgentConfig,
        semantic_model_id: uuid.UUID,
        connector_id: uuid.UUID | None,
        connector_name: str | None,
    ) -> dict[str, Any]:
        if agent.definition:
            return copy.deepcopy(agent.definition)

        system_prompt = (
            "You are a Langbridge analytics agent. Use the configured semantic model and tools "
            "to answer questions with grounded, concise analysis."
        )
        user_instructions = str(agent.instructions or "").strip() or None
        return {
            "prompt": {
                "system_prompt": system_prompt,
                "user_instructions": user_instructions,
                "style_guidance": "Keep answers concise and clearly grounded in query results.",
            },
            "memory": {
                "strategy": "database",
            },
            "features": {
                "bi_copilot_enabled": False,
                "deep_research_enabled": False,
                "visualization_enabled": True,
                "mcp_enabled": False,
            },
            "tools": [
                {
                    "name": f"{agent.name}_semantic_sql",
                    "tool_type": "sql",
                    "description": (
                        f"Semantic analytics access for '{agent.semantic_model}'"
                        + (f" via connector '{connector_name}'." if connector_name else ".")
                    ),
                    "config": {
                        "semantic_model_ids": [str(semantic_model_id)],
                    },
                }
            ],
            "access_policy": {
                "allowed_connectors": [str(connector_id)] if connector_id is not None else [],
                "denied_connectors": [],
            },
            "execution": {
                "mode": "iterative",
                "response_mode": "analyst",
                "max_iterations": 3,
                "max_steps_per_iteration": 5,
                "allow_parallel_tools": False,
            },
            "output": {
                "format": "markdown",
            },
            "guardrails": {
                "moderation_enabled": True,
            },
            "observability": {
                "log_level": "info",
                "emit_traces": False,
                "capture_prompts": False,
                "audit_fields": [],
            },
        }

    @staticmethod
    def _build_dataset_repository_records(
        *,
        datasets: dict[str, DatasetMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
    ) -> tuple[
        dict[uuid.UUID, DatasetMetadata],
        dict[uuid.UUID, list[DatasetColumnMetadata]],
        dict[uuid.UUID, DatasetPolicyMetadata],
    ]:
        dataset_records: dict[uuid.UUID, DatasetMetadata] = {}
        columns_by_dataset: dict[uuid.UUID, list[DatasetColumnMetadata]] = {}
        policies_by_dataset: dict[uuid.UUID, DatasetPolicyMetadata] = {}

        for dataset in datasets.values():
            record = dataset.model_copy(deep=True)
            policy = dataset.policy or DatasetPolicyMetadata(
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
            )
            policy_record = DatasetPolicyMetadata(
                id=policy.id or _stable_uuid("dataset-policy", str(dataset.id)),
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                max_rows_preview=policy.max_rows_preview,
                max_export_rows=policy.max_export_rows,
                redaction_rules=dict(policy.redaction_rules),
                row_filters=list(policy.row_filters),
                allow_dml=policy.allow_dml,
                created_at=dataset.created_at,
                updated_at=dataset.updated_at,
            )
            record.policy = policy_record
            policies_by_dataset[dataset.id] = policy_record

            seen_columns: set[str] = set()
            dataset_columns: list[DatasetColumnMetadata] = []
            ordinal = 0
            if str(dataset.source_kind or "").lower() != "file":
                for semantic_model in semantic_models.values():
                    semantic_dataset = semantic_model.semantic_model.datasets.get(dataset.name)
                    if semantic_dataset is None:
                        continue
                    for field in list(semantic_dataset.dimensions or []) + list(semantic_dataset.measures or []):
                        field_name = str(field.name).strip()
                        field_expression = str(getattr(field, "expression", None) or "").strip()
                        if field_expression and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field_expression):
                            column_name = field_expression
                        else:
                            column_name = field_name
                        if not column_name or column_name.lower() in seen_columns:
                            continue
                        seen_columns.add(column_name.lower())
                        ordinal += 1
                        dataset_columns.append(
                            DatasetColumnMetadata(
                                id=_stable_uuid("dataset-column", f"{dataset.id}:{column_name}"),
                                dataset_id=dataset.id,
                                workspace_id=dataset.workspace_id,
                                name=column_name,
                                data_type=str(getattr(field, "type", None) or "string"),
                                nullable=True,
                                ordinal_position=ordinal,
                                description=getattr(field, "description", None),
                                is_allowed=True,
                                is_computed=False,
                                expression=field_expression or None,
                                created_at=dataset.created_at,
                                updated_at=dataset.updated_at,
                            )
                        )
            record.columns = dataset_columns
            dataset_records[dataset.id] = record
            columns_by_dataset[dataset.id] = dataset_columns

        return dataset_records, columns_by_dataset, policies_by_dataset

    @staticmethod
    def _build_semantic_model_store_records(
        *,
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
        context: RuntimeContext,
    ) -> dict[uuid.UUID, SemanticModelMetadata]:
        records: dict[uuid.UUID, SemanticModelMetadata] = {}
        now = datetime.now(timezone.utc)
        for semantic_model in semantic_models.values():
            records[semantic_model.id] = SemanticModelMetadata(
                id=semantic_model.id,
                organization_id=context.workspace_id,
                project_id=None,
                name=semantic_model.name,
                description=semantic_model.semantic_model.description,
                content_yaml=semantic_model.semantic_model.yml_dump(),
                content_json=None,
                created_at=now,
                updated_at=now,
                connector_id=None,
            )
        return records

    @staticmethod
    def _materialize_semantic_model_payload(
        *,
        semantic_model: LocalRuntimeSemanticModelConfig,
        datasets: dict[str, DatasetMetadata],
    ) -> dict[str, Any]:
        payload = copy.deepcopy(semantic_model.model or {})
        if not payload:
            payload = {
                "version": "1",
                "name": semantic_model.name,
                "description": semantic_model.description,
                "datasets": {},
            }

        raw_datasets = payload.get("datasets")
        if not isinstance(raw_datasets, Mapping):
            raw_datasets = payload.get("tables")
        normalized_datasets: dict[str, Any] = {}
        if isinstance(raw_datasets, Mapping):
            normalized_datasets = {
                str(dataset_name): copy.deepcopy(dataset_payload)
                for dataset_name, dataset_payload in raw_datasets.items()
                if isinstance(dataset_payload, Mapping)
            }

        candidate_dataset_names = list(normalized_datasets.keys()) or list(semantic_model.datasets)
        for dataset_name in candidate_dataset_names:
            dataset = datasets.get(dataset_name)
            if dataset is None:
                continue
            dataset_payload = dict(normalized_datasets.get(dataset_name) or {})
            dataset_payload.setdefault("dataset_id", str(dataset.id))
            if dataset.schema_name and not dataset_payload.get("schema_name") and not dataset_payload.get("schema"):
                dataset_payload["schema_name"] = dataset.schema_name
            if dataset.catalog_name and not dataset_payload.get("catalog_name") and not dataset_payload.get("catalog"):
                dataset_payload["catalog_name"] = dataset.catalog_name
            relation_name = (
                dataset_payload.get("relation_name")
                or dataset_payload.get("relationName")
                or dataset_payload.get("name")
            )
            if not relation_name:
                relation_name = dataset.table_name or dataset.sql_alias
            dataset_payload["relation_name"] = relation_name
            normalized_datasets[dataset_name] = dataset_payload

        payload["datasets"] = normalized_datasets
        payload.setdefault("name", semantic_model.name)
        if semantic_model.description and not payload.get("description"):
            payload["description"] = semantic_model.description
        payload.setdefault("version", "1")
        return payload

    @staticmethod
    def _build_runtime_host(
        *,
        context: RuntimeContext,
        connectors: dict[str, ConnectorMetadata],
        datasets: dict[str, DatasetMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
        llm_connections: dict[str, LocalRuntimeLLMConnectionRecord],
        agents: dict[str, LocalRuntimeAgentRecord],
        dataset_repository: _InMemoryDatasetRepository,
        dataset_column_repository: _InMemoryDatasetColumnRepository,
        dataset_policy_repository: _InMemoryDatasetPolicyRepository,
        dataset_revision_repository: _InMemoryDatasetRevisionRepository,
        lineage_edge_repository: _InMemoryLineageEdgeRepository,
        connector_sync_state_repository: _InMemoryConnectorSyncStateRepository,
        secret_provider_registry: SecretProviderRegistry,
    ) -> tuple[RuntimeHost, _InMemoryThreadRepository, _InMemoryThreadMessageRepository]:
        connector_provider = MemoryConnectorProvider(
            {connector.id: connector for connector in connectors.values()}
        )
        dataset_provider = RepositoryDatasetMetadataProvider(
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            dataset_policy_repository=dataset_policy_repository,
        )
        semantic_model_provider = MemorySemanticModelProvider(
            {
                (context.workspace_id, record.id): SemanticModelMetadata(
                    id=record.id,
                    connector_id=None,
                    organization_id=context.workspace_id,
                    project_id=None,
                    name=record.name,
                    description=record.semantic_model.description,
                    content_yaml=record.semantic_model.yml_dump(),
                    content_json=record.semantic_model.model_dump(exclude_none=True),
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
                for record in semantic_models.values()
            }
        )
        semantic_model_store = _InMemorySemanticModelStore(
            ConfiguredLocalRuntimeHostFactory._build_semantic_model_store_records(
                semantic_models=semantic_models,
                context=context,
            )
        )
        thread_repository = _InMemoryThreadRepository()
        thread_message_repository = _InMemoryThreadMessageRepository()
        memory_repository = _InMemoryConversationMemoryRepository()
        agent_repository = _InMemoryAgentRepository(
            {
                record.id: record.agent_definition
                for record in agents.values()
            }
        )
        llm_repository = _InMemoryLLMConnectionRepository(
            {record.id: record for record in llm_connections.values()},
            registry=secret_provider_registry,
        )
        sync_state_provider = RepositorySyncStateProvider(
            connector_sync_state_repository=connector_sync_state_repository
        )
        credential_provider = SecretRegistryCredentialProvider(registry=secret_provider_registry)
        federated_query_tool = FederatedQueryTool(
            connector_provider=connector_provider,
            credential_provider=credential_provider,
            secret_provider_registry=secret_provider_registry,
        )
        semantic_query_service = (
            SemanticQueryExecutionService(
                dataset_repository=dataset_repository,
                federated_query_tool=federated_query_tool,
                logger=logging.getLogger("langbridge.runtime.semantic.local"),
                dataset_provider=dataset_provider,
                semantic_model_provider=semantic_model_provider,
            )
            if semantic_models
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
            sql_job_result_artifact_store=None,
            dataset_repository=dataset_repository,
            connector_provider=connector_provider,
            dataset_provider=dataset_provider,
            credential_provider=credential_provider,
            secret_provider_registry=secret_provider_registry,
            federated_query_tool=federated_query_tool,
        )
        dataset_sync_service = ConnectorSyncRuntime(
            connector_sync_state_repository=connector_sync_state_repository,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            dataset_policy_repository=dataset_policy_repository,
            dataset_revision_repository=dataset_revision_repository,
            lineage_edge_repository=lineage_edge_repository,
        )
        agent_execution_service = (
            AgentExecutionService(
                agent_definition_repository=agent_repository,
                llm_repository=llm_repository,
                semantic_model_store=semantic_model_store,
                dataset_repository=dataset_repository,
                dataset_column_repository=dataset_column_repository,
                thread_repository=thread_repository,
                thread_message_repository=thread_message_repository,
                memory_repository=memory_repository,
                federated_query_tool=federated_query_tool,
            )
            if agents
            else None
        )
        return RuntimeHost(
            context=context,
            providers=RuntimeProviders(
                dataset_metadata=dataset_provider,
                connector_metadata=connector_provider,
                semantic_models=semantic_model_provider if semantic_models else None,
                sync_state=sync_state_provider,
                credentials=credential_provider,
            ),
            services=RuntimeServices(
                federated_query_tool=federated_query_tool,
                semantic_query=semantic_query_service,
                sql_query=sql_query_service,
                dataset_query=dataset_query_service,
                dataset_sync=dataset_sync_service,
                agent_execution=agent_execution_service,
            ),
        ), thread_repository, thread_message_repository


def build_configured_local_runtime(
    *,
    config_path: str | Path,
    tenant_id: uuid.UUID | None = None,
    workspace_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    roles: list[str] | tuple[str, ...] | None = None,
    request_id: str | None = None,
) -> ConfiguredLocalRuntimeHost:
    resolved_config_path = Path(config_path).resolve()
    runtime_tenant_id = tenant_id or _stable_uuid("tenant", str(resolved_config_path))
    context = RuntimeContext.build(
        tenant_id=runtime_tenant_id,
        workspace_id=workspace_id or runtime_tenant_id,
        user_id=user_id or _stable_uuid("user", str(resolved_config_path)),
        roles=roles,
        request_id=request_id or f"local-runtime:{resolved_config_path.name}",
    )
    return ConfiguredLocalRuntimeHostFactory.build(
        config_path=resolved_config_path,
        context=context,
    )


__all__ = [
    "ConfiguredLocalRuntimeHost",
    "LocalRuntimeConfig",
    "build_configured_local_runtime",
]
