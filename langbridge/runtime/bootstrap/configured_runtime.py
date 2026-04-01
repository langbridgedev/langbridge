import copy
import logging
import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

from langbridge.plugins.connectors import ConnectorPlugin
from langbridge.runtime.application import build_runtime_applications
from langbridge.runtime.application.connectors import _extract_connection_metadata
from langbridge.runtime.config import load_runtime_config, resolve_metadata_store_config
from langbridge.runtime.config.models import (
    LocalRuntimeAgentConfig,
    LocalRuntimeConfig,
    LocalRuntimeConnectorConfig,
    LocalRuntimeDatasetConfig,
    LocalRuntimeDatasetPolicyConfig,
    LocalRuntimeLLMConnectionConfig,
    LocalRuntimeSemanticModelConfig,
    ResolvedLocalRuntimeMetadataStoreConfig,
)
from langbridge.runtime.models.metadata import DatasetStatus, DatasetType, LifecycleState, ManagementMode
from langbridge.runtime.ports import (
    ConnectorSyncStateStore, 
    DatasetCatalogStore, 
    DatasetColumnStore, 
    DatasetPolicyStore,
    ThreadStore,
    ThreadMessageStore
)
from langbridge.runtime.utils.connector_runtime import (
    build_connector_runtime_payload,
    resolve_connector_capabilities,
)
from langbridge.runtime.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
    resolve_dataset_materialization_mode,
)
from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.execution import FederatedQueryTool
from langbridge.runtime.embeddings import EmbeddingProvider, EmbeddingProviderError
from langbridge.runtime.persistence.in_memory import (
    _InMemoryAgentRepository,
    _InMemoryConnectorSyncStateRepository,
    _InMemoryConversationMemoryRepository,
    _InMemoryDatasetColumnRepository,
    _InMemoryDatasetPolicyRepository,
    _InMemoryDatasetRepository,
    _InMemoryDatasetRevisionRepository,
    _InMemoryLLMConnectionRepository,
    _InMemoryLineageEdgeRepository,
    _InMemorySemanticModelStore,
    _InMemoryThreadMessageRepository,
    _InMemoryThreadRepository,
)
from langbridge.runtime.persistence.sql_runtime import (
    build_sql_runtime_resources as build_persisted_runtime_resources,
)
from langbridge.runtime.persistence.migrations import (
    ensure_runtime_metadata_schema_current,
)
from langbridge.runtime.persistence.uow import _ConfiguredRuntimePersistenceController
from langbridge.runtime.models import (
    ConnectionMetadata,
    ConnectionPolicy,
    ConnectorCapabilities,
    ConnectorMetadata,
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMaterializationMode,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetSourceKind,
    DatasetStorageKind,
    LLMConnectionSecret,
    RuntimeAgentDefinition,
    RuntimeThread,
    RuntimeThreadMessage,
    SemanticModelMetadata,
    SecretReference,
)
from langbridge.runtime.models.state import ConnectorSyncMode
from langbridge.runtime.providers import (
    MemoryConnectorProvider,
    MemorySemanticModelProvider,
    MemorySemanticVectorIndexProvider,
    RepositoryDatasetMetadataProvider,
    RepositorySyncStateProvider,
    SecretRegistryCredentialProvider,
)
from langbridge.runtime.security import SecretProviderRegistry
from langbridge.runtime.services.agent_execution_service import AgentExecutionService
from langbridge.runtime.services.dataset_query_service import DatasetQueryService
from langbridge.runtime.services.dataset_sync_service import ConnectorSyncRuntime
from langbridge.runtime.services.runtime_host import (
    RuntimeHost,
    RuntimeProviders,
    RuntimeServices,
)
from langbridge.connectors.base import (
    ApiConnectorFactory,
    ConnectorRuntimeType,
    get_connector_config_factory,
    get_connector_plugin,
    list_connector_plugins
)
from langbridge.runtime.services.semantic_query_execution_service import (
    SemanticQueryExecutionService,
)
from langbridge.runtime.services.semantic_vector_search_service import (
    SemanticVectorSearchService,
)
from langbridge.runtime.services.sql_query_service import SqlQueryService
from langbridge.runtime.settings import runtime_settings as settings
from langbridge.orchestrator.definitions import AgentDefinitionFactory
from langbridge.semantic.loader import (
    SemanticModelError,
    load_semantic_model,
    load_unified_semantic_model,
)
from langbridge.semantic.model import SemanticModel
from langbridge.semantic.query import SemanticQuery


@dataclass(slots=True, frozen=True)
class LocalRuntimeDatasetRecord:
    id: uuid.UUID
    name: str
    label: str
    description: str | None
    connector_name: str | None
    relation_name: str
    semantic_model_name: str | None
    default_time_dimension: str | None


@dataclass(slots=True, frozen=True)
class LocalRuntimeSemanticModelRecord:
    id: uuid.UUID
    name: str
    description: str | None
    workspace_id: uuid.UUID
    semantic_model: SemanticModel | None
    content_yaml: str
    content_json: dict[str, Any]
    management_mode: ManagementMode = ManagementMode.CONFIG_MANAGED


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
    metadata_store: ResolvedLocalRuntimeMetadataStoreConfig
    dataset_repository: Any
    dataset_column_repository: Any
    dataset_policy_repository: Any
    dataset_revision_repository: Any
    lineage_edge_repository: Any
    connector_sync_state_repository: Any
    secret_provider_registry: SecretProviderRegistry
    thread_repository: Any
    thread_message_repository: Any
    persistence_controller: _ConfiguredRuntimePersistenceController | None = None


def _stable_uuid(namespace: str, value: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"langbridge:{namespace}:{value}")


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


def _connector_runtime_type(connector_type: str | ConnectorRuntimeType) -> ConnectorRuntimeType:
    if isinstance(connector_type, ConnectorRuntimeType):
        return connector_type
    return ConnectorRuntimeType(str(connector_type or "").strip().upper())


def _connector_dialect(connector_type: str | ConnectorRuntimeType) -> str:
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
    return dialect_map.get(normalized.value, normalized.value.lower() or "tsql")


def _merge_dataset_tags(*, existing: list[str], required: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for raw_tag in [*existing, *required]:
        tag = str(raw_tag or "").strip()
        if not tag:
            continue
        normalized = tag.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        merged.append(tag)
    return merged


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
        metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
        dataset_repository: DatasetCatalogStore,
        dataset_column_repository: DatasetColumnStore,
        dataset_policy_repository: DatasetPolicyStore,
        connector_sync_state_repository: ConnectorSyncStateStore,
        secret_provider_registry: SecretProviderRegistry,
        thread_repository: ThreadStore,
        thread_message_repository: ThreadMessageStore,
        persistence_controller: _ConfiguredRuntimePersistenceController | None = None,
        owns_runtime_resources: bool = True,
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
        self._metadata_store = metadata_store
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._dataset_policy_repository = dataset_policy_repository
        self._connector_sync_state_repository = connector_sync_state_repository
        self._secret_provider_registry = secret_provider_registry
        self._api_connector_factory = ApiConnectorFactory()
        self._thread_repository = thread_repository
        self._thread_message_repository = thread_message_repository
        self._persistence_controller = persistence_controller
        self._owns_runtime_resources = owns_runtime_resources
        self.context = context
        self._applications = build_runtime_applications(self)

    @property
    def providers(self):
        return self._runtime_host.providers

    @property
    def services(self):
        return self._runtime_host.services

    @property
    def persistence_controller(self):
        return self._persistence_controller

    @property
    def metadata_store(self) -> ResolvedLocalRuntimeMetadataStoreConfig:
        return self._metadata_store

    def __getattr__(self, name: str) -> Any:
        return getattr(self._runtime_host, name)

    def with_context(self, context: RuntimeContext) -> "ConfiguredLocalRuntimeHost":
        return ConfiguredLocalRuntimeHost(
            config_path=self._config_path,
            context=context,
            runtime_host=self._runtime_host.with_context(context),
            datasets=self._datasets,
            datasets_by_id=self._datasets_by_id,
            connectors=self._connectors,
            semantic_models=self._semantic_models,
            agents=self._agents,
            default_agent=self._default_agent,
            default_semantic_model_name=self._default_semantic_model_name,
            metadata_store=self._metadata_store,
            dataset_repository=self._dataset_repository,
            dataset_column_repository=self._dataset_column_repository,
            dataset_policy_repository=self._dataset_policy_repository,
            connector_sync_state_repository=self._connector_sync_state_repository,
            secret_provider_registry=self._secret_provider_registry,
            thread_repository=self._thread_repository,
            thread_message_repository=self._thread_message_repository,
            persistence_controller=self._persistence_controller,
            owns_runtime_resources=False,
        )

    async def aclose(self) -> None:
        if self._owns_runtime_resources and self._persistence_controller is not None:
            await self._persistence_controller.aclose()
        await self._runtime_host.aclose()

    def close(self) -> None:
        import asyncio

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.aclose())
            return
        raise RuntimeError(
            "ConfiguredLocalRuntimeHost.close() cannot be called from an active event loop. "
            "Use await ConfiguredLocalRuntimeHost.aclose() instead."
        )

    @asynccontextmanager
    async def _runtime_operation_scope(self):
        if self._persistence_controller is None:
            yield None
            return
        async with self._persistence_controller.unit_of_work() as uow:
            yield uow

    def _connector_dialect(self, connector_type: str) -> str:
        return _connector_dialect(connector_type)

    async def list_datasets(self) -> list[dict[str, Any]]:
        return await self._applications.datasets.list_datasets()

    async def get_dataset(
        self,
        *,
        dataset_ref: str,
    ) -> dict[str, Any]:
        return await self._applications.datasets.get_dataset(dataset_ref=dataset_ref)

    async def create_dataset(self, *, request) -> dict[str, Any]:
        return await self._applications.datasets.create_dataset(request=request)

    async def update_dataset(
        self,
        *,
        dataset_ref: str,
        request,
    ) -> dict[str, Any]:
        return await self._applications.datasets.update_dataset(
            dataset_ref=dataset_ref,
            request=request,
        )

    async def delete_dataset(
        self,
        *,
        dataset_ref: str,
    ) -> dict[str, Any]:
        return await self._applications.datasets.delete_dataset(dataset_ref=dataset_ref)

    async def list_semantic_models(self) -> list[dict[str, Any]]:
        return await self._applications.semantic.list_semantic_models()

    async def get_semantic_model(
        self,
        *,
        model_ref: str,
    ) -> dict[str, Any]:
        return await self._applications.semantic.get_semantic_model(model_ref=model_ref)

    async def create_semantic_model(self, *, request) -> dict[str, Any]:
        return await self._applications.semantic.create_semantic_model(request=request)

    async def update_semantic_model(
        self,
        *,
        model_ref: str,
        request,
    ) -> dict[str, Any]:
        return await self._applications.semantic.update_semantic_model(
            model_ref=model_ref,
            request=request,
        )

    async def delete_semantic_model(
        self,
        *,
        model_ref: str,
    ) -> dict[str, Any]:
        return await self._applications.semantic.delete_semantic_model(model_ref=model_ref)

    async def query_dataset(self, *, request) -> dict[str, Any]:
        return await self._applications.datasets.query_dataset(request=request)

    async def query_semantic(self, *args: Any, **kwargs: Any) -> Any:
        return await self._applications.semantic.query_semantic(*args, **kwargs)

    async def execute_sql(self, *, request) -> dict[str, Any]:
        return await self._applications.sql.execute_sql(request=request)

    async def create_agent(self, *args: Any, **kwargs: Any) -> Any:
        return await self._applications.agents.create_agent(*args, **kwargs)

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
        return await self._applications.semantic.query_semantic_models(
            semantic_models=semantic_models,
            measures=measures,
            dimensions=dimensions,
            filters=filters,
            limit=limit,
            order=order,
            time_dimensions=time_dimensions,
        )

    def _build_semantic_model_summary(
        self,
        *,
        name: str,
        record: LocalRuntimeSemanticModelRecord,
    ) -> dict[str, Any]:
        management_mode = str(record.management_mode.value)
        summary = {
            "id": record.id,
            "name": name,
            "management_mode": management_mode,
            "managed": management_mode == ManagementMode.CONFIG_MANAGED.value,
            "default": name == self._default_semantic_model_name,
        }
        semantic_model = record.semantic_model
        if semantic_model is not None:
            dataset_keys = list(semantic_model.datasets.keys())
            dimension_count = sum(
                len(dataset.dimensions or [])
                for dataset in semantic_model.datasets.values()
            )
            measure_count = sum(
                len(dataset.measures or [])
                for dataset in semantic_model.datasets.values()
            )
            return {
                **summary,
                "description": semantic_model.description,
                "dataset_count": len(dataset_keys),
                "dataset_names": dataset_keys,
                "dimension_count": dimension_count,
                "measure_count": measure_count,
            }

        configured_unified = SemanticQueryExecutionService.parse_unified_model_config_from_record(
            record
        )
        source_model_names = [
            source.alias or source.name or str(source.id)
            for source in (configured_unified.source_models or [])
        ]
        return {
            **summary,
            "description": record.content_json.get("description"),
            "dataset_count": len(configured_unified.semantic_model_ids),
            "dataset_names": source_model_names,
            "dimension_count": 0,
            "measure_count": len(configured_unified.metrics or {}),
        }

    def _resolve_configured_unified_model(
        self,
        *,
        semantic_models: list[LocalRuntimeSemanticModelRecord],
    ):
        selected_model_ids = [record.id for record in semantic_models]
        selected_model_id_set = set(selected_model_ids)
        matches = []
        for record in self._semantic_models.values():
            if record.semantic_model is not None:
                continue
            configured_unified = (
                SemanticQueryExecutionService.parse_unified_model_config_from_record(record)
            )
            if set(configured_unified.semantic_model_ids) != selected_model_id_set:
                continue
            if len(configured_unified.semantic_model_ids) != len(selected_model_ids):
                continue
            matches.append(configured_unified)

        if len(matches) > 1:
            raise ValueError(
                "Multiple configured unified semantic models match the selected semantic_models."
            )
        return matches[0] if matches else None

    def _rewrite_semantic_query_for_unified_execution(
        self,
        *,
        semantic_query: SemanticQuery,
        semantic_models: list[LocalRuntimeSemanticModelRecord],
        configured_unified,
    ) -> SemanticQuery:
        dataset_source_keys = self._build_unified_dataset_source_keys(
            semantic_models=semantic_models,
            configured_unified=configured_unified,
        )
        payload = semantic_query.model_dump(by_alias=True, exclude_none=True)
        payload["measures"] = [
            self._rewrite_member_for_unified_execution(
                member=member,
                dataset_source_keys=dataset_source_keys,
            )
            for member in semantic_query.measures
        ]
        payload["dimensions"] = [
            self._rewrite_member_for_unified_execution(
                member=member,
                dataset_source_keys=dataset_source_keys,
            )
            for member in semantic_query.dimensions
        ]
        payload["filters"] = [
            {
                **item.model_dump(by_alias=True, exclude_none=True),
                "member": self._rewrite_member_for_unified_execution(
                    member=item.member,
                    dataset_source_keys=dataset_source_keys,
                ),
            }
            for item in semantic_query.filters
        ]
        payload["timeDimensions"] = [
            {
                **item.model_dump(by_alias=True, exclude_none=True),
                "dimension": self._rewrite_member_for_unified_execution(
                    member=item.dimension,
                    dataset_source_keys=dataset_source_keys,
                ),
            }
            for item in semantic_query.time_dimensions
        ]
        payload["order"] = self._rewrite_order_for_unified_execution(
            order=semantic_query.order,
            dataset_source_keys=dataset_source_keys,
        )
        return SemanticQuery.model_validate(payload)

    def _build_unified_dataset_source_keys(
        self,
        *,
        semantic_models: list[LocalRuntimeSemanticModelRecord],
        configured_unified,
    ) -> dict[str, str]:
        configured_source_keys = {}
        if configured_unified is not None:
            configured_source_keys = {
                source.id: str(source.alias or source.name or "").strip() or str(source.id)
                for source in (configured_unified.source_models or [])
            }

        dataset_source_keys: dict[str, str] = {}
        for semantic_model in semantic_models:
            source_key = configured_source_keys.get(semantic_model.id) or semantic_model.name
            for dataset_name in semantic_model.semantic_model.datasets.keys():
                dataset_source_keys[dataset_name] = source_key
        return dataset_source_keys

    @staticmethod
    def _rewrite_member_for_unified_execution(
        *,
        member: str,
        dataset_source_keys: Mapping[str, str],
    ) -> str:
        normalized_member = str(member or "").strip()
        if not normalized_member or "." not in normalized_member:
            return normalized_member
        dataset_name, field_name = normalized_member.split(".", 1)
        source_key = dataset_source_keys.get(dataset_name)
        if not source_key:
            return normalized_member
        return f"{source_key}__{dataset_name}.{field_name}"

    def _rewrite_order_for_unified_execution(
        self,
        *,
        order: dict[str, str] | list[dict[str, str]] | None,
        dataset_source_keys: Mapping[str, str],
    ) -> dict[str, str] | list[dict[str, str]] | None:
        if order is None:
            return None
        entries = [order] if isinstance(order, dict) else list(order)
        rewritten = []
        for entry in entries:
            rewritten.append(
                {
                    self._rewrite_member_for_unified_execution(
                        member=str(member),
                        dataset_source_keys=dataset_source_keys,
                    ): direction
                    for member, direction in entry.items()
                }
            )
        if isinstance(order, dict):
            return rewritten[0] if rewritten else None
        return rewritten

    async def execute_sql_text(
        self,
        *,
        query: str,
        connection_name: str | None = None,
        requested_limit: int | None = None,
    ) -> dict[str, Any]:
        return await self._applications.sql.execute_sql_text(
            query=query,
            connection_name=connection_name,
            requested_limit=requested_limit,
        )

    def _resolve_actor_id(self) -> uuid.UUID:
        return self.context.actor_id or _stable_uuid("local-runtime-actor", str(self._config_path))

    async def ask_agent(
        self,
        *,
        prompt: str,
        agent_name: str | None = None,
        thread_id: uuid.UUID | None = None,
        title: str | None = None,
    ) -> dict[str, Any]:
        return await self._applications.agents.ask_agent(
            prompt=prompt,
            agent_name=agent_name,
            thread_id=thread_id,
            title=title,
        )

    async def create_thread(
        self,
        *,
        title: str | None = None,
    ) -> dict[str, Any]:
        return await self._applications.threads.create_thread(title=title)

    async def update_thread(
        self,
        *,
        thread_id: uuid.UUID,
        title: str | None = None,
    ) -> dict[str, Any]:
        return await self._applications.threads.update_thread(thread_id=thread_id, title=title)

    async def delete_thread(
        self,
        *,
        thread_id: uuid.UUID,
    ) -> dict[str, Any]:
        return await self._applications.threads.delete_thread(thread_id=thread_id)

    async def list_agents(self) -> list[dict[str, Any]]:
        return await self._applications.agents.list_agents()

    async def get_agent(
        self,
        *,
        agent_ref: str,
    ) -> dict[str, Any]:
        return await self._applications.agents.get_agent(agent_ref=agent_ref)

    async def list_threads(self) -> list[dict[str, Any]]:
        return await self._applications.threads.list_threads()

    async def get_thread(
        self,
        *,
        thread_id: uuid.UUID,
    ) -> dict[str, Any]:
        return await self._applications.threads.get_thread(thread_id=thread_id)

    async def list_thread_messages(
        self,
        *,
        thread_id: uuid.UUID,
    ) -> list[dict[str, Any]]:
        return await self._applications.threads.list_thread_messages(thread_id=thread_id)

    async def list_connectors(self) -> list[dict[str, Any]]:
        return await self._applications.connectors.list_connectors()

    async def list_connector_types(self) -> list[dict[str, Any]]:
        return await self._applications.connectors.list_connector_types()

    async def get_connector_type_config(
        self,
        *,
        connector_type: str,
    ) -> dict[str, Any]:
        return await self._applications.connectors.get_connector_type_config(
            connector_type=connector_type
        )

    async def get_connector(
        self,
        *,
        connector_name: str,
    ) -> dict[str, Any]:
        return await self._applications.connectors.get_connector(connector_name=connector_name)

    async def create_connector(self, *, request) -> dict[str, Any]:
        return await self._applications.connectors.create_connector(request=request)

    async def update_connector(
        self,
        *,
        connector_name: str,
        request,
    ) -> dict[str, Any]:
        return await self._applications.connectors.update_connector(
            connector_name=connector_name,
            request=request,
        )

    async def delete_connector(
        self,
        *,
        connector_name: str,
    ) -> dict[str, Any]:
        return await self._applications.connectors.delete_connector(connector_name=connector_name)

    async def list_sync_resources(
        self,
        *,
        connector_name: str,
    ) -> list[dict[str, Any]]:
        return await self._applications.connectors.list_sync_resources(connector_name=connector_name)

    async def list_sync_states(
        self,
        *,
        connector_name: str,
    ) -> list[dict[str, Any]]:
        return await self._applications.connectors.list_sync_states(connector_name=connector_name)

    async def sync_connector_resources(
        self,
        *,
        connector_name: str,
        resources: list[str],
        sync_mode: str = "INCREMENTAL",
        force_full_refresh: bool = False,
    ) -> dict[str, Any]:
        return await self._applications.connectors.sync_connector_resources(
            connector_name=connector_name,
            resources=resources,
            sync_mode=sync_mode,
            force_full_refresh=force_full_refresh,
        )

    async def refresh_semantic_vector_search(self, *args: Any, **kwargs: Any) -> Any:
        return await self._applications.semantic.refresh_semantic_vector_search(*args, **kwargs)

    async def search_semantic_vectors(self, *args: Any, **kwargs: Any) -> Any:
        return await self._applications.semantic.search_semantic_vectors(*args, **kwargs)

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

    def _resolve_agent_record(self, agent_ref: str) -> LocalRuntimeAgentRecord:
        normalized_ref = str(agent_ref or "").strip()
        if not normalized_ref:
            raise ValueError("agent_ref is required.")
        agent = self._agents.get(normalized_ref)
        if agent is not None:
            return agent
        try:
            agent_id = uuid.UUID(normalized_ref)
        except ValueError:
            agent_id = None
        if agent_id is not None:
            for candidate in self._agents.values():
                if candidate.id == agent_id:
                    return candidate
        raise ValueError(f"Unknown agent '{agent_ref}'.")

    def _resolve_semantic_model_record(self, model_ref: str) -> LocalRuntimeSemanticModelRecord:
        normalized_ref = str(model_ref or "").strip()
        if not normalized_ref:
            raise ValueError("model_ref is required.")
        record = self._semantic_models.get(normalized_ref)
        if record is not None:
            return record
        try:
            model_id = uuid.UUID(normalized_ref)
        except ValueError:
            model_id = None
        if model_id is not None:
            for candidate in self._semantic_models.values():
                if candidate.id == model_id:
                    return candidate
        raise ValueError(f"Unknown semantic model '{model_ref}'.")

    async def _resolve_dataset_record(self, dataset_ref: str) -> DatasetMetadata:
        normalized_ref = str(dataset_ref or "").strip()
        if not normalized_ref:
            raise ValueError("dataset_ref is required.")
        try:
            dataset_id = uuid.UUID(normalized_ref)
        except ValueError:
            dataset_id = None
        if dataset_id is not None:
            dataset = await self._dataset_repository.get_for_workspace(
                dataset_id=dataset_id,
                workspace_id=self.context.workspace_id,
            )
            if dataset is not None:
                return dataset
        records = await self._dataset_repository.list_for_workspace(
            workspace_id=self.context.workspace_id,
            limit=1000,
            offset=0,
        )
        for dataset in records:
            if dataset.name == normalized_ref:
                return dataset
        raise ValueError(f"Unknown dataset '{dataset_ref}'.")

    def _connector_for_id(self, connector_id: uuid.UUID | None) -> ConnectorMetadata | None:
        if connector_id is None:
            return None
        return next(
            (candidate for candidate in self._connectors.values() if candidate.id == connector_id),
            None,
        )

    def _upsert_runtime_connector(self, connector: ConnectorMetadata) -> None:
        self._connectors[connector.name] = connector
        connector_provider = self.providers.connector_metadata
        if hasattr(connector_provider, "upsert"):
            connector_provider.upsert(connector)

    def _remove_runtime_connector(self, *, connector_name: str, connector_id: uuid.UUID) -> None:
        self._connectors.pop(connector_name, None)
        connector_provider = self.providers.connector_metadata
        if hasattr(connector_provider, "remove"):
            connector_provider.remove(connector_id=connector_id)

    def _upsert_runtime_dataset_record(self, record: LocalRuntimeDatasetRecord) -> None:
        self._datasets[record.name] = record
        self._datasets_by_id[record.id] = record

    def _remove_runtime_dataset_record(self, *, dataset_name: str, dataset_id: uuid.UUID) -> None:
        self._datasets.pop(dataset_name, None)
        self._datasets_by_id.pop(dataset_id, None)

    def _upsert_runtime_semantic_model_record(
        self,
        record: LocalRuntimeSemanticModelRecord,
    ) -> None:
        self._semantic_models[record.name] = record
        metadata = self._semantic_model_metadata_from_record(record)
        semantic_provider = self.providers.semantic_models
        if hasattr(semantic_provider, "upsert"):
            semantic_provider.upsert(metadata)
        semantic_store = getattr(self.services.agent_execution, "_semantic_model_store", None)
        if hasattr(semantic_store, "upsert"):
            semantic_store.upsert(metadata)

    def _remove_runtime_semantic_model_record(
        self,
        *,
        model_name: str,
        model_id: uuid.UUID,
    ) -> None:
        self._semantic_models.pop(model_name, None)
        if self._default_semantic_model_name == model_name:
            self._default_semantic_model_name = next(iter(self._semantic_models), None)
        semantic_provider = self.providers.semantic_models
        if hasattr(semantic_provider, "remove"):
            semantic_provider.remove(
                workspace_id=self.context.workspace_id,
                semantic_model_id=model_id,
            )
        semantic_store = getattr(self.services.agent_execution, "_semantic_model_store", None)
        if hasattr(semantic_store, "remove"):
            semantic_store.remove(
                workspace_id=self.context.workspace_id,
                semantic_model_id=model_id,
            )

    def _semantic_model_metadata_from_record(
        self,
        record: LocalRuntimeSemanticModelRecord,
    ) -> SemanticModelMetadata:
        description = (
            record.semantic_model.description
            if record.semantic_model is not None
            else record.content_json.get("description")
        )
        return SemanticModelMetadata(
            id=record.id,
            connector_id=None,
            workspace_id=record.workspace_id,
            name=record.name,
            description=description,
            content_yaml=record.content_yaml,
            content_json=copy.deepcopy(record.content_json),
            management_mode=record.management_mode,
            lifecycle_state=LifecycleState.ACTIVE,
        )

    @staticmethod
    def _serialize_thread(thread: RuntimeThread) -> dict[str, Any]:
        return {
            "id": thread.id,
            "workspace_id": thread.workspace_id,
            "title": thread.title,
            "state": thread.state,
            "metadata": dict(thread.metadata_json),
            "created_at": thread.created_at,
            "updated_at": thread.updated_at,
            "created_by": thread.created_by,
            "last_message_id": thread.last_message_id,
        }

    @staticmethod
    def _serialize_thread_message(message: RuntimeThreadMessage) -> dict[str, Any]:
        return {
            "id": message.id,
            "thread_id": message.thread_id,
            "parent_message_id": message.parent_message_id,
            "role": message.role,
            "content": dict(message.content or {}),
            "model_snapshot": message.model_snapshot_json,
            "token_usage": message.token_usage_json,
            "error": dict(message.error or {}) if message.error else None,
            "created_at": message.created_at,
        }

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
                if normalized_key.count("__") >= 2:
                    _, dataset_name, suffix = normalized_key.split("__", 2)
                    if dataset_name in dataset_names:
                        normalized_key = f"{dataset_name}.{suffix}"
                elif "__" in normalized_key:
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
            elif hasattr(self._dataset_repository, "_datasets"):
                dataset = self._dataset_repository._datasets.get(dataset_uuid)
                if dataset is not None:
                    normalized["dataset_name"] = dataset.name
        return normalized
    
    def _get_connector_plugins(self) -> list[ConnectorPlugin]:
        return list_connector_plugins()

    def _get_connector_plugin(self, connector: ConnectorMetadata):
        return self._resolve_connector_plugin_for_type(connector.connector_type_value)

    @staticmethod
    def _resolve_connector_plugin_for_type(connector_type: str | None):
        raw_type = str(connector_type or "").strip().upper()
        if not raw_type:
            return None
        try:
            return get_connector_plugin(ConnectorRuntimeType(raw_type))
        except ValueError:
            return None

    def _connector_capabilities(self, connector: ConnectorMetadata) -> ConnectorCapabilities:
        return resolve_connector_capabilities(
            configured_capabilities=connector.capabilities_json,
            connector_type=connector.connector_type_value,
            plugin=self._get_connector_plugin(connector),
        )

    def _connector_supports_sync(self, connector: ConnectorMetadata) -> bool:
        plugin = self._get_connector_plugin(connector)
        capabilities = self._connector_capabilities(connector)
        return bool(
            plugin is not None
            and capabilities.supports_synced_datasets
            and plugin.api_connector_class is not None
        )

    def _resolve_connector_runtime_type(self, connector: ConnectorMetadata) -> ConnectorRuntimeType:
        if connector.connector_type is None:
            raise ValueError(f"Connector '{connector.name}' does not define a connector_type.")
        return connector.connector_type

    def _build_api_connector(self, connector: ConnectorMetadata):
        if not self._connector_supports_sync(connector):
            raise ValueError(f"Connector '{connector.name}' does not support runtime sync.")
        connector_type = self._resolve_connector_runtime_type(connector)
        runtime_payload = build_connector_runtime_payload(
            config_json=connector.config,
            connection_metadata=(
                connector.connection_metadata.model_dump(mode="json", by_alias=True)
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
    def _normalize_sync_mode(value: str | ConnectorSyncMode | None) -> ConnectorSyncMode:
        normalized = str(getattr(value, "value", value) or ConnectorSyncMode.INCREMENTAL.value).strip().upper()
        if normalized not in {
            ConnectorSyncMode.INCREMENTAL.value,
            ConnectorSyncMode.FULL_REFRESH.value,
        }:
            raise ValueError("sync_mode must be either INCREMENTAL or FULL_REFRESH.")
        return ConnectorSyncMode(normalized)

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
        connector_name: str | None,
        relation_name: str,
        column_name: str,
    ) -> date | None:
        if not str(connector_name or "").strip():
            return None
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
        apply_migrations: bool | None = None,
    ) -> ConfiguredLocalRuntimeHost:
        resolved_config_path = Path(config_path).resolve()
        local_runtime_config: LocalRuntimeConfig = ConfiguredLocalRuntimeHostFactory._load_config(resolved_config_path)
        resources = ConfiguredLocalRuntimeHostFactory._build_resources(
            config_path=resolved_config_path,
            config=local_runtime_config,
            context=context,
            apply_migrations=apply_migrations,
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
            metadata_store=resources.metadata_store,
            dataset_repository=resources.dataset_repository,
            dataset_column_repository=resources.dataset_column_repository,
            dataset_policy_repository=resources.dataset_policy_repository,
            connector_sync_state_repository=resources.connector_sync_state_repository,
            secret_provider_registry=resources.secret_provider_registry,
            thread_repository=resources.thread_repository,
            thread_message_repository=resources.thread_message_repository,
            persistence_controller=resources.persistence_controller,
        )

    @staticmethod
    def _load_config(path: Path) -> LocalRuntimeConfig:
        return load_runtime_config(path)

    @staticmethod
    def _resolve_connector_plugin_for_type(connector_type: str | None):
        raw_type = str(connector_type or "").strip().upper()
        if not raw_type:
            return None
        try:
            return get_connector_plugin(ConnectorRuntimeType(raw_type))
        except ValueError:
            return None

    @staticmethod
    def _build_resources(
        *,
        config_path: Path,
        config: LocalRuntimeConfig,
        context: RuntimeContext,
        apply_migrations: bool | None = None,
    ) -> _ConfiguredLocalRuntimeResources:
        metadata_store = ConfiguredLocalRuntimeHostFactory._resolve_metadata_store_config(
            config_path=config_path,
            config=config,
        )
        should_apply_migrations = (
            config.runtime.migrations.auto_apply
            if apply_migrations is None
            else bool(apply_migrations)
        )
        ensure_runtime_metadata_schema_current(
            metadata_store=metadata_store,
            auto_apply=should_apply_migrations,
            config_path=config_path,
        )
        secret_provider_registry = SecretProviderRegistry()
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
            context=context,
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
        (
            runtime_host,
            dataset_repository,
            dataset_column_repository,
            dataset_policy_repository,
            dataset_revision_repository,
            lineage_edge_repository,
            connector_sync_state_repository,
            thread_repository,
            thread_message_repository,
            persistence_controller,
        ) = ConfiguredLocalRuntimeHostFactory._build_runtime_resources(
            metadata_store=metadata_store,
            context=context,
            config_path=config_path,
            connectors=connector_models,
            datasets=dataset_models,
            semantic_models=semantic_models,
            llm_connections=llm_connections,
            agents=agents,
            dataset_repository_rows=dataset_repository_rows,
            dataset_columns=dataset_columns,
            dataset_policies=dataset_policies,
            secret_provider_registry=secret_provider_registry,
        )
        if metadata_store.type != "in_memory":
            runtime_managed_connectors, runtime_managed_semantic_models = (
                ConfiguredLocalRuntimeHostFactory._load_persisted_runtime_managed_resources(
                    metadata_store=metadata_store,
                    context=context,
                )
            )
            connector_models = {
                **connector_models,
                **runtime_managed_connectors,
            }
            semantic_models = {
                **semantic_models,
                **runtime_managed_semantic_models,
            }
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
            metadata_store=metadata_store,
            dataset_repository=dataset_repository,
            dataset_column_repository=dataset_column_repository,
            dataset_policy_repository=dataset_policy_repository,
            dataset_revision_repository=dataset_revision_repository,
            lineage_edge_repository=lineage_edge_repository,
            connector_sync_state_repository=connector_sync_state_repository,
            secret_provider_registry=secret_provider_registry,
            thread_repository=thread_repository,
            thread_message_repository=thread_message_repository,
            persistence_controller=persistence_controller,
        )

    @staticmethod
    def _load_persisted_runtime_managed_resources(
        *,
        metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
        context: RuntimeContext,
    ) -> tuple[dict[str, ConnectorMetadata], dict[str, LocalRuntimeSemanticModelRecord]]:
        from sqlalchemy import select

        from langbridge.runtime.persistence.db import create_engine_for_url, create_session_factory
        from langbridge.runtime.persistence.db.connector import Connector
        from langbridge.runtime.persistence.db.semantic import SemanticModelEntry
        from langbridge.runtime.persistence.mappers.connectors import from_connector_record
        from langbridge.runtime.persistence.mappers.semantic_models import (
            from_semantic_model_record,
        )

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
            connector_rows = session.scalars(
                select(Connector).where(
                    Connector.workspace_id == context.workspace_id,
                    Connector.management_mode == ManagementMode.RUNTIME_MANAGED.value,
                )
            ).all()
            semantic_rows = session.scalars(
                select(SemanticModelEntry).where(
                    SemanticModelEntry.workspace_id == context.workspace_id,
                    SemanticModelEntry.management_mode == ManagementMode.RUNTIME_MANAGED.value,
                )
            ).all()
            connectors = {
                connector.name: connector
                for row in connector_rows
                if (connector := from_connector_record(row)) is not None
            }
            semantic_models = {
                record.name: record
                for row in semantic_rows
                if (
                    semantic_metadata := from_semantic_model_record(row)
                ) is not None
                and (
                    record := ConfiguredLocalRuntimeHostFactory._build_local_runtime_semantic_model_record(
                        semantic_metadata
                    )
                ) is not None
            }
            return connectors, semantic_models
        finally:
            session.close()
            sync_engine.dispose()

    @staticmethod
    def _build_local_runtime_semantic_model_record(
        semantic_metadata: SemanticModelMetadata,
    ) -> LocalRuntimeSemanticModelRecord:
        content_json = semantic_metadata.content_json
        if isinstance(content_json, str):
            parsed_yaml = yaml.safe_load(content_json)
            content_json = parsed_yaml if isinstance(parsed_yaml, dict) else {}
        elif not isinstance(content_json, dict):
            parsed_yaml = yaml.safe_load(semantic_metadata.content_yaml)
            content_json = parsed_yaml if isinstance(parsed_yaml, dict) else {}
        content_yaml = str(semantic_metadata.content_yaml or "").strip()
        semantic_model = None
        try:
            semantic_model = load_semantic_model(copy.deepcopy(content_json))
            content_yaml = semantic_model.yml_dump()
            content_json = semantic_model.model_dump(exclude_none=True)
        except SemanticModelError:
            load_unified_semantic_model(copy.deepcopy(content_json))
            if not content_yaml:
                content_yaml = yaml.safe_dump(content_json, sort_keys=False).strip()
        return LocalRuntimeSemanticModelRecord(
            id=semantic_metadata.id,
            name=semantic_metadata.name,
            description=semantic_metadata.description,
            workspace_id=semantic_metadata.workspace_id,
            semantic_model=semantic_model,
            content_yaml=content_yaml,
            content_json=copy.deepcopy(content_json),
            management_mode=semantic_metadata.management_mode,
        )

    @staticmethod
    def _resolve_metadata_store_config(
        *,
        config_path: Path,
        config: LocalRuntimeConfig,
    ) -> ResolvedLocalRuntimeMetadataStoreConfig:
        return resolve_metadata_store_config(
            config_path=config_path,
            metadata_store=config.runtime.metadata_store,
        )

    @staticmethod
    def _build_runtime_resources(
        *,
        metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
        context: RuntimeContext,
        config_path: Path,
        connectors: dict[str, ConnectorMetadata],
        datasets: dict[str, DatasetMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
        llm_connections: dict[str, LocalRuntimeLLMConnectionRecord],
        agents: dict[str, LocalRuntimeAgentRecord],
        dataset_repository_rows: dict[uuid.UUID, DatasetMetadata],
        dataset_columns: dict[uuid.UUID, list[DatasetColumnMetadata]],
        dataset_policies: dict[uuid.UUID, DatasetPolicyMetadata],
        secret_provider_registry: SecretProviderRegistry,
    ) -> tuple[Any, Any, Any, Any, Any, Any, Any, Any, Any, _ConfiguredRuntimePersistenceController | None]:
        if metadata_store.type == "in_memory":
            dataset_repository = _InMemoryDatasetRepository(dataset_repository_rows)
            dataset_column_repository = _InMemoryDatasetColumnRepository(dataset_columns)
            dataset_policy_repository = _InMemoryDatasetPolicyRepository(dataset_policies)
            dataset_revision_repository = _InMemoryDatasetRevisionRepository()
            lineage_edge_repository = _InMemoryLineageEdgeRepository()
            connector_sync_state_repository = _InMemoryConnectorSyncStateRepository()
            runtime_host, thread_repository, thread_message_repository = (
                ConfiguredLocalRuntimeHostFactory._build_runtime_host(
                    context=context,
                    connectors=connectors,
                    datasets=datasets,
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
            )
            return (
                runtime_host,
                dataset_repository,
                dataset_column_repository,
                dataset_policy_repository,
                dataset_revision_repository,
                lineage_edge_repository,
                connector_sync_state_repository,
                thread_repository,
                thread_message_repository,
                None,
            )

        return ConfiguredLocalRuntimeHostFactory._build_sql_runtime_resources(
            metadata_store=metadata_store,
            context=context,
            config_path=config_path,
            connectors=connectors,
            semantic_models=semantic_models,
            llm_connections=llm_connections,
            agents=agents,
            dataset_repository_rows=dataset_repository_rows,
            dataset_columns=dataset_columns,
            dataset_policies=dataset_policies,
            secret_provider_registry=secret_provider_registry,
        )

    @staticmethod
    def _build_sql_runtime_resources(
        *,
        metadata_store: ResolvedLocalRuntimeMetadataStoreConfig,
        context: RuntimeContext,
        config_path: Path,
        connectors: dict[str, ConnectorMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
        llm_connections: dict[str, LocalRuntimeLLMConnectionRecord],
        agents: dict[str, LocalRuntimeAgentRecord],
        dataset_repository_rows: dict[uuid.UUID, DatasetMetadata],
        dataset_columns: dict[uuid.UUID, list[DatasetColumnMetadata]],
        dataset_policies: dict[uuid.UUID, DatasetPolicyMetadata],
        secret_provider_registry: SecretProviderRegistry,
    ) -> tuple[Any, Any, Any, Any, Any, Any, Any, Any, Any, _ConfiguredRuntimePersistenceController]:
        return build_persisted_runtime_resources(
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

    @staticmethod
    def _build_connector_models(
        *,
        config_path: Path,
        config: LocalRuntimeConfig,
        context: RuntimeContext,
    ) -> dict[str, ConnectorMetadata]:
        connectors: dict[str, ConnectorMetadata] = {}
        for connector in config.connectors:
            connector_metadata = ConfiguredLocalRuntimeHostFactory._build_single_connector_model(
                connector=connector,
                config_path=config_path,
                context=context,
            )
            connectors[connector.name] = connector_metadata
        return connectors

    @staticmethod
    def _build_single_connector_model(
        *,
        connector: LocalRuntimeConnectorConfig,
        config_path: Path,
        context: RuntimeContext,
    ) -> ConnectorMetadata:
        connection_payload = ConfiguredLocalRuntimeHostFactory._normalize_connector_connection_payload(
            connection=connector.connection,
            connector_type=_connector_runtime_type(connector.type),
        )
        connector_type = _connector_runtime_type(connector.type)
        plugin = ConfiguredLocalRuntimeHostFactory._resolve_connector_plugin_for_type(
            connector_type.value
        )
        metadata_payload = dict(connector.metadata or {})
        merged_connection = {**connection_payload, **metadata_payload}
        connector_id = _stable_uuid("connector", f"{config_path}:{connector.name}")
        capabilities = resolve_connector_capabilities(
            configured_capabilities=connector.capabilities,
            connector_type=connector_type.value,
            plugin=plugin,
        )
        config_factory = get_connector_config_factory(connector_type) if connector_type != ConnectorRuntimeType.LOCAL_FILESYSTEM else None
        metadata_keys = config_factory.get_metadata_keys() if config_factory is not None else set()
        return ConnectorMetadata(
            id=connector_id,
            name=connector.name,
            description=connector.description,
            connector_type=connector_type,
            connector_family=(
                plugin.connector_family
                if plugin is not None
                else None
            ),
            workspace_id=context.workspace_id,
            config={"config": connection_payload},
            connection_metadata=_extract_connection_metadata(merged_connection, metadata_keys),
            secret_references=dict(connector.secrets or {}),
            connection_policy=(
                ConnectionPolicy.model_validate(connector.policy)
                if isinstance(connector.policy, Mapping)
                else None
            ),
            supported_resources=list(plugin.supported_resources) if plugin is not None else [],
            sync_strategy=(
                plugin.sync_strategy
                if plugin is not None and plugin.sync_strategy is not None
                else None
            ),
            capabilities=capabilities,
            is_managed=connector.managed,
            management_mode=ManagementMode.CONFIG_MANAGED,
            lifecycle_state=LifecycleState.ACTIVE,
        )

    @staticmethod
    def _normalize_connector_connection_payload(
        *,
        connection: dict[str, Any] | None,
        connector_type: ConnectorRuntimeType,
    ) -> dict[str, Any]:
        connection_payload = dict(connection or {})
        
        if "path" in connection_payload:
            resolved_path = str(connection_payload.get("path") or "").strip() or None
            if resolved_path:
                if connector_type == ConnectorRuntimeType.SQLITE:
                    connection_payload["location"] = resolved_path
                    connection_payload.pop("path", None)
                else:
                    connection_payload["path"] = resolved_path
        
        if "location" in connection_payload and connector_type == ConnectorRuntimeType.SQLITE:
            normalized_location = str(connection_payload.get("location") or "").strip()
            if normalized_location:
                connection_payload["location"] = normalized_location
        
        return connection_payload

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
        now = datetime.now(timezone.utc)
        for dataset in config.datasets:
            connector = connectors.get(dataset.connector)
            if connector is None:
                raise ValueError(f"Dataset '{dataset.name}' references unknown connector '{dataset.connector}'.")
            connector_capabilities = ConfiguredLocalRuntimeHostFactory._resolve_connector_capabilities_from_record(
                connector
            )
            materialization_mode = resolve_dataset_materialization_mode(
                explicit_materialization_mode=dataset.materialization_mode,
            )

            source_table = str(dataset.source.table or "").strip()
            sync_resource_name = str(dataset.source.resource or "").strip()
            source_sql = str(dataset.source.sql or "").strip()
            source_storage_uri = str(dataset.source.storage_uri or "").strip() or None
            if source_storage_uri is None and dataset.source.path:
                source_storage_uri = Path(str(dataset.source.path)).resolve().as_uri()
            if materialization_mode == DatasetMaterializationMode.LIVE and not connector_capabilities.supports_live_datasets:
                raise ValueError(
                    f"Dataset '{dataset.name}' requests materialization_mode 'live', "
                    f"but connector '{connector.name}' does not support live datasets."
                )
            if materialization_mode == DatasetMaterializationMode.SYNCED:
                if not connector_capabilities.supports_synced_datasets:
                    raise ValueError(
                        f"Dataset '{dataset.name}' requests materialization_mode 'synced', "
                        f"but connector '{connector.name}' does not support synced datasets."
                    )
                plugin = ConfiguredLocalRuntimeHostFactory._resolve_connector_plugin_for_type(
                    connector.connector_type_value
                )
                if plugin is None or plugin.api_connector_class is None:
                    raise ValueError(
                        f"Dataset '{dataset.name}' requests materialization_mode 'synced', "
                        f"but connector '{connector.name}' does not expose a runtime sync path yet."
                    )
                if source_sql or source_storage_uri:
                    raise ValueError(
                        f"Dataset '{dataset.name}' requests materialization_mode 'synced', "
                        "but synced datasets must use source.resource to name the connector resource to materialize."
                    )
                if not sync_resource_name:
                    raise ValueError(
                        f"Dataset '{dataset.name}' requests materialization_mode 'synced', "
                        "but is missing source.resource for the connector resource name."
                    )
                supported_resources = {
                    str(item or "").strip()
                    for item in (connector.supported_resources or [])
                    if str(item or "").strip()
                }
                if supported_resources and sync_resource_name not in supported_resources:
                    raise ValueError(
                        f"Dataset '{dataset.name}' requests synced resource '{sync_resource_name}', "
                        f"but connector '{connector.name}' only exposes: {', '.join(sorted(supported_resources))}."
                    )
            if (
                materialization_mode == DatasetMaterializationMode.LIVE
                and (source_table or source_sql)
                and not connector_capabilities.supports_query_pushdown
            ):
                raise ValueError(
                    f"Dataset '{dataset.name}' uses a live table/sql source, "
                    f"but connector '{connector.name}' does not expose live query pushdown."
                )
            dataset_id = _stable_uuid("dataset", f"{config_path}:{dataset.name}")
            if materialization_mode == DatasetMaterializationMode.SYNCED:
                catalog_name = None
                schema_name = None
                table_name = _dataset_sql_alias(dataset.name)
                relation_name = table_name
                dataset_type = DatasetType.FILE
                sql_text = None
                storage_kind = DatasetStorageKind.PARQUET
                source_kind = DatasetSourceKind.API
                dialect = "duckdb"
                storage_uri = None
                file_config = {
                        "format": "parquet",
                        "managed_dataset": True,
                        "connector_sync": {
                            "connector_id": str(connector.id),
                            "connector_type": connector.connector_type_value,
                            "connector_family": connector.connector_family_value,
                            "resource_name": sync_resource_name,
                            "root_resource_name": sync_resource_name,
                            "parent_resource_name": None,
                        },
                    }
            elif source_table:
                catalog_name, schema_name, table_name = _relation_parts(source_table)
                relation_name = source_table
                dataset_type = DatasetType.TABLE
                sql_text = None
                storage_kind = DatasetStorageKind.TABLE
                source_kind = DatasetSourceKind.DATABASE
                dialect = _connector_dialect(connector.connector_type or "")
                storage_uri = None
                file_config = None
            else:
                catalog_name = None
                schema_name = None
                table_name = _dataset_sql_alias(dataset.name)
                relation_name = table_name
                if source_sql:
                    dataset_type = DatasetType.SQL
                    sql_text = source_sql
                    storage_kind = DatasetStorageKind.VIEW
                    source_kind = DatasetSourceKind.DATABASE
                    dialect = _connector_dialect(connector.connector_type or "")
                    storage_uri = None
                    file_config = None
                else:
                    dataset_type = DatasetType.FILE
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
                    source_kind = DatasetSourceKind.FILE
                    storage_kind = DatasetStorageKind(file_format)
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

            relation_identity = build_dataset_relation_identity(
                dataset_id=dataset_id,
                connector_id=connector.id,
                dataset_name=dataset.name,
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=table_name,
                storage_uri=storage_uri,
                source_kind=source_kind,
                storage_kind=storage_kind,
            )
            execution_capabilities = build_dataset_execution_capabilities(
                source_kind=source_kind,
                storage_kind=storage_kind,
            )

            policy = dataset.policy or LocalRuntimeDatasetPolicyConfig()
            datasets[dataset.name] = DatasetMetadata(
                id=dataset_id,
                workspace_id=context.workspace_id,
                connection_id=connector.id,
                owner_id=context.actor_id,
                created_by=context.actor_id,
                updated_by=context.actor_id,
                name=dataset.name,
                sql_alias=_dataset_sql_alias(dataset.name),
                description=(
                    dataset.description
                    or (
                        f"Configured synced dataset awaiting connector sync for resource '{sync_resource_name}'."
                        if materialization_mode == DatasetMaterializationMode.SYNCED
                        else None
                    )
                ),
                tags=_merge_dataset_tags(
                    existing=list(dataset.tags),
                    required=(
                        [
                            "managed",
                            "api-connector",
                            connector.connector_type_value.lower(),
                            f"resource:{sync_resource_name.strip().lower()}",
                        ]
                        if materialization_mode == DatasetMaterializationMode.SYNCED
                        else []
                    ),
                ),
                dataset_type=dataset_type,
                materialization_mode=materialization_mode,
                source_kind=source_kind,
                connector_kind=(connector.connector_type_value.lower() if connector.connector_type is not None else None),
                storage_kind=storage_kind,
                dialect=dialect,
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=table_name,
                storage_uri=storage_uri,
                sql_text=sql_text,
                relation_identity=relation_identity.model_dump(mode="json"),
                execution_capabilities=execution_capabilities.model_dump(mode="json"),
                referenced_dataset_ids=[],
                federated_plan=None,
                file_config=file_config,
                status=(
                    DatasetStatus.PENDING_SYNC
                    if materialization_mode == DatasetMaterializationMode.SYNCED
                    else DatasetStatus.PUBLISHED
                ),
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
                management_mode=ManagementMode.CONFIG_MANAGED,
                lifecycle_state=LifecycleState.ACTIVE,
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
    def _resolve_connector_capabilities_from_record(
        connector: ConnectorMetadata,
    ) -> ConnectorCapabilities:
        return resolve_connector_capabilities(
            configured_capabilities=connector.capabilities_json,
            connector_type=connector.connector_type_value,
            plugin=ConfiguredLocalRuntimeHostFactory._resolve_connector_plugin_for_type(
                connector.connector_type_value
            ),
        )

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
            try:
                semantic_model = load_semantic_model(payload)
                content_yaml = semantic_model.yml_dump()
                content_json = semantic_model.model_dump(exclude_none=True)
            except SemanticModelError:
                load_unified_semantic_model(payload)
                semantic_model = None
                content_json = copy.deepcopy(payload)
                content_yaml = yaml.safe_dump(content_json, sort_keys=False).strip()
            semantic_model_id = _stable_uuid("semantic-model", f"{context.workspace_id}:{item.name}")
            semantic_models[item.name] = LocalRuntimeSemanticModelRecord(
                id=semantic_model_id,
                description=item.description,
                workspace_id=context.workspace_id,
                name=item.name,
                semantic_model=semantic_model,
                content_yaml=content_yaml,
                content_json=content_json,
                management_mode=ManagementMode.CONFIG_MANAGED,
            )
        return semantic_models

    @staticmethod
    def _build_llm_connection_records(
        *,
        config_path: Path,
        config: LocalRuntimeConfig,
        context: RuntimeContext,
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
                    workspace_id=context.workspace_id,
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

            agent_id = _stable_uuid("agent", f"{config_path}:{agent.name}")
            definition = ConfiguredLocalRuntimeHostFactory._build_agent_definition_payload(
                agent=agent,
                datasets=datasets,
                connectors=connectors,
                semantic_models=semantic_models,
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
                    management_mode=ManagementMode.CONFIG_MANAGED,
                    lifecycle_state=LifecycleState.ACTIVE,
                ),
            )
        return records

    @staticmethod
    def _build_agent_definition_payload(
        *,
        agent: LocalRuntimeAgentConfig,
        datasets: dict[str, DatasetMetadata],
        connectors: dict[str, ConnectorMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
    ) -> dict[str, Any]:
        payload = (
            copy.deepcopy(agent.definition)
            if agent.definition
            else ConfiguredLocalRuntimeHostFactory._build_shorthand_agent_definition_payload(
                agent=agent,
                datasets=datasets,
                semantic_models=semantic_models,
            )
        )
        normalized = ConfiguredLocalRuntimeHostFactory._normalize_agent_definition_payload(
            agent=agent,
            definition=payload,
            datasets=datasets,
            connectors=connectors,
            semantic_models=semantic_models,
        )
        definition_model = AgentDefinitionFactory().create_agent_definition(normalized)
        return definition_model.model_dump(mode="json", exclude_none=True)

    @staticmethod
    def _build_shorthand_agent_definition_payload(
        *,
        agent: LocalRuntimeAgentConfig,
        datasets: dict[str, DatasetMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
    ) -> dict[str, Any]:
        semantic_model_name = str(agent.semantic_model or "").strip()
        dataset_name = str(agent.dataset or "").strip()
        semantic_model = semantic_models.get(semantic_model_name) if semantic_model_name else None
        dataset = datasets.get(dataset_name) if dataset_name else None
        if semantic_model_name and semantic_model is None:
            raise ValueError(
                f"Agent '{agent.name}' references unknown semantic model '{semantic_model_name}'."
            )
        if dataset_name and dataset is None:
            raise ValueError(f"Agent '{agent.name}' references unknown dataset '{dataset_name}'.")

        selected_connector_ids: list[str] = []
        if dataset is not None and dataset.connection_id is not None:
            selected_connector_ids.append(str(dataset.connection_id))
        if semantic_model is not None:
            for dataset_key in semantic_model.semantic_model.datasets.keys():
                semantic_dataset = datasets.get(dataset_key)
                if semantic_dataset is None or semantic_dataset.connection_id is None:
                    continue
                connector_id = str(semantic_dataset.connection_id)
                if connector_id not in selected_connector_ids:
                    selected_connector_ids.append(connector_id)

        tool_name_suffix = semantic_model_name or dataset_name or agent.name
        tool_config: dict[str, list[str]]
        tool_description: str
        if semantic_model is not None:
            tool_config = {"semantic_model_ids": [str(semantic_model.id)]}
            tool_description = f"Semantic analytics access for '{semantic_model.name}'."
        elif dataset is not None:
            tool_config = {"dataset_ids": [str(dataset.id)]}
            tool_description = f"Dataset analytics access for '{dataset.name}'."
        else:
            raise ValueError(
                f"Agent '{agent.name}' must define definition or a shorthand semantic_model/dataset binding."
            )

        system_prompt = (
            "You are a Langbridge analytics agent. Use the configured tools "
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
                    "name": f"{agent.name}_{_dataset_sql_alias(tool_name_suffix)}_sql",
                    "tool_type": "sql",
                    "description": tool_description,
                    "config": tool_config,
                }
            ],
            "access_policy": {
                "allowed_connectors": selected_connector_ids,
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
    def _normalize_agent_definition_payload(
        *,
        agent: LocalRuntimeAgentConfig,
        definition: Mapping[str, Any],
        datasets: dict[str, DatasetMetadata],
        connectors: dict[str, ConnectorMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
    ) -> dict[str, Any]:
        payload = copy.deepcopy(dict(definition))

        tools_payload = payload.get("tools")
        if tools_payload is not None:
            if not isinstance(tools_payload, list):
                raise ValueError(f"Agent '{agent.name}' definition.tools must be a list.")
            normalized_tools: list[dict[str, Any]] = []
            for raw_tool in tools_payload:
                if not isinstance(raw_tool, Mapping):
                    raise ValueError(f"Agent '{agent.name}' definition.tools entries must be mappings.")
                tool_payload = copy.deepcopy(dict(raw_tool))
                if str(tool_payload.get("tool_type") or "").strip().lower() == "sql":
                    config_payload = tool_payload.get("config") or {}
                    if not isinstance(config_payload, Mapping):
                        raise ValueError(
                            f"Agent '{agent.name}' SQL tool '{tool_payload.get('name')}' must define a mapping config."
                        )
                    tool_payload["config"] = ConfiguredLocalRuntimeHostFactory._normalize_local_sql_tool_config(
                        agent_name=agent.name,
                        tool_name=str(tool_payload.get("name") or "").strip() or "sql_tool",
                        config=dict(config_payload),
                        datasets=datasets,
                        semantic_models=semantic_models,
                    )
                normalized_tools.append(tool_payload)
            payload["tools"] = normalized_tools

        access_policy_payload = payload.get("access_policy")
        if isinstance(access_policy_payload, Mapping):
            normalized_access_policy = copy.deepcopy(dict(access_policy_payload))
            for field_name in ("allowed_connectors", "denied_connectors"):
                if field_name in normalized_access_policy:
                    normalized_access_policy[field_name] = (
                        ConfiguredLocalRuntimeHostFactory._normalize_local_connector_refs(
                            agent_name=agent.name,
                            field_name=field_name,
                            raw_values=normalized_access_policy.get(field_name),
                            connectors=connectors,
                        )
                    )
            payload["access_policy"] = normalized_access_policy

        return payload

    @staticmethod
    def _normalize_local_sql_tool_config(
        *,
        agent_name: str,
        tool_name: str,
        config: dict[str, Any],
        datasets: dict[str, DatasetMetadata],
        semantic_models: dict[str, LocalRuntimeSemanticModelRecord],
    ) -> dict[str, Any]:
        normalized = copy.deepcopy(config)
        normalized.setdefault("dataset_ids", [])
        normalized.setdefault("semantic_model_ids", [])
        if "dataset_ids" in normalized:
            normalized["dataset_ids"] = ConfiguredLocalRuntimeHostFactory._normalize_named_uuid_refs(
                agent_name=agent_name,
                field_name=f"tool '{tool_name}' dataset_ids",
                raw_values=normalized.get("dataset_ids"),
                by_name=datasets,
                by_id={record.id: record for record in datasets.values()},
            )
        if "semantic_model_ids" in normalized:
            normalized["semantic_model_ids"] = ConfiguredLocalRuntimeHostFactory._normalize_named_uuid_refs(
                agent_name=agent_name,
                field_name=f"tool '{tool_name}' semantic_model_ids",
                raw_values=normalized.get("semantic_model_ids"),
                by_name=semantic_models,
                by_id={record.id: record for record in semantic_models.values()},
            )
        return normalized

    @staticmethod
    def _normalize_local_connector_refs(
        *,
        agent_name: str,
        field_name: str,
        raw_values: Any,
        connectors: dict[str, ConnectorMetadata],
    ) -> list[str]:
        return ConfiguredLocalRuntimeHostFactory._normalize_named_uuid_refs(
            agent_name=agent_name,
            field_name=field_name,
            raw_values=raw_values,
            by_name=connectors,
            by_id={record.id: record for record in connectors.values()},
        )

    @staticmethod
    def _normalize_named_uuid_refs(
        *,
        agent_name: str,
        field_name: str,
        raw_values: Any,
        by_name: Mapping[str, Any],
        by_id: Mapping[uuid.UUID, Any],
    ) -> list[str]:
        if raw_values is None:
            return []
        if not isinstance(raw_values, list):
            raise ValueError(f"Agent '{agent_name}' {field_name} must be a list.")

        normalized_values: list[str] = []
        for raw_value in raw_values:
            normalized_ref = str(raw_value or "").strip()
            if not normalized_ref:
                raise ValueError(f"Agent '{agent_name}' {field_name} must not contain empty values.")
            resolved_id: uuid.UUID | None = None
            try:
                candidate_id = uuid.UUID(normalized_ref)
            except (TypeError, ValueError):
                candidate_id = None
            if candidate_id is not None:
                if candidate_id not in by_id:
                    raise ValueError(
                        f"Agent '{agent_name}' {field_name} references unknown id '{normalized_ref}'."
                    )
                resolved_id = candidate_id
            else:
                referenced_item = by_name.get(normalized_ref)
                if referenced_item is None:
                    raise ValueError(
                        f"Agent '{agent_name}' {field_name} references unknown name '{normalized_ref}'."
                    )
                resolved_id = referenced_item.id
            resolved_id_str = str(resolved_id)
            if resolved_id_str not in normalized_values:
                normalized_values.append(resolved_id_str)
        return normalized_values

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
                    if semantic_model.semantic_model is None:
                        continue
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
                workspace_id=context.workspace_id,
                name=semantic_model.name,
                description=(
                    semantic_model.semantic_model.description
                    if semantic_model.semantic_model is not None
                    else semantic_model.content_json.get("description")
                ),
                content_yaml=semantic_model.content_yaml,
                content_json=copy.deepcopy(semantic_model.content_json),
                created_at=now,
                updated_at=now,
                connector_id=None,
                management_mode=semantic_model.management_mode,
                lifecycle_state=LifecycleState.ACTIVE,
            )
        return records

    @staticmethod
    def _materialize_semantic_model_payload(
        *,
        semantic_model: LocalRuntimeSemanticModelConfig,
        datasets: dict[str, DatasetMetadata],
    ) -> dict[str, Any]:
        payload = copy.deepcopy(semantic_model.model or {})
        is_unified_model = bool(
            payload.get("source_models")
            or payload.get("sourceModels")
        )
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

        if is_unified_model:
            payload.pop("datasets", None)
            payload.pop("tables", None)
        else:
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
                    workspace_id=context.workspace_id,
                    name=record.name,
                    description=(
                        record.semantic_model.description
                        if record.semantic_model is not None
                        else record.content_json.get("description")
                    ),
                    content_yaml=record.content_yaml,
                    content_json=copy.deepcopy(record.content_json),
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                    management_mode=record.management_mode,
                    lifecycle_state=LifecycleState.ACTIVE,
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
        semantic_vector_index_store = MemorySemanticVectorIndexProvider(
            {}
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
        default_embedding_provider = None
        first_llm_connection = next(iter(llm_connections.values()), None)
        if first_llm_connection is not None:
            try:
                default_embedding_provider = EmbeddingProvider.from_llm_connection(
                    first_llm_connection.connection
                )
            except EmbeddingProviderError:
                default_embedding_provider = None
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
        semantic_vector_search_service = (
            SemanticVectorSearchService(
                dataset_repository=dataset_repository,
                federated_query_tool=federated_query_tool,
                logger=logging.getLogger("langbridge.runtime.semantic.vector.local"),
                dataset_provider=dataset_provider,
                semantic_model_provider=semantic_model_provider,
                semantic_vector_index_store=semantic_vector_index_store,
                connector_provider=connector_provider,
                credential_provider=credential_provider,
                embedding_provider=default_embedding_provider,
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
            connector_provider=connector_provider,
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
                semantic_vector_search_service=semantic_vector_search_service,
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
                semantic_vector_indexes=semantic_vector_index_store if semantic_models else None,
                sync_state=sync_state_provider,
                credentials=credential_provider,
            ),
            services=RuntimeServices(
                federated_query_tool=federated_query_tool,
                semantic_query=semantic_query_service,
                semantic_vector_search=semantic_vector_search_service,
                sql_query=sql_query_service,
                dataset_query=dataset_query_service,
                dataset_sync=dataset_sync_service,
                agent_execution=agent_execution_service,
            ),
        ), thread_repository, thread_message_repository


def build_configured_local_runtime(
    *,
    config_path: str | Path,
    workspace_id: uuid.UUID | None = None,
    actor_id: uuid.UUID | None = None,
    roles: list[str] | tuple[str, ...] | None = None,
    request_id: str | None = None,
    apply_migrations: bool | None = None,
) -> ConfiguredLocalRuntimeHost:
    resolved_config_path = Path(config_path).resolve()
    # Use a stable UUID based on the config path for the workspace ID if not provided, to ensure consistency across runs with the same config.
    resolved_workspace_id = workspace_id or _stable_uuid("workspace", str(resolved_config_path))
    context = RuntimeContext.build(
        workspace_id=resolved_workspace_id,
        actor_id=actor_id or _stable_uuid("actor", str(resolved_config_path)),
        roles=roles,
        request_id=request_id or f"local-runtime:{resolved_config_path.name}",
    )
    return ConfiguredLocalRuntimeHostFactory.build(
        config_path=resolved_config_path,
        context=context,
        apply_migrations=apply_migrations,
    )


__all__ = [
    "ConfiguredLocalRuntimeHost",
    "ConfiguredLocalRuntimeHostFactory",
    "LocalRuntimeConfig",
    "_stable_uuid",
    "build_configured_local_runtime",
]
