from dataclasses import dataclass, replace
import inspect
from typing import Any

from langbridge.runtime.context import RuntimeContext
from langbridge.runtime.execution import FederatedQueryTool
from langbridge.runtime.providers import (
    ConnectorMetadataProvider,
    CredentialProvider,
    DatasetMetadataProvider,
    SemanticModelMetadataProvider,
    SemanticVectorIndexMetadataProvider,
    SyncStateProvider,
)
from langbridge.runtime.services.agent_execution_service import AgentExecutionService
from langbridge.runtime.services.dataset_query_service import DatasetQueryService
from langbridge.runtime.services.dataset_sync_service import ConnectorSyncRuntime
from langbridge.runtime.services.semantic_query_execution_service import (
    SemanticQueryExecutionService,
)
from langbridge.runtime.services.semantic_sql_query_service import SemanticSqlQueryService
from langbridge.runtime.services.semantic_vector_search_service import (
    SemanticVectorSearchService,
)
from langbridge.runtime.services.sql_query_service import SqlQueryService


@dataclass(slots=True)
class RuntimeProviders:
    dataset_metadata: DatasetMetadataProvider
    connector_metadata: ConnectorMetadataProvider
    semantic_models: SemanticModelMetadataProvider
    semantic_vector_indexes: SemanticVectorIndexMetadataProvider
    sync_state: SyncStateProvider
    credentials: CredentialProvider


@dataclass(slots=True)
class RuntimeServices:
    federated_query_tool: FederatedQueryTool
    semantic_query: SemanticQueryExecutionService
    semantic_vector_search: SemanticVectorSearchService
    sql_query: SqlQueryService
    dataset_query: DatasetQueryService
    dataset_sync: ConnectorSyncRuntime
    agent_execution: AgentExecutionService
    semantic_sql_query: SemanticSqlQueryService | None = None


@dataclass(slots=True)
class RuntimeHost:
    context: RuntimeContext
    providers: RuntimeProviders
    services: RuntimeServices

    def with_context(self, context: RuntimeContext) -> "RuntimeHost":
        return replace(self, context=context)

    async def aclose(self) -> None:
        federated_query_tool = getattr(self.services, "federated_query_tool", None)
        if federated_query_tool is None:
            return None
        aclose = getattr(federated_query_tool, "aclose", None)
        if callable(aclose):
            result = aclose()
            if inspect.isawaitable(result):
                await result
            return None
        close = getattr(federated_query_tool, "close", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result
        return None

    def close(self) -> None:
        federated_query_tool = getattr(self.services, "federated_query_tool", None)
        if federated_query_tool is None:
            return None
        close = getattr(federated_query_tool, "close", None)
        if callable(close):
            close()
        return None

    async def query_dataset(self, *args: Any, **kwargs: Any) -> Any:
        if self.services.dataset_query is None:
            raise RuntimeError("DatasetQueryService is not configured for this runtime host.")
        return await self.services.dataset_query.query_dataset(*args, **kwargs)

    async def execute_sql(self, *args: Any, **kwargs: Any) -> Any:
        if self.services.sql_query is None:
            raise RuntimeError("SqlQueryService is not configured for this runtime host.")
        return await self.services.sql_query.execute_sql(*args, **kwargs)

    async def sync_dataset(self, *args: Any, **kwargs: Any) -> Any:
        if self.services.dataset_sync is None:
            raise RuntimeError("DatasetSyncService is not configured for this runtime host.")
        return await self.services.dataset_sync.sync_dataset(*args, **kwargs)

    async def create_agent(self, *args: Any, **kwargs: Any) -> Any:
        if self.services.agent_execution is None:
            raise RuntimeError("AgentExecutionService is not configured for this runtime host.")
        execute = getattr(self.services.agent_execution, "execute", None)
        if execute is None:
            raise RuntimeError("AgentExecutionService does not expose an execute method.")
        return await execute(*args, **kwargs)

    async def query_semantic(self, *args: Any, **kwargs: Any) -> Any:
        if self.services.semantic_query is None:
            raise RuntimeError("SemanticQueryExecutionService is not configured for this runtime host.")
        return await self.services.semantic_query.execute_standard_query(*args, **kwargs)

    async def query_semantic_graph(self, *args: Any, **kwargs: Any) -> Any:
        if self.services.semantic_query is None:
            raise RuntimeError("SemanticQueryExecutionService is not configured for this runtime host.")
        return await self.services.semantic_query.execute_semantic_graph_query(*args, **kwargs)

    async def query_unified_semantic(self, *args: Any, **kwargs: Any) -> Any:
        return await self.query_semantic_graph(*args, **kwargs)

    def parse_semantic_sql_query(self, *args: Any, **kwargs: Any) -> Any:
        service = self.services.semantic_sql_query or SemanticSqlQueryService()
        return service.parse_query(*args, **kwargs)

    def build_semantic_sql_query(self, *args: Any, **kwargs: Any) -> Any:
        service = self.services.semantic_sql_query or SemanticSqlQueryService()
        return service.build_query_plan(*args, **kwargs)

    async def refresh_semantic_vector_search(self, *args: Any, **kwargs: Any) -> Any:
        if self.services.semantic_vector_search is None:
            raise RuntimeError("SemanticVectorSearchService is not configured for this runtime host.")
        kwargs.setdefault("workspace_id", self.context.workspace_id)
        return await self.services.semantic_vector_search.refresh_workspace(*args, **kwargs)

    async def search_semantic_vectors(self, *args: Any, **kwargs: Any) -> Any:
        if self.services.semantic_vector_search is None:
            raise RuntimeError("SemanticVectorSearchService is not configured for this runtime host.")
        kwargs.setdefault("workspace_id", self.context.workspace_id)
        return await self.services.semantic_vector_search.search(*args, **kwargs)

    def can_refresh_semantic_vector_search(self) -> bool:
        if self.services.semantic_vector_search is None:
            return False
        capability = getattr(self.services.semantic_vector_search, "can_refresh", None)
        if callable(capability):
            return bool(capability())
        return True

    def semantic_vector_refresh_unavailable_reason(self) -> str | None:
        if self.services.semantic_vector_search is None:
            return "SemanticVectorSearchService is not configured for this runtime host."
        reason = getattr(
            self.services.semantic_vector_search,
            "refresh_unavailable_reason",
            None,
        )
        if callable(reason):
            return reason()
        return None
