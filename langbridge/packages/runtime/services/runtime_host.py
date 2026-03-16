from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from langbridge.packages.runtime.context import RuntimeContext
from langbridge.packages.runtime.execution import FederatedQueryTool
from langbridge.packages.runtime.providers import (
    ConnectorMetadataProvider,
    CredentialProvider,
    DatasetMetadataProvider,
    SemanticModelMetadataProvider,
    SyncStateProvider,
)
from langbridge.packages.runtime.services.dataset_sync_service import ConnectorSyncRuntime
from langbridge.packages.runtime.services.semantic_query_execution_service import (
    SemanticQueryExecutionService,
)


@dataclass(slots=True)
class RuntimeProviders:
    dataset_metadata: DatasetMetadataProvider | None = None
    connector_metadata: ConnectorMetadataProvider | None = None
    semantic_models: SemanticModelMetadataProvider | None = None
    sync_state: SyncStateProvider | None = None
    credentials: CredentialProvider | None = None


@dataclass(slots=True)
class RuntimeServices:
    federated_query_tool: FederatedQueryTool | None = None
    semantic_query: SemanticQueryExecutionService | None = None
    sql_query: Any | None = None
    dataset_query: Any | None = None
    dataset_sync: ConnectorSyncRuntime | None = None
    agent_execution: Any | None = None


@dataclass(slots=True)
class RuntimeHost:
    context: RuntimeContext
    providers: RuntimeProviders
    services: RuntimeServices

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
        return await self.services.dataset_sync.sync_resource(*args, **kwargs)

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
