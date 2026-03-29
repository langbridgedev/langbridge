
from typing import Any

__all__ = [
    "AgentExecutionResult",
    "AgentExecutionService",
    "ConnectorSyncRuntime",
    "DatasetExecutionResolver",
    "DatasetQueryService",
    "DatasetSyncService",
    "MaterializedDatasetResult",
    "RuntimeHost",
    "RuntimeProviders",
    "RuntimeServices",
    "SemanticQueryExecutionService",
    "SemanticVectorSearchService",
    "SqlQueryService",
    "build_binding_for_dataset",
    "build_file_scan_sql",
    "synthetic_file_connector_id",
]


def __getattr__(name: str) -> Any:
    if name in {
        "DatasetExecutionResolver",
        "build_binding_for_dataset",
        "build_file_scan_sql",
        "synthetic_file_connector_id",
    }:
        from langbridge.runtime.services.dataset_execution import (
            DatasetExecutionResolver,
            build_binding_for_dataset,
            build_file_scan_sql,
            synthetic_file_connector_id,
        )

        values = {
            "DatasetExecutionResolver": DatasetExecutionResolver,
            "build_binding_for_dataset": build_binding_for_dataset,
            "build_file_scan_sql": build_file_scan_sql,
            "synthetic_file_connector_id": synthetic_file_connector_id,
        }
        return values[name]
    if name in {"AgentExecutionResult", "AgentExecutionService"}:
        from langbridge.runtime.services.agent_execution_service import (
            AgentExecutionResult,
            AgentExecutionService,
        )

        return {
            "AgentExecutionResult": AgentExecutionResult,
            "AgentExecutionService": AgentExecutionService,
        }[name]
    if name == "DatasetQueryService":
        from langbridge.runtime.services.dataset_query_service import (
            DatasetQueryService,
        )

        return DatasetQueryService
    if name in {"ConnectorSyncRuntime", "DatasetSyncService", "MaterializedDatasetResult"}:
        from langbridge.runtime.services.dataset_sync_service import (
            ConnectorSyncRuntime,
            DatasetSyncService,
            MaterializedDatasetResult,
        )

        return {
            "ConnectorSyncRuntime": ConnectorSyncRuntime,
            "DatasetSyncService": DatasetSyncService,
            "MaterializedDatasetResult": MaterializedDatasetResult,
        }[name]
    if name in {"RuntimeHost", "RuntimeProviders", "RuntimeServices"}:
        from langbridge.runtime.services.runtime_host import (
            RuntimeHost,
            RuntimeProviders,
            RuntimeServices,
        )

        return {
            "RuntimeHost": RuntimeHost,
            "RuntimeProviders": RuntimeProviders,
            "RuntimeServices": RuntimeServices,
        }[name]
    if name == "SemanticQueryExecutionService":
        from langbridge.runtime.services.semantic_query_execution_service import (
            SemanticQueryExecutionService,
        )

        return SemanticQueryExecutionService
    if name == "SemanticVectorSearchService":
        from langbridge.runtime.services.semantic_vector_search_service import (
            SemanticVectorSearchService,
        )

        return SemanticVectorSearchService
    if name == "SqlQueryService":
        from langbridge.runtime.services.sql_query_service import SqlQueryService

        return SqlQueryService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
