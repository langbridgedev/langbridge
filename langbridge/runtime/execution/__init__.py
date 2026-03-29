
from typing import Any

__all__ = [
    "DuckDbExecutionEngine",
    "ExecutionEngine",
    "ExecutionResult",
    "FederatedQueryExecutor",
    "FederatedQueryTool",
]


def __getattr__(name: str) -> Any:
    if name in {"ExecutionEngine", "ExecutionResult"}:
        from langbridge.runtime.execution.engine import ExecutionEngine, ExecutionResult

        return {
            "ExecutionEngine": ExecutionEngine,
            "ExecutionResult": ExecutionResult,
        }[name]
    if name in {"FederatedQueryExecutor", "FederatedQueryTool"}:
        from langbridge.runtime.execution.federated_query_tool import (
            FederatedQueryExecutor,
            FederatedQueryTool,
        )

        return {
            "FederatedQueryExecutor": FederatedQueryExecutor,
            "FederatedQueryTool": FederatedQueryTool,
        }[name]
    if name == "DuckDbExecutionEngine":
        from langbridge.runtime.execution.duckdb_engine import DuckDbExecutionEngine

        return DuckDbExecutionEngine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
