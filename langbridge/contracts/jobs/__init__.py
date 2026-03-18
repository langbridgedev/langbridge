from __future__ import annotations

from importlib import import_module
from typing import Any

_MODULE_EXPORTS = {
    "agent_job": (
        "CreateAgentJobRequest",
        "JobEventVisibility",
        "JobEventResponse",
        "JobFinalResponse",
        "AgentJobStateResponse",
        "AgentJobCancelResponse",
    ),
    "agentic_semantic_model_job": ("CreateAgenticSemanticModelJobRequest",),
    "connector_job": ("CreateConnectorSyncJobRequest",),
    "copilot_dashboard_job": (
        "CreateCopilotDashboardJobRequest",
        "CopilotDashboardAssistRequest",
        "CopilotDashboardJobResponse",
    ),
    "dataset_job": (
        "CreateDatasetPreviewJobRequest",
        "CreateDatasetProfileJobRequest",
        "CreateDatasetCsvIngestJobRequest",
        "CreateDatasetBulkCreateJobRequest",
    ),
    "semantic_query_job": ("CreateSemanticQueryJobRequest",),
    "sql_job": ("CreateSqlJobRequest",),
    "type": ("JobType",),
}

__all__ = [name for names in _MODULE_EXPORTS.values() for name in names]


def __getattr__(name: str) -> Any:
    for module_name, export_names in _MODULE_EXPORTS.items():
        if name not in export_names:
            continue

        module = import_module(f"{__name__}.{module_name}")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
