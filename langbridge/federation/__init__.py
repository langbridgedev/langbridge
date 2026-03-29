
from typing import Any

from langbridge.federation.models.smq import SMQFilter, SMQOrderItem, SMQQuery, SMQTimeDimension
from langbridge.federation.models.virtual_dataset import (
    FederationWorkflow,
    TableStatistics,
    VirtualDataset,
    VirtualRelationship,
    VirtualTableBinding,
)
from langbridge.federation.models.plans import (
    ExecutionSummary,
    FederatedExplainPlan,
    LogicalPlan,
    PhysicalPlan,
    ResultHandle,
)

__all__ = [
    "FederatedQueryService",
    "SMQFilter",
    "SMQOrderItem",
    "SMQQuery",
    "SMQTimeDimension",
    "FederationWorkflow",
    "TableStatistics",
    "VirtualDataset",
    "VirtualRelationship",
    "VirtualTableBinding",
    "ExecutionSummary",
    "FederatedExplainPlan",
    "LogicalPlan",
    "PhysicalPlan",
    "ResultHandle",
]


def __getattr__(name: str) -> Any:
    if name == "FederatedQueryService":
        from langbridge.federation.service import FederatedQueryService

        return FederatedQueryService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
