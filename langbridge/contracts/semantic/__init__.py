from __future__ import annotations

from importlib import import_module
from typing import Any

_MODULE_EXPORTS = {
    "semantic_models": (
        "SemanticModelCreateRequest",
        "SemanticModelCatalogFieldResponse",
        "SemanticModelCatalogDatasetResponse",
        "SemanticModelCatalogResponse",
        "SemanticModelSelectionGenerateRequest",
        "SemanticModelSelectionGenerateResponse",
        "SemanticModelAgenticJobCreateRequest",
        "SemanticModelAgenticJobCreateResponse",
        "SemanticModelRecordResponse",
        "SemanticModelUpdateRequest",
    ),
    "semantic_query": (
        "SemanticQueryRequest",
        "SemanticQueryResponse",
        "SemanticQueryMetaResponse",
        "SemanticQueryJobResponse",
        "UnifiedSemanticRelationshipRequest",
        "UnifiedSemanticMetricRequest",
        "UnifiedSemanticSourceModelRequest",
        "UnifiedSemanticQueryMetaRequest",
        "UnifiedSemanticQueryMetaResponse",
        "UnifiedSemanticQueryRequest",
        "UnifiedSemanticQueryResponse",
    ),
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
