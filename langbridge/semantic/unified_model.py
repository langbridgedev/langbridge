from langbridge.semantic.graph import (
    SemanticGraph,
    SemanticGraphRelationship,
    SemanticGraphSourceModel,
)

UnifiedSemanticModelSource = SemanticGraphSourceModel
UnifiedSemanticRelationship = SemanticGraphRelationship
UnifiedSemanticModel = SemanticGraph

__all__ = [
    "SemanticGraph",
    "SemanticGraphRelationship",
    "SemanticGraphSourceModel",
    "UnifiedSemanticModel",
    "UnifiedSemanticRelationship",
    "UnifiedSemanticModelSource",
]
