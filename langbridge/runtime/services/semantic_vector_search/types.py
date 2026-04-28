import uuid
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SemanticVectorSearchHit:
    index_id: uuid.UUID
    semantic_model_id: uuid.UUID
    dataset_key: str
    dimension_name: str
    matched_value: str
    score: float
    source_text: str
