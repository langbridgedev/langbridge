import uuid
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class MaterializedDatasetResult:
    dataset_id: uuid.UUID
    dataset_name: str
    source_key: str
    row_count: int
    bytes_written: int | None
    schema_drift: dict[str, Any] | None = None
