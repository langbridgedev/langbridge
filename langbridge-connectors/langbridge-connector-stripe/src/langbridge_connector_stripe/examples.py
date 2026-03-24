from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CustomDatasetResource(_Base):
    name: str
    path: str
    primary_key: str
    use_connector_pagination: bool = True
    use_connector_incremental: bool = True


class DatasetSyncSelection(_Base):
    resource_key: str | None = None
    resource: CustomDatasetResource | None = None
    sync_mode: str
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_selection(self) -> "DatasetSyncSelection":
        if bool(self.resource_key) == bool(self.resource):
            raise ValueError("Exactly one of 'resource_key' or 'resource' must be provided.")
        return self


class DeclarativeDatasetExample(_Base):
    name: str
    description: str
    connector: str
    connector_sync: DatasetSyncSelection


class DeclarativeDatasetConnectorReference(_Base):
    name: str
    type: str
    package: str
    note: str


class DeclarativeDatasetExampleSet(_Base):
    connector: DeclarativeDatasetConnectorReference
    examples: list[DeclarativeDatasetExample]


@lru_cache(maxsize=1)
def load_dataset_examples() -> DeclarativeDatasetExampleSet:
    examples_path = Path(__file__).resolve().parents[2] / "examples" / "dataset_selection_examples.yaml"
    payload = yaml.safe_load(examples_path.read_text(encoding="utf-8"))
    return DeclarativeDatasetExampleSet.model_validate(payload)
