from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


class TableStatistics(BaseModel):
    row_count_estimate: float | None = None
    bytes_per_row: float = 128.0
    distinct_estimates: dict[str, float] = Field(default_factory=dict)


class DatasetExecutionDescriptor(BaseModel):
    dataset_id: UUID | None = None
    connector_id: UUID | None = None
    name: str | None = None
    source_kind: str
    connector_kind: str | None = None
    storage_kind: str
    relation_identity: dict[str, Any] = Field(default_factory=dict)
    execution_capabilities: dict[str, Any] = Field(default_factory=dict)
    legacy_dataset_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class VirtualTableBinding(BaseModel):
    table_key: str
    source_id: str
    connector_id: UUID | None = None
    schema: str | None = None
    table: str
    catalog: str | None = None
    stats: TableStatistics | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    dataset_descriptor: DatasetExecutionDescriptor | None = None

    @property
    def full_name(self) -> str:
        parts = [self.catalog, self.schema, self.table]
        return ".".join([part for part in parts if part])


class VirtualRelationship(BaseModel):
    name: str
    left_table: str
    right_table: str
    join_type: str = "inner"
    condition: str


class VirtualDataset(BaseModel):
    id: str
    name: str
    workspace_id: str
    tables: dict[str, VirtualTableBinding]
    relationships: list[VirtualRelationship] = Field(default_factory=list)
    stats_overrides: dict[str, TableStatistics] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_tables(self) -> "VirtualDataset":
        if not self.tables:
            raise ValueError("VirtualDataset requires at least one table binding.")
        return self


class FederationWorkflow(BaseModel):
    id: str
    workspace_id: str
    dataset: VirtualDataset
    broadcast_threshold_bytes: int = 64 * 1024 * 1024
    partition_count: int = 8
    max_stage_retries: int = 2
    stage_parallelism: int = 4
