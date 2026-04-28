from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import Field

from langbridge.runtime.models.base import RuntimeModel


class FederationDiagnosticsSummary(RuntimeModel):
    query_type: str
    plan_id: str
    datasets: list[str] = Field(default_factory=list)
    tables: list[str] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    stage_count: int = 0
    source_count: int = 0
    full_query_pushdown: bool = False
    pushdown_reason: str | None = None
    total_runtime_ms: int | None = None
    total_source_elapsed_ms: int | None = None
    final_rows: int | None = None
    final_bytes: int | None = None
    local_materialized_rows: int | None = None
    local_materialized_bytes: int | None = None
    cache_hits: int = 0
    cache_misses: int = 0
    cache_bypasses: int = 0


class FederationLogicalTable(RuntimeModel):
    alias: str
    table_key: str
    dataset: str | None = None
    source_id: str
    connector_id: str | None = None
    catalog: str | None = None
    schema_name: str | None = None
    table: str


class FederationLogicalJoin(RuntimeModel):
    left_alias: str
    right_alias: str
    join_type: str
    on_sql: str
    strategy: str | None = None


class FederationLogicalPlanSurface(RuntimeModel):
    query_type: str
    sql: str
    from_alias: str
    tables: list[FederationLogicalTable] = Field(default_factory=list)
    joins: list[FederationLogicalJoin] = Field(default_factory=list)
    where_sql: str | None = None
    having_sql: str | None = None
    group_by_sql: list[str] = Field(default_factory=list)
    order_by_sql: list[str] = Field(default_factory=list)
    limit: int | None = None
    offset: int | None = None
    has_cte: bool = False


class FederationPhysicalStageSurface(RuntimeModel):
    stage_id: str
    stage_type: str
    source_id: str | None = None
    alias: str | None = None
    table_key: str | None = None
    dependencies: list[str] = Field(default_factory=list)
    local_sql: str | None = None
    remote_sql: str | None = None
    sql_dialect: str | None = None
    resource: str | None = None
    estimated_rows: float | None = None
    estimated_bytes: float | None = None


class FederationPhysicalPlanSurface(RuntimeModel):
    plan_id: str
    result_stage_id: str
    join_order: list[str] = Field(default_factory=list)
    join_strategies: dict[str, str] = Field(default_factory=dict)
    full_query_pushdown: bool = False
    pushdown_reasons: list[str] = Field(default_factory=list)
    stages: list[FederationPhysicalStageSurface] = Field(default_factory=list)


class FederationPushdownOperation(RuntimeModel):
    pushed: bool = False
    supported: bool | None = None
    reason: str | None = None
    details: list[str] = Field(default_factory=list)


class FederationStagePushdownSurface(RuntimeModel):
    full_query: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    filter: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    projection: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    aggregation: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    limit: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    join: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)


class FederationStageCacheInputSurface(RuntimeModel):
    kind: str
    cache_policy: str
    source_id: str | None = None
    table_key: str | None = None
    dataset_id: UUID | None = None
    dataset_name: str | None = None
    canonical_reference: str | None = None
    materialization_mode: str | None = None
    revision_id: UUID | None = None
    dependency_stage_id: str | None = None
    freshness_key_present: bool = False
    reason: str | None = None


class FederationStageCacheSurface(RuntimeModel):
    cacheable: bool = False
    status: str | None = None
    reason: str | None = None
    inputs: list[FederationStageCacheInputSurface] = Field(default_factory=list)


class FederationStageMovementSurface(RuntimeModel):
    rows: int | None = None
    bytes_written: int | None = None
    estimated_rows: float | None = None
    estimated_bytes: float | None = None


class FederationStageDiagnosticsSurface(RuntimeModel):
    stage_id: str
    stage_type: str
    source_id: str | None = None
    alias: str | None = None
    dataset: str | None = None
    table_key: str | None = None
    dependencies: list[str] = Field(default_factory=list)
    attempts: int | None = None
    runtime_ms: int | None = None
    source_elapsed_ms: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    cached: bool = False
    local_sql: str | None = None
    remote_sql: str | None = None
    resource: str | None = None
    movement: FederationStageMovementSurface = Field(default_factory=FederationStageMovementSurface)
    cache: FederationStageCacheSurface = Field(default_factory=FederationStageCacheSurface)
    pushdown: FederationStagePushdownSurface = Field(default_factory=FederationStagePushdownSurface)


class FederationSourceDiagnosticsSurface(RuntimeModel):
    source_id: str
    datasets: list[str] = Field(default_factory=list)
    tables: list[str] = Field(default_factory=list)
    stage_ids: list[str] = Field(default_factory=list)
    stage_count: int = 0
    total_runtime_ms: int | None = None
    total_source_elapsed_ms: int | None = None
    total_rows: int | None = None
    total_bytes_written: int | None = None
    estimated_rows: float | None = None
    estimated_bytes: float | None = None
    full_query_pushdown: bool = False
    cache_hits: int = 0
    cache_misses: int = 0
    cache_bypasses: int = 0


class FederationCacheStageSurface(RuntimeModel):
    stage_id: str
    source_id: str | None = None
    status: str | None = None
    cacheable: bool = False
    reason: str | None = None


class FederationCacheSurface(RuntimeModel):
    hits: int = 0
    misses: int = 0
    bypasses: int = 0
    cacheable_stages: int = 0
    stages: list[FederationCacheStageSurface] = Field(default_factory=list)


class FederationPushdownStageSurface(RuntimeModel):
    stage_id: str
    source_id: str | None = None
    alias: str | None = None
    full_query: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    filter: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    projection: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    aggregation: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    limit: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)
    join: FederationPushdownOperation = Field(default_factory=FederationPushdownOperation)


class FederationPushdownSurface(RuntimeModel):
    full_query_pushdown: bool = False
    reasons: list[str] = Field(default_factory=list)
    stages: list[FederationPushdownStageSurface] = Field(default_factory=list)


class RuntimeFederationDiagnostics(RuntimeModel):
    summary: FederationDiagnosticsSummary
    logical_plan: FederationLogicalPlanSurface
    physical_plan: FederationPhysicalPlanSurface
    stages: list[FederationStageDiagnosticsSurface] = Field(default_factory=list)
    sources: list[FederationSourceDiagnosticsSurface] = Field(default_factory=list)
    cache: FederationCacheSurface = Field(default_factory=FederationCacheSurface)
    pushdown: FederationPushdownSurface = Field(default_factory=FederationPushdownSurface)
