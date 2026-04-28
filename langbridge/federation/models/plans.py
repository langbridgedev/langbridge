
import time
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class QueryType(str, Enum):
    SMQ = "smq"
    SQL = "sql"


class JoinStrategy(str, Enum):
    BROADCAST = "broadcast"
    PARTITIONED_HASH = "partitioned_hash"


class StageType(str, Enum):
    REMOTE_SCAN = "remote_scan"
    REMOTE_FULL_QUERY = "remote_full_query"
    LOCAL_COMPUTE = "local_compute"


class StageCacheStatus(str, Enum):
    HIT = "hit"
    MISS = "miss"
    BYPASS = "bypass"


class TableRef(BaseModel):
    alias: str
    table_key: str
    source_id: str
    connector_id: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    table: str
    catalog: str | None = None


class JoinRef(BaseModel):
    left_alias: str
    right_alias: str
    join_type: str
    on_sql: str


class LogicalPlan(BaseModel):
    query_type: QueryType
    sql: str
    from_alias: str
    tables: dict[str, TableRef]
    joins: list[JoinRef] = Field(default_factory=list)
    where_sql: str | None = None
    having_sql: str | None = None
    group_by_sql: list[str] = Field(default_factory=list)
    order_by_sql: list[str] = Field(default_factory=list)
    limit: int | None = None
    offset: int | None = None
    has_cte: bool = False


class SourceSubplan(BaseModel):
    stage_id: str
    source_id: str
    alias: str
    table_key: str
    sql: str | None = None
    resource: str | None = None
    projected_columns: list[str] = Field(default_factory=list)
    pushed_filters: list[str] = Field(default_factory=list)
    pushed_limit: int | None = None
    estimated_rows: float | None = None
    estimated_bytes: float | None = None
    pushdown: "PushdownDiagnostics" = Field(default_factory=lambda: PushdownDiagnostics())


class PushdownDecision(BaseModel):
    pushed: bool = False
    supported: bool | None = None
    reason: str | None = None
    details: list[str] = Field(default_factory=list)


class PushdownDiagnostics(BaseModel):
    full_query: PushdownDecision = Field(default_factory=PushdownDecision)
    filter: PushdownDecision = Field(default_factory=PushdownDecision)
    projection: PushdownDecision = Field(default_factory=PushdownDecision)
    aggregation: PushdownDecision = Field(default_factory=PushdownDecision)
    limit: PushdownDecision = Field(default_factory=PushdownDecision)
    join: PushdownDecision = Field(default_factory=PushdownDecision)


class StageDefinition(BaseModel):
    stage_id: str
    stage_type: StageType
    dependencies: list[str] = Field(default_factory=list)
    source_id: str | None = None
    sql: str | None = None
    sql_dialect: str | None = None
    subplan: SourceSubplan | None = None
    retry_limit: int = 2
    metadata: dict[str, Any] = Field(default_factory=dict)


class PhysicalPlan(BaseModel):
    plan_id: str
    logical_plan: LogicalPlan
    stages: list[StageDefinition]
    result_stage_id: str
    join_order: list[str] = Field(default_factory=list)
    join_strategies: dict[str, JoinStrategy] = Field(default_factory=dict)
    pushdown_full_query: bool = False
    pushdown_reasons: list[str] = Field(default_factory=list)


class StageArtifact(BaseModel):
    stage_id: str
    artifact_key: str
    rows: int
    bytes_written: int
    content_hash: str


class StageCacheInputSnapshot(BaseModel):
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


class StageMetrics(BaseModel):
    stage_id: str
    stage_type: StageType
    source_id: str | None = None
    attempts: int
    runtime_ms: int
    rows: int
    bytes_written: int
    source_elapsed_ms: int | None = None
    cached: bool = False
    cacheable: bool = False
    cache_status: StageCacheStatus = StageCacheStatus.BYPASS
    cache_reason: str | None = None
    cache_inputs: list[StageCacheInputSnapshot] = Field(default_factory=list)
    started_at: float = Field(default_factory=time.time)
    finished_at: float | None = None


class ExecutionSummary(BaseModel):
    plan_id: str
    total_runtime_ms: int
    stage_metrics: list[StageMetrics]


class ResultHandle(BaseModel):
    handle_id: str
    workspace_id: str
    plan_id: str
    result_stage_id: str
    artifact_key: str
    created_at: float = Field(default_factory=time.time)
    execution: ExecutionSummary
    logical_plan: LogicalPlan | None = None
    physical_plan: PhysicalPlan | None = None


class FederatedExplainPlan(BaseModel):
    logical_plan: LogicalPlan
    physical_plan: PhysicalPlan
