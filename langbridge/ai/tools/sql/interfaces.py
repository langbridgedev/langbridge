"""
Protocol and data model definitions for layered analytical tooling.
"""
from enum import Enum
from typing import Any, Literal, Protocol, Sequence

from pydantic import BaseModel, Field, model_validator
from langbridge.semantic.model import SemanticModel


class SqlQueryScope(str, Enum):
    semantic = "semantic"
    dataset = "dataset"
    source = "source"


class ConnectorQueryResult(Protocol):
    """
    Generic tabular runtime result shape.
    """

    columns: Sequence[str]
    rows: Sequence[Sequence[Any]]
    elapsed_ms: int | None
    rowcount: int | None
    sql: str | None


class QueryResult(BaseModel):
    """
    Normalized query result returned by the SQL analyst tool.
    """

    columns: list[str]
    rows: list[Sequence[Any]]
    rowcount: int | None = Field(default=None)
    elapsed_ms: int | None = Field(default=None)
    source_sql: str | None = Field(default=None, description="SQL text executed by the analytical runtime.")

    @classmethod
    def from_connector(cls, result: ConnectorQueryResult) -> "QueryResult":
        return cls(
            columns=list(result.columns),
            rows=[tuple(row) for row in result.rows],
            rowcount=getattr(result, "rowcount", None),
            elapsed_ms=getattr(result, "elapsed_ms", None),
            source_sql=getattr(result, "sql", None),
        )


class AnalystOutcomeStatus(str, Enum):
    success = "success"
    empty_result = "empty_result"
    access_denied = "access_denied"
    invalid_request = "invalid_request"
    query_error = "query_error"
    selection_error = "selection_error"
    execution_error = "execution_error"
    needs_clarification = "needs_clarification"


class AnalystOutcomeStage(str, Enum):
    request = "request"
    authorization = "authorization"
    selection = "selection"
    query = "query"
    execution = "execution"
    result = "result"
    clarification = "clarification"


class AnalystRecoveryAction(BaseModel):
    action: str
    rationale: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class AnalystExecutionOutcome(BaseModel):
    status: AnalystOutcomeStatus
    stage: AnalystOutcomeStage | None = None
    message: str | None = None
    original_error: str | None = None
    recoverable: bool = False
    terminal: bool = True
    retry_attempted: bool = False
    rewrite_attempted: bool = False
    retry_count: int = Field(default=0, ge=0)
    retry_rationale: str | None = None
    selected_tool_name: str | None = None
    selected_asset_id: str | None = None
    selected_asset_name: str | None = None
    selected_asset_type: Literal["dataset", "semantic_model"] | None = None
    attempted_query_scope: SqlQueryScope | None = None
    final_query_scope: SqlQueryScope | None = None
    fallback_from_query_scope: SqlQueryScope | None = None
    fallback_to_query_scope: SqlQueryScope | None = None
    fallback_reason: str | None = None
    selected_semantic_model_id: str | None = None
    selected_dataset_ids: list[str] = Field(default_factory=list)
    recovery_actions: list[AnalystRecoveryAction] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_error(self) -> bool:
        return self.status in {
            AnalystOutcomeStatus.access_denied,
            AnalystOutcomeStatus.invalid_request,
            AnalystOutcomeStatus.query_error,
            AnalystOutcomeStatus.selection_error,
            AnalystOutcomeStatus.execution_error,
            AnalystOutcomeStatus.needs_clarification,
        }


class AnalystQueryRequest(BaseModel):
    """
    Request payload for the SQL analyst tool.
    """

    question: str = Field(..., min_length=1)
    conversation_context: str | None = Field(
        default=None,
        description="Optional conversation history to help interpret follow-up questions.",
    )
    filters: dict[str, Any] | None = None
    limit: int | None = Field(default=1000)
    semantic_search_result_prompts: list[str] | None = Field(
        default=None,
        description="Optional list of formatted semantic search results to include in the prompt.",
    )
    error_retries: int = Field(default=0, ge=0, description="Number of times the agent should retry on error.")
    error_history: list[str] = Field(
        default_factory=list,
        description="Optional list of error messages to include in the prompt.",
    )


class AnalyticalColumn(BaseModel):
    name: str
    data_type: str | None = None
    description: str | None = None


class AnalyticalField(BaseModel):
    name: str
    expression: str | None = None
    synonyms: list[str] = Field(default_factory=list)


class AnalyticalMetric(BaseModel):
    name: str
    expression: str | None = None
    description: str | None = None


class AnalyticalDatasetBinding(BaseModel):
    dataset_id: str
    dataset_name: str
    sql_alias: str
    description: str | None = None
    source_kind: str | None = None
    storage_kind: str | None = None
    columns: list[AnalyticalColumn] = Field(default_factory=list)


class AnalyticalContext(BaseModel):
    query_scope: SqlQueryScope
    asset_type: Literal["dataset", "semantic_model"]
    asset_id: str
    asset_name: str
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    execution_mode: Literal["federated"] = "federated"
    dialect: str = "postgres"
    datasets: list[AnalyticalDatasetBinding] = Field(default_factory=list)
    tables: list[str] = Field(default_factory=list)
    dimensions: list[AnalyticalField] = Field(default_factory=list)
    measures: list[AnalyticalField] = Field(default_factory=list)
    metrics: list[AnalyticalMetric] = Field(default_factory=list)
    relationships: list[str] = Field(default_factory=list)


class AnalystQueryResponse(BaseModel):
    """
    Response payload emitted by the SQL analyst tool.
    """

    analysis_path: Literal["dataset", "semantic_model"]
    query_scope: SqlQueryScope | None = None
    execution_mode: Literal["federated"]
    asset_type: Literal["dataset", "semantic_model"]
    asset_id: str
    asset_name: str
    selected_semantic_model_id: str | None = None
    sql_canonical: str
    sql_executable: str
    dialect: str
    selected_datasets: list[AnalyticalDatasetBinding] = Field(default_factory=list)
    result: QueryResult | None = None
    error: str | None = None
    execution_time_ms: int | None = None
    outcome: AnalystExecutionOutcome | None = None

    @model_validator(mode="after")
    def _ensure_outcome(self) -> "AnalystQueryResponse":
        if self.selected_semantic_model_id is None and self.asset_type == "semantic_model" and self.asset_id:
            self.selected_semantic_model_id = self.asset_id

        inferred = self.outcome or self._infer_outcome()
        if inferred.selected_asset_id is None and self.asset_id:
            inferred.selected_asset_id = self.asset_id
        if inferred.selected_asset_name is None and self.asset_name:
            inferred.selected_asset_name = self.asset_name
        if inferred.selected_asset_type is None and self.asset_type:
            inferred.selected_asset_type = self.asset_type
        if inferred.attempted_query_scope is None and self.query_scope is not None:
            inferred.attempted_query_scope = self.query_scope
        if inferred.final_query_scope is None and self.query_scope is not None:
            inferred.final_query_scope = self.query_scope
        if inferred.selected_semantic_model_id is None and self.selected_semantic_model_id:
            inferred.selected_semantic_model_id = self.selected_semantic_model_id
        if not inferred.selected_dataset_ids and self.selected_datasets:
            inferred.selected_dataset_ids = [dataset.dataset_id for dataset in self.selected_datasets]
        self.outcome = inferred

        if not self.error and inferred.message:
            self.error = inferred.message
        elif self.error and not inferred.message:
            self.outcome = inferred.model_copy(
                update={
                    "message": self.error,
                    "original_error": inferred.original_error or self.error,
                }
            )
        return self

    def _infer_outcome(self) -> AnalystExecutionOutcome:
        if self.error:
            lowered_error = str(self.error).strip().lower()
            if any(
                token in lowered_error
                for token in ("access denied", "forbidden", "unauthor", "blocked by policy")
            ):
                return AnalystExecutionOutcome(
                    status=AnalystOutcomeStatus.access_denied,
                    stage=AnalystOutcomeStage.authorization,
                    message=self.error,
                    original_error=self.error,
                    recoverable=False,
                    terminal=True,
                    attempted_query_scope=self.query_scope,
                    final_query_scope=self.query_scope,
                    selected_semantic_model_id=self.selected_semantic_model_id,
                    selected_dataset_ids=[dataset.dataset_id for dataset in self.selected_datasets],
                )
            return AnalystExecutionOutcome(
                status=AnalystOutcomeStatus.query_error,
                stage=AnalystOutcomeStage.query,
                message=self.error,
                original_error=self.error,
                recoverable=False,
                terminal=True,
                attempted_query_scope=self.query_scope,
                final_query_scope=self.query_scope,
                selected_semantic_model_id=self.selected_semantic_model_id,
                selected_dataset_ids=[dataset.dataset_id for dataset in self.selected_datasets],
            )

        row_count = self.row_count
        if row_count == 0:
            return AnalystExecutionOutcome(
                status=AnalystOutcomeStatus.empty_result,
                stage=AnalystOutcomeStage.result,
                message="No rows matched the query.",
                recoverable=False,
                terminal=True,
                attempted_query_scope=self.query_scope,
                final_query_scope=self.query_scope,
                selected_semantic_model_id=self.selected_semantic_model_id,
                selected_dataset_ids=[dataset.dataset_id for dataset in self.selected_datasets],
            )

        return AnalystExecutionOutcome(
            status=AnalystOutcomeStatus.success,
            stage=AnalystOutcomeStage.result,
            recoverable=False,
            terminal=True,
            attempted_query_scope=self.query_scope,
            final_query_scope=self.query_scope,
            selected_semantic_model_id=self.selected_semantic_model_id,
            selected_dataset_ids=[dataset.dataset_id for dataset in self.selected_datasets],
        )

    @property
    def row_count(self) -> int | None:
        if self.result is None:
            return None
        if self.result.rowcount is not None:
            return self.result.rowcount
        return len(self.result.rows)

    @property
    def has_rows(self) -> bool:
        row_count = self.row_count
        return bool(row_count and row_count > 0)

    @property
    def is_success(self) -> bool:
        return bool(self.outcome and self.outcome.status == AnalystOutcomeStatus.success)

    @property
    def is_empty_result(self) -> bool:
        return bool(self.outcome and self.outcome.status == AnalystOutcomeStatus.empty_result)

    @property
    def has_error(self) -> bool:
        return bool(self.outcome and self.outcome.is_error)


class AnalyticalQueryExecutionResult(BaseModel):
    executable_query: str
    result: QueryResult
    metadata: dict[str, Any] = Field(default_factory=dict)


class AnalyticalQueryExecutionFailure(RuntimeError):
    def __init__(
        self,
        *,
        stage: AnalystOutcomeStage,
        message: str,
        original_error: str | None = None,
        recoverable: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.stage = stage
        self.message = message
        self.original_error = original_error or message
        self.recoverable = recoverable
        self.metadata = dict(metadata or {})


class AnalyticalQueryExecutor(Protocol):
    async def execute_query(
        self,
        *,
        query: str,
        query_dialect: str,
        requested_limit: int | None = None,
    ) -> AnalyticalQueryExecutionResult:
        ...


SemanticModelLike = SemanticModel

