"""LLM-backed SQL analyst tool for Langbridge AI."""
import asyncio
import inspect
import logging
import re
import time
from typing import Any

from langbridge.ai.events import AIEventEmitter, AIEventSource
from langbridge.ai.llm.base import LLMProvider
from langbridge.ai.tools.semantic_search import SemanticSearchTool
from langbridge.ai.tools.sql.interfaces import (
    AnalyticalContext,
    AnalyticalQueryExecutionFailure,
    AnalyticalQueryExecutionResult,
    AnalyticalQueryExecutor,
    AnalystExecutionOutcome,
    AnalystOutcomeStage,
    AnalystOutcomeStatus,
    AnalystQueryRequest,
    AnalystQueryResponse,
    SemanticModelLike,
    SqlQueryScope,
)
from langbridge.ai.tools.sql.prompts import (
    DATASET_SQL_ORCHESTRATION_INSTRUCTION,
    SEMANTIC_SQL_ORCHESTRATION_INSTRUCTION,
    SQL_ORCHESTRATION_INSTRUCTION,
)
from langbridge.ai.tools.sql.renderer import render_analysis_context

SQL_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)


class SqlAnalysisTool(AIEventSource):
    """Generates governed SQL with an LLM and executes it through a runtime executor."""

    def __init__(
        self,
        *,
        llm_provider: LLMProvider,
        context: AnalyticalContext,
        query_executor: AnalyticalQueryExecutor,
        semantic_model: SemanticModelLike | None = None,
        semantic_search_tools: list[SemanticSearchTool] | None = None,
        name: str | None = None,
        description: str | None = None,
        llm_temperature: float = 0.0,
        logger: logging.Logger | None = None,
        event_emitter: AIEventEmitter | None = None,
    ) -> None:
        super().__init__(event_emitter=event_emitter)
        self._llm = llm_provider
        self.context = context
        self.semantic_model = semantic_model
        self._query_executor = query_executor
        self._semantic_search_tools = list(semantic_search_tools or [])
        self._name = str(name or context.asset_name or "sql_analysis").strip()
        self.description = str(description or context.description or "").strip() or None
        self.llm_temperature = float(llm_temperature)
        self.logger = logger or logging.getLogger(__name__)

    @property
    def name(self) -> str:
        return self._name

    @property
    def asset_type(self) -> str:
        return self.context.asset_type

    @property
    def query_scope(self) -> SqlQueryScope:
        return self.context.query_scope

    def describe(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "asset_type": self.context.asset_type,
            "asset_id": self.context.asset_id,
            "asset_name": self.context.asset_name,
            "query_scope": self.context.query_scope.value,
            "datasets": [dataset.model_dump(mode="json") for dataset in self.context.datasets],
            "tables": list(self.context.tables),
            "dimensions": [field.model_dump(mode="json") for field in self.context.dimensions],
            "measures": [field.model_dump(mode="json") for field in self.context.measures],
            "metrics": [metric.model_dump(mode="json") for metric in self.context.metrics],
        }

    def run(self, request: AnalystQueryRequest) -> AnalystQueryResponse:
        try:
            return asyncio.run(self.arun(request))
        except RuntimeError as exc:  # pragma: no cover
            if "asyncio.run() cannot be called from a running event loop" in str(exc):
                raise RuntimeError("Use `await SqlAnalysisTool.arun(...)` inside an event loop.") from exc
            raise

    async def arun(self, request: AnalystQueryRequest) -> AnalystQueryResponse:
        started = time.perf_counter()
        await self._emit_ai_event(
            event_type="SqlAnalysisStarted",
            message=f"Preparing {self.context.query_scope.value} SQL analysis.",
            source=self.name,
            details={
                "tool": self.name,
                "asset_type": self.context.asset_type,
                "asset_name": self.context.asset_name,
                "query_scope": self.context.query_scope.value,
            },
        )
        active_request = await self._augment_request(request)
        await self._emit_ai_event(
            event_type="SqlGenerationStarted",
            message="Generating governed SQL.",
            source=self.name,
            details={"tool": self.name, "query_scope": self.context.query_scope.value},
        )
        canonical_sql = self._extract_sql(await self._generate_sql(active_request))
        await self._emit_ai_event(
            event_type="SqlGenerated",
            message="SQL generated.",
            source=self.name,
            details={"tool": self.name, "query_scope": self.context.query_scope.value},
        )
        execution_result: AnalyticalQueryExecutionResult | None = None
        execution_outcome: AnalystExecutionOutcome | None = None
        executable_sql = ""

        try:
            await self._emit_ai_event(
                event_type="SqlExecutionStarted",
                message="Running query through Langbridge runtime.",
                source=self.name,
                details={"tool": self.name, "query_scope": self.context.query_scope.value},
            )
            execution_result = await self._query_executor.execute_query(
                query=canonical_sql,
                query_dialect=self.context.dialect,
                requested_limit=active_request.limit,
            )
            executable_sql = execution_result.executable_query
            await self._emit_ai_event(
                event_type="SqlExecutionCompleted",
                message=f"Query returned {execution_result.result.rowcount} row(s).",
                source=self.name,
                details={
                    "tool": self.name,
                    "rowcount": execution_result.result.rowcount,
                    "columns": list(execution_result.result.columns),
                },
            )
        except AnalyticalQueryExecutionFailure as exc:
            executable_sql = str(exc.metadata.get("executable_query") or "")
            await self._emit_ai_event(
                event_type="SqlExecutionFailed",
                message=exc.message,
                source=self.name,
                details={
                    "tool": self.name,
                    "stage": exc.stage.value,
                    "recoverable": exc.recoverable,
                },
            )
            execution_outcome = self._outcome(
                status=(
                    AnalystOutcomeStatus.query_error
                    if exc.stage == AnalystOutcomeStage.query
                    else AnalystOutcomeStatus.execution_error
                ),
                stage=exc.stage,
                message=exc.message,
                original_error=exc.original_error,
                recoverable=exc.recoverable,
                terminal=False,
                metadata=exc.metadata,
            )

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        if execution_outcome is None and execution_result is not None:
            row_count = execution_result.result.rowcount
            has_rows = bool(row_count and row_count > 0) or bool(execution_result.result.rows)
            execution_outcome = self._outcome(
                status=AnalystOutcomeStatus.success if has_rows else AnalystOutcomeStatus.empty_result,
                stage=AnalystOutcomeStage.result,
                message=None if has_rows else "No rows matched the query.",
                recoverable=not has_rows,
                terminal=has_rows,
                metadata=execution_result.metadata,
            )

        await self._emit_ai_event(
            event_type="SqlAnalysisCompleted",
            message="SQL analysis complete.",
            source=self.name,
            details={
                "tool": self.name,
                "status": execution_outcome.status.value if execution_outcome else None,
                "elapsed_ms": elapsed_ms,
            },
        )
        return AnalystQueryResponse(
            analysis_path=self.context.asset_type,
            query_scope=self.context.query_scope,
            execution_mode=self.context.execution_mode,
            asset_type=self.context.asset_type,
            asset_id=self.context.asset_id,
            asset_name=self.context.asset_name,
            selected_semantic_model_id=(
                self.context.asset_id if self.context.asset_type == "semantic_model" else None
            ),
            sql_canonical=canonical_sql,
            sql_executable=executable_sql,
            dialect=self.context.dialect,
            selected_datasets=list(self.context.datasets),
            result=execution_result.result if execution_result else None,
            error=execution_outcome.message if execution_outcome else None,
            execution_time_ms=elapsed_ms,
            outcome=execution_outcome,
        )

    async def _augment_request(self, request: AnalystQueryRequest) -> AnalystQueryRequest:
        if not self._semantic_search_tools:
            return request
        prompts = list(request.semantic_search_result_prompts or [])
        for tool in self._semantic_search_tools:
            prompts.extend(await tool.search_prompts(request.question))
        return request.model_copy(update={"semantic_search_result_prompts": prompts})

    async def _generate_sql(self, request: AnalystQueryRequest) -> str:
        prompt = self._build_prompt(request)
        completion = self._llm.acomplete(prompt, temperature=self.llm_temperature, max_tokens=1200)
        if inspect.isawaitable(completion):
            return await completion
        return str(completion)

    def _build_prompt(self, request: AnalystQueryRequest) -> str:
        search_text = ""
        if request.semantic_search_result_prompts:
            search_text = "Search hints:\n" + "\n".join(request.semantic_search_result_prompts) + "\n"
        filters_text = ""
        if request.filters:
            filters_text = "Filters:\n" + "\n".join(
                f"- {key}: {value}" for key, value in request.filters.items()
            ) + "\n"
        conversation_text = f"Conversation:\n{request.conversation_context}\n" if request.conversation_context else ""
        limit_text = f"Requested limit: {request.limit}\n" if request.limit else ""
        orchestration = getattr(self.semantic_model, "orchestration", None)
        orchestration_text = (
            SQL_ORCHESTRATION_INSTRUCTION.format(instruction=orchestration)
            if orchestration
            else ""
        )
        shared_sections = (
            f"{render_analysis_context(self.context, self.semantic_model)}\n"
            f"{orchestration_text}"
            f"{search_text}"
            f"{filters_text}"
            f"{conversation_text}"
            f"{limit_text}"
            f"Question: {request.question}\n"
        )
        if self.query_scope == SqlQueryScope.semantic:
            return SEMANTIC_SQL_ORCHESTRATION_INSTRUCTION.format(
                shared_sections=shared_sections,
                relation_name=self._semantic_relation_name(),
            )
        return DATASET_SQL_ORCHESTRATION_INSTRUCTION.format(shared_sections=shared_sections)

    def _semantic_relation_name(self) -> str:
        relation_name = str(self.context.asset_name or "").strip()
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", relation_name):
            return relation_name
        return f'"{relation_name.replace(chr(34), chr(34) + chr(34))}"'

    def _outcome(
        self,
        *,
        status: AnalystOutcomeStatus,
        stage: AnalystOutcomeStage,
        message: str | None = None,
        original_error: str | None = None,
        recoverable: bool = False,
        terminal: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> AnalystExecutionOutcome:
        return AnalystExecutionOutcome(
            status=status,
            stage=stage,
            message=message,
            original_error=original_error,
            recoverable=recoverable,
            terminal=terminal,
            selected_tool_name=self.name,
            selected_asset_id=self.context.asset_id,
            selected_asset_name=self.context.asset_name,
            selected_asset_type=self.context.asset_type,
            attempted_query_scope=self.context.query_scope,
            final_query_scope=self.context.query_scope,
            selected_semantic_model_id=(
                self.context.asset_id if self.context.asset_type == "semantic_model" else None
            ),
            selected_dataset_ids=[dataset.dataset_id for dataset in self.context.datasets],
            metadata=dict(metadata or {}),
        )

    @staticmethod
    def _extract_sql(raw: str) -> str:
        match = SQL_FENCE_RE.search(raw.strip())
        return (match.group(1) if match else raw).strip()


__all__ = ["SqlAnalysisTool"]
