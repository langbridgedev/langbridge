"""
Federated analytical tool for dataset-backed and semantic-model-backed SQL generation.
"""


import asyncio
import logging
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import sqlglot
from sqlglot import exp

from langbridge.runtime.embeddings import EmbeddingProvider
from langbridge.runtime.events import (
    AgentEventVisibility,
    AgentEventEmitter,
)
from langbridge.runtime.services.semantic_vector_search_service import (
    SemanticVectorSearchService,
)
from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.runtime.utils.sql import enforce_preview_limit
from .interfaces import (
    AnalyticalContext,
    AnalyticalField,
    AnalyticalMetric,
    AnalystQueryRequest,
    AnalystQueryResponse,
    FederatedSqlExecutor,
    QueryResult,
    SemanticModel,
)

SQL_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)
VECTOR_SIMILARITY_THRESHOLD = 0.83
TEMPORAL_TYPE_NAMES = {"date", "datetime", "time", "timestamp", "timestamptz"}


@dataclass(slots=True)
class ToolTelemetry:
    canonical_sql: str
    executable_sql: str


@dataclass(slots=True)
class VectorMatch:
    entity: str
    column: str
    value: str
    similarity: float
    source_text: str


SemanticModelLike = SemanticModel


class SqlAnalystTool:
    """
    Generate federated SQL for an analytical asset and execute it through the
    dataset federation layer.
    """

    def __init__(
        self,
        *,
        llm: LLMProvider,
        context: AnalyticalContext,
        federated_sql_executor: FederatedSqlExecutor,
        semantic_model: SemanticModelLike,
        logger: logging.Logger,
        llm_temperature: float = 0.0,
        priority: int = 0,
        embedder: Optional[EmbeddingProvider] = None,
        event_emitter: Optional[AgentEventEmitter] = None,
        semantic_vector_search_service: SemanticVectorSearchService | None = None,
        semantic_vector_search_workspace_id: uuid.UUID | None = None,
        semantic_vector_search_model_id: uuid.UUID | None = None,
    ) -> None:
        self.llm = llm
        self.context = context
        self.semantic_model = semantic_model
        self._federated_sql_executor = federated_sql_executor
        self.dialect = str(context.dialect or "postgres").strip().lower() or "postgres"
        self.logger = logger or logging.getLogger(__name__)
        self.llm_temperature = llm_temperature
        self.priority = priority
        self.embedder = embedder
        self._event_emitter = event_emitter
        self._semantic_vector_search_service = semantic_vector_search_service
        self._semantic_vector_search_workspace_id = semantic_vector_search_workspace_id
        self._semantic_vector_search_model_id = semantic_vector_search_model_id

    @property
    def name(self) -> str:
        return self.context.asset_name or "analytical_asset"

    @property
    def asset_type(self) -> str:
        return self.context.asset_type

    def describe_for_selection(self, *, tool_id: str) -> dict[str, Any]:
        return {
            "id": tool_id,
            "priority": self.priority,
            "asset_type": self.context.asset_type,
            "asset_name": self.context.asset_name,
            "description": self.context.description,
            "tags": list(self.context.tags or []),
            "execution_mode": self.context.execution_mode,
            "datasets": [
                {
                    "dataset_id": dataset.dataset_id,
                    "dataset_name": dataset.dataset_name,
                    "sql_alias": dataset.sql_alias,
                    "source_kind": dataset.source_kind,
                    "storage_kind": dataset.storage_kind,
                    "columns": [column.name for column in dataset.columns],
                }
                for dataset in self.context.datasets
            ],
            "tables": list(self.context.tables or []),
            "dimensions": [field.model_dump(mode="json") for field in self.context.dimensions],
            "measures": [field.model_dump(mode="json") for field in self.context.measures],
            "metrics": [metric.model_dump(mode="json") for metric in self.context.metrics],
            "relationships": list(self.context.relationships or []),
            "keywords": sorted(self.selection_keywords()),
        }

    def selection_keywords(self) -> set[str]:
        keywords: set[str] = set()

        def _consume(value: str | None) -> None:
            if value is None:
                return
            normalized = str(value).strip().lower()
            if normalized:
                keywords.add(normalized)

        def _consume_field(field: AnalyticalField) -> None:
            _consume(field.name)
            for synonym in field.synonyms or []:
                _consume(synonym)

        def _consume_metric(metric: AnalyticalMetric) -> None:
            _consume(metric.name)
            _consume(metric.description)

        _consume(self.context.asset_name)
        _consume(self.context.description)
        for tag in self.context.tags or []:
            _consume(tag)
        for dataset in self.context.datasets:
            _consume(dataset.dataset_name)
            _consume(dataset.sql_alias)
            _consume(dataset.description)
            _consume(dataset.source_kind)
            _consume(dataset.storage_kind)
            for column in dataset.columns:
                _consume(column.name)
                _consume(column.description)
                _consume(column.data_type)
        for table in self.context.tables or []:
            _consume(table)
        for relationship in self.context.relationships or []:
            _consume(relationship)
        for field in self.context.dimensions:
            _consume_field(field)
        for field in self.context.measures:
            _consume_field(field)
        for metric in self.context.metrics:
            _consume_metric(metric)
        return keywords

    def run(self, query_request: AnalystQueryRequest) -> AnalystQueryResponse:
        try:
            return asyncio.run(self.arun(query_request))
        except RuntimeError as exc:  # pragma: no cover
            if "asyncio.run() cannot be called from a running event loop" in str(exc):
                raise RuntimeError(
                    "SqlAnalystTool.run cannot be invoked inside an active event loop. "
                    "Use `await tool.arun(...)` instead."
                ) from exc
            raise

    async def arun(self, query_request: AnalystQueryRequest) -> AnalystQueryResponse:
        await self._emit_event(
            event_type="AnalyticalToolStarted",
            message="Analyzing the selected analytical asset.",
            visibility=AgentEventVisibility.public,
            details={
                "asset_name": self.name,
                "asset_type": self.asset_type,
                "execution_mode": self.context.execution_mode,
            },
        )
        start_ts = time.perf_counter()
        active_request = query_request

        if self.embedder and self._semantic_vector_search_service is not None:
            try:
                active_request = await self._maybe_augment_request_with_vectors(query_request)
            except Exception as exc:  # pragma: no cover
                self.logger.warning("Vector search failed; continuing without augmentation: %s", exc)
                active_request = query_request

        try:
            canonical_sql = await asyncio.to_thread(self._generate_canonical_sql, active_request)
        except Exception as exc:  # pragma: no cover
            self.logger.exception("LLM failed to generate SQL for asset %s", self.name)
            await self._emit_event(
                event_type="AnalyticalSqlGenerationFailed",
                message="Failed to generate SQL from your request.",
                visibility=AgentEventVisibility.public,
                details={"asset_name": self.name, "asset_type": self.asset_type, "error": str(exc)},
            )
            return self._build_response(
                sql_canonical="",
                sql_executable="",
                error=f"SQL generation failed: {exc}",
                elapsed_ms=None,
            )

        canonical_sql = self._extract_sql(canonical_sql.strip())
        canonical_sql = self._expand_semantic_measure_references(canonical_sql)
        canonical_sql = self._normalize_temporal_predicates(canonical_sql)
        await self._emit_event(
            event_type="AnalyticalSqlGenerated",
            message="SQL was generated for federated execution.",
            visibility=AgentEventVisibility.internal,
            details={
                "asset_name": self.name,
                "asset_type": self.asset_type,
                "sql_canonical": canonical_sql,
            },
        )

        sql_validation_error: str | None = None
        try:
            sqlglot.parse_one(canonical_sql, read="postgres")
        except sqlglot.ParseError as exc:
            sql_validation_error = f"Canonical SQL failed to parse: {exc}"

        if sql_validation_error:
            elapsed_ms = int((time.perf_counter() - start_ts) * 1000)
            await self._emit_event(
                event_type="AnalyticalSqlValidationFailed",
                message="Generated SQL did not pass validation.",
                visibility=AgentEventVisibility.internal,
                details={
                    "asset_name": self.name,
                    "asset_type": self.asset_type,
                    "error": sql_validation_error,
                    "sql_canonical": canonical_sql,
                },
            )
            return self._build_response(
                sql_canonical=canonical_sql,
                sql_executable="",
                error=sql_validation_error,
                elapsed_ms=elapsed_ms,
            )

        executable_sql = canonical_sql
        if active_request.limit:
            executable_sql, _ = enforce_preview_limit(
                canonical_sql,
                max_rows=active_request.limit,
                dialect="postgres",
            )

        telemetry = ToolTelemetry(
            canonical_sql=canonical_sql,
            executable_sql=executable_sql,
        )
        self._log_sql(telemetry)

        await self._emit_event(
            event_type="AnalyticalSqlExecutionPrepared",
            message="Prepared federated SQL statement.",
            visibility=AgentEventVisibility.internal,
            details={
                "asset_name": self.name,
                "asset_type": self.asset_type,
                "dialect": self.dialect,
                "sql_canonical": canonical_sql,
                "sql_executable": executable_sql,
                "max_rows": active_request.limit,
            },
        )
        await self._emit_event(
            event_type="AnalyticalSqlExecutionStarted",
            message="Running federated analytical query.",
            visibility=AgentEventVisibility.public,
            details={
                "asset_name": self.name,
                "asset_type": self.asset_type,
                "dialect": self.dialect,
                "execution_mode": self.context.execution_mode,
                "max_rows": active_request.limit,
            },
        )

        result_payload: QueryResult | None = None
        execution_error: str | None = None
        try:
            result_payload = await self._federated_sql_executor.execute_sql(
                sql=executable_sql,
                dialect=self.dialect,
                max_rows=active_request.limit,
            )
            await self._emit_event(
                event_type="AnalyticalSqlExecutionCompleted",
                message="Federated analytical query completed.",
                visibility=AgentEventVisibility.public,
                details={
                    "asset_name": self.name,
                    "asset_type": self.asset_type,
                    "row_count": result_payload.rowcount,
                    "elapsed_ms": result_payload.elapsed_ms,
                },
            )
        except Exception as exc:  # pragma: no cover
            self.logger.exception("Federated execution failed for asset %s", self.name)
            execution_error = f"Execution failed: {exc}"
            await self._emit_event(
                event_type="AnalyticalSqlExecutionFailed",
                message="Federated analytical query failed.",
                visibility=AgentEventVisibility.public,
                details={
                    "asset_name": self.name,
                    "asset_type": self.asset_type,
                    "error": str(exc),
                },
            )

        elapsed_ms = int((time.perf_counter() - start_ts) * 1000)
        return self._build_response(
            sql_canonical=canonical_sql,
            sql_executable=executable_sql,
            result=result_payload,
            error=execution_error,
            elapsed_ms=elapsed_ms,
        )

    def _build_response(
        self,
        *,
        sql_canonical: str,
        sql_executable: str,
        result: QueryResult | None = None,
        error: str | None = None,
        elapsed_ms: int | None = None,
    ) -> AnalystQueryResponse:
        return AnalystQueryResponse(
            analysis_path=self.context.asset_type,
            execution_mode=self.context.execution_mode,
            asset_type=self.context.asset_type,
            asset_id=self.context.asset_id,
            asset_name=self.context.asset_name,
            sql_canonical=sql_canonical,
            sql_executable=sql_executable,
            dialect=self.dialect,
            selected_datasets=list(self.context.datasets),
            result=result,
            error=error,
            execution_time_ms=elapsed_ms,
        )

    async def _emit_event(
        self,
        *,
        event_type: str,
        message: str,
        visibility: AgentEventVisibility,
        details: dict[str, Any] | None = None,
    ) -> None:
        if not self._event_emitter:
            return
        try:
            await self._event_emitter.emit(
                event_type=event_type,
                message=message,
                visibility=visibility,
                source=f"tool:analyst:{self.name}",
                details=details,
            )
        except Exception as exc:  # pragma: no cover
            self.logger.warning("Failed to emit analytical tool event %s: %s", event_type, exc)
            
    def _build_sql_orchestration_instructions(self) -> str:
        if self.semantic_model.orchestration is not None:
            return (
                "Orchestration instructions (provide guidance on how to generate SQL for this semantic model, including how to use its semantic definitions and how to join its tables if applicable):\n"
                f"{self.semantic_model.orchestration}\n"
            )
        return ""

    def _build_prompt(self, request: AnalystQueryRequest) -> str:
        conversation_text = ""
        if request.conversation_context:
            conversation_text = f"Conversation context:\n{request.conversation_context}\n"

        filters_text = ""
        if request.filters:
            filters_kv = ", ".join(f"{key} = {value!r}" for key, value in request.filters.items())
            filters_text = f"Filters to apply: {filters_kv}\n"

        limit_hint = ""
        if request.limit:
            limit_hint = f"Prefer applying LIMIT {request.limit} if appropriate.\n"

        search_text = ""
        if request.semantic_search_result_prompts:
            search_text = "Search hints:\n" + "\n".join(request.semantic_search_result_prompts) + "\n"

        return (
            "You are an expert analytics engineer generating SQL for Langbridge.\n"
            "Execution happens through federated dataset query execution by default.\n"
            "Semantic models are an optional governed layer over datasets, not a direct connector target.\n"
            f"{self._render_analysis_context()}\n"
            "Rules:\n"
            "- Return a single SELECT statement.\n"
            "- The SQL must target PostgreSQL dialect.\n"
            "- Do not include comments, explanations, or additional text.\n"
            "- Use only datasets, tables, relationships, dimensions, measures, and metrics defined in the context.\n"
            "- Use dataset SQL aliases exactly as listed in the context.\n"
            "- Fully qualify columns as alias.column. Do not use SELECT *.\n"
            "- Only join tables that are explicitly available in this context.\n"
            "- If the context includes relationships, use only those relationships.\n"
            "- If the context includes metrics, expand them faithfully.\n"
            "- Treat semantic measures as logical aliases and expand them to their configured expressions.\n"
            "- Group only by non-aggregated selected dimensions.\n"
            "- Prefer a single query; CTEs are allowed when they simplify the plan.\n"
            "- Do not invent columns, tables, metrics, or joins.\n"
            "- Use ANSI-friendly PostgreSQL syntax.\n"
            "- Use search hints only as grounding for filters when they are relevant.\n"
            f"{self.__build_sql_orchestration_instructions()}"
            f"{limit_hint}"
            f"{filters_text}"
            f"{conversation_text}"
            f"{search_text}"
            f"Question: {request.question}\n"
            "Return SQL in PostgreSQL dialect only. No comments or explanation."
        )

    def _generate_canonical_sql(self, request: AnalystQueryRequest) -> str:
        prompt = self._build_prompt(request)
        self.logger.info("Invoking LLM for analytical asset %s", self.name)
        self.logger.debug("Prompt for %s:\n%s", self.name, prompt)
        return self.llm.complete(prompt, temperature=self.llm_temperature)

    def _render_analysis_context(self) -> str:
        if self.context.asset_type == "semantic_model":
            return self._render_semantic_model_context()
        return self._render_dataset_context()

    def _render_dataset_context(self) -> str:
        parts: list[str] = [f"Dataset asset: {self.context.asset_name}"]
        if self.context.description:
            parts.append(f"Description: {self.context.description}")
        if self.context.tags:
            parts.append(f"Tags: {', '.join(self.context.tags)}")
        if self.context.datasets:
            parts.append("Datasets:")
            for dataset in self.context.datasets:
                line = f"  - {dataset.sql_alias} ({dataset.dataset_name})"
                descriptor = ", ".join(
                    value
                    for value in (dataset.source_kind, dataset.storage_kind)
                    if value
                )
                if descriptor:
                    line = f"{line} [{descriptor}]"
                parts.append(line)
                if dataset.description:
                    parts.append(f"      description: {dataset.description}")
                if dataset.columns:
                    parts.append("      columns:")
                    for column in dataset.columns:
                        column_line = f"        * {dataset.sql_alias}.{column.name}"
                        if column.data_type:
                            column_line = f"{column_line} ({column.data_type})"
                        parts.append(column_line)
        if self.context.relationships:
            parts.append("Relationships:")
            for relationship in self.context.relationships:
                parts.append(f"  - {relationship}")
        return "\n".join(parts)

    def _render_semantic_model_context(self) -> str:
        parts: list[str] = [f"Semantic model asset: {self.context.asset_name}"]
        if self.context.description:
            parts.append(f"Description: {self.context.description}")
        if self.context.tags:
            parts.append(f"Tags: {', '.join(self.context.tags)}")
        if self.context.datasets:
            parts.append("Backed by datasets:")
            for dataset in self.context.datasets:
                parts.append(f"  - {dataset.sql_alias} ({dataset.dataset_name})")
        if self.context.tables:
            parts.append("Tables:")
            for table in self.context.tables:
                parts.append(f"  - {table}")
        if self.semantic_model is not None:
            parts.extend(self._render_semantic_model_definitions())
        self._append_field_block(parts, "Dimensions", self.context.dimensions)
        self._append_field_block(parts, "Measures", self.context.measures)
        if self.context.metrics:
            parts.append("Metrics:")
            for metric in self.context.metrics:
                line = f"  - {metric.name}"
                if metric.expression:
                    line = f"{line}: {metric.expression}"
                if metric.description:
                    line = f"{line} ({metric.description})"
                parts.append(line)
        if self.context.relationships:
            parts.append("Relationships:")
            for relationship in self.context.relationships:
                parts.append(f"  - {relationship}")
        return "\n".join(parts)

    @staticmethod
    def _append_field_block(parts: list[str], title: str, fields: list[AnalyticalField]) -> None:
        if not fields:
            return
        parts.append(f"{title}:")
        for field in fields:
            line = f"  - {field.name}"
            if field.synonyms:
                line = f"{line} (synonyms: {', '.join(field.synonyms)})"
            parts.append(line)

    async def _maybe_augment_request_with_vectors(self, request: AnalystQueryRequest) -> AnalystQueryRequest:
        if not self.embedder or self._semantic_vector_search_service is None:
            return request
        if not self._semantic_vector_search_workspace_id or not self._semantic_vector_search_model_id:
            return request
        matches = await self._resolve_vector_matches(request.question)
        if not matches:
            return request

        augmented_question = self._augment_question_with_matches(request.question, matches)
        filters: Dict[str, Any] = dict(request.filters or {})
        prompts = list(request.semantic_search_result_prompts or [])
        for match in matches:
            key = f"{match.entity}.{match.column}"
            filters[key] = match.value
            prompts.append(
                f"{match.entity}.{match.column} ~= '{match.value}' "
                f"(matched '{match.source_text}', similarity {match.similarity:.2f})"
            )

        return request.model_copy(
            update={
                "question": augmented_question,
                "filters": filters or request.filters,
                "semantic_search_result_prompts": prompts or request.semantic_search_result_prompts,
            }
        )

    async def _resolve_vector_matches(self, question: str) -> List[VectorMatch]:
        phrases = self._extract_candidate_phrases(question)
        if (
            not phrases
            or not self.embedder
            or self._semantic_vector_search_service is None
            or not self._semantic_vector_search_workspace_id
            or not self._semantic_vector_search_model_id
        ):
            return []

        raw_hits = await self._semantic_vector_search_service.search(
            workspace_id=self._semantic_vector_search_workspace_id,
            semantic_model_id=self._semantic_vector_search_model_id,
            queries=phrases,
            embedding_provider=self.embedder,
            top_k=10,
        )
        if not raw_hits:
            return []

        matches = [
            VectorMatch(
                entity=hit.dataset_key,
                column=hit.dimension_name,
                value=hit.matched_value,
                similarity=hit.score,
                source_text=hit.source_text,
            )
            for hit in raw_hits
            if hit.score >= VECTOR_SIMILARITY_THRESHOLD
        ]
        return matches[:10]

    def _extract_candidate_phrases(self, question: str) -> List[str]:
        base = question.strip()
        candidates: List[str] = []
        seen: set[str] = set()

        def _add(text: str) -> None:
            cleaned = text.strip()
            if not cleaned:
                return
            lowered = cleaned.lower()
            if lowered in seen:
                return
            seen.add(lowered)
            candidates.append(cleaned)

        if base:
            _add(base)

        for quoted in re.findall(r'"([^"]+)"', question):
            _add(quoted)
        for quoted in re.findall(r"'([^']+)'", question):
            _add(quoted)
        for keyword_match in re.findall(
            r"\b(?:in|at|for|from|by|with)\s+([A-Za-z0-9][^,.;:]+)",
            question,
            flags=re.IGNORECASE,
        ):
            cleaned = re.split(r"[.,;:?!]", keyword_match, 1)[0]
            _add(cleaned)
        for capitalized in re.findall(r"\b([A-Z][\w-]*(?:\s+[A-Z][\w-]*)+)\b", question):
            _add(capitalized)

        return candidates[:8]

    @staticmethod
    def _augment_question_with_matches(question: str, matches: List[VectorMatch]) -> str:
        hints = "\n".join(
            f"- Use {match.entity}.{match.column} = '{match.value}' "
            f"(matched phrase '{match.source_text}', similarity {match.similarity:.2f})"
            for match in matches
        )
        prefix = question.strip() or question
        if not hints:
            return prefix
        return (
            f"{prefix}\n\nResolved entities from vector search:\n"
            f"{hints}\nApply these as explicit filters in the SQL."
        )

    @staticmethod
    def _extract_sql(raw: str) -> str:
        match = SQL_FENCE_RE.search(raw)
        if match:
            return match.group(1).strip()
        return raw.strip()

    def _log_sql(self, telemetry: ToolTelemetry) -> None:
        self.logger.debug("Canonical SQL [%s]: %s", self.name, telemetry.canonical_sql)
        self.logger.debug("Executable SQL [%s -> %s]: %s", self.name, self.dialect, telemetry.executable_sql)

    def _expand_semantic_measure_references(self, sql: str) -> str:
        if self.semantic_model is None:
            return sql

        try:
            expression = sqlglot.parse_one(sql, read="postgres")
        except sqlglot.ParseError:
            return sql

        measure_map = {
            (str(table_key).strip().lower(), str(measure.name).strip().lower()): str(
                measure.expression or measure.name
            ).strip()
            for table_key, table in self.semantic_model.tables.items()
            for measure in (table.measures or [])
            if str(measure.expression or measure.name).strip()
        }
        if not measure_map:
            return sql

        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column):
                return node
            table_name = str(node.table or "").strip().lower()
            column_name = str(node.name or "").strip().lower()
            if not table_name or not column_name:
                return node
            expression_sql = measure_map.get((table_name, column_name))
            if not expression_sql or expression_sql.lower() == column_name:
                return node
            replacement = self._build_measure_expression(
                table_name=table_name,
                expression_sql=expression_sql,
            )
            return replacement or node

        return expression.transform(_transform).sql(dialect="postgres")

    def _build_measure_expression(
        self,
        *,
        table_name: str,
        expression_sql: str,
    ) -> exp.Expression | None:
        try:
            expression = sqlglot.parse_one(expression_sql, read="postgres")
        except sqlglot.ParseError:
            return None

        def _qualify(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Column) and not node.table:
                return sqlglot.parse_one(f"{table_name}.{node.name}", read="postgres")
            return node

        return expression.transform(_qualify)

    def _render_semantic_model_definitions(self) -> list[str]:
        if self.semantic_model is None:
            return []

        parts: list[str] = ["Semantic definitions:"]
        for table_key, table in self.semantic_model.tables.items():
            parts.append(f"  - table {table_key}")
            for dimension in table.dimensions or []:
                expression_sql = str(dimension.expression or dimension.name).strip()
                line = f"      dimension {table_key}.{dimension.name}"
                if expression_sql:
                    line = f"{line} => {expression_sql}"
                if dimension.type:
                    line = f"{line} [{dimension.type}]"
                parts.append(line)
            for measure in table.measures or []:
                expression_sql = str(measure.expression or measure.name).strip()
                line = f"      measure {table_key}.{measure.name}"
                aggregation = str(measure.aggregation or "").strip().lower()
                if aggregation:
                    line = f"{line} ({aggregation})"
                if expression_sql:
                    line = f"{line} => {expression_sql}"
                parts.append(line)
        return parts

    def _normalize_temporal_predicates(self, sql: str) -> str:
        temporal_columns = self._temporal_columns_by_name()
        if not temporal_columns:
            return sql

        try:
            expression = sqlglot.parse_one(sql, read="postgres")
        except sqlglot.ParseError:
            return sql

        def _transform(node: exp.Expression) -> exp.Expression:
            if not isinstance(node, exp.Column):
                return node
            cast_target = self._cast_target_for_column(node=node, temporal_columns=temporal_columns)
            if cast_target is None:
                return node
            if self._has_explicit_temporal_cast(node):
                return node
            if not self._is_temporal_predicate_context(node):
                return node
            return exp.Cast(this=node.copy(), to=exp.DataType.build(cast_target))

        return expression.transform(_transform).sql(dialect="postgres")

    def _temporal_columns_by_name(self) -> dict[tuple[str | None, str], str]:
        columns: dict[tuple[str | None, str], str] = {}
        unqualified_targets: dict[str, set[str]] = {}

        for dataset in self.context.datasets:
            alias = str(dataset.sql_alias or "").strip().lower() or None
            for column in dataset.columns:
                cast_target = self._cast_target_for_type(column.data_type)
                if cast_target is None:
                    continue
                column_name = str(column.name or "").strip().lower()
                if not column_name:
                    continue
                columns[(alias, column_name)] = cast_target
                unqualified_targets.setdefault(column_name, set()).add(cast_target)

        if self.semantic_model is not None:
            for table_key, table in self.semantic_model.tables.items():
                alias = str(table_key or "").strip().lower() or None
                for dimension in table.dimensions or []:
                    cast_target = self._cast_target_for_type(getattr(dimension, "type", None))
                    if cast_target is None:
                        continue
                    column_name = str(getattr(dimension, "name", "") or "").strip().lower()
                    if not column_name:
                        continue
                    columns[(alias, column_name)] = cast_target
                    unqualified_targets.setdefault(column_name, set()).add(cast_target)

        for column_name, targets in unqualified_targets.items():
            if len(targets) == 1:
                columns[(None, column_name)] = next(iter(targets))

        return columns

    @staticmethod
    def _cast_target_for_type(data_type: str | None) -> str | None:
        normalized = str(data_type or "").strip().lower()
        if not normalized:
            return None
        if normalized in {"date"}:
            return "DATE"
        if normalized in TEMPORAL_TYPE_NAMES or "timestamp" in normalized:
            return "TIMESTAMP"
        return None

    def _cast_target_for_column(
        self,
        *,
        node: exp.Column,
        temporal_columns: dict[tuple[str | None, str], str],
    ) -> str | None:
        column_name = str(node.name or "").strip().lower()
        table_name = str(node.table or "").strip().lower() or None
        if not column_name:
            return None
        return temporal_columns.get((table_name, column_name)) or temporal_columns.get((None, column_name))

    @staticmethod
    def _has_explicit_temporal_cast(node: exp.Column) -> bool:
        ancestor = node.parent
        while ancestor is not None:
            if isinstance(ancestor, (exp.Cast, exp.TryCast)):
                return True
            if isinstance(ancestor, exp.Anonymous):
                function_name = str(ancestor.name or "").strip().lower()
                if function_name in {"date", "datetime", "timestamp"}:
                    return True
            if isinstance(ancestor, (exp.Where, exp.Having, exp.Join, exp.Select, exp.Subquery)):
                return False
            ancestor = ancestor.parent
        return False

    @staticmethod
    def _is_temporal_predicate_context(node: exp.Column) -> bool:
        comparison_types = (
            exp.EQ,
            exp.NEQ,
            exp.GT,
            exp.GTE,
            exp.LT,
            exp.LTE,
            exp.Between,
            exp.Is,
            exp.In,
            exp.Like,
            exp.ILike,
        )
        boundary_types = (exp.Where, exp.Having, exp.Join, exp.Select, exp.Subquery)

        ancestor = node.parent
        while ancestor is not None:
            if isinstance(ancestor, comparison_types):
                return True
            if isinstance(ancestor, boundary_types):
                return False
            ancestor = ancestor.parent
        return False


__all__ = ["SqlAnalystTool"]
