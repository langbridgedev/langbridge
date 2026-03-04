"""
High-level SQL analyst tool that generates canonical SQL, transpiles it to a target dialect,
and executes the statement through the configured database connector.
"""


import asyncio
import logging
import math
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import sqlglot
from sqlglot import exp

from langbridge.packages.common.langbridge_common.interfaces.agent_events import (
    AgentEventVisibility,
    IAgentEventEmitter,
)
from langbridge.packages.common.langbridge_common.utils.sql import (
    enforce_preview_limit,
)
from langbridge.packages.connectors.langbridge_connectors.api import SqlConnector
from langbridge.packages.orchestrator.langbridge_orchestrator.llm.provider import LLMProvider
from .interfaces import (
    AnalystQueryRequest,
    AnalystQueryResponse,
    FederatedSqlExecutor,
    QueryResult,
    SemanticModel,
)
from langbridge.packages.common.langbridge_common.utils.embedding_provider import EmbeddingProvider

SQL_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)


@dataclass(slots=True)
class ToolTelemetry:
    """Capture SQL artefacts for logging/diagnostics."""

    canonical_sql: str
    transpiled_sql: str


@dataclass(slots=True)
class VectorizedValue:
    value: str
    embedding: List[float]


@dataclass(slots=True)
class VectorizedColumn:
    entity: str
    column: str
    values: List[VectorizedValue]


@dataclass(slots=True)
class VectorMatch:
    entity: str
    column: str
    value: str
    similarity: float
    source_text: str


@dataclass(slots=True)
class SourceAnalysis:
    source_ids: set[str]
    unresolved_tables: list[str]
    ambiguous_tables: list[str]
    has_catalog_qualified_refs: bool = False

    @property
    def is_cross_source(self) -> bool:
        return len(self.source_ids) > 1


VECTOR_SIMILARITY_THRESHOLD = 0.83


SemanticModelLike = SemanticModel


class SqlAnalystTool:
    """
    Generate SQL using an LLM with semantic guidance, transpile it to the target dialect,
    and execute through the provided connector.
    """

    def __init__(
        self,
        *,
        llm: LLMProvider,
        semantic_model: SemanticModelLike,
        connector: SqlConnector | None,
        dialect: str,
        logger: Optional[logging.Logger] = None,
        llm_temperature: float = 0.0,
        priority: int = 0,
        embedder: Optional[EmbeddingProvider] = None,
        event_emitter: Optional[IAgentEventEmitter] = None,
        federated_sql_executor: FederatedSqlExecutor | None = None,
        table_source_map: dict[str, str] | None = None,
        prefer_federated_execution: bool = False,
    ) -> None:
        self.llm = llm
        self.semantic_model = semantic_model
        self.connector = connector
        self.dialect = dialect
        self.logger = logger or logging.getLogger(__name__)
        self.llm_temperature = llm_temperature
        self.priority = priority
        self._model_summary = self._render_semantic_model()
        self.embedder = embedder
        self._vector_columns = self._extract_vector_columns()
        self._event_emitter = event_emitter
        self._federated_sql_executor = federated_sql_executor
        self._prefer_federated_execution = bool(prefer_federated_execution)
        self._table_source_map = {
            str(table_key).strip().lower(): str(source_id).strip()
            for table_key, source_id in (table_source_map or {}).items()
            if str(table_key).strip() and str(source_id).strip()
        }
        (
            self._sources_by_table_key,
            self._sources_by_catalog_schema_table,
            self._sources_by_schema_table,
            self._sources_by_table_name,
        ) = self._build_table_source_indexes()

    @property
    def name(self) -> str:
        name = getattr(self.semantic_model, "name", None)
        return name or "semantic_model"

    def _extract_vector_columns(self) -> List[VectorizedColumn]:
        catalog: List[VectorizedColumn] = []
        for table_key, table in self.semantic_model.tables.items():
            for dimension in table.dimensions or []:
                if not dimension.vectorized:
                    continue
                index_meta = dimension.vector_index or {}
                values_meta = index_meta.get("values") or []
                vector_values: List[VectorizedValue] = []
                for entry in values_meta:
                    value = str((entry or {}).get("value", "")).strip()
                    embedding = (entry or {}).get("embedding")
                    if not value or not isinstance(embedding, list):
                        continue
                    try:
                        vector = [float(component) for component in embedding]
                    except (TypeError, ValueError):
                        continue
                    vector_values.append(VectorizedValue(value=value, embedding=vector))
                if vector_values:
                    catalog.append(
                        VectorizedColumn(
                            entity=table_key,
                            column=dimension.name,
                            values=vector_values,
                        )
                    )
        return catalog

    def _build_table_source_indexes(
        self,
    ) -> tuple[
        dict[str, set[str]],
        dict[tuple[str | None, str | None, str], set[str]],
        dict[tuple[str | None, str], set[str]],
        dict[str, set[str]],
    ]:
        by_table_key: dict[str, set[str]] = defaultdict(set)
        by_catalog_schema_table: dict[tuple[str | None, str | None, str], set[str]] = defaultdict(set)
        by_schema_table: dict[tuple[str | None, str], set[str]] = defaultdict(set)
        by_table_name: dict[str, set[str]] = defaultdict(set)

        for table_key, table in self.semantic_model.tables.items():
            source_id = self._table_source_map.get(str(table_key).strip().lower())
            if not source_id:
                continue
            table_name = str(table.name or "").strip().lower()
            if not table_name:
                continue
            schema_name = str(table.schema or "").strip().lower() or None
            catalog_name = str(table.catalog or "").strip().lower() or None

            by_table_key[str(table_key).strip().lower()].add(source_id)
            by_catalog_schema_table[(catalog_name, schema_name, table_name)].add(source_id)
            by_schema_table[(schema_name, table_name)].add(source_id)
            by_table_name[table_name].add(source_id)

        return by_table_key, by_catalog_schema_table, by_schema_table, by_table_name

    def _analyze_query_sources(
        self,
        *,
        sql: str,
        dialect: str,
    ) -> SourceAnalysis:
        try:
            expression = sqlglot.parse_one(sql, read=dialect)
        except sqlglot.ParseError:
            return SourceAnalysis(
                source_ids=set(),
                unresolved_tables=[],
                ambiguous_tables=[],
                has_catalog_qualified_refs=False,
            )

        resolved_sources: set[str] = set()
        unresolved_tables: list[str] = []
        ambiguous_tables: list[str] = []
        has_catalog_qualified_refs = False
        track_sources = bool(self._table_source_map)

        for table in expression.find_all(exp.Table):
            if table.catalog:
                has_catalog_qualified_refs = True
            if not track_sources:
                continue
            candidate_sources = self._resolve_sources_for_table(table)
            if not candidate_sources:
                unresolved_tables.append(table.sql())
                continue
            if len(candidate_sources) > 1:
                ambiguous_tables.append(table.sql())
                continue
            resolved_sources.update(candidate_sources)

        if not has_catalog_qualified_refs:
            for column in expression.find_all(exp.Column):
                if column.catalog:
                    has_catalog_qualified_refs = True
                    break

        return SourceAnalysis(
            source_ids=resolved_sources,
            unresolved_tables=unresolved_tables,
            ambiguous_tables=ambiguous_tables,
            has_catalog_qualified_refs=has_catalog_qualified_refs,
        )

    def _resolve_sources_for_table(self, table: exp.Table) -> set[str]:
        table_name = str(table.name or "").strip().lower()
        if not table_name:
            return set()

        table_key_sources = self._sources_by_table_key.get(table_name, set())
        if table_key_sources:
            return set(table_key_sources)

        schema_name = str(table.db or "").strip().lower() or None
        catalog_name = str(table.catalog or "").strip().lower() or None

        candidates: set[str] = set()
        if catalog_name is not None:
            candidates.update(
                self._sources_by_catalog_schema_table.get(
                    (catalog_name, schema_name, table_name),
                    set(),
                )
            )
            if candidates:
                return candidates

        if schema_name is not None:
            candidates.update(self._sources_by_schema_table.get((schema_name, table_name), set()))
            if candidates:
                return candidates

        return set(self._sources_by_table_name.get(table_name, set()))

    def _resolve_execution_route(self, source_analysis: SourceAnalysis) -> tuple[bool, str]:
        if self._federated_sql_executor is None:
            return False, "federation_unavailable"

        if self._prefer_federated_execution:
            return True, "prefer_federated_execution"

        if source_analysis.is_cross_source:
            return True, "cross_source_detected"

        if source_analysis.has_catalog_qualified_refs:
            return True, "catalog_qualified_sql_detected"

        return False, "single_source_detected"

    def run(self, query_request: AnalystQueryRequest) -> AnalystQueryResponse:
        """
        Synchronous wrapper around the async execution path.
        """

        try:
            return asyncio.run(self.arun(query_request))
        except RuntimeError as exc:  # pragma: no cover - triggered only inside existing event loop
            if "asyncio.run() cannot be called from a running event loop" in str(exc):
                raise RuntimeError(
                    "SqlAnalystTool.run cannot be invoked inside an active event loop. "
                    "Use `await tool.arun(...)` instead."
                ) from exc
            raise

    async def arun(self, query_request: AnalystQueryRequest) -> AnalystQueryResponse:
        """
        Execute the full NL -> SQL -> execution pipeline asynchronously.
        """

        await self._emit_event(
            event_type="SqlToolStarted",
            message="Analyzing structured data.",
            visibility=AgentEventVisibility.public,
            details={"model": self.name},
        )
        start_ts = time.perf_counter()
        active_request = query_request

        if self.embedder and self._vector_columns:
            try:
                active_request = await self._maybe_augment_request_with_vectors(query_request)
            except Exception as exc:  # pragma: no cover - defensive guard
                self.logger.warning("Vector search failed; continuing without augmentation: %s", exc)
                active_request = query_request

        try:
            canonical_sql = await asyncio.to_thread(self._generate_canonical_sql, active_request)
        except Exception as exc:  # pragma: no cover - defensive: LLM failure surfaces clean error
            self.logger.exception("LLM failed to generate SQL for model %s", self.name)
            await self._emit_event(
                event_type="SqlGenerationFailed",
                message="Failed to generate SQL from your request.",
                visibility=AgentEventVisibility.public,
                details={"model": self.name, "error": str(exc)},
            )
            return AnalystQueryResponse(
                sql_canonical="",
                sql_executable="",
                dialect=self.dialect,
                model_name=self.name,
                error=f"SQL generation failed: {exc}",
            )

        canonical_sql = canonical_sql.strip()
        canonical_sql = self._extract_sql(canonical_sql)
        await self._emit_event(
            event_type="SqlGenerated",
            message="SQL was generated.",
            visibility=AgentEventVisibility.internal,
            details={"model": self.name, "sql_canonical": canonical_sql},
        )
        sql_validation_error: Optional[str] = None
        try:
            sqlglot.parse_one(canonical_sql, read="postgres")
        except sqlglot.ParseError as exc:
            sql_validation_error = f"Canonical SQL failed to parse: {exc}"

        if sql_validation_error:
            elapsed = int((time.perf_counter() - start_ts) * 1000)
            await self._emit_event(
                event_type="SqlValidationFailed",
                message="Generated SQL did not pass validation.",
                visibility=AgentEventVisibility.internal,
                details={
                    "model": self.name,
                    "error": sql_validation_error,
                    "sql_canonical": canonical_sql,
                },
            )
            return AnalystQueryResponse(
                sql_canonical=canonical_sql,
                sql_executable="",
                dialect=self.dialect,
                model_name=self.name,
                error=sql_validation_error,
                execution_time_ms=elapsed,
            )

        source_analysis = self._analyze_query_sources(sql=canonical_sql, dialect="postgres")
        routed_to_federation, route_reason = self._resolve_execution_route(source_analysis)

        self.logger.info(
            "sql_tool_route model=%s routed_to_federation=%s reason=%s source_count=%d",
            self.name,
            routed_to_federation,
            route_reason,
            len(source_analysis.source_ids),
        )
        await self._emit_event(
            event_type="SqlExecutionRouting",
            message="Resolved SQL execution route.",
            visibility=AgentEventVisibility.internal,
            details={
                "model": self.name,
                "routed_to_federation": routed_to_federation,
                "route_reason": route_reason,
                "detected_source_ids": sorted(source_analysis.source_ids),
                "unresolved_tables": source_analysis.unresolved_tables,
                "ambiguous_tables": source_analysis.ambiguous_tables,
            },
        )

        if source_analysis.is_cross_source and self._federated_sql_executor is None:
            elapsed = int((time.perf_counter() - start_ts) * 1000)
            error = (
                "Cross-source query detected for this SQL tool, but federated execution is not configured. "
                "Enable federation for this unified model or query a single source."
            )
            await self._emit_event(
                event_type="SqlExecutionRoutingFailed",
                message="Cross-source SQL requires federated execution.",
                visibility=AgentEventVisibility.public,
                details={
                    "model": self.name,
                    "error": error,
                    "detected_source_ids": sorted(source_analysis.source_ids),
                },
            )
            return AnalystQueryResponse(
                sql_canonical=canonical_sql,
                sql_executable="",
                dialect=self.dialect,
                model_name=self.name,
                error=error,
                execution_time_ms=elapsed,
            )

        execution_dialect = self.dialect
        try:
            if routed_to_federation:
                execution_dialect = "postgres"
                transpiled_sql = canonical_sql
                if active_request.limit:
                    transpiled_sql, _ = enforce_preview_limit(
                        canonical_sql,
                        max_rows=active_request.limit,
                        dialect="postgres",
                    )
                await self._emit_event(
                    event_type="SqlTranspiled",
                    message="SQL prepared for federated execution.",
                    visibility=AgentEventVisibility.internal,
                    details={
                        "model": self.name,
                        "dialect": execution_dialect,
                        "sql_canonical": canonical_sql,
                        "sql_executable": transpiled_sql,
                    },
                )
            else:
                self.logger.debug("Transpiling %s - postgres -> %s", canonical_sql, self.dialect)
                transpiled_sql = sqlglot.transpile(
                    canonical_sql,
                    read="postgres",
                    write=self.dialect,
                )[0]
                self.logger.info("Successful Transpile %s", transpiled_sql)
                await self._emit_event(
                    event_type="SqlTranspiled",
                    message="SQL transpiled for connector dialect.",
                    visibility=AgentEventVisibility.internal,
                    details={
                        "model": self.name,
                        "dialect": execution_dialect,
                        "sql_canonical": canonical_sql,
                        "sql_executable": transpiled_sql,
                    },
                )
        except Exception as exc:  # pragma: no cover - sqlglot error path
            elapsed = int((time.perf_counter() - start_ts) * 1000)
            self.logger.exception("Transpile failed for model %s", self.name)
            await self._emit_event(
                event_type="SqlTranspileFailed",
                message="Failed to prepare SQL for execution.",
                visibility=AgentEventVisibility.public,
                details={"model": self.name, "error": str(exc)},
            )
            return AnalystQueryResponse(
                sql_canonical=canonical_sql,
                sql_executable="",
                dialect=execution_dialect,
                model_name=self.name,
                error=f"Transpile failed: {exc}",
                execution_time_ms=elapsed,
            )

        telemetry = ToolTelemetry(
            canonical_sql=canonical_sql,
            transpiled_sql=transpiled_sql,
        )
        self._log_sql(telemetry)

        result_payload: QueryResult | None = None
        execution_error: Optional[str] = None
        execution_mode = "federated" if routed_to_federation else "single"
        await self._emit_event(
            event_type="SqlExecutionPrepared",
            message="Prepared executable SQL statement.",
            visibility=AgentEventVisibility.internal,
            details={
                "model": self.name,
                "dialect": execution_dialect,
                "execution_mode": execution_mode,
                "sql_executable": transpiled_sql,
                "sql_canonical": canonical_sql,
                "max_rows": active_request.limit,
            },
        )
        await self._emit_event(
            event_type="SqlExecutionStarted",
            message="Running SQL query.",
            visibility=AgentEventVisibility.public,
            details={
                "model": self.name,
                "dialect": execution_dialect,
                "execution_mode": execution_mode,
                "max_rows": active_request.limit,
            },
        )
        try:
            if routed_to_federation:
                if self._federated_sql_executor is None:
                    raise RuntimeError("Federated SQL executor is not configured.")
                result_payload = await self._federated_sql_executor.execute_sql(
                    sql=transpiled_sql,
                    dialect=execution_dialect,
                    max_rows=active_request.limit,
                )
            else:
                if self.connector is None:
                    raise RuntimeError("SQL connector is not configured.")
                connector_result = await self.connector.execute(
                    transpiled_sql,
                    max_rows=active_request.limit,
                )
                result_payload = QueryResult.from_connector(connector_result)

            await self._emit_event(
                event_type="SqlExecutionCompleted",
                message="SQL query completed.",
                visibility=AgentEventVisibility.public,
                details={
                    "model": self.name,
                    "execution_mode": execution_mode,
                    "row_count": result_payload.rowcount,
                    "elapsed_ms": result_payload.elapsed_ms,
                },
            )
            await self._emit_event(
                event_type="SqlExecutionAudit",
                message="Execution completed with SQL audit details.",
                visibility=AgentEventVisibility.internal,
                details={
                    "model": self.name,
                    "dialect": execution_dialect,
                    "execution_mode": execution_mode,
                    "sql_executable": transpiled_sql,
                    "sql_canonical": canonical_sql,
                    "row_count": result_payload.rowcount,
                    "elapsed_ms": result_payload.elapsed_ms,
                    "columns": result_payload.columns,
                },
            )
        except Exception as exc:  # pragma: no cover - depends on runtime executor implementations
            self.logger.exception("Execution failed for model %s", self.name)
            execution_error = f"Execution failed: {exc}"
            await self._emit_event(
                event_type="SqlExecutionFailed",
                message="SQL query failed.",
                visibility=AgentEventVisibility.public,
                details={
                    "model": self.name,
                    "execution_mode": execution_mode,
                    "error": str(exc),
                },
            )
            await self._emit_event(
                event_type="SqlExecutionFailedInternal",
                message="Execution failed with SQL audit details. Exception: %s" % exc,
                visibility=AgentEventVisibility.internal,
                details={
                    "model": self.name,
                    "dialect": execution_dialect,
                    "execution_mode": execution_mode,
                    "sql_executable": transpiled_sql,
                    "sql_canonical": canonical_sql,
                    "error": str(exc),
                },
            )

        elapsed_ms = int((time.perf_counter() - start_ts) * 1000)

        return AnalystQueryResponse(
            sql_canonical=canonical_sql,
            sql_executable=transpiled_sql,
            dialect=execution_dialect,
            model_name=self.name,
            result=result_payload,
            error=execution_error,
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
                source=f"tool:sql:{self.name}",
                details=details,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            self.logger.warning("Failed to emit SQL tool event %s: %s", event_type, exc)

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

        execution_rules = (
            "- This model can span multiple connectors; cross-source joins are supported via federation.\n"
            "- Use table names exactly as listed in the model. If catalog is shown, use catalog.schema.table to disambiguate.\n"
            if self._federated_sql_executor is not None
            else "- This tool executes on a single connector. Do not generate cross-source joins across different connectors.\n"
        )

        return (
            "You are an expert analytics engineer generating SQL.\n"
            f"{self._model_summary}\n"
            "Rules:\n"
            "- Return a single SELECT statement.\n"
            "- The SQL must target PostgreSQL dialect.\n"
            "- Do not include comments, explanations, or additional text.\n"
            "- Use only tables, relationships, measures, dimensions, and metrics defined above.\n"
            f"{execution_rules}"
            """
            - Fully qualify columns as table.column. No SELECT *.
            - Use the physical table names shown in the model; model keys are labels only.
            - Use only relationships defined in the model; INNER JOIN by default.
            - Expand metrics using their expression verbatim.
            - Apply table filters when the request mentions their name or synonyms.
            - Group only by non-aggregated selected dimensions.
            - Prefer a single query; CTEs allowed: base_fact -> joined -> final.
            - Do NOT invent columns/joins. If something is missing, omit it safely.
            - Use PostgreSQL syntax only.
            - Use ANSI-friendly constructs that also parse in PostgreSQL (CAST, COALESCE, CASE, standard aggregates).
            - Prefer EXTRACT(YEAR FROM <date_col>) for year filters/grouping.
            - Prefer CONCAT(EXTRACT(YEAR FROM <date_col>), '-Q', EXTRACT(QUARTER FROM <date_col>)) for quarter labels.
            - Do NOT use SQLite-specific functions (e.g., strftime, julianday).
            - Use semantic search results to resolve ambiguous entity references. Incorporate them as explicit filters if relevant.
            - Use table identifiers as defined in the model if semantic search results provide them.
            """
            f"{limit_hint}"
            f"{filters_text}"
            f"{conversation_text}"
            f"Semantic search results:\n"
            f"{request.semantic_search_result_prompts or 'None'}\n"
            f"Question: {request.question}\n"
            "Return SQL in PostgreSQL dialect only. No comments or explanation."
        )

    def _generate_canonical_sql(self, request: AnalystQueryRequest) -> str:
        prompt = self._build_prompt(request)
        self.logger.info("Invoking LLM for model %s", self.name)
        self.logger.info("Prompt:\n%s", prompt)
        return self.llm.complete(prompt, temperature=self.llm_temperature)

    async def _maybe_augment_request_with_vectors(self, request: AnalystQueryRequest) -> AnalystQueryRequest:
        if not self.embedder or not self._vector_columns:
            return request
        matches = await self._resolve_vector_matches(request.question)
        if not matches:
            return request

        augmented_question = self._augment_question_with_matches(request.question, matches)
        filters: Dict[str, Any] = dict(request.filters or {})
        for match in matches:
            key = f"{match.entity}.{match.column}"
            filters[key] = match.value

        return request.model_copy(
            update={
                "question": augmented_question,
                "filters": filters or request.filters,
            }
        )

    async def _resolve_vector_matches(self, question: str) -> List[VectorMatch]:
        phrases = self._extract_candidate_phrases(question)
        if not phrases or not self.embedder:
            return []

        embeddings = await self.embedder.embed(phrases)
        if not embeddings:
            return []

        phrase_vectors = list(zip(phrases, embeddings))
        matches: List[VectorMatch] = []
        for column in self._vector_columns:
            best_match: Optional[VectorMatch] = None
            for phrase, vector in phrase_vectors:
                for candidate in column.values:
                    similarity = _cosine_similarity(vector, candidate.embedding)
                    if similarity is None:
                        continue
                    if not best_match or similarity > best_match.similarity:
                        best_match = VectorMatch(
                            entity=column.entity,
                            column=column.column,
                            value=candidate.value,
                            similarity=similarity,
                            source_text=phrase,
                        )
            if best_match and best_match.similarity >= VECTOR_SIMILARITY_THRESHOLD:
                matches.append(best_match)
        return sorted(matches, key=lambda match: match.similarity, reverse=True)[:3]

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
            cleaned = re.split(r"[.,;:]", keyword_match, 1)[0]
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
            f"{prefix}\n\nResolved entities from semantic vector search:\n"
            f"{hints}\nApply these as explicit filters in the SQL."
        )

    @staticmethod
    def _table_ref(model: SemanticModel, table_key: str) -> str:
        table = model.tables.get(table_key)
        if table is None:
            return table_key
        if table.catalog and table.schema:
            return f"{table.catalog}.{table.schema}.{table.name}"
        if table.catalog:
            return f"{table.catalog}.{table.name}"
        if table.schema:
            return f"{table.schema}.{table.name}"
        return table.name

    @staticmethod
    def _replace_table_refs(expression: str, table_refs: Dict[str, str]) -> str:
        updated = expression
        for table_key, table_ref in table_refs.items():
            updated = re.sub(rf"\b{re.escape(table_key)}\.", f"{table_ref}.", updated)
        return updated

    @staticmethod
    def _relationship_join_type(value: str | None) -> str:
        if not value:
            return "INNER"
        normalized = value.strip().lower()
        if normalized in {"left", "right", "full", "inner"}:
            return normalized.upper()
        if normalized in {"one_to_many", "many_to_one", "one_to_one"}:
            return "LEFT"
        return "INNER"

    @staticmethod
    def _extract_sql(raw: str) -> str:
        match = SQL_FENCE_RE.search(raw)
        if match:
            return match.group(1).strip()
        return raw.strip()

    def _render_semantic_model(self) -> str:
        return self._render_single_model(self.semantic_model)

    def _render_single_model(self, model: SemanticModel) -> str:
        parts: list[str] = [f"Semantic model: {model.name or 'semantic_model'}"]
        if model.description:
            parts.append(f"Description: {model.description}")

        table_refs = {key: self._table_ref(model, key) for key in model.tables}

        if model.tables:
            parts.append("Tables:")
            for table_key, table in model.tables.items():
                table_ref = table_refs.get(table_key, table.name)
                parts.append(f"  - {table_key} ({table_ref})")
                if table.description:
                    parts.append(f"      description: {table.description}")
                if table.dimensions:
                    parts.append("      dimensions:")
                    for dimension in table.dimensions:
                        label = f"{table_ref}.{dimension.name if dimension.expression is None else dimension.expression} ({dimension.type})"
                        if dimension.primary_key:
                            label = f"{label} [pk]"
                        parts.append(f"        * {label}")
                if table.measures:
                    parts.append("      measures:")
                    for measure in table.measures:
                        label = f"{table_ref}.{measure.name if measure.expression is None else measure.expression} ({measure.type})"
                        if measure.aggregation:
                            label = f"{label} agg={measure.aggregation}"
                        parts.append(f"        * {label}")
                if table.filters:
                    parts.append("      filters:")
                    for filter_name, filter_meta in table.filters.items():
                        parts.append(f"        * {filter_name}: {filter_meta.condition}")

        if model.relationships:
            parts.append("Relationships:")
            for rel in model.relationships:
                left = table_refs.get(rel.from_, rel.from_)
                right = table_refs.get(rel.to, rel.to)
                condition = self._replace_table_refs(rel.join_on, table_refs)
                join_type = self._relationship_join_type(rel.type)
                parts.append(f"  - {join_type} join {left} -> {right} on {condition}")

        if model.metrics:
            parts.append("Metrics:")
            for metric_name, metric in model.metrics.items():
                expression = self._replace_table_refs(metric.expression, table_refs)
                line = f"{metric_name}: {expression}"
                if metric.description:
                    line = f"{line} ({metric.description})"
                parts.append(f"  - {line}")

        if model.tags:
            parts.append(f"Tags: {', '.join(model.tags)}")

        return "\n".join(parts)

    def _log_sql(self, telemetry: ToolTelemetry) -> None:
        self.logger.debug("Canonical SQL [%s]: %s", self.name, telemetry.canonical_sql)
        self.logger.debug("Transpiled SQL [%s -> %s]: %s", self.name, self.dialect, telemetry.transpiled_sql)


__all__ = ["SqlAnalystTool"]


def _cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> Optional[float]:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return None
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for component_a, component_b in zip(vec_a, vec_b):
        dot += component_a * component_b
        norm_a += component_a * component_a
        norm_b += component_b * component_b
    if norm_a == 0 or norm_b == 0:
        return None
    return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))
