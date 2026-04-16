"""
Analyst agent that selects an analytical context and executes it through federation.
"""

import asyncio
import inspect
import json
import logging
import re
from datetime import date
from typing import Any, Optional, Sequence

from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.orchestrator.runtime.access_policy import (
    AnalyticalAccessScope,
    build_access_denied_response,
)
from langbridge.orchestrator.tools.semantic_search.interfaces import SemanticSearchResultCollection
from langbridge.orchestrator.tools.semantic_search.tool import SemanticSearchTool
from langbridge.orchestrator.tools.sql_analyst.interfaces import (
    AnalystExecutionOutcome,
    AnalystOutcomeStage,
    AnalystOutcomeStatus,
    AnalystQueryRequest,
    AnalystQueryResponse,
    AnalystRecoveryAction,
)
from langbridge.orchestrator.tools.sql_analyst.tool import SqlAnalystTool
from langbridge.runtime.models import SqlQueryScope

from .selector import (
    AnalyticalBindingSelection,
    AnalyticalContextSelector,
    ToolSelectionError,
)

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
_ENTITY_HINT_RE = re.compile(r"(\"[^\"]+\"|'[^']+'|\b[A-Z][A-Za-z0-9_-]+(?:\s+[A-Z][A-Za-z0-9_-]+)+\b)")


class AnalystAgent:
    """
    Route an analytical question to the most relevant dataset-backed or
    semantic-model-backed federated analytical context.
    """

    DEFAULT_LIMIT = 1000
    MAX_LIMIT = 5000

    def __init__(
        self,
        llm: LLMProvider,
        tools: Sequence[SqlAnalystTool],
        semantic_search_tools: Sequence[SemanticSearchTool] | None = None,
        semantic_search_tools_by_asset: dict[str, Sequence[SemanticSearchTool]] | None = None,
        *,
        access_scope: AnalyticalAccessScope | None = None,
        logger: Optional[logging.Logger] = None,
        max_retries: int = 1,
        semantic_search_top_k: int = 10,
    ) -> None:
        self.llm = llm
        self._tools = list(tools)
        if not self._tools:
            raise ValueError("At least one analytical tool must be provided to AnalystAgent.")
        self._semantic_search_tools = list(semantic_search_tools) if semantic_search_tools else []
        self._semantic_search_tools_by_asset = {
            str(asset_id): list(search_tools)
            for asset_id, search_tools in (semantic_search_tools_by_asset or {}).items()
        }
        self._access_scope = access_scope or AnalyticalAccessScope()
        self.selector = AnalyticalContextSelector(self.llm, self._tools)
        self.logger = logger or logging.getLogger(__name__)
        self.max_retries = max(0, int(max_retries))
        self.semantic_search_top_k = max(1, int(semantic_search_top_k))

    def answer(
        self,
        question: str,
        *,
        conversation_context: Optional[str] = None,
        filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> AnalystQueryResponse:
        request = self._build_request(
            question=question,
            conversation_context=conversation_context,
            filters=filters,
            limit=limit,
            semantic_search_results=[],
        )
        return self.answer_with_request(request)

    def answer_with_request(self, request: AnalystQueryRequest) -> AnalystQueryResponse:
        try:
            return asyncio.run(self.answer_with_request_async(request))
        except RuntimeError as exc:  # pragma: no cover
            if "asyncio.run() cannot be called from a running event loop" in str(exc):
                raise RuntimeError(
                    "AnalystAgent.answer_with_request cannot be invoked inside an active event loop. "
                    "Use `await answer_with_request_async(...)` instead."
                ) from exc
            raise

    async def answer_async(
        self,
        question: str,
        *,
        conversation_context: Optional[str] = None,
        filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> AnalystQueryResponse:
        request = self._build_request(
            question=question,
            conversation_context=conversation_context,
            filters=filters,
            limit=limit,
            semantic_search_results=[],
        )
        return await self.answer_with_request_async(request)

    async def answer_with_request_async(self, request: AnalystQueryRequest) -> AnalystQueryResponse:
        working_request = request.model_copy(deep=True, update={"error_retries": 0})
        prepared_request, recovery_actions, invalid_response = self._prepare_request(working_request)
        if invalid_response is not None:
            return invalid_response
        denied_asset = self._access_scope.match_denied_request(
            question=prepared_request.question,
            filters=prepared_request.filters,
        )
        if denied_asset is not None:
            return build_access_denied_response(
                request=prepared_request,
                access_scope=self._access_scope,
                denied_asset=denied_asset,
                recovery_actions=recovery_actions,
            )

        try:
            binding_selection = self.selector.select_binding(prepared_request)
            tool = self.selector.select_tool(
                prepared_request,
                binding_name=binding_selection.binding_name,
                query_scope=binding_selection.initial_scope,
            )
        except ToolSelectionError as exc:
            return self._build_failure_response(
                request=prepared_request,
                status=AnalystOutcomeStatus.selection_error,
                stage=AnalystOutcomeStage.selection,
                message=str(exc),
                original_error=str(exc),
                recovery_actions=recovery_actions,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            self.logger.exception("Analyst selection failed.")
            return self._build_failure_response(
                request=prepared_request,
                status=AnalystOutcomeStatus.selection_error,
                stage=AnalystOutcomeStage.selection,
                message="Failed to select an analytical context for this request.",
                original_error=str(exc),
                recovery_actions=recovery_actions,
            )

        self.logger.info(
            "AnalystAgent selected binding '%s' with %s scope on asset '%s'",
            binding_selection.binding_name,
            tool.query_scope.value,
            tool.name,
        )

        active_tool = tool
        active_request = await self._augment_request_with_semantic_search(
            request=prepared_request,
            tool=active_tool,
        )
        retry_count = 0
        last_retry_rationale: str | None = None
        fallback_from_scope: SqlQueryScope | None = None
        fallback_to_scope: SqlQueryScope | None = None
        fallback_reason: str | None = None

        while True:
            response = await active_tool.arun(active_request)
            response = self._finalize_response(
                response=response,
                request=active_request,
                tool=active_tool,
                retry_count=retry_count,
                retry_rationale=last_retry_rationale,
                recovery_actions=recovery_actions,
                initial_scope=binding_selection.initial_scope,
                fallback_from_scope=fallback_from_scope,
                fallback_to_scope=fallback_to_scope,
                fallback_reason=fallback_reason,
            )

            if self._should_attempt_semantic_rewrite_before_scope_fallback(
                response=response,
                current_tool=active_tool,
                retry_count=retry_count,
            ):
                retry_request, retry_rationale, retry_actions = await self._prepare_retry_request(
                    request=active_request,
                    response=response,
                    tool=active_tool,
                )
                if retry_request is not None:
                    retry_count += 1
                    last_retry_rationale = retry_rationale
                    recovery_actions = recovery_actions + retry_actions
                    active_request = await self._augment_request_with_semantic_search(
                        request=retry_request,
                        tool=active_tool,
                    )
                    continue

            fallback_tool, updated_request, fallback_action = self._prepare_scope_fallback(
                request=active_request,
                response=response,
                current_tool=active_tool,
                binding_selection=binding_selection,
            )
            if fallback_tool is not None:
                fallback_from_scope = active_tool.query_scope
                fallback_to_scope = fallback_tool.query_scope
                fallback_reason = response.error or fallback_action.rationale or "Scope fallback applied."
                recovery_actions = recovery_actions + [fallback_action]
                active_tool = fallback_tool
                active_request = await self._augment_request_with_semantic_search(
                    request=updated_request or active_request,
                    tool=active_tool,
                )
                continue

            if not self._should_retry(response=response, request=active_request, retry_count=retry_count):
                return response

            retry_request, retry_rationale, retry_actions = await self._prepare_retry_request(
                request=active_request,
                response=response,
                tool=active_tool,
            )
            if retry_request is None:
                return self._mark_terminal(
                    response=response,
                    request=active_request,
                    tool=active_tool,
                    retry_count=retry_count,
                    retry_rationale=last_retry_rationale,
                    recovery_actions=recovery_actions,
                    initial_scope=binding_selection.initial_scope,
                    fallback_from_scope=fallback_from_scope,
                    fallback_to_scope=fallback_to_scope,
                    fallback_reason=fallback_reason,
                )

            retry_count += 1
            last_retry_rationale = retry_rationale
            recovery_actions = recovery_actions + retry_actions
            active_request = await self._augment_request_with_semantic_search(
                request=retry_request,
                tool=active_tool,
            )
            
    async def _run_semantic_search_tool(
        self,
        prompt: str,
        *,
        tools: Sequence[SemanticSearchTool],
    ) -> list[SemanticSearchResultCollection]:
        search_result_collections: list[SemanticSearchResultCollection] = []
        for tool in tools:
            try:
                #TODO: consider running these in parallel if there are multiple semantic search tools and the LLM provider supports it
                search_result_collection: SemanticSearchResultCollection = await tool.search(prompt, self.semantic_search_top_k)
                search_result_collections.append(search_result_collection)
            except Exception as exc:
                self.logger.warning("Semantic search tool '%s' failed: %s", tool.name, exc)
                continue
        return search_result_collections

    def _semantic_search_tools_for_tool(self, tool: SqlAnalystTool) -> list[SemanticSearchTool]:
        asset_tools = self._semantic_search_tools_by_asset.get(str(tool.context.asset_id), [])
        if tool.context.asset_type != "semantic_model":
            return list(self._semantic_search_tools)
        if asset_tools:
            return list(asset_tools)
        return list(self._semantic_search_tools)

    async def _augment_request_with_semantic_search(
        self,
        *,
        request: AnalystQueryRequest,
        tool: SqlAnalystTool,
    ) -> AnalystQueryRequest:
        search_tools = self._semantic_search_tools_for_tool(tool)
        if not search_tools:
            if request.semantic_search_result_prompts is None:
                return request
            return request.model_copy(update={"semantic_search_result_prompts": None})

        semantic_search_results = await self._run_semantic_search_tool(
            request.question,
            tools=search_tools,
        )
        prompts = self._build_semantic_search_prompt_strings(semantic_search_results)
        if not prompts:
            return request.model_copy(update={"semantic_search_result_prompts": None})
        return request.model_copy(update={"semantic_search_result_prompts": prompts})

    @staticmethod
    def _build_semantic_search_prompt_strings(
        semantic_search_results: Sequence[SemanticSearchResultCollection],
    ) -> list[str]:
        prompts: list[str] = []
        seen: set[str] = set()
        for collection in semantic_search_results:
            for prompt in collection.to_prompt_strings():
                normalized = str(prompt or "").strip()
                if not normalized:
                    continue
                if normalized in seen:
                    continue
                seen.add(normalized)
                prompts.append(normalized)
        return prompts

    def _build_request(
        self,
        *,
        question: str,
        conversation_context: Optional[str],
        filters: Optional[dict[str, Any]],
        limit: Optional[int],
        semantic_search_results: list[SemanticSearchResultCollection],
    ) -> AnalystQueryRequest:
        return AnalystQueryRequest(
            question=question,
            conversation_context=conversation_context,
            filters=filters,
            limit=limit if limit is not None else self.DEFAULT_LIMIT,
            error_retries=0,
            semantic_search_result_prompts=self._build_semantic_search_prompt_strings(
                semantic_search_results
            ) or None,
        )

    def _prepare_request(
        self,
        request: AnalystQueryRequest,
    ) -> tuple[AnalystQueryRequest, list[AnalystRecoveryAction], AnalystQueryResponse | None]:
        recovery_actions: list[AnalystRecoveryAction] = []
        normalized_limit, limit_action = self._normalize_limit(request.limit)
        if limit_action is not None:
            recovery_actions.append(limit_action)
            request = request.model_copy(update={"limit": normalized_limit})
        elif request.limit is None:
            request = request.model_copy(update={"limit": self.DEFAULT_LIMIT})

        question = str(request.question or "").strip()
        if not question:
            return request, recovery_actions, self._build_failure_response(
                request=request,
                status=AnalystOutcomeStatus.invalid_request,
                stage=AnalystOutcomeStage.request,
                message="The analytical request is empty. Provide the metric or question you want answered.",
                original_error="empty_question",
                recovery_actions=recovery_actions,
            )

        tokens = _TOKEN_RE.findall(question)
        if not tokens:
            return request, recovery_actions, self._build_failure_response(
                request=request,
                status=AnalystOutcomeStatus.invalid_request,
                stage=AnalystOutcomeStage.request,
                message="The analytical request does not contain enough usable text to run.",
                original_error="invalid_question_shape",
                recovery_actions=recovery_actions,
            )

        return request.model_copy(update={"question": question}), recovery_actions, None

    def _normalize_limit(self, limit: int | None) -> tuple[int, AnalystRecoveryAction | None]:
        if limit is None:
            return self.DEFAULT_LIMIT, None
        try:
            normalized = int(limit)
        except (TypeError, ValueError):
            return self.DEFAULT_LIMIT, AnalystRecoveryAction(
                action="normalize_limit",
                rationale="The requested row limit was invalid, so the default limit was applied.",
                details={"requested": limit, "applied": self.DEFAULT_LIMIT},
            )
        if normalized < 1:
            return self.DEFAULT_LIMIT, AnalystRecoveryAction(
                action="normalize_limit",
                rationale="The requested row limit was below 1, so the default limit was applied.",
                details={"requested": normalized, "applied": self.DEFAULT_LIMIT},
            )
        if normalized > self.MAX_LIMIT:
            return self.MAX_LIMIT, AnalystRecoveryAction(
                action="normalize_limit",
                rationale="The requested row limit was capped to keep the query bounded.",
                details={"requested": normalized, "applied": self.MAX_LIMIT},
            )
        return normalized, None

    def _should_retry(
        self,
        *,
        response: AnalystQueryResponse,
        request: AnalystQueryRequest,
        retry_count: int,
    ) -> bool:
        outcome = response.outcome
        if outcome is None or retry_count >= self.max_retries:
            return False
        if outcome.status in {AnalystOutcomeStatus.query_error, AnalystOutcomeStatus.execution_error}:
            return bool(outcome.recoverable)
        if outcome.status == AnalystOutcomeStatus.empty_result:
            return self._should_retry_empty_result(request)
        return False

    def _should_retry_empty_result(self, request: AnalystQueryRequest) -> bool:
        question = str(request.question or "")
        return bool(request.filters) or bool(_ENTITY_HINT_RE.search(question))

    def _should_attempt_semantic_rewrite_before_scope_fallback(
        self,
        *,
        response: AnalystQueryResponse,
        current_tool: SqlAnalystTool,
        retry_count: int,
    ) -> bool:
        if retry_count >= self.max_retries:
            return False
        outcome = response.outcome
        if outcome is None:
            return False
        if current_tool.query_scope != SqlQueryScope.semantic:
            return False
        if outcome.status not in {
            AnalystOutcomeStatus.query_error,
            AnalystOutcomeStatus.execution_error,
        }:
            return False
        metadata = outcome.metadata if isinstance(outcome.metadata, dict) else {}
        semantic_failure_kind = str(metadata.get("semantic_failure_kind") or "").strip().lower()
        return semantic_failure_kind == "unsupported_semantic_sql_shape"

    def _prepare_scope_fallback(
        self,
        *,
        request: AnalystQueryRequest,
        response: AnalystQueryResponse,
        current_tool: SqlAnalystTool,
        binding_selection: AnalyticalBindingSelection,
    ) -> tuple[SqlAnalystTool | None, AnalystQueryRequest | None, AnalystRecoveryAction | None]:
        outcome = response.outcome
        if outcome is None:
            return None, None, None

        fallback_scope = self.selector.fallback_scope(
            binding_selection,
            current_scope=current_tool.query_scope,
        )
        if fallback_scope is None:
            return None, None, None
        if not self._is_scope_fallback_eligible(response=response, current_tool=current_tool):
            return None, None, None

        try:
            fallback_tool = self.selector.select_tool(
                request,
                binding_name=binding_selection.binding_name,
                query_scope=fallback_scope,
            )
        except ToolSelectionError:
            return None, None, None
        metadata = outcome.metadata if isinstance(outcome.metadata, dict) else {}
        failure_kind = str(metadata.get("semantic_failure_kind") or "").strip().lower()
        fallback_reason = (
            outcome.message
            or response.error
            or f"{current_tool.query_scope.value} scope could not satisfy the request."
        )
        error_history = list(request.error_history or [])
        if fallback_reason:
            error_history.append(fallback_reason)
        updated_request = request.model_copy(
            deep=True,
            update={
                "error_history": error_history,
                "error_retries": 0,
            },
        )
        if failure_kind == "semantic_coverage_gap":
            rationale = (
                f"Falling back from {current_tool.query_scope.value} scope to {fallback_scope.value} "
                "scope because the requested concept is not fully covered by the governed semantic layer."
            )
        elif failure_kind == "unsupported_semantic_sql_shape":
            rationale = (
                f"Falling back from {current_tool.query_scope.value} scope to {fallback_scope.value} "
                "scope because the requested query shape is not supported by governed semantic SQL."
            )
        else:
            rationale = (
                f"Falling back from {current_tool.query_scope.value} scope to "
                f"{fallback_scope.value} scope because the governed scope could not satisfy the request."
            )
        action = AnalystRecoveryAction(
            action="fallback_query_scope",
            rationale=rationale,
            details={
                "from_scope": current_tool.query_scope.value,
                "to_scope": fallback_scope.value,
                "binding_name": binding_selection.binding_name,
                "reason": fallback_reason,
                "semantic_failure_kind": failure_kind or None,
            },
        )
        return fallback_tool, updated_request, action

    def _is_scope_fallback_eligible(
        self,
        *,
        response: AnalystQueryResponse,
        current_tool: SqlAnalystTool,
    ) -> bool:
        outcome = response.outcome
        if outcome is None:
            return False
        if current_tool.query_scope != SqlQueryScope.semantic:
            return False
        if outcome.status not in {
            AnalystOutcomeStatus.query_error,
            AnalystOutcomeStatus.execution_error,
        }:
            return False
        metadata = outcome.metadata if isinstance(outcome.metadata, dict) else {}
        if metadata.get("scope_fallback_eligible") is True:
            return True
        error_text = " ".join(
            part
            for part in (
                outcome.message,
                outcome.original_error,
                response.error,
            )
            if isinstance(part, str) and part.strip()
        ).lower()
        if not error_text:
            return False
        limitation_markers = (
            "semantic sql scope does not support",
            "unknown semantic member",
            "semantic query translation failed",
            "semantic model not found",
            "could not resolve a selected semantic member",
            "semantic sql group by",
            "semantic sql order by",
        )
        return any(marker in error_text for marker in limitation_markers)

    async def _prepare_retry_request(
        self,
        *,
        request: AnalystQueryRequest,
        response: AnalystQueryResponse,
        tool: SqlAnalystTool,
    ) -> tuple[AnalystQueryRequest | None, str | None, list[AnalystRecoveryAction]]:
        outcome = response.outcome
        if outcome is None:
            return None, None, []

        error_text = outcome.message or response.error or outcome.status.value
        error_history = list(request.error_history or [])
        if error_text:
            error_history.append(error_text)

        recovery_actions: list[AnalystRecoveryAction] = []
        retry_rationale: str | None = None
        conversation_context = request.conversation_context
        metadata = outcome.metadata if isinstance(outcome.metadata, dict) else {}
        semantic_failure_kind = str(metadata.get("semantic_failure_kind") or "").strip().lower()
        retry_request = request.model_copy(
            deep=True,
            update={
                "error_history": error_history,
                "error_retries": 0,
            },
        )

        if outcome.status in {AnalystOutcomeStatus.query_error, AnalystOutcomeStatus.execution_error}:
            if (
                tool.query_scope == SqlQueryScope.semantic
                and semantic_failure_kind == "unsupported_semantic_sql_shape"
            ):
                retry_rationale = (
                    "Retrying once by rewriting the request into a semantic-safe governed shape "
                    "before falling back to dataset scope."
                )
            else:
                retry_rationale = "Retrying once with the prior failure captured as analyst guidance."
            recovery_actions.append(
                AnalystRecoveryAction(
                    action="retry_query",
                    rationale=retry_rationale,
                    details={
                        "status": outcome.status.value,
                        "error": error_text,
                        "semantic_failure_kind": semantic_failure_kind or None,
                    },
                )
            )
        elif outcome.status == AnalystOutcomeStatus.empty_result:
            retry_rationale = "Retrying once after normalizing the request for an empty result."
            conversation_context = self._merge_conversation_context(
                conversation_context,
                (
                    "Previous attempt returned no rows. Re-express the request using canonical entity names "
                    "or less restrictive wording when appropriate, but keep the same metric and time frame."
                ),
            )
            retry_request = retry_request.model_copy(update={"conversation_context": conversation_context})
            recovery_actions.append(
                AnalystRecoveryAction(
                    action="retry_empty_result",
                    rationale=retry_rationale,
                    details={"status": outcome.status.value},
                )
            )
        else:
            return None, None, []

        rewritten_question, rewrite_rationale = await self._rewrite_question(
            request=retry_request,
            outcome=outcome,
            tool=tool,
        )
        if rewritten_question and rewritten_question.strip().lower() != retry_request.question.strip().lower():
            retry_request = retry_request.model_copy(update={"question": rewritten_question.strip()})
            recovery_actions.append(
                AnalystRecoveryAction(
                    action="rewrite_question",
                    rationale=rewrite_rationale or "Rewrote the analytical request for a bounded retry.",
                    details={"rewritten_question": rewritten_question.strip()},
                )
            )

        return retry_request, retry_rationale, recovery_actions

    async def _rewrite_question(
        self,
        *,
        request: AnalystQueryRequest,
        outcome: AnalystExecutionOutcome,
        tool: SqlAnalystTool,
    ) -> tuple[str | None, str | None]:
        metadata = outcome.metadata if isinstance(outcome.metadata, dict) else {}
        semantic_failure_kind = str(metadata.get("semantic_failure_kind") or "").strip().lower()
        prompt_sections = [
            "You rewrite analytical questions after a failed or empty execution.",
            "Return ONLY JSON with keys: rewritten_question, rationale.",
            "Do not add new metrics, dimensions, filters, dates, or entities that were not already implied.",
            "Keep the request bounded and preserve the user's analytical intent.",
            f"Selected asset: {tool.name} ({tool.asset_type}, scope={tool.query_scope.value})",
            f"Analytical context: {json.dumps(self._build_tool_context(tool), ensure_ascii=True)}",
            f"Outcome status: {outcome.status.value}",
            f"Outcome detail: {outcome.message or ''}",
            f"Current question: {request.question}",
        ]
        if request.filters:
            prompt_sections.append(
                "Filters: " + json.dumps(request.filters, ensure_ascii=True, sort_keys=True)
            )
        if (
            tool.query_scope == SqlQueryScope.semantic
            and semantic_failure_kind == "unsupported_semantic_sql_shape"
        ):
            prompt_sections.extend(
                [
                    "The previous governed semantic query used an unsupported SQL shape.",
                    "Rewrite the request so it can be answered with named semantic members, supported semantic time buckets, and literal filters only.",
                    (
                        "For relative time windows, convert them into explicit literal dates or explicit years "
                        f"using the current date {date.today().isoformat()} when needed."
                    ),
                    "Do not ask for raw SQL functions, CURRENT_DATE arithmetic, INTERVAL expressions, casts, or free-form SQL predicates.",
                ]
            )
        if request.conversation_context:
            prompt_sections.append(f"Conversation context: {request.conversation_context}")
        if request.error_history:
            prompt_sections.append("Recent errors:\n" + "\n".join(f"- {item}" for item in request.error_history[-3:]))
        prompt = "\n".join(prompt_sections)

        payload = await self._complete_json_prompt(prompt)
        if not payload:
            return None, None

        rewritten_question = payload.get("rewritten_question") or payload.get("question")
        rationale = payload.get("rationale")
        if not isinstance(rewritten_question, str) or not rewritten_question.strip():
            return None, None
        return rewritten_question.strip(), str(rationale).strip() if isinstance(rationale, str) else None

    async def _complete_json_prompt(self, prompt: str) -> dict[str, Any] | None:
        async_completion = getattr(self.llm, "acomplete", None)
        if callable(async_completion):
            result = async_completion(prompt, temperature=0.0, max_tokens=220)
            response_text = await result if inspect.isawaitable(result) else result
        else:
            response_text = await asyncio.to_thread(
                self.llm.complete,
                prompt,
                temperature=0.0,
                max_tokens=220,
            )

        if not isinstance(response_text, str):
            response_text = str(response_text or "")
        return self._parse_json_payload(response_text)

    @staticmethod
    def _parse_json_payload(response_text: str) -> dict[str, Any] | None:
        blob = AnalystAgent._extract_json_blob(response_text)
        if not blob:
            return None
        try:
            payload = json.loads(blob)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _extract_json_blob(text: str) -> str | None:
        if not text:
            return None
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        for index in range(start, len(text)):
            char = text[index]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return text[start : index + 1]
        return None

    @staticmethod
    def _build_tool_context(tool: SqlAnalystTool) -> dict[str, Any]:
        return {
            "binding_name": tool.binding_name,
            "asset_name": tool.context.asset_name,
            "asset_type": tool.context.asset_type,
            "query_scope": tool.query_scope.value,
            "tables": list(tool.context.tables or []),
            "datasets": [dataset.dataset_name for dataset in tool.context.datasets],
            "dimensions": [field.name for field in tool.context.dimensions[:12]],
            "measures": [field.name for field in tool.context.measures[:12]],
            "metrics": [metric.name for metric in tool.context.metrics[:12]],
        }

    def _finalize_response(
        self,
        *,
        response: AnalystQueryResponse,
        request: AnalystQueryRequest,
        tool: SqlAnalystTool,
        retry_count: int,
        retry_rationale: str | None,
        recovery_actions: list[AnalystRecoveryAction],
        initial_scope: SqlQueryScope | None,
        fallback_from_scope: SqlQueryScope | None,
        fallback_to_scope: SqlQueryScope | None,
        fallback_reason: str | None,
    ) -> AnalystQueryResponse:
        outcome = response.outcome or AnalystExecutionOutcome(status=AnalystOutcomeStatus.success)
        status = outcome.status
        terminal = outcome.terminal
        recoverable = outcome.recoverable
        message = outcome.message

        if status == AnalystOutcomeStatus.success:
            terminal = True
            recoverable = False
            message = None
        elif status == AnalystOutcomeStatus.empty_result:
            terminal = retry_count >= self.max_retries or not self._should_retry_empty_result(request)
            recoverable = not terminal
            message = message or "No rows matched the query."
        elif status in {AnalystOutcomeStatus.query_error, AnalystOutcomeStatus.execution_error}:
            terminal = terminal or retry_count >= self.max_retries or not recoverable
            recoverable = recoverable and not terminal

        updated_outcome = outcome.model_copy(
            update={
                "message": message,
                "recoverable": recoverable,
                "terminal": terminal,
                "retry_attempted": retry_count > 0,
                "rewrite_attempted": any(action.action == "rewrite_question" for action in recovery_actions),
                "retry_count": retry_count,
                "retry_rationale": retry_rationale,
                "selected_tool_name": tool.name,
                "selected_asset_id": response.asset_id or tool.context.asset_id,
                "selected_asset_name": response.asset_name or tool.context.asset_name,
                "selected_asset_type": response.asset_type or tool.context.asset_type,
                "attempted_query_scope": initial_scope or tool.query_scope,
                "final_query_scope": tool.query_scope,
                "fallback_from_query_scope": fallback_from_scope,
                "fallback_to_query_scope": fallback_to_scope,
                "fallback_reason": fallback_reason,
                "selected_semantic_model_id": (
                    response.selected_semantic_model_id
                    or (
                        response.asset_id
                        if response.asset_type == "semantic_model" and response.asset_id
                        else None
                    )
                ),
                "selected_dataset_ids": [dataset.dataset_id for dataset in response.selected_datasets],
                "recovery_actions": list(recovery_actions),
            }
        )
        return response.model_copy(update={"outcome": updated_outcome, "error": updated_outcome.message})

    def _mark_terminal(
        self,
        *,
        response: AnalystQueryResponse,
        request: AnalystQueryRequest,
        tool: SqlAnalystTool,
        retry_count: int,
        retry_rationale: str | None,
        recovery_actions: list[AnalystRecoveryAction],
        initial_scope: SqlQueryScope | None,
        fallback_from_scope: SqlQueryScope | None,
        fallback_to_scope: SqlQueryScope | None,
        fallback_reason: str | None,
    ) -> AnalystQueryResponse:
        response = self._finalize_response(
            response=response,
            request=request,
            tool=tool,
            retry_count=retry_count,
            retry_rationale=retry_rationale,
            recovery_actions=recovery_actions,
            initial_scope=initial_scope,
            fallback_from_scope=fallback_from_scope,
            fallback_to_scope=fallback_to_scope,
            fallback_reason=fallback_reason,
        )
        outcome = response.outcome or AnalystExecutionOutcome(status=AnalystOutcomeStatus.query_error)
        updated_outcome = outcome.model_copy(update={"terminal": True, "recoverable": False})
        return response.model_copy(update={"outcome": updated_outcome, "error": updated_outcome.message})

    def _build_failure_response(
        self,
        *,
        request: AnalystQueryRequest,
        status: AnalystOutcomeStatus,
        stage: AnalystOutcomeStage,
        message: str,
        original_error: str,
        recovery_actions: list[AnalystRecoveryAction],
    ) -> AnalystQueryResponse:
        outcome = AnalystExecutionOutcome(
            status=status,
            stage=stage,
            message=message,
            original_error=original_error,
            recoverable=False,
            terminal=True,
            retry_attempted=False,
            rewrite_attempted=False,
            retry_count=0,
            recovery_actions=list(recovery_actions),
            metadata={"question": request.question, "limit": request.limit, "filters": request.filters or {}},
        )
        return AnalystQueryResponse(
            analysis_path="dataset",
            query_scope=None,
            execution_mode="federated",
            asset_type="dataset",
            asset_id="",
            asset_name="",
            sql_canonical="",
            sql_executable="",
            dialect="n/a",
            selected_datasets=[],
            result=None,
            error=message,
            execution_time_ms=None,
            outcome=outcome,
        )

    @staticmethod
    def _merge_conversation_context(base: Optional[str], extra: str) -> str:
        base_text = str(base or "").strip()
        extra_text = str(extra or "").strip()
        if not extra_text:
            return base_text
        if base_text:
            return f"{base_text}\n\n{extra_text}"
        return extra_text


__all__ = ["AnalystAgent"]
