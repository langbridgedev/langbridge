"""
Analyst agent that selects an analytical context and executes it through federation.
"""

import asyncio
import inspect
import json
import logging
import re
from typing import Any, Optional, Sequence

from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.orchestrator.runtime.access_policy import (
    AnalyticalAccessScope,
    build_access_denied_response,
)
from langbridge.orchestrator.tools.sql_analyst.interfaces import (
    AnalystExecutionOutcome,
    AnalystOutcomeStage,
    AnalystOutcomeStatus,
    AnalystQueryRequest,
    AnalystQueryResponse,
    AnalystRecoveryAction,
)
from langbridge.orchestrator.tools.sql_analyst.tool import SqlAnalystTool

from .selector import AnalyticalContextSelector, ToolSelectionError

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
        *,
        access_scope: AnalyticalAccessScope | None = None,
        logger: Optional[logging.Logger] = None,
        max_retries: int = 1,
    ) -> None:
        self.llm = llm
        self._tools = list(tools)
        if not self._tools:
            raise ValueError("At least one analytical tool must be provided to AnalystAgent.")
        self._access_scope = access_scope or AnalyticalAccessScope()
        self.selector = AnalyticalContextSelector(self.llm, self._tools)
        self.logger = logger or logging.getLogger(__name__)
        self.max_retries = max(0, int(max_retries))

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
            tool = self.selector.select(prepared_request)
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
            "AnalystAgent selected %s analytical asset '%s'",
            tool.asset_type,
            tool.name,
        )

        active_request = prepared_request
        retry_count = 0
        last_retry_rationale: str | None = None

        while True:
            response = await tool.arun(active_request)
            response = self._finalize_response(
                response=response,
                request=active_request,
                tool=tool,
                retry_count=retry_count,
                retry_rationale=last_retry_rationale,
                recovery_actions=recovery_actions,
            )

            if not self._should_retry(response=response, request=active_request, retry_count=retry_count):
                return response

            retry_request, retry_rationale, retry_actions = await self._prepare_retry_request(
                request=active_request,
                response=response,
                tool=tool,
            )
            if retry_request is None:
                return self._mark_terminal(
                    response=response,
                    request=active_request,
                    tool=tool,
                    retry_count=retry_count,
                    retry_rationale=last_retry_rationale,
                    recovery_actions=recovery_actions,
                )

            retry_count += 1
            last_retry_rationale = retry_rationale
            recovery_actions = recovery_actions + retry_actions
            active_request = retry_request

    def _build_request(
        self,
        *,
        question: str,
        conversation_context: Optional[str],
        filters: Optional[dict[str, Any]],
        limit: Optional[int],
    ) -> AnalystQueryRequest:
        return AnalystQueryRequest(
            question=question,
            conversation_context=conversation_context,
            filters=filters,
            limit=limit if limit is not None else self.DEFAULT_LIMIT,
            error_retries=0,
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
        retry_request = request.model_copy(
            deep=True,
            update={
                "error_history": error_history,
                "error_retries": 0,
            },
        )

        if outcome.status in {AnalystOutcomeStatus.query_error, AnalystOutcomeStatus.execution_error}:
            retry_rationale = "Retrying once with the prior failure captured as analyst guidance."
            recovery_actions.append(
                AnalystRecoveryAction(
                    action="retry_query",
                    rationale=retry_rationale,
                    details={"status": outcome.status.value, "error": error_text},
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
        prompt_sections = [
            "You rewrite analytical questions after a failed or empty execution.",
            "Return ONLY JSON with keys: rewritten_question, rationale.",
            "Do not add new metrics, dimensions, filters, dates, or entities that were not already implied.",
            "Keep the request bounded and preserve the user's analytical intent.",
            f"Selected asset: {tool.name} ({tool.asset_type})",
            f"Analytical context: {json.dumps(self._build_tool_context(tool), ensure_ascii=True)}",
            f"Outcome status: {outcome.status.value}",
            f"Outcome detail: {outcome.message or ''}",
            f"Current question: {request.question}",
        ]
        if request.filters:
            prompt_sections.append(
                "Filters: " + json.dumps(request.filters, ensure_ascii=True, sort_keys=True)
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
            "asset_name": tool.context.asset_name,
            "asset_type": tool.context.asset_type,
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
    ) -> AnalystQueryResponse:
        response = self._finalize_response(
            response=response,
            request=request,
            tool=tool,
            retry_count=retry_count,
            retry_rationale=retry_rationale,
            recovery_actions=recovery_actions,
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
