"""LLM-powered copilot for the semantic query builder experience."""

from __future__ import annotations

import asyncio
import json
import logging
import textwrap
from dataclasses import dataclass
from typing import Dict, Optional, Protocol, Tuple
from uuid import UUID

from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.packages.contracts.semantic import (
    SemanticQueryMetaResponse,
    SemanticQueryRequest,
    SemanticQueryResponse,
)
from langbridge.packages.orchestrator.langbridge_orchestrator.llm.provider import LLMProvider
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery

from .schemas import (
    QueryBuilderCopilotRequest,
    QueryBuilderCopilotResponse,
)


@dataclass(slots=True)
class _CopilotSuggestion:
    """Internal representation of the LLM suggestion."""

    semantic_query: SemanticQuery
    actions: list[str]
    explanation: Optional[str]
    raw_json: str


class SemanticQueryServiceLike(Protocol):
    async def get_meta(
        self,
        *,
        semantic_model_id: UUID,
        organization_id: UUID,
    ) -> SemanticQueryMetaResponse: ...

    async def query_request(
        self,
        request: SemanticQueryRequest,
    ) -> SemanticQueryResponse: ...


class SemanticQueryBuilderCopilotTool:
    """Generate updated semantic queries plus previews for the dashboard builder."""

    DEFAULT_SYSTEM_PROMPT = textwrap.dedent(
        """
        You are an analytics engineer assisting with a semantic query builder.
        Users provide goals like "trend revenue by product" and the current builder state.
        Respond with JSON describing the updated semantic query along with a short
        explanation of the actions you took. Only use members that exist in the
        provided semantic model definition.
        """
    ).strip()

    RESPONSE_INSTRUCTIONS = textwrap.dedent(
        """
        Return STRICT JSON using this schema:
        {
          "explanation": "short summary of what changed",
          "actions": ["<step 1>", "<step 2>", ...],
          "semanticQuery": <SemanticQuery JSON matching the builder_state schema>
        }
        Ensure semanticQuery includes measures, dimensions, timeDimensions,
        filters, segments, order, limit, offset, and timezone keys when present.
        Reuse the existing builder_state values when you do not need to change them.
        Do not include any keys outside of that schema.
        """
    ).strip()

    def __init__(
        self,
        *,
        llm: LLMProvider,
        semantic_query_service: SemanticQueryServiceLike,
        logger: Optional[logging.Logger] = None,
        llm_temperature: float = 0.0,
        max_tokens: Optional[int] = 1200,
        system_prompt: Optional[str] = None,
    ) -> None:
        self.llm = llm
        self.semantic_query_service = semantic_query_service
        self.logger = logger or logging.getLogger(__name__)
        self.llm_temperature = llm_temperature
        self.max_tokens = max_tokens
        self.system_prompt = system_prompt or self.DEFAULT_SYSTEM_PROMPT
        self._meta_cache: Dict[Tuple[UUID, Optional[UUID], UUID], SemanticQueryMetaResponse] = {}

    def run(self, request: QueryBuilderCopilotRequest) -> QueryBuilderCopilotResponse:
        """Blocking helper that wraps the async implementation."""

        try:
            return asyncio.run(self.arun(request))
        except RuntimeError as exc:  # pragma: no cover - defensive guard
            if "asyncio.run() cannot be called" in str(exc):
                raise RuntimeError(
                    "SemanticQueryBuilderCopilotTool.run cannot execute inside an active event loop. "
                    "Use 'await tool.arun(...)' instead."
                ) from exc
            raise

    async def arun(self, request: QueryBuilderCopilotRequest) -> QueryBuilderCopilotResponse:
        """Primary entrypoint for the copilot tool."""

        meta = await self._get_meta(
            organization_id=request.organization_id,
            project_id=request.project_id,
            semantic_model_id=request.semantic_model_id,
        )
        prompt = self._build_prompt(request, meta)
        self.logger.info("Semantic query copilot prompt built for model %s", meta.name)

        raw_response = await self.llm.acomplete(
            prompt,
            temperature=self.llm_temperature,
            max_tokens=self.max_tokens,
        )
        self.logger.debug("Copilot raw response: %s", raw_response)
        suggestion = self._parse_model_response(raw_response)

        actions = list(suggestion.actions)
        explanation = suggestion.explanation
        preview: Optional[SemanticQueryResponse] = None
        if request.generate_preview:
            try:
                preview = await self._execute_preview(request, suggestion.semantic_query)
            except BusinessValidationError as exc:
                self.logger.warning("Semantic preview failed: %s", exc)
                actions.append(f"Preview failed: {exc}")

        return QueryBuilderCopilotResponse(
            updated_query=suggestion.semantic_query,
            actions=actions,
            explanation=explanation,
            preview=preview,
            raw_model_response=suggestion.raw_json,
        )

    async def _get_meta(
        self,
        *,
        organization_id: UUID,
        project_id: Optional[UUID],
        semantic_model_id: UUID,
    ) -> SemanticQueryMetaResponse:
        cache_key = (organization_id, project_id, semantic_model_id)
        cached = self._meta_cache.get(cache_key)
        if cached:
            return cached
        meta = await self.semantic_query_service.get_meta(
            semantic_model_id=semantic_model_id,
            organization_id=organization_id,
        )
        self._meta_cache[cache_key] = meta
        return meta

    def _build_prompt(
        self,
        request: QueryBuilderCopilotRequest,
        meta: SemanticQueryMetaResponse,
    ) -> str:
        semantic_model_json = json.dumps(meta.semantic_model, indent=2, sort_keys=True)
        builder_state_json = json.dumps(
            request.builder_state.model_dump(by_alias=True, exclude_none=True),
            indent=2,
            sort_keys=True,
        )
        context_lines = []
        if request.context and request.context.summary:
            context_lines.append(f"Dashboard summary: {request.context.summary}")
        if request.context and request.context.focus:
            context_lines.append(f"Current focus: {request.context.focus}")
        if request.context and request.context.timezone:
            context_lines.append(f"Preferred timezone: {request.context.timezone}")
        if request.conversation_context:
            context_lines.append(
                f"Conversation context:\n{request.conversation_context.strip()}"
            )
        context_blob = "\n".join(context_lines) or "(no extra context)"

        return (
            f"{self.system_prompt}\n"
            f"Semantic model metadata (JSON):\n{semantic_model_json}\n\n"
            f"Current builder_state JSON (use same shape when responding):\n{builder_state_json}\n\n"
            f"Contextual hints:\n{context_blob}\n\n"
            f"User request: {request.instructions.strip()}\n\n"
            f"{self.RESPONSE_INSTRUCTIONS}"
        )

    def _parse_model_response(self, raw_text: str) -> _CopilotSuggestion:
        blob = self._extract_json_blob(raw_text)
        if not blob:
            raise BusinessValidationError(
                "Copilot LLM response did not include a JSON payload."
            )
        try:
            data = json.loads(blob)
        except json.JSONDecodeError as exc:
            raise BusinessValidationError(
                f"Copilot response was not valid JSON: {exc}"
            ) from exc

        explanation_raw = data.get("explanation")
        explanation = str(explanation_raw).strip() if explanation_raw else None
        actions_raw = data.get("actions") or []
        if isinstance(actions_raw, str):
            actions = [actions_raw.strip()] if actions_raw.strip() else []
        elif isinstance(actions_raw, list):
            actions = [str(item).strip() for item in actions_raw if str(item).strip()]
        else:
            actions = []

        semantic_payload = (
            data.get("semanticQuery")
            or data.get("query")
            or data.get("builderState")
        )
        if not isinstance(semantic_payload, dict):
            raise BusinessValidationError(
                "Copilot response did not include a 'semanticQuery' object."
            )

        semantic_query = SemanticQuery.model_validate(semantic_payload)
        return _CopilotSuggestion(
            semantic_query=semantic_query,
            actions=actions,
            explanation=explanation,
            raw_json=blob,
        )

    async def _execute_preview(
        self,
        request: QueryBuilderCopilotRequest,
        semantic_query: SemanticQuery,
    ) -> SemanticQueryResponse:
        payload = semantic_query.model_dump(by_alias=True, exclude_none=True)
        query_request = SemanticQueryRequest(
            organization_id=request.organization_id,
            project_id=request.project_id,
            semantic_model_id=request.semantic_model_id,
            query=payload,
        )
        return await self.semantic_query_service.query_request(query_request)

    @staticmethod
    def _extract_json_blob(text: str) -> Optional[str]:
        if not text:
            return None
        stripped = text.strip()
        if stripped.startswith("{"):
            return stripped
        start = stripped.find("{")
        if start == -1:
            return None
        depth = 0
        for idx in range(start, len(stripped)):
            char = stripped[idx]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return stripped[start : idx + 1]
        return None

