"""
Analytical context selection strategy for dataset-first federated analysis.
"""


import json
import re
from dataclasses import dataclass
from typing import Sequence

from langbridge.orchestrator.llm.provider import LLMProvider
from langbridge.orchestrator.tools.sql_analyst.interfaces import AnalystQueryRequest
from langbridge.orchestrator.tools.sql_analyst.tool import SqlAnalystTool

TOKEN_RE = re.compile(r"\b\w+\b")


class ToolSelectionError(RuntimeError):
    """Raised when the agent cannot determine an appropriate analytical context."""


@dataclass(frozen=True)
class ToolCandidate:
    tool: SqlAnalystTool
    score: float
    priority: int
    order: int


class AnalyticalContextSelector:
    """
    Select the analytical context that best matches the question.
    """

    def __init__(
        self,
        llm: LLMProvider,
        tools: Sequence[SqlAnalystTool],
    ) -> None:
        if not tools:
            raise ValueError("AnalyticalContextSelector requires at least one analytical tool.")

        self._llm = llm
        self._tools = list(tools)
        self._tool_descriptions = {
            str(idx): tool.describe_for_selection(tool_id=str(idx))
            for idx, tool in enumerate(self._tools)
        }
        self._tool_by_id = {
            desc["id"]: tool for tool, desc in zip(self._tools, self._tool_descriptions.values())
        }
        self._keywords = {
            tool: tool.selection_keywords()
            for tool in self._tools
        }

    def select(self, request: AnalystQueryRequest) -> SqlAnalystTool:
        if not self._tools:
            raise ToolSelectionError("No analytical tools are available for selection.")
        if len(self._tools) == 1:
            return self._tools[0]

        try:
            llm_choice = self._select_with_llm(request)
            if llm_choice is not None:
                return llm_choice
        except Exception:
            pass

        return self._fallback_select(request)

    def _select_with_llm(self, request: AnalystQueryRequest) -> SqlAnalystTool | None:
        prompt = self._build_llm_prompt(request)
        response_text = self._llm.complete(prompt=prompt, temperature=0.0)

        try:
            data = json.loads(response_text)
        except json.JSONDecodeError:
            return None

        tool_id = str(data.get("tool_id") or data.get("tool") or "").strip()
        if not tool_id:
            return None
        return self._tool_by_id.get(tool_id)

    def _build_llm_prompt(self, request: AnalystQueryRequest) -> str:
        question = request.question.strip()
        filters = request.filters or {}
        tools_block = json.dumps(
            list(self._tool_descriptions.values()),
            indent=2,
            default=str,
        )

        return f"""
You are routing an analytics request to the best analytical context.

Analytical contexts can be:
- Dataset assets queried through federated execution.
- Semantic model assets that govern datasets and still execute through federated execution.

Your job:
1. Read the question and filters.
2. Choose the SINGLE best analytical context/tool.
3. Prefer the context whose datasets, metrics, tables, and tags best match the request.
4. If multiple contexts fit, choose the one with the highest priority.
5. Execution is always federated; do not prefer a context just because it looks closer to direct SQL.

Return STRICT JSON and nothing else:
{{
  "tool_id": "<ID of the chosen tool>",
  "reason": "<very short explanation>"
}}

Question:
{question}

Filters (if any):
{json.dumps(filters, indent=2)}

Available analytical contexts:
{tools_block}
""".strip()

    def _fallback_select(self, request: AnalystQueryRequest) -> SqlAnalystTool:
        tokens = self._tokenize(request.question)
        if request.filters:
            tokens.update(self._tokenize(" ".join(request.filters.keys())))

        candidates: list[ToolCandidate] = []
        for idx, tool in enumerate(self._tools):
            score = self._score(tokens, self._keywords[tool])
            candidates.append(
                ToolCandidate(
                    tool=tool,
                    score=score,
                    priority=getattr(tool, "priority", 0),
                    order=idx,
                )
            )

        best = max(
            candidates,
            key=lambda candidate: (candidate.score, candidate.priority, -candidate.order),
        )
        if best.score == 0:
            return max(candidates, key=lambda candidate: (candidate.priority, -candidate.order)).tool
        return best.tool

    @staticmethod
    def _tokenize(text: str) -> set[str]:
        return {token.lower() for token in TOKEN_RE.findall(text or "")}

    @staticmethod
    def _score(tokens: set[str], keywords: set[str]) -> float:
        if not keywords:
            return 0.0
        matches = tokens.intersection(keywords)
        return float(len(matches)) / float(len(keywords))


__all__ = ["AnalyticalContextSelector", "ToolSelectionError"]
