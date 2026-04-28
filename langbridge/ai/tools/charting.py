"""LLM-backed chart specification tool."""

import json
from typing import Any

from pydantic import BaseModel, Field

from langbridge.ai.events import AIEventEmitter, AIEventSource
from langbridge.ai.llm.base import LLMProvider


class ChartSpec(BaseModel):
    chart_type: str
    title: str
    x: str | None = None
    y: str | list[str] | None = None
    series: str | None = None
    encoding: dict[str, Any] = Field(default_factory=dict)
    rationale: str | None = None


class ChartingTool(AIEventSource):
    """Builds chart specifications through an LLM provider."""

    def __init__(self, *, llm_provider: LLMProvider, event_emitter: AIEventEmitter | None = None) -> None:
        super().__init__(event_emitter=event_emitter)
        self._llm = llm_provider

    async def build_chart(
        self,
        data: dict[str, Any],
        *,
        question: str,
        title: str | None = None,
        user_intent: str | None = None,
    ) -> ChartSpec | None:
        columns = data.get("columns")
        rows = data.get("rows")
        if not isinstance(columns, list) or not isinstance(rows, list) or not columns or not rows:
            return None
        await self._emit_ai_event(
            event_type="ChartingStarted",
            message="Building chart specification.",
            source="charting",
            details={"column_count": len(columns), "row_count": len(rows)},
        )
        prompt = self._build_prompt(
            columns=[str(column) for column in columns],
            rows=rows[:20],
            question=question,
            title=title,
            user_intent=user_intent,
        )
        raw = await self._llm.acomplete(prompt, temperature=0.0, max_tokens=700)
        chart = self._normalize_chart_spec(
            self._parse_json_object(raw),
            columns=[str(column) for column in columns],
            rows=rows[:20],
            question=question,
            title=title,
            user_intent=user_intent,
        )
        await self._emit_ai_event(
            event_type="ChartingCompleted",
            message=f"Chart specification ready: {chart.chart_type}.",
            source="charting",
            details={"chart_type": chart.chart_type},
        )
        return chart

    @staticmethod
    def _build_prompt(
        *,
        columns: list[str],
        rows: list[Any],
        question: str,
        title: str | None,
        user_intent: str | None,
    ) -> str:
        return (
            "Create a chart specification for verified tabular data.\n"
            "Return STRICT JSON only.\n"
            "Schema: {\"chart_type\":\"bar|line|area|scatter|pie|table\","
            "\"title\":\"...\",\"x\":\"column\",\"y\":\"column|[columns]\",\"series\":\"column|null\","
            "\"encoding\":{},\"rationale\":\"...\"}\n"
            "Use only provided column names. Return table if chart is not appropriate.\n"
            "For questions that compare or rank multiple requested metrics, set y to an array of all requested numeric columns.\n"
            "Use grouped bars for categorical comparisons with multiple measures; do not drop a requested measure.\n"
            f"Question: {question}\n"
            f"Title: {title or ''}\n"
            f"User intent: {user_intent or ''}\n"
            f"Columns: {json.dumps(columns)}\n"
            f"Rows sample: {json.dumps(rows, default=str)}\n"
        )

    @classmethod
    def _normalize_chart_spec(
        cls,
        payload: dict[str, Any],
        *,
        columns: list[str],
        rows: list[Any],
        question: str,
        title: str | None,
        user_intent: str | None,
    ) -> ChartSpec:
        chart = ChartSpec.model_validate(payload)
        chart = cls._normalize_chart_columns(
            chart=chart,
            columns=columns,
            rows=rows,
        )
        if not cls._supports_multi_measure(chart.chart_type):
            return chart
        requested = cls._requested_numeric_columns(
            columns=columns,
            rows=rows,
            context_text=" ".join(
                part
                for part in (
                    question,
                    title or "",
                    user_intent or "",
                    chart.title,
                    chart.rationale or "",
                )
                if part
            ),
        )
        if len(requested) <= 1:
            return chart

        current_y = chart.y if isinstance(chart.y, list) else ([chart.y] if chart.y else [])
        if not current_y or any(column in requested for column in current_y):
            chart.y = requested
        return chart

    @classmethod
    def _normalize_chart_columns(
        cls,
        *,
        chart: ChartSpec,
        columns: list[str],
        rows: list[Any],
    ) -> ChartSpec:
        if not columns:
            return chart

        raw_y = chart.y if isinstance(chart.y, list) else ([chart.y] if chart.y else [])
        resolved_y = [
            resolved
            for resolved in (
                cls._resolve_column_reference(
                    selected=selected,
                    columns=columns,
                    rows=rows,
                    require_numeric=True,
                )
                for selected in raw_y
            )
            if resolved
        ]
        if not resolved_y and chart.chart_type != "table":
            fallback_y = cls._fallback_numeric_column(columns=columns, rows=rows)
            if fallback_y:
                resolved_y = [fallback_y]

        resolved_x = cls._resolve_column_reference(
            selected=chart.x,
            columns=columns,
            rows=rows,
            require_numeric=False,
            exclude=set(resolved_y),
        )
        if not resolved_x and chart.chart_type != "table":
            resolved_x = cls._fallback_dimension_column(
                columns=columns,
                rows=rows,
                exclude=set(resolved_y),
            )

        resolved_series = cls._resolve_column_reference(
            selected=chart.series,
            columns=columns,
            rows=rows,
            require_numeric=False,
            exclude={value for value in [resolved_x, *resolved_y] if value},
        )

        chart.x = resolved_x or chart.x
        if resolved_y:
            chart.y = resolved_y if isinstance(chart.y, list) and len(resolved_y) > 1 else resolved_y[0]
        chart.series = resolved_series
        return chart

    @classmethod
    def _resolve_column_reference(
        cls,
        *,
        selected: Any,
        columns: list[str],
        rows: list[Any],
        require_numeric: bool,
        exclude: set[str] | None = None,
    ) -> str | None:
        text = str(selected or "").strip()
        if not text:
            return None
        excluded = exclude or set()
        for column in columns:
            if column == text and column not in excluded and (
                not require_numeric or cls._column_has_numeric_values(rows, columns.index(column), column)
            ):
                return column

        normalized = cls._normalize_phrase(text)
        for index, column in enumerate(columns):
            if column in excluded:
                continue
            if require_numeric and not cls._column_has_numeric_values(rows, index, column):
                continue
            if normalized in cls._column_match_phrases(column):
                return column
        return None

    @classmethod
    def _fallback_numeric_column(
        cls,
        *,
        columns: list[str],
        rows: list[Any],
        exclude: set[str] | None = None,
    ) -> str | None:
        excluded = exclude or set()
        for index, column in enumerate(columns):
            if column not in excluded and cls._column_has_numeric_values(rows, index, column):
                return column
        return None

    @classmethod
    def _fallback_dimension_column(
        cls,
        *,
        columns: list[str],
        rows: list[Any],
        exclude: set[str] | None = None,
    ) -> str | None:
        excluded = exclude or set()
        for index, column in enumerate(columns):
            if column in excluded:
                continue
            if not cls._column_has_numeric_values(rows, index, column):
                return column
        for column in columns:
            if column not in excluded:
                return column
        return None

    @staticmethod
    def _supports_multi_measure(chart_type: str) -> bool:
        normalized = str(chart_type or "").strip().lower().replace("_", "-")
        return normalized in {"bar", "line", "area", "stacked-bar"}

    @classmethod
    def _requested_numeric_columns(
        cls,
        *,
        columns: list[str],
        rows: list[Any],
        context_text: str,
    ) -> list[str]:
        normalized_context = cls._normalize_phrase(context_text)
        if not normalized_context:
            return []
        requested: list[str] = []
        for index, column in enumerate(columns):
            if not cls._column_has_numeric_values(rows, index, column):
                continue
            phrases = cls._column_match_phrases(column)
            if any(phrase and f" {phrase} " in f" {normalized_context} " for phrase in phrases):
                requested.append(column)
        return requested

    @staticmethod
    def _column_has_numeric_values(rows: list[Any], index: int, column: str) -> bool:
        for row in rows:
            value: Any = None
            if isinstance(row, list) and index < len(row):
                value = row[index]
            elif isinstance(row, dict):
                value = row.get(column)
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return True
            if isinstance(value, str):
                try:
                    float(
                        value.replace(",", "")
                        .replace("$", "")
                        .replace("£", "")
                        .replace("€", "")
                        .replace("%", "")
                    )
                    return True
                except ValueError:
                    continue
        return False

    @classmethod
    def _column_match_phrases(cls, column: str) -> set[str]:
        raw = str(column or "")
        normalized = cls._normalize_phrase(raw)
        fragments = {normalized}
        tail = cls._normalize_phrase(raw.split("__")[-1].split(".")[-1])
        if tail:
            fragments.add(tail)
        stripped = tail
        for prefix in ("monthly ", "total ", "sum ", "avg ", "average "):
            if stripped.startswith(prefix):
                stripped = stripped[len(prefix) :]
        if stripped:
            fragments.add(stripped)
        return {fragment for fragment in fragments if fragment}

    @staticmethod
    def _normalize_phrase(value: str) -> str:
        return " ".join(
            str(value or "")
            .lower()
            .replace("__", " ")
            .replace("_", " ")
            .replace("-", " ")
            .replace(".", " ")
            .split()
        )

    @staticmethod
    def _parse_json_object(raw: str) -> dict[str, Any]:
        text = raw.strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end < start:
            raise ValueError("Charting LLM response did not contain a JSON object.")
        return json.loads(text[start : end + 1])


__all__ = ["ChartSpec", "ChartingTool"]
