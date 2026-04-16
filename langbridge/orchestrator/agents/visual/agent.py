"""
Visualization agent that converts tabular data into declarative chart specifications.
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

import pandas as pd

from langbridge.orchestrator.llm.provider import LLMProvider
from .titles import (
    is_placeholder_visualization_title,
    suggest_visualization_title,
)

TabularInput = Union[Dict[str, Any], List[Dict[str, Any]], "pd.DataFrame"]  # type: ignore[name-defined]

_TIME_KEYWORDS = (
    "trend",
    "over time",
    "time series",
    "timeline",
    "daily",
    "weekly",
    "monthly",
    "quarter",
    "year",
    "growth",
    "yoy",
    "mom",
)
_COMPARISON_KEYWORDS = (
    "compare",
    "comparison",
    "versus",
    "vs",
    "difference",
    "change",
    "increase",
    "decrease",
    "higher",
    "lower",
)
_RANKING_KEYWORDS = (
    "top",
    "bottom",
    "rank",
    "ranking",
    "highest",
    "lowest",
    "best",
    "worst",
)
_DISTRIBUTION_KEYWORDS = (
    "distribution",
    "histogram",
    "spread",
    "percentile",
    "variance",
)
_COMPOSITION_KEYWORDS = (
    "share",
    "composition",
    "portion",
    "percentage",
    "percent",
    "breakdown",
    "mix",
)
_CORRELATION_KEYWORDS = (
    "correlation",
    "relationship",
    "scatter",
    "association",
    "impact",
    "driver",
)
_TIME_NAME_HINTS = ("date", "time", "month", "year", "quarter", "qtr", "week", "day")
_ID_LIKE_PATTERN = re.compile(r"(?:^|_)(id|uuid|key|code)$")
_ALLOWED_CHART_TYPES = ("bar", "line", "scatter", "pie", "table")
_CURRENCY_SYMBOL_RE = re.compile(r"[$£€]")
_WHITESPACE_RE = re.compile(r"\s+")


def _to_dataframe(data: TabularInput) -> "pd.DataFrame":  # type: ignore[name-defined]
    if pd is None:  # pragma: no cover - optional dependency
        raise ImportError("pandas is required to convert data into a DataFrame.")

    if isinstance(data, dict):
        if "columns" in data and "rows" in data:
            return pd.DataFrame(data["rows"], columns=data["columns"])
        return pd.DataFrame([data])
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, pd.DataFrame):
        return data.copy()

    raise TypeError("Unsupported tabular input. Provide a DataFrame, list of dicts, or {columns, rows}.")


@dataclass
class VisualizationSpec:
    """
    Declarative visualization specification.
    """

    chart_type: str
    x: Optional[str] = None
    y: Optional[Union[str, Sequence[str]]] = None
    group_by: Optional[str] = None
    title: Optional[str] = None
    options: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "chart_type": self.chart_type,
            "x": self.x,
            "y": self.y,
            "group_by": self.group_by,
            "title": self.title,
        }
        if self.options:
            payload["options"] = self.options
        return {k: v for k, v in payload.items() if v is not None}


@dataclass(frozen=True)
class VisualSignals:
    has_time: bool = False
    has_comparison: bool = False
    has_distribution: bool = False
    has_ranking: bool = False
    has_correlation: bool = False
    has_composition: bool = False


@dataclass(frozen=True)
class DataProfile:
    columns: List[str]
    numeric_cols: List[str]
    categorical_cols: List[str]
    datetime_cols: List[str]
    low_card_numeric_cols: List[str]
    measure_cols: List[str]
    row_count: int
    unique_counts: Dict[str, int]


@dataclass(frozen=True)
class ChartCandidates:
    line_dimension: Optional[str]
    bar_dimension: Optional[str]
    pie_dimension: Optional[str]
    line_measures: List[str]
    bar_measures: List[str]
    pie_measures: List[str]
    scatter_measures: List[str]


class VisualAgent:
    """
    Lightweight agent that infers a visualization configuration for tabular results.
    """

    def __init__(
        self,
        *,
        llm: Optional[LLMProvider] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.llm = llm

    @staticmethod
    def _numeric_columns(df: "pd.DataFrame") -> List[str]:  # type: ignore[name-defined]
        return [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]

    @staticmethod
    def _categorical_columns(df: "pd.DataFrame") -> List[str]:  # type: ignore[name-defined]
        categorical_cols = []
        for col in df.columns:
            series = df[col]
            if (
                pd.api.types.is_object_dtype(series)
                or pd.api.types.is_categorical_dtype(series)
                or pd.api.types.is_bool_dtype(series)
                or pd.api.types.is_string_dtype(series)
            ):
                categorical_cols.append(col)
        return categorical_cols

    @staticmethod
    def _contains_keyword(text: str, keywords: Iterable[str]) -> bool:
        for keyword in keywords:
            if " " in keyword:
                if keyword in text:
                    return True
            else:
                pattern = rf"\b{re.escape(keyword)}\b"
                if re.search(pattern, text):
                    return True
        return False

    @staticmethod
    def _is_time_like_name(column: str) -> bool:
        lowered = column.lower()
        return any(hint in lowered for hint in _TIME_NAME_HINTS)

    @staticmethod
    def _is_identifier_column(column: str) -> bool:
        lowered = column.lower()
        return bool(_ID_LIKE_PATTERN.search(lowered))

    @staticmethod
    def _low_cardinality_threshold(row_count: int) -> int:
        if row_count <= 0:
            return 0
        return min(20, max(3, int(row_count * 0.2)))

    def _datetime_columns(self, df: "pd.DataFrame") -> List[str]:  # type: ignore[name-defined]
        datetime_cols: List[str] = []
        for col in df.columns:
            series = df[col]
            if pd.api.types.is_datetime64_any_dtype(series) or pd.api.types.is_datetime64tz_dtype(series):
                datetime_cols.append(col)
                continue
            if self._is_time_like_name(col):
                if pd.api.types.is_numeric_dtype(series):
                    datetime_cols.append(col)
                    continue
                if self._sample_looks_like_datetime(series):
                    datetime_cols.append(col)
                    continue
            if self._sample_looks_like_datetime(series):
                datetime_cols.append(col)
        return datetime_cols

    @staticmethod
    def _sample_looks_like_datetime(series: "pd.Series") -> bool:  # type: ignore[name-defined]
        if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
            return False
        sample = series.dropna()
        if sample.empty:
            return False
        sample = sample.astype(str).head(12)
        try:
            parsed = pd.to_datetime(sample, errors="coerce")
        except (TypeError, ValueError):
            return False
        return parsed.notna().mean() >= 0.6

    @staticmethod
    def _clean_numeric_text(value: str) -> str:
        cleaned = _WHITESPACE_RE.sub("", str(value).strip())
        cleaned = cleaned.replace(",", "")
        cleaned = _CURRENCY_SYMBOL_RE.sub("", cleaned)
        if cleaned.endswith("%"):
            cleaned = cleaned[:-1]
        return cleaned

    def _normalize_dataframe(self, df: "pd.DataFrame") -> "pd.DataFrame":  # type: ignore[name-defined]
        """
        Normalize string-like columns into numeric values when conversion is reliable.
        This keeps table labels intact while making chartability detection robust.
        """
        normalized = df.copy()
        for col in normalized.columns:
            series = normalized[col]
            if not (
                pd.api.types.is_object_dtype(series)
                or pd.api.types.is_string_dtype(series)
                or pd.api.types.is_categorical_dtype(series)
            ):
                continue

            non_null = series.dropna()
            if non_null.empty:
                continue

            cleaned = non_null.astype(str).map(self._clean_numeric_text)
            converted = pd.to_numeric(cleaned, errors="coerce")
            valid_ratio = float(converted.notna().sum()) / float(len(non_null))
            if valid_ratio >= 0.8:
                full_cleaned = series.astype(str).map(self._clean_numeric_text)
                normalized[col] = pd.to_numeric(full_cleaned, errors="coerce")

        return normalized

    def _profile_dataframe(self, df: "pd.DataFrame") -> DataProfile:  # type: ignore[name-defined]
        columns = list(df.columns)
        row_count = len(df)
        unique_counts: Dict[str, int] = {}
        for col in columns:
            try:
                unique_counts[col] = int(df[col].nunique(dropna=True))
            except Exception:
                unique_counts[col] = 0

        numeric_cols = self._numeric_columns(df)
        datetime_cols = self._datetime_columns(df)
        categorical_cols = [col for col in self._categorical_columns(df) if col not in datetime_cols]

        low_card_threshold = self._low_cardinality_threshold(row_count)
        low_card_numeric_cols = [
            col
            for col in numeric_cols
            if col not in datetime_cols
            and not self._is_identifier_column(col)
            and 1 < unique_counts.get(col, 0) <= low_card_threshold
        ]

        measure_cols = [
            col
            for col in numeric_cols
            if col not in datetime_cols and not self._is_identifier_column(col)
        ]
        if not measure_cols:
            measure_cols = [col for col in numeric_cols if col not in datetime_cols]

        return DataProfile(
            columns=columns,
            numeric_cols=numeric_cols,
            categorical_cols=categorical_cols,
            datetime_cols=datetime_cols,
            low_card_numeric_cols=low_card_numeric_cols,
            measure_cols=measure_cols,
            row_count=row_count,
            unique_counts=unique_counts,
        )

    def _extract_signals(self, question: Optional[str], user_intent: Optional[str]) -> VisualSignals:
        text = (question or "").lower()
        return VisualSignals(
            has_time=bool(user_intent == "time_series_comparison")
            or self._contains_keyword(text, _TIME_KEYWORDS),
            has_comparison=self._contains_keyword(text, _COMPARISON_KEYWORDS),
            has_distribution=self._contains_keyword(text, _DISTRIBUTION_KEYWORDS),
            has_ranking=self._contains_keyword(text, _RANKING_KEYWORDS),
            has_correlation=self._contains_keyword(text, _CORRELATION_KEYWORDS),
            has_composition=self._contains_keyword(text, _COMPOSITION_KEYWORDS),
        )

    @staticmethod
    def _detect_requested_chart_type(
        question: Optional[str],
        user_intent: Optional[str],
    ) -> Optional[str]:
        text = f"{question or ''} {user_intent or ''}".lower()
        if not text.strip():
            return None

        if "pie chart" in text or "donut chart" in text or "doughnut chart" in text:
            return "pie"
        if "pie" in text or "donut" in text or "doughnut" in text:
            return "pie"
        if "bar chart" in text or "bar graph" in text:
            return "bar"
        if "line chart" in text or "line graph" in text:
            return "line"
        if "scatter plot" in text or "scatter chart" in text:
            return "scatter"
        if "table" in text and "chart" not in text:
            return "table"
        return None

    def _select_dimension(
        self,
        profile: DataProfile,
        *,
        prefer_datetime: bool = False,
        max_cardinality: Optional[int] = None,
    ) -> Optional[str]:
        candidates: List[str] = []
        if prefer_datetime:
            candidates.extend(profile.datetime_cols)
        candidates.extend(profile.categorical_cols)
        for col in profile.low_card_numeric_cols:
            if col not in candidates:
                candidates.append(col)
        candidates = [col for col in candidates if profile.unique_counts.get(col, 0) > 1]
        if max_cardinality:
            filtered = [
                col for col in candidates if profile.unique_counts.get(col, 0) <= max_cardinality
            ]
            if filtered:
                candidates = filtered
        if not candidates:
            return None
        candidates.sort(key=lambda col: profile.unique_counts.get(col, 0) or 0)
        return candidates[0]

    def _select_measures(
        self,
        df: "pd.DataFrame",  # type: ignore[name-defined]
        profile: DataProfile,
        *,
        max_measures: int = 2,
        exclude: Optional[Iterable[str]] = None,
    ) -> List[str]:
        excluded = set(exclude or [])
        candidates = [
            col for col in (profile.measure_cols or profile.numeric_cols) if col not in excluded
        ]
        if not candidates:
            return []

        def _score(col: str) -> tuple[int, float, int]:
            series = df[col].dropna()
            variance = float(series.var()) if len(series) > 1 else 0.0
            if variance != variance:
                variance = 0.0
            return (1 if self._is_identifier_column(col) else 0, -variance, -len(series))

        candidates.sort(key=_score)
        return candidates[:max_measures]

    def _select_group_by(self, profile: DataProfile, *, exclude: Iterable[str]) -> Optional[str]:
        excluded = set(exclude)
        for col in profile.categorical_cols:
            if col not in excluded:
                return col
        return None

    def _build_candidates(
        self,
        df: "pd.DataFrame",  # type: ignore[name-defined]
        profile: DataProfile,
    ) -> ChartCandidates:
        line_dimension = self._select_dimension(profile, prefer_datetime=True, max_cardinality=60)
        bar_dimension = self._select_dimension(profile, prefer_datetime=False, max_cardinality=60)
        pie_dimension = self._select_dimension(profile, prefer_datetime=False, max_cardinality=8)

        line_measures = self._select_measures(
            df,
            profile,
            max_measures=2,
            exclude=[line_dimension] if line_dimension else None,
        )
        bar_measures = self._select_measures(
            df,
            profile,
            max_measures=2,
            exclude=[bar_dimension] if bar_dimension else None,
        )
        pie_measures = self._select_measures(
            df,
            profile,
            max_measures=1,
            exclude=[pie_dimension] if pie_dimension else None,
        )
        scatter_measures = self._select_measures(df, profile, max_measures=2)

        return ChartCandidates(
            line_dimension=line_dimension,
            bar_dimension=bar_dimension,
            pie_dimension=pie_dimension,
            line_measures=line_measures,
            bar_measures=bar_measures,
            pie_measures=pie_measures,
            scatter_measures=scatter_measures,
        )

    def _build_spec_for_chart(
        self,
        chart_type: str,
        profile: DataProfile,
        candidates: ChartCandidates,
    ) -> VisualizationSpec:
        x = y = group_by = None
        resolved_chart_type = chart_type
        if chart_type == "line" and candidates.line_dimension and candidates.line_measures:
            x = candidates.line_dimension
            y = (
                candidates.line_measures
                if len(candidates.line_measures) > 1
                else candidates.line_measures[0]
            )
            group_by = self._select_group_by(profile, exclude=[x, *candidates.line_measures])
        elif chart_type == "bar" and candidates.bar_dimension and candidates.bar_measures:
            x = candidates.bar_dimension
            y = (
                candidates.bar_measures
                if len(candidates.bar_measures) > 1
                else candidates.bar_measures[0]
            )
            group_by = self._select_group_by(profile, exclude=[x, *candidates.bar_measures])
        elif chart_type == "scatter" and len(candidates.scatter_measures) >= 2:
            x, y = candidates.scatter_measures[:2]
            group_by = self._select_group_by(profile, exclude=candidates.scatter_measures[:2])
        elif chart_type == "pie" and candidates.pie_dimension and candidates.pie_measures:
            x = candidates.pie_dimension
            y = candidates.pie_measures[0]
        else:
            resolved_chart_type = "table"

        return VisualizationSpec(
            chart_type=resolved_chart_type,
            x=x,
            y=y,
            group_by=group_by,
            title=None,
            options={"row_count": profile.row_count},
        )

    @staticmethod
    def _normalize_chart_type(value: Any) -> Optional[str]:
        if value is None:
            return None
        lowered = str(value).strip().lower()
        if not lowered:
            return None
        if "bar" in lowered:
            return "bar"
        if "line" in lowered:
            return "line"
        if "scatter" in lowered:
            return "scatter"
        if "pie" in lowered or "donut" in lowered:
            return "pie"
        if "table" in lowered:
            return "table"
        return None

    @staticmethod
    def _resolve_column_reference(value: Any, columns: Sequence[str]) -> Optional[str]:
        if not value or not isinstance(value, str):
            return None
        cleaned = value.strip()
        if not cleaned:
            return None
        if cleaned in columns:
            return cleaned
        lowered = cleaned.lower()
        for column in columns:
            if column.lower() == lowered:
                return column
        return None

    def _coerce_y_value(
        self,
        value: Any,
        columns: Sequence[str],
    ) -> Optional[Union[str, List[str]]]:
        if isinstance(value, (list, tuple)):
            resolved: List[str] = []
            for item in value:
                resolved_name = self._resolve_column_reference(item, columns)
                if resolved_name:
                    resolved.append(resolved_name)
            if not resolved:
                return None
            if len(resolved) == 1:
                return resolved[0]
            return resolved
        if isinstance(value, str):
            return self._resolve_column_reference(value, columns)
        return None

    @staticmethod
    def _extract_json_blob(text: str) -> Optional[str]:
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

    def _parse_llm_payload(self, response: str) -> Optional[Dict[str, Any]]:
        blob = self._extract_json_blob(response)
        if not blob:
            return None
        try:
            parsed = json.loads(blob)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        return parsed

    @staticmethod
    def _sample_rows(df: "pd.DataFrame", max_rows: int = 5) -> List[Dict[str, Any]]:  # type: ignore[name-defined]
        if df.empty:
            return []
        sample = df.head(max_rows)
        try:
            return sample.to_dict(orient="records")
        except Exception:
            return []

    def _build_llm_prompt(
        self,
        *,
        question: Optional[str],
        user_intent: Optional[str],
        requested_chart_type: Optional[str],
        profile: DataProfile,
        sample_rows: List[Dict[str, Any]],
    ) -> str:
        sample_json = json.dumps(sample_rows, default=str, ensure_ascii=True)
        prompt_sections = [
            "You are a data visualization assistant. Choose the best single chart specification.",
            "Return ONLY a JSON object with keys: chart_type, x, y, group_by, title, options.",
            f"chart_type must be one of: {', '.join(_ALLOWED_CHART_TYPES)}.",
            "Use column names exactly as provided. y can be a string or list of strings.",
            "Prefer line for time series, bar for comparisons or rankings, scatter for correlation,",
            "pie for composition with <= 6 categories. Use table if no chart fits.",
            (
                f"Requested chart type: {requested_chart_type}."
                if requested_chart_type
                else "Requested chart type: none."
            ),
            (
                "If the requested chart type can be supported by the provided columns, you MUST use it."
                if requested_chart_type
                else "Use the best chart for the question."
            ),
            (
                "If requested chart type cannot be supported, set chart_type='table' and include "
                "options.visualization_warning with a concise reason."
            ),
            f"Question: {question or 'n/a'}",
            f"User intent: {user_intent or 'n/a'}",
            f"Columns: {profile.columns}",
            f"Numeric columns: {profile.numeric_cols}",
            f"Categorical columns: {profile.categorical_cols}",
            f"Datetime columns: {profile.datetime_cols}",
            f"Row count: {profile.row_count}",
            f"Sample rows (JSON): {sample_json}",
        ]
        return "\n".join(prompt_sections)

    def _coerce_llm_payload(
        self,
        payload: Dict[str, Any],
        profile: DataProfile,
        candidates: ChartCandidates,
    ) -> Optional[VisualizationSpec]:
        chart_type = self._normalize_chart_type(
            payload.get("chart_type") or payload.get("chartType") or payload.get("type")
        )
        if not chart_type or chart_type not in _ALLOWED_CHART_TYPES:
            return None

        base_spec = self._build_spec_for_chart(chart_type, profile, candidates)
        if base_spec.chart_type == "table" and chart_type != "table":
            return None

        x_value = payload.get("x") or payload.get("x_axis")
        y_value = payload.get("y") or payload.get("y_axis")
        group_value = payload.get("group_by") or payload.get("groupBy") or payload.get("group")

        x = self._resolve_column_reference(x_value, profile.columns) or base_spec.x
        y = self._coerce_y_value(y_value, profile.columns) or base_spec.y
        group_by = (
            self._resolve_column_reference(group_value, profile.columns) or base_spec.group_by
        )

        options: Dict[str, Any] = {"row_count": profile.row_count}
        payload_options = payload.get("options") or payload.get("chart_options") or payload.get("chartOptions")
        if isinstance(payload_options, dict):
            options.update(payload_options)

        return VisualizationSpec(
            chart_type=base_spec.chart_type,
            x=x,
            y=y,
            group_by=group_by,
            title=payload.get("title") or base_spec.title,
            options=options,
        )

    def _choose_chart_with_llm(
        self,
        df: "pd.DataFrame",  # type: ignore[name-defined]
        *,
        question: Optional[str],
        user_intent: Optional[str],
        requested_chart_type: Optional[str],
        profile: DataProfile,
        candidates: ChartCandidates,
    ) -> Optional[VisualizationSpec]:
        if not self.llm:
            return None
        sample_rows = self._sample_rows(df, max_rows=5)
        prompt = self._build_llm_prompt(
            question=question,
            user_intent=user_intent,
            requested_chart_type=requested_chart_type,
            profile=profile,
            sample_rows=sample_rows,
        )
        try:
            response = self.llm.complete(prompt, temperature=0.0, max_tokens=350)
        except Exception as exc:  # pragma: no cover - defensive guard
            self.logger.warning("VisualAgent LLM selection failed: %s", exc)
            return None

        payload = self._parse_llm_payload(str(response))
        if not payload:
            return None
        return self._coerce_llm_payload(payload, profile, candidates)

    def _choose_chart_heuristic(
        self,
        profile: DataProfile,
        candidates: ChartCandidates,
        signals: VisualSignals,
        user_intent: Optional[str],
    ) -> VisualizationSpec:
        scores: Dict[str, float] = {"table": 0.0}
        if candidates.line_dimension and candidates.line_measures:
            scores["line"] = 0.7
            if candidates.line_dimension in profile.datetime_cols:
                scores["line"] += 0.5
            if signals.has_time:
                scores["line"] += 1.0
            if signals.has_comparison:
                scores["line"] += 0.2
        if candidates.bar_dimension and candidates.bar_measures:
            scores["bar"] = 0.8
            if signals.has_comparison:
                scores["bar"] += 0.8
            if signals.has_ranking:
                scores["bar"] += 0.9
            if signals.has_distribution:
                scores["bar"] += 0.6
            if signals.has_composition:
                scores["bar"] += 0.3
            unique = profile.unique_counts.get(candidates.bar_dimension, 0)
            if unique and unique > 40:
                scores["bar"] -= 0.5
        if len(candidates.scatter_measures) >= 2:
            scores["scatter"] = 0.6
            if signals.has_correlation:
                scores["scatter"] += 1.2
            if signals.has_comparison:
                scores["scatter"] += 0.2
            if profile.row_count > 20:
                scores["scatter"] += 0.1
        if candidates.pie_dimension and candidates.pie_measures:
            scores["pie"] = 0.4
            if signals.has_composition:
                scores["pie"] += 1.0
            if profile.unique_counts.get(candidates.pie_dimension, 0) <= 6:
                scores["pie"] += 0.4

        intent_bias = {
            "time_series_comparison": {"line": 1.2},
            "comparative_view": {"bar": 0.8, "line": 0.3},
            "distribution_analysis": {"bar": 0.7},
            "ranked_highlights": {"bar": 0.8},
            "insight_visualization": {"bar": 0.2, "line": 0.2},
        }
        biases = intent_bias.get(user_intent or "", {})
        for chart_type, bias in biases.items():
            if chart_type in scores:
                scores[chart_type] += bias

        priority = ["line", "bar", "scatter", "pie", "table"]
        chart_type = max(
            priority,
            key=lambda chart: (scores.get(chart, float("-inf")), -priority.index(chart)),
        )
        return self._build_spec_for_chart(chart_type, profile, candidates)

    def _choose_chart(
        self,
        df: "pd.DataFrame",  # type: ignore[name-defined]
        *,
        question: Optional[str],
        user_intent: Optional[str],
    ) -> VisualizationSpec:
        normalized_df = self._normalize_dataframe(df)
        profile = self._profile_dataframe(normalized_df)
        candidates = self._build_candidates(normalized_df, profile)
        signals = self._extract_signals(question, user_intent)
        requested_chart_type = self._detect_requested_chart_type(question, user_intent)
        heuristic_spec = self._choose_chart_heuristic(profile, candidates, signals, user_intent)
        llm_spec = self._choose_chart_with_llm(
            normalized_df,
            question=question,
            user_intent=user_intent,
            requested_chart_type=requested_chart_type,
            profile=profile,
            candidates=candidates,
        )
        chosen_spec = llm_spec or heuristic_spec

        if (
            requested_chart_type
            and requested_chart_type in _ALLOWED_CHART_TYPES
            and requested_chart_type != chosen_spec.chart_type
        ):
            requested_spec = self._build_spec_for_chart(requested_chart_type, profile, candidates)
            if requested_spec.chart_type != "table" or requested_chart_type == "table":
                requested_spec.options = {
                    **(requested_spec.options or {}),
                    "requested_chart_type": requested_chart_type,
                }
                return requested_spec
            chosen_spec = self._build_spec_for_chart("table", profile, candidates)
            chosen_spec.options = {
                **(chosen_spec.options or {}),
                "requested_chart_type": requested_chart_type,
                "visualization_warning": (
                    f"Requested {requested_chart_type} chart could not be created from returned columns."
                ),
            }

        elif requested_chart_type:
            chosen_spec.options = {
                **(chosen_spec.options or {}),
                "requested_chart_type": requested_chart_type,
            }

        return chosen_spec

    def run(
        self,
        data: TabularInput,
        *,
        title: Optional[str] = None,
        question: Optional[str] = None,
        user_intent: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a visualization specification for the provided tabular data.
        """
        self.logger.info("VisualAgent.run invoked with data type %s", type(data).__name__)
        df = _to_dataframe(data)
        spec = self._choose_chart(df, question=question, user_intent=user_intent)
        if not is_placeholder_visualization_title(title, question=question):
            spec.title = str(title).strip()
        elif is_placeholder_visualization_title(spec.title, question=question):
            spec.title = suggest_visualization_title(
                chart_type=spec.chart_type,
                x=spec.x,
                y=spec.y,
                group_by=spec.group_by,
            )
        return spec.to_dict()


__all__ = ["VisualAgent", "VisualizationSpec"]
