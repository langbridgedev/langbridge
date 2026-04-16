"""
Utilities for deriving concise visualization titles from chart structure.
"""

from __future__ import annotations

import re
from typing import Optional, Sequence, Union

_GENERIC_VISUAL_TITLES = {
    "automated insight",
    "runtime chart",
    "chart preview",
    "requested chart",
}
_TITLE_TOKEN_RE = re.compile(r"[_\-.]+")
_TITLE_WHITESPACE_RE = re.compile(r"\s+")
_TIME_BUCKET_LABELS = {
    "year": "Year",
    "quarter": "Quarter",
    "month": "Month",
    "week": "Week",
    "day": "Day",
}
_UPPER_TOKENS = {"id", "ids", "pct", "usd", "eur", "gbp", "aud", "cad", "yoy", "mom", "qoq"}


def is_placeholder_visualization_title(title: Optional[str], *, question: Optional[str] = None) -> bool:
    text = str(title or "").strip()
    if not text:
        return True
    lowered = text.lower()
    normalized_question = str(question or "").strip().lower()
    if normalized_question and lowered == normalized_question:
        return True
    if lowered in _GENERIC_VISUAL_TITLES:
        return True
    if lowered.startswith("visualization for "):
        return True
    if lowered.startswith("chart for "):
        return True
    return False


def _humanize_field_name(value: Optional[str]) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "__" in text:
        text = text.rsplit("__", 1)[-1]
    if "." in text:
        text = text.rsplit(".", 1)[-1]

    lowered = text.lower()
    for suffix, label in _TIME_BUCKET_LABELS.items():
        if lowered.endswith(f"_{suffix}") and any(token in lowered for token in ("date", "time", "timestamp")):
            return label

    normalized = _TITLE_TOKEN_RE.sub(" ", text)
    parts = [part for part in _TITLE_WHITESPACE_RE.split(normalized) if part]
    if not parts:
        return ""

    rendered: list[str] = []
    for part in parts:
        lowered_part = part.lower()
        if part.isupper() and len(part) <= 5:
            rendered.append(part)
        elif lowered_part in _UPPER_TOKENS:
            rendered.append(lowered_part.upper())
        else:
            rendered.append(part.capitalize())
    return " ".join(rendered)


def _coerce_measure_labels(value: Optional[Union[str, Sequence[str]]]) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        label = _humanize_field_name(value)
        return [label] if label else []

    labels: list[str] = []
    for item in value:
        label = _humanize_field_name(str(item))
        if label:
            labels.append(label)
    return labels


def _join_labels(labels: Sequence[str]) -> str:
    cleaned = [label for label in labels if label]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"


def suggest_visualization_title(
    *,
    chart_type: str,
    x: Optional[str] = None,
    y: Optional[Union[str, Sequence[str]]] = None,
    group_by: Optional[str] = None,
) -> str:
    chart = str(chart_type or "table").strip().lower() or "table"
    x_label = _humanize_field_name(x)
    measure_label = _join_labels(_coerce_measure_labels(y))
    group_label = _humanize_field_name(group_by)

    if chart == "scatter":
        if measure_label and x_label:
            title = f"{measure_label} vs {x_label}"
        elif measure_label:
            title = measure_label
        else:
            title = "Scatter Analysis"
        if group_label:
            title += f" by {group_label}"
        return title

    if chart == "pie":
        if measure_label and x_label:
            return f"{measure_label} by {x_label}"
        if x_label:
            return f"Composition by {x_label}"
        if measure_label:
            return measure_label
        return "Composition Breakdown"

    if measure_label and x_label:
        title = f"{measure_label} by {x_label}"
    elif measure_label and group_label:
        title = f"{measure_label} by {group_label}"
    elif measure_label:
        title = measure_label
    elif x_label:
        if chart == "line":
            title = f"Trend by {x_label}"
        if chart == "bar":
            title = f"Breakdown by {x_label}"
        if chart == "table":
            title = f"Results by {x_label}"
    else:
        title = ""

    if not title:
        if chart == "line":
            title = "Trend Analysis"
        elif chart == "bar":
            title = "Category Comparison"
        elif chart == "table":
            title = "Results Table"
        else:
            title = "Visualization"

    if group_label and x_label and group_label.lower() != x_label.lower() and chart in {"bar", "line", "table"}:
        title += f", split by {group_label}"
    return title
