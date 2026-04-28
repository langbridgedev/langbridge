"""Shared timeframe parsing and resolution helpers for continuation state."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict


class TimeframeState(BaseModel):
    model_config = ConfigDict(extra="ignore")

    kind: Literal["quarter", "year", "rolling_window"]
    label: str
    quarter: str | None = None
    year: str | None = None
    quantity: int | None = None
    unit: Literal["day", "week", "month", "year"] | None = None
    relation: Literal["last", "past", "trailing"] | None = None


def extract_timeframe_state(text: str | None) -> TimeframeState | None:
    match = _find_timeframe_match(text)
    return match[0] if match is not None else None


def same_timeframe(left: TimeframeState | None, right: TimeframeState | None) -> bool:
    if left is None or right is None:
        return False
    return left.model_dump(mode="json") == right.model_dump(mode="json")


def resolve_requested_timeframe(
    *,
    question: str,
    prior_timeframe: TimeframeState | None = None,
) -> TimeframeState | None:
    requested_timeframe = extract_timeframe_state(question)
    if requested_timeframe is None:
        return None
    if (
        requested_timeframe.kind == "quarter"
        and not requested_timeframe.year
        and prior_timeframe
        and prior_timeframe.year
    ):
        return requested_timeframe.model_copy(
            update={
                "year": prior_timeframe.year,
                "label": requested_timeframe.quarter + (f" {prior_timeframe.year}" if prior_timeframe.year else ""),
            }
        )
    return requested_timeframe


def rewrite_prior_question_for_requested_timeframe(
    *,
    question: str,
    prior_question: str,
    prior_timeframe: TimeframeState | None = None,
) -> str | None:
    if not prior_question:
        return None
    requested_timeframe = resolve_requested_timeframe(
        question=question,
        prior_timeframe=prior_timeframe,
    )
    if requested_timeframe is None:
        return None
    if same_timeframe(requested_timeframe, prior_timeframe):
        return None
    prior_match = _find_timeframe_match(prior_question)
    if prior_match is not None:
        prior_timeframe_match, match = prior_match
        if same_timeframe(requested_timeframe, prior_timeframe_match):
            return None
        start, end = match.span()
        return prior_question[:start] + requested_timeframe.label + prior_question[end:]
    instruction = f"Use {requested_timeframe.label} as the time period."
    stripped_prior_question = prior_question.rstrip()
    if stripped_prior_question.endswith("?"):
        return f"{stripped_prior_question.rstrip('?').rstrip()}. {instruction}"
    if stripped_prior_question.endswith((".", "!")):
        return f"{stripped_prior_question} {instruction}"
    return f"{stripped_prior_question}. {instruction}"


def _find_timeframe_match(text: str | None) -> tuple[TimeframeState, re.Match[str]] | None:
    value = str(text or "").strip()
    if not value:
        return None

    rolling_match = re.search(
        r"\b(?P<relation>last|past|trailing)\s+(?P<quantity>\d+)\s+(?P<unit>day|days|week|weeks|month|months|year|years)\b",
        value,
        flags=re.IGNORECASE,
    )
    if rolling_match is not None:
        relation = str(rolling_match.group("relation") or "").strip().lower()
        quantity = int(str(rolling_match.group("quantity") or "0").strip() or "0")
        unit = _normalize_unit(str(rolling_match.group("unit") or "").strip())
        if relation in {"last", "past", "trailing"} and quantity > 0 and unit:
            label = f"{relation} {quantity} {unit}" + ("" if quantity == 1 else "s")
            return (
                TimeframeState(
                    kind="rolling_window",
                    label=label,
                    quantity=quantity,
                    unit=unit,
                    relation=relation,
                ),
                rolling_match,
            )

    quarter_match = re.search(r"\b(Q[1-4])(?:\s+([12][0-9]{3}))?\b", value, flags=re.IGNORECASE)
    if quarter_match is not None:
        quarter = str(quarter_match.group(1) or "").upper()
        year = str(quarter_match.group(2) or "").strip() or None
        label = quarter + (f" {year}" if year else "")
        return (
            TimeframeState(kind="quarter", quarter=quarter, year=year, label=label),
            quarter_match,
        )

    year_match = re.search(r"\b([12][0-9]{3})\b", value)
    if year_match is not None:
        year = str(year_match.group(1) or "").strip()
        return (
            TimeframeState(kind="year", year=year, label=year),
            year_match,
        )
    return None


def _normalize_unit(value: str) -> Literal["day", "week", "month", "year"] | None:
    normalized = str(value or "").strip().casefold()
    singular = normalized[:-1] if normalized.endswith("s") else normalized
    return singular if singular in {"day", "week", "month", "year"} else None


__all__ = [
    "TimeframeState",
    "extract_timeframe_state",
    "resolve_requested_timeframe",
    "rewrite_prior_question_for_requested_timeframe",
    "same_timeframe",
]
