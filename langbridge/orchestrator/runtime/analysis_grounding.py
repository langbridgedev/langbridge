
from datetime import date, datetime
import re
from typing import Any

_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "in",
    "is",
    "of",
    "on",
    "or",
    "show",
    "tell",
    "than",
    "the",
    "to",
    "what",
    "which",
    "who",
}
_BOTTOM_TERMS = {"bottom", "laggard", "least", "lowest", "smallest", "worst"}
_COMPARISON_TERMS = {"compare", "comparison", "difference", "gap", "higher", "lower", "versus", "vs"}
_TREND_TERMS = {"trend", "trending", "growth", "decline", "month", "quarter", "week", "year", "over", "time"}
_SHARE_TERMS = {"share", "contribution", "mix", "portion"}
_TEMPORAL_NAME_HINTS = {"date", "day", "month", "quarter", "week", "year", "time", "period"}
_MEASURE_HINTS = {"amount", "count", "margin", "profit", "rate", "refund", "revenue", "sales", "spend", "total"}


def build_analyst_grounding(
    question: str,
    payload: dict[str, Any] | None,
    *,
    max_key_rows: int = 5,
) -> dict[str, Any]:
    try:
        return _build_analyst_grounding(
            question,
            payload,
            max_key_rows=max_key_rows,
        )
    except Exception:
        columns, row_dicts = _safe_shape(payload)
        return {
            "question": str(question or "").strip(),
            "question_focus": "top_rank",
            "row_count": len(row_dicts),
            "column_count": len(columns),
            "columns": columns,
            "primary_dimension": None,
            "primary_measure": None,
            "analysis_type": "fallback",
            "key_rows": row_dicts[:max_key_rows],
            "aggregates": {},
            "observed_facts": [],
            "interpretations": [],
            "caveats": [
                "The result was returned, but deeper analytical interpretation was skipped because the result shape was not handled safely."
            ],
        }


def _build_analyst_grounding(
    question: str,
    payload: dict[str, Any] | None,
    *,
    max_key_rows: int = 5,
) -> dict[str, Any]:
    columns, row_dicts = _normalize_payload(payload)
    row_count = len(row_dicts)
    question_text = str(question or "").strip()
    question_tokens = _tokenize(question_text)
    wants_bottom = bool(question_tokens & _BOTTOM_TERMS)
    wants_comparison = bool(question_tokens & _COMPARISON_TERMS)
    wants_trend = bool(question_tokens & _TREND_TERMS)
    wants_share = bool(question_tokens & _SHARE_TERMS)

    grounding: dict[str, Any] = {
        "question": question_text,
        "question_focus": (
            "trend"
            if wants_trend
            else "bottom_rank"
            if wants_bottom
            else "comparison"
            if wants_comparison
            else "top_rank"
        ),
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "primary_dimension": None,
        "primary_measure": None,
        "analysis_type": "empty",
        "key_rows": [],
        "aggregates": {},
        "observed_facts": [],
        "interpretations": [],
        "caveats": [],
    }

    if not columns:
        grounding["caveats"].append("The result did not include usable column metadata.")
        return grounding

    if row_count == 0:
        grounding["caveats"].append(
            "No rows matched the query, so there is not enough result data for a grounded analytical conclusion."
        )
        return grounding

    numeric_columns = [column for column in columns if _is_numeric_column(column, row_dicts)]
    temporal_columns = [column for column in columns if _is_temporal_column(column, row_dicts)]
    dimension_columns = [column for column in columns if column not in numeric_columns]

    primary_measure = _choose_primary_measure(question_tokens, numeric_columns)
    primary_dimension = _choose_primary_dimension(
        question_tokens,
        dimension_columns,
        temporal_columns,
        wants_trend=wants_trend,
    )

    grounding["primary_measure"] = primary_measure
    grounding["primary_dimension"] = primary_dimension

    if primary_measure is None:
        grounding["analysis_type"] = "categorical"
        grounding["key_rows"] = row_dicts[:max_key_rows]
        grounding["observed_facts"].extend(_categorical_observations(columns, row_dicts))
        grounding["caveats"].append(
            "This result set does not expose a clear numeric measure, so ranking and contribution analysis are limited."
        )
        if row_count <= 2:
            grounding["caveats"].append(
                "Only a small number of rows were returned, so the result is mainly descriptive rather than comparative."
            )
        return grounding

    measure_values = [
        {
            "row": row,
            "raw": row.get(primary_measure),
            "value": _coerce_numeric(row.get(primary_measure)),
        }
        for row in row_dicts
    ]
    valid_measure_values = [item for item in measure_values if item["value"] is not None]
    grounding["aggregates"] = _build_measure_aggregates(valid_measure_values)

    if primary_dimension is None:
        grounding["analysis_type"] = "single_measure"
        grounding["key_rows"] = row_dicts[:max_key_rows]
        grounding["observed_facts"].extend(
            _single_measure_observations(primary_measure, valid_measure_values)
        )
        if row_count <= 2:
            grounding["caveats"].append(
                "Only a small number of rows were returned, so comparisons inside the result set are limited."
            )
        return grounding

    entries = []
    for row in row_dicts:
        value = _coerce_numeric(row.get(primary_measure))
        label = row.get(primary_dimension)
        if value is None or label is None:
            continue
        entries.append(
            {
                "label": str(label),
                "value": value,
                "display_value": _format_value(row.get(primary_measure)),
                "dimension_value": row.get(primary_dimension),
                "row": row,
            }
        )

    grounding["key_rows"] = _select_key_rows(entries, primary_dimension, primary_measure, max_key_rows=max_key_rows)
    if not entries:
        grounding["analysis_type"] = "sparse_measure"
        grounding["caveats"].append(
            f"The result includes the measure '{primary_measure}' but not enough populated values to interpret it reliably."
        )
        return grounding

    if primary_dimension in temporal_columns:
        trend_facts = _temporal_trend_observations(primary_dimension, primary_measure, entries)
        if trend_facts:
            grounding["analysis_type"] = "trend"
            grounding["observed_facts"].extend(trend_facts["observed_facts"])
            grounding["interpretations"].extend(trend_facts["interpretations"])
            if row_count <= 2:
                grounding["caveats"].append(
                    "Only two returned periods are available, so the trend should be treated as directional rather than conclusive."
                )
            return grounding

    ranking = _ranking_observations(
        primary_dimension=primary_dimension,
        primary_measure=primary_measure,
        entries=entries,
        wants_bottom=wants_bottom,
        wants_comparison=wants_comparison,
        wants_share=wants_share,
    )
    grounding["analysis_type"] = "ranking"
    grounding["observed_facts"].extend(ranking["observed_facts"])
    grounding["interpretations"].extend(ranking["interpretations"])
    if row_count <= 2:
        grounding["caveats"].append(
            "Only two rows were returned, so the comparison is grounded but narrow."
        )
    return grounding


def compose_analyst_summary(
    grounding: dict[str, Any] | None,
    *,
    assumptions: list[str] | None = None,
    extra_note: str | None = None,
) -> str:
    assumptions = [item.strip() for item in (assumptions or []) if isinstance(item, str) and item.strip()]
    note = str(extra_note or "").strip()
    if not isinstance(grounding, dict):
        summary = "I could not build a grounded analytical summary from the returned result."
        if note:
            summary = summary + " " + note
        if assumptions:
            summary = summary + " Assumptions: " + "; ".join(assumptions)
        return summary

    row_count = int(grounding.get("row_count") or 0)
    observed = [item.strip() for item in grounding.get("observed_facts", []) if isinstance(item, str) and item.strip()]
    interpretations = [
        item.strip() for item in grounding.get("interpretations", []) if isinstance(item, str) and item.strip()
    ]
    caveats = [item.strip() for item in grounding.get("caveats", []) if isinstance(item, str) and item.strip()]

    if row_count == 0:
        summary = caveats[0] if caveats else (
            "No rows matched the query, so there is not enough result data for a grounded analytical answer."
        )
    else:
        sentences: list[str] = []
        sentences.extend(observed[:2])
        if interpretations:
            sentences.append(interpretations[0])
        if caveats and (row_count <= 2 or not observed or grounding.get("analysis_type") == "categorical"):
            sentences.append(caveats[0])
        summary = " ".join(sentence for sentence in sentences if sentence).strip()
        if not summary:
            summary = "I returned the result set, but it does not support a stronger grounded interpretation."

    if note:
        summary = summary + " " + note
    if assumptions:
        summary = summary + " Assumptions: " + "; ".join(assumptions)
    return summary


def render_analyst_grounding_for_prompt(grounding: dict[str, Any] | None) -> str:
    if not isinstance(grounding, dict):
        return ""

    lines: list[str] = []
    row_count = grounding.get("row_count")
    column_count = grounding.get("column_count")
    if row_count is not None and column_count is not None:
        lines.append(f"Shape: {row_count} rows x {column_count} columns.")

    columns = grounding.get("columns")
    if isinstance(columns, list) and columns:
        lines.append("Columns: " + ", ".join(str(column) for column in columns))

    primary_measure = grounding.get("primary_measure")
    primary_dimension = grounding.get("primary_dimension")
    if primary_measure:
        lines.append(f"Primary measure: {primary_measure}.")
    if primary_dimension:
        lines.append(f"Primary dimension: {primary_dimension}.")

    aggregates = grounding.get("aggregates")
    if isinstance(aggregates, dict) and aggregates:
        aggregate_parts = []
        for key in ("total", "average", "min", "max"):
            value = aggregates.get(key)
            if value is None:
                continue
            aggregate_parts.append(f"{key}={_format_value(value)}")
        if aggregate_parts:
            lines.append("Aggregates: " + ", ".join(aggregate_parts))

    observed = grounding.get("observed_facts")
    if isinstance(observed, list) and observed:
        lines.append("Observed facts:")
        lines.extend(f"- {item}" for item in observed[:4] if isinstance(item, str) and item.strip())

    interpretations = grounding.get("interpretations")
    if isinstance(interpretations, list) and interpretations:
        lines.append("Reasonable interpretations:")
        lines.extend(f"- {item}" for item in interpretations[:3] if isinstance(item, str) and item.strip())

    caveats = grounding.get("caveats")
    if isinstance(caveats, list) and caveats:
        lines.append("Limits:")
        lines.extend(f"- {item}" for item in caveats[:3] if isinstance(item, str) and item.strip())

    key_rows = grounding.get("key_rows")
    if isinstance(key_rows, list) and key_rows:
        lines.append("Key rows:")
        for row in key_rows[:3]:
            if isinstance(row, dict):
                rendered = ", ".join(f"{key}={_format_value(value)}" for key, value in row.items())
                lines.append(f"- {rendered}")

    return "\n".join(lines).strip()


def _safe_shape(payload: dict[str, Any] | None) -> tuple[list[str], list[dict[str, Any]]]:
    try:
        return _normalize_payload(payload)
    except Exception:
        return [], []


def _normalize_payload(payload: dict[str, Any] | None) -> tuple[list[str], list[dict[str, Any]]]:
    if not isinstance(payload, dict):
        return [], []

    columns_raw = payload.get("columns")
    rows_raw = payload.get("rows")
    if not isinstance(rows_raw, list):
        return [], []

    columns = [str(column) for column in columns_raw] if isinstance(columns_raw, list) else []
    if not columns and rows_raw and isinstance(rows_raw[0], dict):
        columns = [str(key) for key in rows_raw[0].keys()]

    row_dicts: list[dict[str, Any]] = []
    for row in rows_raw:
        if isinstance(row, dict):
            row_dicts.append({column: row.get(column) for column in columns})
            continue
        if isinstance(row, (list, tuple)):
            values = list(row)
            if len(values) < len(columns):
                values.extend([None] * (len(columns) - len(values)))
            row_dicts.append({column: values[index] for index, column in enumerate(columns)})
            continue
        if columns:
            row_dicts.append({columns[0]: row})
    return columns, row_dicts


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", str(text or "").lower())
        if len(token) > 1 and token not in _STOPWORDS
    }


def _is_numeric_column(column: str, rows: list[dict[str, Any]]) -> bool:
    observed = 0
    numeric = 0
    for row in rows[:50]:
        value = row.get(column)
        if value is None:
            continue
        observed += 1
        if _coerce_numeric(value) is not None:
            numeric += 1
    if observed == 0:
        return False
    return numeric / observed >= 0.8


def _choose_primary_measure(question_tokens: set[str], numeric_columns: list[str]) -> str | None:
    if not numeric_columns:
        return None

    def score(column: str) -> tuple[int, int, str]:
        column_tokens = _tokenize(column)
        overlap = len(column_tokens & question_tokens)
        hint_bonus = 1 if column_tokens & _MEASURE_HINTS else 0
        return (overlap, hint_bonus, column.lower())

    return max(numeric_columns, key=score)


def _choose_primary_dimension(
    question_tokens: set[str],
    dimension_columns: list[str],
    temporal_columns: list[str],
    *,
    wants_trend: bool,
) -> str | None:
    if not dimension_columns:
        return None
    if wants_trend and temporal_columns:
        return temporal_columns[0]

    def score(column: str) -> tuple[int, int, str]:
        column_tokens = _tokenize(column)
        overlap = len(column_tokens & question_tokens)
        temporal_penalty = 0 if column not in temporal_columns else -1
        return (overlap, temporal_penalty, column.lower())

    return max(dimension_columns, key=score)


def _build_measure_aggregates(values: list[dict[str, Any]]) -> dict[str, Any]:
    if not values:
        return {}
    numeric_values = [float(item["value"]) for item in values if item.get("value") is not None]
    if not numeric_values:
        return {}
    total = sum(numeric_values)
    return {
        "total": total,
        "average": total / len(numeric_values),
        "min": min(numeric_values),
        "max": max(numeric_values),
    }


def _categorical_observations(columns: list[str], rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    if len(rows) == 1:
        row = rows[0]
        pairs = [f"{column} is {_format_value(row.get(column))}" for column in columns[:3]]
        return ["Based on the returned row, " + " and ".join(pairs) + "."]
    first_column = columns[0]
    labels = [_format_value(row.get(first_column)) for row in rows[:4]]
    return [
        f"Based on the returned rows, {first_column} includes {', '.join(labels[:3])}"
        + (f", and {labels[3]}" if len(labels) == 4 else "")
        + "."
    ]


def _single_measure_observations(
    measure: str,
    valid_measure_values: list[dict[str, Any]],
) -> list[str]:
    if not valid_measure_values:
        return []
    if len(valid_measure_values) == 1:
        return [
            f"Based on the returned row, {measure} is {_format_value(valid_measure_values[0]['raw'])}."
        ]

    numeric_values = [float(item["value"]) for item in valid_measure_values]
    return [
        f"Based on the returned rows, {measure} ranges from {_format_value(min(numeric_values))} to {_format_value(max(numeric_values))}.",
        f"The average {measure} across this result set is {_format_value(sum(numeric_values) / len(numeric_values))}.",
    ]


def _temporal_trend_observations(
    dimension: str,
    measure: str,
    entries: list[dict[str, Any]],
) -> dict[str, list[str]] | None:
    parsed = []
    for entry in entries:
        parsed_value = _parse_temporal(entry["dimension_value"])
        if parsed_value is None:
            return None
        parsed.append((parsed_value, entry))

    ascending = parsed == sorted(parsed, key=lambda item: item[0])
    descending = parsed == sorted(parsed, key=lambda item: item[0], reverse=True)
    if not ascending and not descending:
        return None

    ordered = sorted(parsed, key=lambda item: item[0])
    first_entry = ordered[0][1]
    last_entry = ordered[-1][1]
    delta = float(last_entry["value"]) - float(first_entry["value"])
    direction = "increased" if delta > 0 else "decreased" if delta < 0 else "was flat"

    observed = [
        f"Based on the returned rows, {measure} moves from {first_entry['display_value']} in {first_entry['label']} "
        f"to {last_entry['display_value']} in {last_entry['label']}."
    ]
    interpretations: list[str] = []
    if delta != 0:
        percent_change = None
        if float(first_entry["value"]) != 0:
            percent_change = abs(delta) / abs(float(first_entry["value"]))
        change_text = f"{_format_value(abs(delta))}"
        if percent_change is not None:
            change_text = change_text + f" ({percent_change:.1%})"
        interpretations.append(
            f"In this result set, that means {measure} {direction} by {change_text} across the returned periods."
        )
    else:
        interpretations.append(f"In this result set, {measure} is flat across the returned periods.")

    return {"observed_facts": observed, "interpretations": interpretations}


def _ranking_observations(
    *,
    primary_dimension: str,
    primary_measure: str,
    entries: list[dict[str, Any]],
    wants_bottom: bool,
    wants_comparison: bool,
    wants_share: bool,
) -> dict[str, list[str]]:
    descending = sorted(entries, key=lambda item: item["value"], reverse=True)
    ascending = list(reversed(descending))
    leader = descending[0]
    laggard = ascending[0]
    runner_up = descending[1] if len(descending) > 1 else None

    observed_facts: list[str] = []
    interpretations: list[str] = []

    if wants_bottom:
        observed_facts.append(
            f"Based on the returned rows, {laggard['label']} has the lowest {primary_measure} at {laggard['display_value']}."
        )
        if len(descending) > 1:
            next_lowest = ascending[1]
            delta = float(next_lowest["value"]) - float(laggard["value"])
            observed_facts.append(
                f"It trails {next_lowest['label']} by {_format_value(delta)} in this result set."
            )
    elif wants_comparison and len(entries) == 2:
        other = descending[1]
        delta = float(descending[0]["value"]) - float(other["value"])
        observed_facts.append(
            f"Based on the returned rows, {descending[0]['label']} is higher on {primary_measure} than {other['label']} "
            f"({_format_value(descending[0]['display_value'])} vs {_format_value(other['display_value'])})."
        )
        observed_facts.append(
            f"The gap between them is {_format_value(delta)}."
        )
    else:
        observed_facts.append(
            f"Based on the returned rows, {leader['label']} has the highest {primary_measure} at {leader['display_value']}."
        )
        if runner_up is not None:
            delta = float(leader["value"]) - float(runner_up["value"])
            observed_facts.append(
                f"It leads {runner_up['label']} by {_format_value(delta)}, while {laggard['label']} is lowest at {laggard['display_value']}."
            )

    ranked_labels = [f"{entry['label']} ({entry['display_value']})" for entry in descending[:4]]
    if len(ranked_labels) >= 3:
        interpretations.append(
            f"The ranking by {primary_measure} is " + ", ".join(ranked_labels[:-1]) + f", then {ranked_labels[-1]}."
        )

    total = sum(float(entry["value"]) for entry in entries)
    if total > 0 and (wants_share or len(entries) >= 3):
        top_share = float(leader["value"]) / total
        interpretations.append(
            f"The strongest contributor appears to be {leader['label']}, representing {top_share:.1%} of the {primary_measure} total in this result set."
        )

    return {"observed_facts": observed_facts, "interpretations": interpretations}


def _select_key_rows(
    entries: list[dict[str, Any]],
    primary_dimension: str,
    primary_measure: str,
    *,
    max_key_rows: int,
) -> list[dict[str, Any]]:
    ranked = sorted(entries, key=lambda item: item["value"], reverse=True)
    key_rows = []
    for entry in ranked[:max_key_rows]:
        key_rows.append(
            {
                primary_dimension: entry["label"],
                primary_measure: entry["display_value"],
            }
        )
    return key_rows


def _is_temporal_column(column: str, rows: list[dict[str, Any]]) -> bool:
    if _tokenize(column) & _TEMPORAL_NAME_HINTS:
        return True
    non_null = [row.get(column) for row in rows[:20] if row.get(column) is not None]
    if not non_null:
        return False
    parsed = sum(1 for value in non_null if _parse_temporal(value) is not None)
    return parsed / len(non_null) >= 0.8


def _parse_temporal(value: Any) -> datetime | date | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    quarter_match = re.match(r"^(\d{4})[-/\s]?Q([1-4])$", text, re.IGNORECASE)
    if quarter_match:
        year = int(quarter_match.group(1))
        quarter = int(quarter_match.group(2))
        return date(year, ((quarter - 1) * 3) + 1, 1)

    if re.match(r"^\d{4}$", text):
        return date(int(text), 1, 1)

    if re.match(r"^\d{4}-\d{2}$", text):
        return date.fromisoformat(text + "-01")

    candidate = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


def _coerce_numeric(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        cleaned = cleaned.replace("$", "").replace("£", "").replace("€", "")
        if cleaned.endswith("%"):
            cleaned = cleaned[:-1]
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _format_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
        magnitude = abs(value)
        if magnitude >= 100:
            precision = 1
        elif magnitude >= 1:
            precision = 2
        else:
            precision = 4
        return f"{value:,.{precision}f}".rstrip("0").rstrip(".")
    return str(value)


__all__ = [
    "build_analyst_grounding",
    "compose_analyst_summary",
    "render_analyst_grounding_for_prompt",
]
