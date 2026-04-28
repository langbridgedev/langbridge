import re
from datetime import date, datetime
from typing import Any, Optional, Tuple

from sqlglot import exp, parse_one


NUMERIC_TYPES = {"integer", "int", "decimal", "numeric", "float", "double", "real"}
BOOLEAN_TYPES = {"bool", "boolean"}
DATE_TYPES = {"date", "datetime", "timestamp", "time"}

_RELATIVE_RE = re.compile(r"^(last|next)\s+(\d+)\s+(day|week|month|quarter|year)s?$", re.I)
_THIS_LAST_NEXT_RE = re.compile(r"^(this|last|next)\s+(week|month|quarter|year)$", re.I)
_SINGLE_DATE_OPERATOR_RE = re.compile(r"^(before|after|on)\s*:\s*(.+)$", re.I)
_ISO_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def quote_identifier(value: str) -> str:
    escaped = value.replace("]", "]]")
    return f"[{escaped}]"


def quote_compound(value: str) -> str:
    parts = [part for part in value.split(".") if part]
    return ".".join(quote_identifier(part) for part in parts)


def format_literal(
    value: Any,
    data_type: Optional[str] = None,
    dialect: str = "tsql",
) -> exp.Expression:
    dialect_key = (dialect or "tsql").lower()
    if value is None:
        return exp.Null()

    if isinstance(value, bool):
        if dialect_key in {"tsql", "sqlserver", "mssql"}:
            return exp.Literal.number(1 if value else 0)
        return exp.Boolean(this=value)

    if isinstance(value, (int, float)):
        return exp.Literal.number(value)

    if isinstance(value, (date, datetime)):
        return exp.Literal.string(value.isoformat())

    value_str = str(value)
    normalized_type = (data_type or "").strip().lower()

    if normalized_type in BOOLEAN_TYPES:
        lowered = value_str.lower()
        if lowered in {"true", "1", "yes"}:
            if dialect_key in {"tsql", "sqlserver", "mssql"}:
                return exp.Literal.number(1)
            return exp.Boolean(this=True)
        if lowered in {"false", "0", "no"}:
            if dialect_key in {"tsql", "sqlserver", "mssql"}:
                return exp.Literal.number(0)
            return exp.Boolean(this=False)

    if normalized_type in NUMERIC_TYPES and _is_numeric(value_str):
        return exp.Literal.number(value_str)

    return exp.Literal.string(value_str)


_SUPPORTED_UNITS = {"week", "month", "quarter", "year", "day", "hour", "minute", "second"}

def date_trunc(granularity: str, col: exp.Expression, dialect: str = "tsql") -> exp.Expression:
    unit = granularity.strip().lower()
    if unit not in _SUPPORTED_UNITS:
        raise ValueError(f"Unsupported granularity '{granularity}'.")

    d = (dialect or "tsql").lower()
    if d in {"duckdb", "postgres", "postgresql", "sqlite", "snowflake"}:
        return exp.DateTrunc(this=col, unit=exp.Literal.string(unit))

    base = exp.Literal.number(0)
    u = exp.Var(this=unit)
    return exp.DateAdd(
        this=base,
        expression=exp.DateDiff(this=col, expression=base, unit=u),
        unit=u,
    )


def _interval_literal(amount: int, unit: str) -> exp.Expression:
    unit_sql = unit.upper()
    if amount != 1:
        unit_sql = f"{unit_sql}S"
    return parse_one(f"INTERVAL '{amount} {unit_sql}'", read="postgres")


def _shift_datetime(
    *,
    base: exp.Expression,
    amount: int,
    unit: str,
    dialect: str,
) -> exp.Expression:
    dialect_key = (dialect or "tsql").lower()
    if dialect_key in {"duckdb", "postgres", "postgresql"}:
        interval = _interval_literal(abs(amount), unit)
        if amount < 0:
            return exp.Sub(this=base, expression=interval)
        return exp.Add(this=base, expression=interval)

    return exp.DateAdd(
        this=base,
        expression=exp.Literal.number(amount),
        unit=exp.Var(this=unit),
    )

def parse_relative_date_range(
    range_str: str, dialect: str = "tsql"
) -> Optional[Tuple[exp.Expression, exp.Expression]]:
    text = range_str.strip().lower()
    normalized = re.sub(r"[_-]+", " ", text)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    dialect_key = (dialect or "tsql").lower()
    if dialect_key in {"tsql", "sqlserver", "mssql"}:
        getdate = exp.Anonymous(this="GETDATE")
        current_date = exp.Cast(this=getdate, to=exp.DataType.build("date"))
        current_ts = getdate
    else:
        current_date = exp.CurrentDate()
        current_ts = exp.CurrentTimestamp()

    if normalized in {"today"}:
        start = current_date
        end = exp.DateAdd(this=current_date, expression=exp.Literal.number(1), unit=exp.Var(this="day"))
        return start, end
    if normalized in {"yesterday"}:
        start = exp.DateAdd(this=current_date, expression=exp.Literal.number(-1), unit=exp.Var(this="day"))
        end = current_date
        return start, end
    if normalized in {"tomorrow"}:
        start = exp.DateAdd(this=current_date, expression=exp.Literal.number(1), unit=exp.Var(this="day"))
        end = exp.DateAdd(this=current_date, expression=exp.Literal.number(2), unit=exp.Var(this="day"))
        return start, end
    if normalized in {"last 7 days"}:
        start = exp.DateAdd(this=current_date, expression=exp.Literal.number(-6), unit=exp.Var(this="day"))
        end = exp.DateAdd(this=current_date, expression=exp.Literal.number(1), unit=exp.Var(this="day"))
        return start, end
    if normalized in {"last 30 days"}:
        start = exp.DateAdd(this=current_date, expression=exp.Literal.number(-29), unit=exp.Var(this="day"))
        end = exp.DateAdd(this=current_date, expression=exp.Literal.number(1), unit=exp.Var(this="day"))
        return start, end
    if normalized in {"month to date"}:
        start = date_trunc("month", current_date, dialect=dialect)
        end = exp.DateAdd(this=current_date, expression=exp.Literal.number(1), unit=exp.Var(this="day"))
        return start, end
    if normalized in {"year to date"}:
        start = date_trunc("year", current_date, dialect=dialect)
        end = exp.DateAdd(this=current_date, expression=exp.Literal.number(1), unit=exp.Var(this="day"))
        return start, end

    match = _RELATIVE_RE.match(normalized)
    if match:
        direction, amount_str, unit = match.groups()
        amount = int(amount_str)
        unit_var = exp.Var(this=unit)
        if unit == "day":
            if direction == "last":
                start = exp.DateAdd(
                    this=current_date,
                    expression=exp.Literal.number(-max(amount - 1, 0)),
                    unit=unit_var,
                )
                end = exp.DateAdd(this=current_date, expression=exp.Literal.number(1), unit=unit_var)
                return start, end
            start = current_date
            end = exp.DateAdd(this=current_date, expression=exp.Literal.number(amount), unit=unit_var)
            return start, end

        if direction == "last":
            start = exp.DateAdd(this=current_ts, expression=exp.Literal.number(-amount), unit=unit_var)
            end = current_ts
            return start, end
        start = current_ts
        end = exp.DateAdd(this=current_ts, expression=exp.Literal.number(amount), unit=unit_var)
        return start, end

    match = _THIS_LAST_NEXT_RE.match(normalized)
    if match:
        direction, unit = match.groups()
        if dialect_key in {"duckdb", "postgres", "postgresql", "snowflake"}:
            start_of_period = date_trunc(unit, current_date, dialect=dialect)
            if direction == "this":
                return start_of_period, _shift_datetime(
                    base=start_of_period,
                    amount=1,
                    unit=unit,
                    dialect=dialect,
                )
            if direction == "last":
                return (
                    _shift_datetime(
                        base=start_of_period,
                        amount=-1,
                        unit=unit,
                        dialect=dialect,
                    ),
                    start_of_period,
                )
            next_start = _shift_datetime(
                base=start_of_period,
                amount=1,
                unit=unit,
                dialect=dialect,
            )
            return (
                next_start,
                _shift_datetime(
                    base=start_of_period,
                    amount=2,
                    unit=unit,
                    dialect=dialect,
                ),
            )

        unit_var = exp.Var(this=unit)
        base = exp.DateDiff(this=current_ts, expression=exp.Literal.number(0), unit=unit_var)
        if direction == "this":
            start = exp.DateAdd(this=exp.Literal.number(0), expression=base, unit=unit_var)
            end = exp.DateAdd(
                this=exp.Literal.number(0),
                expression=exp.Add(this=base, expression=exp.Literal.number(1)),
                unit=unit_var,
            )
            return start, end
        if direction == "last":
            start = exp.DateAdd(
                this=exp.Literal.number(0),
                expression=exp.Sub(this=base, expression=exp.Literal.number(1)),
                unit=unit_var,
            )
            end = exp.DateAdd(this=exp.Literal.number(0), expression=base, unit=unit_var)
            return start, end
        start = exp.DateAdd(
            this=exp.Literal.number(0),
            expression=exp.Add(this=base, expression=exp.Literal.number(1)),
            unit=unit_var,
        )
        end = exp.DateAdd(
            this=exp.Literal.number(0),
            expression=exp.Add(this=base, expression=exp.Literal.number(2)),
            unit=unit_var,
        )
        return start, end

    return None


def _build_single_date_operator_condition(
    column_expr: exp.Expression,
    date_range: str,
    data_type: Optional[str] = None,
    dialect: str = "tsql",
) -> Optional[exp.Expression]:
    match = _SINGLE_DATE_OPERATOR_RE.match(date_range.strip())
    if not match:
        return None
    operator = match.group(1).lower()
    value = match.group(2).strip()
    if not value:
        return None

    literal = format_literal(value, data_type, dialect=dialect)
    if operator == "before":
        return exp.LT(this=column_expr, expression=literal)
    if operator == "after":
        return exp.GT(this=column_expr, expression=literal)
    if operator == "on":
        end_base = _cast_date_literal_if_needed(literal, value, data_type)
        end = exp.DateAdd(this=end_base, expression=exp.Literal.number(1), unit=exp.Var(this="day"))
        return exp.and_(
            exp.GTE(this=column_expr, expression=literal),
            exp.LT(this=column_expr, expression=end),
        )
    return None


def _cast_date_literal_if_needed(
    literal: exp.Expression,
    raw_value: Any,
    data_type: Optional[str] = None,
) -> exp.Expression:
    normalized_type = (data_type or "").strip().lower()
    if (
        normalized_type in DATE_TYPES
        and isinstance(raw_value, str)
        and _ISO_DATE_ONLY_RE.match(raw_value.strip())
    ):
        return exp.Cast(this=literal, to=exp.DataType.build("date"))
    return literal

def build_date_range_condition(
    column_expr: exp.Expression,
    date_range: Any,
    data_type: Optional[str] = None,
    dialect: str = "tsql",
) -> exp.Expression:
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        normalized_type = (data_type or "").strip().lower()
        start_raw = date_range[0]
        end_raw = date_range[1]
        start = format_literal(date_range[0], data_type, dialect=dialect)
        end = format_literal(date_range[1], data_type, dialect=dialect)
        if (
            normalized_type in DATE_TYPES
            and isinstance(start_raw, str)
            and isinstance(end_raw, str)
            and _ISO_DATE_ONLY_RE.match(start_raw.strip())
            and _ISO_DATE_ONLY_RE.match(end_raw.strip())
        ):
            end_exclusive = exp.DateAdd(
                this=_cast_date_literal_if_needed(end, end_raw, data_type),
                expression=exp.Literal.number(1),
                unit=exp.Var(this="day"),
            )
            return exp.and_(
                exp.GTE(this=column_expr, expression=start),
                exp.LT(this=column_expr, expression=end_exclusive),
            )
        return exp.and_(
            exp.GTE(this=column_expr, expression=start),
            exp.LTE(this=column_expr, expression=end),
        )

    if isinstance(date_range, str):
        custom = _build_single_date_operator_condition(
            column_expr,
            date_range,
            data_type=data_type,
            dialect=dialect,
        )
        if custom:
            return custom
        relative = parse_relative_date_range(date_range, dialect=dialect)
        if relative:
            start, end = relative
            return exp.and_(
                exp.GTE(this=column_expr, expression=start),
                exp.LT(this=column_expr, expression=end),
            )
        return exp.EQ(
            this=column_expr,
            expression=format_literal(date_range, data_type, dialect=dialect),
        )

    raise ValueError("Unsupported date range format.")


def _is_numeric(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False
