
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import sqlglot


def normalize_sql(
    sql: str,
    *,
    read_dialect: str | None = None,
    write_dialect: str | None = None,
) -> str:
    read = str(read_dialect or write_dialect or "duckdb").strip().lower() or "duckdb"
    write = str(write_dialect or read).strip().lower() or read
    try:
        return sqlglot.parse_one(sql, read=read).sql(dialect=write, pretty=False)
    except Exception:
        return " ".join(str(sql).split())


def normalize_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def normalize_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {str(key): normalize_scalar(value) for key, value in row.items()}
        for row in rows
    ]
    return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True, default=str))


def stable_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"

