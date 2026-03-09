import hashlib
import json
import re
from datetime import date, datetime, time
from typing import Any

import sqlglot
from sqlglot import exp


_PARAM_TEMPLATE_PATTERN = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")
_PARAM_COLON_PATTERN = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)\b")
_FIRST_TOKEN_PATTERN = re.compile(r"^\s*([a-zA-Z_]+)")
_ERROR_SECRET_PATTERN = re.compile(
    r"(?i)(password|pwd|secret|token)\s*=\s*([^;\s,]+)"
)
_DIALECT_ALIASES = {
    "tsql": "tsql",
    "sqlserver": "tsql",
    "mssql": "tsql",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "snowflake": "snowflake",
    "redshift": "redshift",
    "bigquery": "bigquery",
    "oracle": "oracle",
    "sqlite": "sqlite",
    "trino": "trino",
}


def fingerprint_query(query: str) -> str:
    normalized = " ".join(query.strip().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def render_sql_with_params(query: str, params: dict[str, Any]) -> str:
    params = params or {}
    rendered = query

    def _replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in params:
            raise ValueError(f"Missing SQL parameter '{key}'.")
        return _to_sql_literal(params[key])

    rendered = _PARAM_TEMPLATE_PATTERN.sub(_replace, rendered)
    rendered = _PARAM_COLON_PATTERN.sub(_replace, rendered)
    return rendered


def normalize_sql_dialect(dialect: str | None, *, default: str = "tsql") -> str:
    if dialect is None:
        return default
    normalized = str(dialect).strip().lower()
    if not normalized:
        return default
    return _DIALECT_ALIASES.get(normalized, normalized)


def transpile_sql(
    query: str,
    *,
    source_dialect: str,
    target_dialect: str,
) -> str:
    source = normalize_sql_dialect(source_dialect)
    target = normalize_sql_dialect(target_dialect)
    if source == target:
        return query
    try:
        expression = sqlglot.parse_one(query, read=source)
    except sqlglot.ParseError as exc:
        raise ValueError(f"Unable to parse SQL using source dialect '{source}'.") from exc
    return expression.sql(dialect=target)


def enforce_read_only_sql(query: str, *, allow_dml: bool, dialect: str = "tsql") -> None:
    if allow_dml:
        return

    sqlglot_dialect = normalize_sql_dialect(dialect)
    try:
        statements = sqlglot.parse(query, read=sqlglot_dialect)
    except sqlglot.ParseError as exc:
        raise ValueError(f"SQL parse failed: {exc}") from exc

    if not statements:
        raise ValueError("Query is empty.")
    if len(statements) != 1:
        raise ValueError("Only a single SQL statement is allowed.")

    statement = statements[0]
    token_match = _FIRST_TOKEN_PATTERN.search(query)
    token = token_match.group(1).lower() if token_match else ""
    if token not in {"select", "with"}:
        raise ValueError("Workspace policy only allows SELECT statements.")

    forbidden_nodes = tuple(
        node
        for node in (
            getattr(exp, "Insert", None),
            getattr(exp, "Update", None),
            getattr(exp, "Delete", None),
            getattr(exp, "Drop", None),
            getattr(exp, "Alter", None),
            getattr(exp, "Create", None),
            getattr(exp, "Merge", None),
            getattr(exp, "TruncateTable", None),
        )
        if node is not None
    )

    for node_type in forbidden_nodes:
        if isinstance(statement, node_type) or next(statement.find_all(node_type), None):
            raise ValueError("Workspace policy only allows SELECT statements.")


def extract_table_references(query: str, *, dialect: str = "tsql") -> list[tuple[str | None, str]]:
    sqlglot_dialect = normalize_sql_dialect(dialect)
    try:
        expression = sqlglot.parse_one(query, read=sqlglot_dialect)
    except sqlglot.ParseError:
        return []

    refs: list[tuple[str | None, str]] = []
    seen: set[tuple[str | None, str]] = set()
    for table in expression.find_all(exp.Table):
        schema = (table.db or "").strip().lower() or None
        name = (table.name or "").strip().lower()
        if not name:
            continue
        key = (schema, name)
        if key in seen:
            continue
        seen.add(key)
        refs.append(key)
    return refs


def enforce_table_allowlist(
    query: str,
    *,
    allowed_schemas: list[str],
    allowed_tables: list[str],
    dialect: str = "tsql",
) -> None:
    normalized_schemas = {schema.strip().lower() for schema in allowed_schemas if schema.strip()}
    normalized_tables = {table.strip().lower() for table in allowed_tables if table.strip()}
    if not normalized_schemas and not normalized_tables:
        return

    for schema, table in extract_table_references(query, dialect=dialect):
        if normalized_schemas and schema and schema not in normalized_schemas:
            raise ValueError(f"Schema '{schema}' is not permitted by workspace policy.")
        if normalized_tables:
            if schema:
                qualified = f"{schema}.{table}"
                if qualified in normalized_tables or table in normalized_tables:
                    continue
                raise ValueError(f"Table '{qualified}' is not permitted by workspace policy.")
            if table not in normalized_tables:
                raise ValueError(f"Table '{table}' is not permitted by workspace policy.")


def enforce_preview_limit(query: str, *, max_rows: int, dialect: str = "tsql") -> tuple[str, int]:
    if max_rows < 1:
        raise ValueError("max_rows must be greater than zero.")

    sqlglot_dialect = normalize_sql_dialect(dialect)
    try:
        expression = sqlglot.parse_one(query, read=sqlglot_dialect)
    except sqlglot.ParseError:
        fallback = query.strip().rstrip(";")
        if sqlglot_dialect == "tsql":
            return f"SELECT TOP {max_rows} * FROM ({fallback}) AS langbridge_sql_preview", max_rows
        return f"SELECT * FROM ({fallback}) AS langbridge_sql_preview LIMIT {max_rows}", max_rows

    current_limit = _read_limit(expression)
    if current_limit is not None and current_limit <= max_rows:
        return expression.sql(dialect=sqlglot_dialect), current_limit

    rewritten = expression.copy()
    rewritten.set("limit", exp.Limit(expression=exp.Literal.number(max_rows)))
    return rewritten.sql(dialect=sqlglot_dialect), max_rows


def detect_sql_risk_hints(query: str) -> dict[str, Any]:
    warnings: list[str] = []
    dangerous: list[str] = []
    lowered = query.lower()

    for keyword in ("drop", "truncate", "delete", "update", "insert", "alter", "create", "merge"):
        if re.search(rf"\b{keyword}\b", lowered):
            dangerous.append(keyword.upper())

    try:
        expression = sqlglot.parse_one(query, read="tsql")
    except sqlglot.ParseError:
        if " join " in lowered:
            warnings.append("Query contains JOINs and may scan large datasets.")
        if " where " not in lowered:
            warnings.append("Query has no WHERE clause.")
        if " top " not in lowered and " limit " not in lowered:
            warnings.append("Query has no explicit row cap.")
        return {
            "is_expensive": bool(warnings),
            "warnings": warnings,
            "dangerous_statements": dangerous,
        }

    joins = sum(1 for _ in expression.find_all(exp.Join))
    has_where = expression.args.get("where") is not None
    has_limit = expression.args.get("limit") is not None
    select_all = any(isinstance(node, exp.Star) for node in expression.find_all(exp.Star))

    if joins >= 3:
        warnings.append("Query joins 3 or more tables.")
    if not has_where and expression.args.get("from") is not None:
        warnings.append("Query has no WHERE clause.")
    if not has_limit:
        warnings.append("Query has no explicit row cap.")
    if select_all:
        warnings.append("SELECT * can increase scan cost and payload size.")

    return {
        "is_expensive": bool(warnings),
        "warnings": warnings,
        "dangerous_statements": dangerous,
    }


def sanitize_sql_error_message(error: str, *, max_length: int = 600) -> str:
    normalized = " ".join(str(error).split())
    normalized = _ERROR_SECRET_PATTERN.sub(r"\1=***", normalized)
    if len(normalized) > max_length:
        return f"{normalized[:max_length].rstrip()}..."
    return normalized


def apply_result_redaction(
    *,
    rows: list[dict[str, Any]],
    redaction_rules: dict[str, str],
) -> tuple[list[dict[str, Any]], bool]:
    if not rows or not redaction_rules:
        return rows, False

    normalized_rules = {key.lower(): value.lower() for key, value in redaction_rules.items() if key}
    if not normalized_rules:
        return rows, False

    redacted_rows: list[dict[str, Any]] = []
    redaction_applied = False
    for row in rows:
        next_row = dict(row)
        for column, value in row.items():
            rule = normalized_rules.get(column.lower())
            if rule is None or value is None:
                continue
            redaction_applied = True
            if rule in {"omit", "drop", "remove"}:
                next_row.pop(column, None)
            elif rule == "null":
                next_row[column] = None
            elif rule == "hash":
                digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()
                next_row[column] = digest[:16]
            else:
                next_row[column] = "***"
        redacted_rows.append(next_row)
    return redacted_rows, redaction_applied


def _read_limit(expression: exp.Expression) -> int | None:
    raw_limit = expression.args.get("limit")
    if not isinstance(raw_limit, exp.Limit):
        return None
    literal = raw_limit.expression
    if isinstance(literal, exp.Literal) and literal.is_number:
        try:
            return int(literal.this)
        except (TypeError, ValueError):
            return None
    return None


def _to_sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return f"'{value.isoformat()}'"
    if isinstance(value, (list, tuple, set)):
        items = ", ".join(_to_sql_literal(item) for item in value)
        return f"({items})"
    if isinstance(value, dict):
        serialized = json.dumps(value, ensure_ascii=True).replace("'", "''")
        return "'" + serialized + "'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"
