
from dataclasses import dataclass
from typing import Iterable

import sqlglot
from sqlglot import exp
import re

from langbridge.federation.models.plans import JoinRef, LogicalPlan, QueryType, TableRef
from langbridge.federation.models.virtual_dataset import VirtualDataset, VirtualTableBinding


class QueryParsingError(ValueError):
    pass


@dataclass(slots=True)
class ParsedSql:
    expression: exp.Expression
    select: exp.Select


def parse_sql(sql: str, *, dialect: str = "tsql") -> ParsedSql:
    normalized_sql = _normalize_portable_sql(sql)
    try:
        expression = sqlglot.parse_one(normalized_sql, read=dialect)
    except sqlglot.ParseError as exc:
        raise QueryParsingError(str(exc)) from exc

    select = _extract_select(expression)
    if select is None:
        raise QueryParsingError("Only SELECT/CTE queries are supported in federation v1.")

    return ParsedSql(expression=expression, select=select)


def logical_plan_from_sql(
    *,
    sql: str,
    virtual_dataset: VirtualDataset,
    dialect: str = "tsql",
    query_type: QueryType = QueryType.SQL,
) -> tuple[LogicalPlan, exp.Expression]:
    normalized_sql = _normalize_portable_sql(sql)
    parsed = parse_sql(normalized_sql, dialect=dialect)
    select_expr = parsed.select

    cte_names = _extract_cte_names(select_expr)
    has_cte = bool(cte_names)

    table_map: dict[str, TableRef]
    base_alias: str
    joins: list[JoinRef]
    if has_cte:
        table_map = _resolve_physical_tables(
            expression=parsed.expression,
            virtual_dataset=virtual_dataset,
            cte_names=cte_names,
        )
        if not table_map:
            raise QueryParsingError(
                f"Query does not reference any mapped physical tables in virtual dataset '{virtual_dataset.id}'."
            )
        base_alias = next(iter(table_map.keys()))
        joins = []
    else:
        table_map = {}
        base_table = select_expr.args.get("from")
        if base_table is None or base_table.this is None:
            raise QueryParsingError("Query must include a FROM clause.")
        base_alias, base_binding = _resolve_table(base_table.this, virtual_dataset)
        table_map[base_alias] = _table_ref(alias=base_alias, binding=base_binding)

        joins = []
        for join in select_expr.args.get("joins") or []:
            if not isinstance(join, exp.Join):
                continue
            alias, binding = _resolve_table(join.this, virtual_dataset)
            table_map[alias] = _table_ref(alias=alias, binding=binding)
            join_kind = _join_kind(join)
            on_expr = join.args.get("on")
            joins.append(
                JoinRef(
                    left_alias=joins[-1].right_alias if joins else base_alias,
                    right_alias=alias,
                    join_type=join_kind,
                    on_sql=on_expr.sql(dialect=dialect) if on_expr is not None else "1=1",
                )
            )

    where_expr = select_expr.args.get("where")
    having_expr = select_expr.args.get("having")
    group_expr = select_expr.args.get("group")
    order_expr = select_expr.args.get("order")

    logical_plan = LogicalPlan(
        query_type=query_type,
        sql=normalized_sql,
        from_alias=base_alias,
        tables=table_map,
        joins=joins,
        where_sql=where_expr.this.sql(dialect=dialect) if isinstance(where_expr, exp.Where) else None,
        having_sql=having_expr.this.sql(dialect=dialect) if isinstance(having_expr, exp.Having) else None,
        group_by_sql=[item.sql(dialect=dialect) for item in (group_expr.expressions if isinstance(group_expr, exp.Group) else [])],
        order_by_sql=[item.sql(dialect=dialect) for item in (order_expr.expressions if isinstance(order_expr, exp.Order) else [])],
        limit=_extract_int(select_expr.args.get("limit")),
        offset=_extract_int(select_expr.args.get("offset")),
        has_cte=has_cte,
    )
    return logical_plan, parsed.expression


_INTERVAL_LITERAL_WITH_UNIT_RE = re.compile(
    r"(?i)\bINTERVAL\s+'([^']+)'\s+"
    r"(DAY|DAYS|WEEK|WEEKS|MONTH|MONTHS|QUARTER|QUARTERS|YEAR|YEARS|HOUR|HOURS|MINUTE|MINUTES|SECOND|SECONDS)\b"
)
_TRUNC_FUNCTION_RE = re.compile(
    r"(?i)\b(?:TIMESTAMP_TRUNC|DATE_TRUNC)\s*\(\s*"
    r"(?P<expr>(?:CURRENT_DATE|CURRENT_TIMESTAMP|NOW\(\)|[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*))"
    r"\s*,\s*(?P<unit>[A-Za-z_]+)\s*\)"
)


def _normalize_portable_sql(sql: str) -> str:
    normalized = _INTERVAL_LITERAL_WITH_UNIT_RE.sub(
        lambda match: f"INTERVAL '{match.group(1)} {match.group(2)}'",
        sql,
    )

    def _replace_trunc(match: re.Match[str]) -> str:
        expr_sql = str(match.group("expr") or "").strip()
        unit_sql = str(match.group("unit") or "").strip().lower()
        if not expr_sql or not unit_sql:
            return match.group(0)
        return f"DATE_TRUNC('{unit_sql}', {expr_sql})"

    return _TRUNC_FUNCTION_RE.sub(_replace_trunc, normalized)


def extract_required_columns(
    expression: exp.Expression,
    table_aliases: Iterable[str],
) -> tuple[dict[str, set[str]], bool]:
    aliases = set(table_aliases)
    required: dict[str, set[str]] = {alias: set() for alias in aliases}
    has_unqualified = False

    for star in expression.find_all(exp.Star):
        _ = star
        for alias in aliases:
            required[alias].add("*")

    for column in expression.find_all(exp.Column):
        column_name = column.name
        table_name = column.table
        if not column_name:
            continue
        if table_name and table_name in aliases:
            required[table_name].add(column_name)
        elif table_name:
            continue
        else:
            has_unqualified = True

    return required, has_unqualified


def split_conjunctive_predicates(where_clause: exp.Expression | None) -> list[exp.Expression]:
    if where_clause is None:
        return []

    predicates: list[exp.Expression] = []

    def _walk(node: exp.Expression) -> None:
        if isinstance(node, exp.And):
            _walk(node.left)
            _walk(node.right)
            return
        predicates.append(node)

    _walk(where_clause)
    return predicates


def predicate_aliases(predicate: exp.Expression, table_aliases: Iterable[str]) -> set[str]:
    aliases = set(table_aliases)
    referenced: set[str] = set()
    for column in predicate.find_all(exp.Column):
        if column.table in aliases:
            referenced.add(column.table)
    return referenced


def _extract_select(expression: exp.Expression) -> exp.Select | None:
    if isinstance(expression, exp.Select):
        return expression
    if isinstance(expression, exp.Subquery):
        return expression if isinstance(expression, exp.Select) else None
    if isinstance(expression, exp.Union):
        return expression.left if isinstance(expression.left, exp.Select) else None
    if isinstance(expression, exp.With):
        body = expression.this
        return body if isinstance(body, exp.Select) else None
    return expression.find(exp.Select)


def _extract_cte_names(select_expr: exp.Select) -> set[str]:
    names: set[str] = set()
    with_clause = select_expr.args.get("with")
    if not isinstance(with_clause, exp.With):
        return names

    for entry in with_clause.expressions or []:
        alias = str(getattr(entry, "alias_or_name", "") or "").strip()
        if alias:
            names.add(alias.lower())
    return names


def _resolve_physical_tables(
    *,
    expression: exp.Expression,
    virtual_dataset: VirtualDataset,
    cte_names: set[str],
) -> dict[str, TableRef]:
    table_map: dict[str, TableRef] = {}
    for table_expression in expression.find_all(exp.Table):
        table_name = str(table_expression.name or "").strip()
        schema_name = str(table_expression.db or "").strip()
        catalog_name = str(table_expression.catalog or "").strip()
        if (
            table_name
            and table_name.lower() in cte_names
            and not schema_name
            and not catalog_name
        ):
            continue

        alias, binding = _resolve_table(table_expression, virtual_dataset)
        candidate = _table_ref(alias=alias, binding=binding)
        existing = table_map.get(alias)
        if existing is None:
            table_map[alias] = candidate
            continue
        if existing.table_key != candidate.table_key:
            raise QueryParsingError(
                f"Alias '{alias}' resolves to multiple physical tables in virtual dataset '{virtual_dataset.id}'. "
                "Use explicit table aliases to disambiguate."
            )

    return table_map


def _resolve_table(
    table_expression: exp.Expression,
    virtual_dataset: VirtualDataset,
) -> tuple[str, VirtualTableBinding]:
    if not isinstance(table_expression, exp.Table):
        raise QueryParsingError("Only table references are supported in FROM/JOIN clauses for v1.")

    alias = table_expression.alias_or_name
    table_name = table_expression.name
    schema_name = table_expression.db
    catalog_name = table_expression.catalog

    direct = virtual_dataset.virtual_tables.get(table_name.lower())
    if direct is not None:
        return alias, direct
    direct = virtual_dataset.tables.get(f"{schema_name}.{table_name}" if schema_name else table_name)
    if direct is not None:
        return alias, direct
    candidates = []
    for table_key, binding in virtual_dataset.tables.items():
        if table_key == table_name:
            candidates.append(binding)
            continue
        if binding.table != table_name:
            continue
        if schema_name and (binding.schema_name or "") != schema_name:
            continue
        if catalog_name and (binding.catalog or "") != catalog_name:
            continue
        candidates.append(binding)

    if len(candidates) == 1:
        return alias, candidates[0]
    if not candidates:
        raise QueryParsingError(
            f"Table '{table_expression.sql()}' is not mapped in virtual dataset '{virtual_dataset.id}'."
        )
    raise QueryParsingError(f"Table '{table_expression.sql()}' has ambiguous source mappings.")


def _table_ref(*, alias: str, binding: VirtualTableBinding) -> TableRef:
    return TableRef(
        alias=alias,
        table_key=binding.table_key,
        source_id=binding.source_id,
        connector_id=(str(binding.connector_id) if binding.connector_id is not None else None),
        schema=binding.schema_name,
        table=binding.table,
        catalog=binding.catalog,
    )


def _join_kind(join: exp.Join) -> str:
    kind = join.args.get("kind")
    if isinstance(kind, exp.Expression):
        value = kind.sql().lower()
    elif kind is not None:
        value = str(kind).lower()
    else:
        value = "inner"
    return value or "inner"


def _extract_int(limit_or_offset: exp.Expression | None) -> int | None:
    if limit_or_offset is None:
        return None

    node = limit_or_offset
    if isinstance(node, (exp.Limit, exp.Offset)):
        node = node.expression

    if isinstance(node, exp.Literal) and node.is_int:
        return int(node.this)

    try:
        return int(node.sql())
    except Exception:
        return None
