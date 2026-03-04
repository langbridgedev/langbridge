from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import sqlglot
from sqlglot import exp

from langbridge.packages.federation.models.plans import JoinRef, LogicalPlan, QueryType, TableRef
from langbridge.packages.federation.models.virtual_dataset import VirtualDataset, VirtualTableBinding


class QueryParsingError(ValueError):
    pass


@dataclass(slots=True)
class ParsedSql:
    expression: exp.Expression
    select: exp.Select


def parse_sql(sql: str, *, dialect: str = "tsql") -> ParsedSql:
    try:
        expression = sqlglot.parse_one(sql, read=dialect)
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
    parsed = parse_sql(sql, dialect=dialect)
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
        sql=sql,
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


def rewrite_tables_to_stage_sql(
    expression: exp.Expression,
    *,
    stage_tables: dict[str, str],
) -> str:
    alias_lookup = _build_table_alias_lookup(expression)

    def _replace(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column):
            return _rewrite_column_for_stage(
                column=node,
                stage_tables=stage_tables,
                alias_lookup=alias_lookup,
            )
        if not isinstance(node, exp.Table):
            return node
        alias = node.alias_or_name
        stage_table = stage_tables.get(alias)
        if stage_table is None:
            return node
        return exp.table_(stage_table, alias=alias, quoted=False)

    transformed = expression.transform(_replace)
    return transformed.sql(dialect="duckdb")


def _build_table_alias_lookup(expression: exp.Expression) -> dict[str, str]:
    candidate_aliases: dict[str, set[str]] = {}

    def _add(key: str | None, alias: str | None) -> None:
        if not key or not alias:
            return
        normalized_key = key.strip().lower()
        normalized_alias = alias.strip()
        if not normalized_key or not normalized_alias:
            return
        candidate_aliases.setdefault(normalized_key, set()).add(normalized_alias)

    for table in expression.find_all(exp.Table):
        alias = str(table.alias_or_name or "").strip()
        if not alias:
            continue

        table_name = str(table.name or "").strip()
        schema_name = str(table.db or "").strip()
        catalog_name = str(table.catalog or "").strip()

        _add(alias, alias)
        _add(table_name, alias)
        if schema_name and table_name:
            _add(f"{schema_name}.{table_name}", alias)
        if catalog_name and schema_name and table_name:
            _add(f"{catalog_name}.{schema_name}.{table_name}", alias)

    return {
        key: next(iter(aliases))
        for key, aliases in candidate_aliases.items()
        if len(aliases) == 1
    }


def _rewrite_column_for_stage(
    *,
    column: exp.Column,
    stage_tables: dict[str, str],
    alias_lookup: dict[str, str],
) -> exp.Column:
    table_name = str(column.table or "").strip()
    schema_name = str(column.db or "").strip()
    catalog_name = str(column.catalog or "").strip()

    resolved_alias: str | None = None
    candidates: list[str] = []
    if table_name:
        candidates.append(table_name)
    if schema_name and table_name:
        candidates.append(f"{schema_name}.{table_name}")
    if catalog_name and schema_name and table_name:
        candidates.append(f"{catalog_name}.{schema_name}.{table_name}")

    for candidate in candidates:
        normalized_candidate = candidate.strip().lower()
        alias = alias_lookup.get(normalized_candidate)
        if alias:
            resolved_alias = alias
            break

    if resolved_alias is None and table_name and table_name in stage_tables:
        resolved_alias = table_name

    rewritten = column.copy()
    if resolved_alias:
        rewritten.set("table", exp.Identifier(this=resolved_alias, quoted=False))
    rewritten.set("db", None)
    rewritten.set("catalog", None)
    return rewritten


def _extract_select(expression: exp.Expression) -> exp.Select | None:
    if isinstance(expression, exp.Select):
        return expression
    if isinstance(expression, exp.Subqueryable):
        return expression if isinstance(expression, exp.Select) else None
    if isinstance(expression, exp.Union):
        return None
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

    direct = virtual_dataset.tables.get(table_name)
    if direct is not None:
        return alias, direct

    candidates = []
    for table_key, binding in virtual_dataset.tables.items():
        if table_key == table_name:
            candidates.append(binding)
            continue
        if binding.table != table_name:
            continue
        if schema_name and (binding.schema or "") != schema_name:
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
        connector_id=str(binding.connector_id),
        schema=binding.schema,
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
