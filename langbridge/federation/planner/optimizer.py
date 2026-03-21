from __future__ import annotations

from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from langbridge.federation.utils import enforce_preview_limit
from langbridge.federation.connectors import estimate_bytes
from langbridge.federation.models.plans import JoinStrategy, LogicalPlan, SourceSubplan
from langbridge.federation.models.virtual_dataset import TableStatistics, VirtualDataset
from langbridge.federation.planner.parser import (
    extract_required_columns,
    predicate_aliases,
    rewrite_tables_to_stage_sql,
    split_conjunctive_predicates,
)


@dataclass(slots=True)
class OptimizedPlan:
    logical_plan: LogicalPlan
    source_subplans: list[SourceSubplan]
    local_stage_sql: str
    join_order: list[str]
    join_strategies: dict[str, JoinStrategy]
    pushdown_full_query: bool


class FederatedOptimizer:
    def __init__(self, *, broadcast_threshold_bytes: int) -> None:
        self._broadcast_threshold_bytes = broadcast_threshold_bytes

    def optimize(
        self,
        *,
        logical_plan: LogicalPlan,
        expression: exp.Expression,
        virtual_dataset: VirtualDataset,
        stats_by_table: dict[str, TableStatistics],
        source_dialects: dict[str, str],
        input_dialect: str,
    ) -> OptimizedPlan:
        aliases = list(logical_plan.tables.keys())
        required_columns, has_unqualified = extract_required_columns(expression, aliases)

        where_node = logical_plan.where_sql
        where_expr = None
        if where_node:
            where_expr = sqlglot.parse_one(where_node, read=input_dialect)

        predicate_map: dict[str, list[exp.Expression]] = {alias: [] for alias in aliases}
        if where_expr is not None:
            for predicate in split_conjunctive_predicates(where_expr):
                refs = predicate_aliases(predicate, aliases)
                if len(refs) == 1:
                    alias = next(iter(refs))
                    predicate_map[alias].append(predicate)

        distinct_sources = {table.source_id for table in logical_plan.tables.values()}
        requires_physical_name_rewrite = any(
            _binding_requires_scan_level_rewrite(virtual_dataset.tables[table.table_key])
            for table in logical_plan.tables.values()
        )
        requires_logical_name_rewrite = any(
            _binding_requires_logical_name_rewrite(virtual_dataset.tables[table.table_key])
            for table in logical_plan.tables.values()
        )
        requires_cross_dialect_rewrite = any(
            _normalize_dialect(source_dialects.get(table.source_id)) != _normalize_dialect(input_dialect)
            for table in logical_plan.tables.values()
        )
        pushdown_full_query = (
            len(distinct_sources) == 1
            and not logical_plan.has_cte
            and not requires_physical_name_rewrite
            and not requires_logical_name_rewrite
            and not requires_cross_dialect_rewrite
        )

        source_subplans: list[SourceSubplan] = []
        if pushdown_full_query:
            source_id = next(iter(distinct_sources))
            target_dialect = source_dialects.get(source_id, input_dialect)
            full_sql = _transpile(logical_plan.sql, read=input_dialect, write=target_dialect)
            source_subplans.append(
                SourceSubplan(
                    stage_id="scan_full_query",
                    source_id=source_id,
                    alias=logical_plan.from_alias,
                    table_key=logical_plan.tables[logical_plan.from_alias].table_key,
                    sql=full_sql,
                )
            )
            local_stage_sql = "SELECT * FROM scan_full_query"
            join_order = [logical_plan.from_alias]
            join_strategies: dict[str, JoinStrategy] = {}
            return OptimizedPlan(
                logical_plan=logical_plan,
                source_subplans=source_subplans,
                local_stage_sql=local_stage_sql,
                join_order=join_order,
                join_strategies=join_strategies,
                pushdown_full_query=True,
            )

        for alias, table_ref in logical_plan.tables.items():
            binding = virtual_dataset.tables[table_ref.table_key]
            projected_columns = sorted(required_columns.get(alias) or [])
            if has_unqualified and "*" not in projected_columns:
                projected_columns = ["*"]

            pushed_filters = predicate_map.get(alias) or []
            source_id = table_ref.source_id
            target_dialect = source_dialects.get(source_id, input_dialect)
            pushable_filters = [
                predicate
                for predicate in pushed_filters
                if _can_push_filter(
                    predicate=predicate,
                    input_dialect=input_dialect,
                    target_dialect=target_dialect,
                )
            ]
            sql = _build_scan_sql(
                alias=alias,
                binding=binding,
                projected_columns=projected_columns,
                pushed_filters=pushable_filters,
                pushed_limit=None,
                dialect=target_dialect,
            )
            stats = stats_by_table.get(table_ref.table_key) or binding.stats or TableStatistics()
            estimated_rows = stats.row_count_estimate
            estimated_bytes = estimate_bytes(rows=estimated_rows, bytes_per_row=stats.bytes_per_row)
            source_subplans.append(
                SourceSubplan(
                    stage_id=f"scan_{alias}",
                    source_id=source_id,
                    alias=alias,
                    table_key=table_ref.table_key,
                    sql=sql,
                    projected_columns=projected_columns,
                    pushed_filters=[
                        expr.sql(dialect=target_dialect)
                        for expr in pushable_filters
                    ],
                    pushed_limit=None,
                    estimated_rows=estimated_rows,
                    estimated_bytes=estimated_bytes,
                )
            )

        join_order = _choose_join_order(logical_plan=logical_plan, stats_by_table=stats_by_table)
        join_strategies = _choose_join_strategies(
            logical_plan=logical_plan,
            stats_by_table=stats_by_table,
            broadcast_threshold_bytes=self._broadcast_threshold_bytes,
        )

        stage_table_map = {alias: f"scan_{alias}" for alias in logical_plan.tables}
        local_stage_sql = rewrite_tables_to_stage_sql(expression, stage_tables=stage_table_map)

        return OptimizedPlan(
            logical_plan=logical_plan,
            source_subplans=source_subplans,
            local_stage_sql=local_stage_sql,
            join_order=join_order,
            join_strategies=join_strategies,
            pushdown_full_query=False,
        )


def _build_scan_sql(
    *,
    alias: str,
    binding,
    projected_columns: list[str],
    pushed_filters: list[exp.Expression],
    pushed_limit: int | None,
    dialect: str,
) -> str:
    metadata = binding.metadata if isinstance(getattr(binding, "metadata", None), dict) else {}
    physical_sql = metadata.get("physical_sql")
    normalized_filters = [
        _rewrite_filter_for_scan(
            expression=expression,
            alias=alias,
            binding=binding,
        )
        for expression in pushed_filters
    ]

    if isinstance(physical_sql, str) and physical_sql.strip():
        sql_text = physical_sql.strip().rstrip(";")
        alias_identifier = exp.Identifier(this=alias, quoted=False).sql(dialect=dialect)
        if not projected_columns or "*" in projected_columns:
            select_clause = "*"
        else:
            projected = []
            for column in projected_columns:
                column_identifier = exp.Identifier(this=column, quoted=False).sql(dialect=dialect)
                projected.append(f"{alias_identifier}.{column_identifier}")
            select_clause = ", ".join(projected)
        query_sql = f"SELECT {select_clause} FROM ({sql_text}) AS {alias_identifier}"
        if normalized_filters:
            where_sql = " AND ".join(expression.sql(dialect=dialect) for expression in normalized_filters)
            query_sql = f"{query_sql} WHERE {where_sql}"
        if pushed_limit is not None:
            query_sql, _ = enforce_preview_limit(query_sql, max_rows=pushed_limit, dialect=dialect)
        return query_sql

    physical_catalog = metadata.get("physical_catalog", binding.catalog)
    physical_schema = metadata.get("physical_schema", binding.schema_name)
    physical_table = metadata.get("physical_table", binding.table)
    if bool(metadata.get("skip_catalog_in_pushdown")):
        physical_catalog = None

    table_ref = exp.table_(
        physical_table,
        db=physical_schema or None,
        catalog=physical_catalog or None,
        alias=alias,
        quoted=False,
    )

    if not projected_columns or "*" in projected_columns:
        select_expr = exp.select(exp.Star()).from_(table_ref)
    else:
        columns = [
            exp.Column(
                this=exp.Identifier(this=column, quoted=False),
                table=exp.Identifier(this=alias, quoted=False),
            )
            for column in projected_columns
        ]
        select_expr = exp.select(*columns).from_(table_ref)

    if normalized_filters:
        select_expr = select_expr.where(exp.and_(*normalized_filters))

    if pushed_limit is not None:
        select_expr = select_expr.limit(pushed_limit)

    return select_expr.sql(dialect=dialect)


_CROSS_DIALECT_SIMPLE_PREDICATE_NODES = (
    exp.Column,
    exp.Identifier,
    exp.Literal,
    exp.Null,
    exp.Boolean,
    exp.Paren,
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.Is,
    exp.Not,
    exp.In,
    exp.Neg,
    exp.Or,
)


def _can_push_filter(
    *,
    predicate: exp.Expression,
    input_dialect: str,
    target_dialect: str,
) -> bool:
    if _normalize_dialect(target_dialect) == _normalize_dialect(input_dialect):
        return True
    return _is_cross_dialect_simple_predicate(predicate)


def _is_cross_dialect_simple_predicate(predicate: exp.Expression) -> bool:
    for node in predicate.walk():
        if isinstance(node, _CROSS_DIALECT_SIMPLE_PREDICATE_NODES):
            continue
        return False
    return True


def _rewrite_filter_for_scan(
    *,
    expression: exp.Expression,
    alias: str,
    binding,
) -> exp.Expression:
    metadata = binding.metadata if isinstance(getattr(binding, "metadata", None), dict) else {}
    physical_catalog = metadata.get("physical_catalog", binding.catalog)
    physical_schema = metadata.get("physical_schema", binding.schema_name)
    physical_table = metadata.get("physical_table", binding.table)

    valid_tables = {
        str(alias).strip().lower(),
        str(binding.table_key).strip().lower(),
        str(binding.table).strip().lower(),
        str(physical_table).strip().lower(),
    }
    valid_schemas = {
        str(schema).strip().lower()
        for schema in (binding.schema_name, physical_schema)
        if str(schema).strip()
    }
    valid_catalogs = {
        str(catalog).strip().lower()
        for catalog in (binding.catalog, physical_catalog)
        if str(catalog).strip()
    }

    def _replace(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Column):
            return node

        table_name = str(node.table or "").strip().lower()
        schema_name = str(node.db or "").strip().lower() or None
        catalog_name = str(node.catalog or "").strip().lower() or None
        should_rebind = False
        if table_name:
            should_rebind = table_name in valid_tables
            if should_rebind and schema_name is not None and valid_schemas and schema_name not in valid_schemas:
                should_rebind = False
            if should_rebind and catalog_name is not None and valid_catalogs and catalog_name not in valid_catalogs:
                should_rebind = False

        rewritten = node.copy()
        if should_rebind:
            rewritten.set("table", exp.Identifier(this=alias, quoted=False))
        rewritten.set("db", None)
        rewritten.set("catalog", None)
        return rewritten

    return expression.transform(_replace)


def _binding_requires_scan_level_rewrite(binding) -> bool:
    metadata = binding.metadata if isinstance(getattr(binding, "metadata", None), dict) else {}
    if bool(metadata.get("physical_sql")):
        return True
    if bool(metadata.get("skip_catalog_in_pushdown")):
        return True

    physical_catalog = metadata.get("physical_catalog")
    physical_schema = metadata.get("physical_schema")
    physical_table = metadata.get("physical_table")
    if physical_catalog is not None and physical_catalog != binding.catalog:
        return True
    if physical_schema is not None and physical_schema != binding.schema_name:
        return True
    if physical_table is not None and physical_table != binding.table:
        return True
    return False


def _binding_requires_logical_name_rewrite(binding) -> bool:
    metadata = binding.metadata if isinstance(getattr(binding, "metadata", None), dict) else {}

    logical_names = {
        str(value).strip().lower()
        for value in (
            binding.table_key,
            metadata.get("dataset_alias"),
        )
        if str(value).strip()
    }
    physical_names = {
        str(value).strip().lower()
        for value in (
            binding.table,
            metadata.get("physical_table"),
        )
        if str(value).strip()
    }
    if not logical_names or not physical_names:
        return False
    return not logical_names.issubset(physical_names)


def _choose_join_order(
    *,
    logical_plan: LogicalPlan,
    stats_by_table: dict[str, TableStatistics],
) -> list[str]:
    alias_rows: dict[str, float] = {}
    for alias, table_ref in logical_plan.tables.items():
        stats = stats_by_table.get(table_ref.table_key)
        alias_rows[alias] = stats.row_count_estimate if (stats and stats.row_count_estimate is not None) else 1_000_000.0

    sorted_aliases = sorted(alias_rows.items(), key=lambda item: item[1])
    if not sorted_aliases:
        return []

    ordered = [sorted_aliases[0][0]]
    remaining = {alias for alias in logical_plan.tables if alias != ordered[0]}

    while remaining:
        next_alias = None
        next_rows = None
        for join in logical_plan.joins:
            left_in = join.left_alias in ordered
            right_in = join.right_alias in ordered
            candidate = None
            if left_in and join.right_alias in remaining:
                candidate = join.right_alias
            elif right_in and join.left_alias in remaining:
                candidate = join.left_alias
            if candidate is None:
                continue
            rows = alias_rows.get(candidate, 1_000_000.0)
            if next_rows is None or rows < next_rows:
                next_rows = rows
                next_alias = candidate
        if next_alias is None:
            next_alias = min(remaining, key=lambda alias: alias_rows.get(alias, 1_000_000.0))
        ordered.append(next_alias)
        remaining.remove(next_alias)

    return ordered


def _choose_join_strategies(
    *,
    logical_plan: LogicalPlan,
    stats_by_table: dict[str, TableStatistics],
    broadcast_threshold_bytes: int,
) -> dict[str, JoinStrategy]:
    strategies: dict[str, JoinStrategy] = {}

    def _table_bytes(alias: str) -> float:
        table_ref = logical_plan.tables[alias]
        stats = stats_by_table.get(table_ref.table_key) or TableStatistics()
        rows = stats.row_count_estimate if stats.row_count_estimate is not None else 1_000_000.0
        return rows * stats.bytes_per_row

    for join in logical_plan.joins:
        left_bytes = _table_bytes(join.left_alias)
        right_bytes = _table_bytes(join.right_alias)
        strategy = (
            JoinStrategy.BROADCAST
            if min(left_bytes, right_bytes) <= float(broadcast_threshold_bytes)
            else JoinStrategy.PARTITIONED_HASH
        )
        strategies[f"{join.left_alias}->{join.right_alias}"] = strategy

    return strategies


def _transpile(sql: str, *, read: str, write: str) -> str:
    try:
        return sqlglot.transpile(sql, read=read, write=write)[0]
    except Exception:
        return sql


def _normalize_dialect(value: str | None) -> str:
    return str(value or "").strip().lower() or "tsql"
