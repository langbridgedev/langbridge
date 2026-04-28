
from dataclasses import dataclass
import re

import sqlglot
from sqlglot import exp

from langbridge.federation.utils import enforce_preview_limit
from langbridge.federation.connectors import SourceCapabilities, estimate_bytes
from langbridge.federation.models.plans import (
    JoinStrategy,
    LogicalPlan,
    PushdownDecision,
    PushdownDiagnostics,
    SourceSubplan,
)
from langbridge.federation.models.virtual_dataset import TableStatistics, VirtualDataset
from langbridge.federation.planner.parser import (
    extract_required_columns,
    predicate_aliases,
    split_conjunctive_predicates,
)
from langbridge.federation.planner.sql_renderer import render_local_stage_sql


@dataclass(slots=True)
class OptimizedPlan:
    logical_plan: LogicalPlan
    source_subplans: list[SourceSubplan]
    local_stage_sql: str
    local_stage_dialect: str
    join_order: list[str]
    join_strategies: dict[str, JoinStrategy]
    pushdown_full_query: bool
    pushdown_reasons: list[str]


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
        source_capabilities: dict[str, SourceCapabilities],
        input_dialect: str,
        local_dialect: str,
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
        requires_cross_dialect_transpile = any(
            _normalize_dialect(source_dialects.get(table.source_id, input_dialect))
            != _normalize_dialect(input_dialect)
            for table in logical_plan.tables.values()
        )
        single_source_id = next(iter(distinct_sources)) if len(distinct_sources) == 1 else None
        single_source_target_dialect = (
            source_dialects.get(single_source_id, input_dialect)
            if single_source_id is not None
            else input_dialect
        )
        single_source_capabilities = (
            source_capabilities.get(single_source_id)
            if single_source_id is not None
            else None
        )
        can_remote_rewrite_full_query = (
            single_source_id is not None
            and all(
                _binding_can_support_full_query_rewrite(virtual_dataset.tables[table.table_key])
                for table in logical_plan.tables.values()
            )
        )
        has_join = bool(logical_plan.joins)
        has_filter = where_expr is not None
        has_limit = logical_plan.limit is not None
        has_aggregation = bool(logical_plan.group_by_sql or logical_plan.having_sql) or expression.find(exp.AggFunc) is not None
        cross_dialect_block_reason = _cross_dialect_full_query_block_reason(
            expression=expression,
            input_dialect=input_dialect,
            target_dialect=single_source_target_dialect,
        )
        pushdown_reasons = _full_query_pushdown_reasons(
            distinct_source_count=len(distinct_sources),
            has_cte=logical_plan.has_cte,
            requires_physical_name_rewrite=(
                requires_physical_name_rewrite and not can_remote_rewrite_full_query
            ),
            requires_logical_name_rewrite=(
                requires_logical_name_rewrite and not can_remote_rewrite_full_query
            ),
            cross_dialect_block_reason=cross_dialect_block_reason,
            has_join=has_join,
            has_filter=has_filter,
            has_aggregation=has_aggregation,
            has_limit=has_limit,
            source_capabilities=single_source_capabilities,
        )
        pushdown_full_query = not pushdown_reasons

        source_subplans: list[SourceSubplan] = []
        if pushdown_full_query:
            source_id = next(iter(distinct_sources))
            target_dialect = source_dialects.get(source_id, input_dialect)
            rewritten_expression = (
                _rewrite_expression_for_full_query_pushdown(
                    expression=expression,
                    logical_plan=logical_plan,
                    virtual_dataset=virtual_dataset,
                )
                if can_remote_rewrite_full_query and (
                    requires_physical_name_rewrite or requires_logical_name_rewrite
                )
                else expression
            )
            full_sql = _transpile(
                rewritten_expression.sql(dialect=input_dialect),
                read=input_dialect,
                write=target_dialect,
            )
            query_projection = ["*"] if has_unqualified else sorted(
                {
                    f"{alias}.{column}"
                    for alias, columns in required_columns.items()
                    for column in columns
                }
            )
            source_subplans.append(
                SourceSubplan(
                    stage_id="scan_full_query",
                    source_id=source_id,
                    alias=logical_plan.from_alias,
                    table_key=logical_plan.tables[logical_plan.from_alias].table_key,
                    sql=full_sql,
                    pushdown=PushdownDiagnostics(
                        full_query=PushdownDecision(
                            pushed=True,
                            supported=True,
                            reason=(
                                "Single-source query can execute remotely after mapping logical tables to physical relations."
                                if can_remote_rewrite_full_query and (
                                    requires_physical_name_rewrite or requires_logical_name_rewrite
                                )
                                else (
                                    "Single-source query can execute remotely after transpiling to the source dialect."
                                    if requires_cross_dialect_transpile
                                    else "Single-source query can execute remotely without local rewrite."
                                )
                            ),
                        ),
                        filter=PushdownDecision(
                            pushed=has_filter,
                            supported=True if has_filter else None,
                            details=[logical_plan.where_sql] if logical_plan.where_sql else [],
                        ),
                        projection=PushdownDecision(
                            pushed=bool(query_projection),
                            supported=True,
                            details=query_projection,
                            reason=(
                                "Wildcard selection requires the full row shape."
                                if "*" in query_projection
                                else None
                            ),
                        ),
                        aggregation=PushdownDecision(
                            pushed=has_aggregation,
                            supported=True if has_aggregation else None,
                            details=list(logical_plan.group_by_sql or []),
                        ),
                        limit=PushdownDecision(
                            pushed=has_limit,
                            supported=True if has_limit else None,
                            details=[str(logical_plan.limit)] if logical_plan.limit is not None else [],
                        ),
                        join=PushdownDecision(
                            pushed=has_join,
                            supported=True if has_join else None,
                            details=[
                                f"{join.left_alias} {join.join_type} {join.right_alias}"
                                for join in logical_plan.joins
                            ],
                        ),
                    ),
                )
            )
            local_stage_sql = "SELECT * FROM scan_full_query"
            join_order = [logical_plan.from_alias]
            join_strategies: dict[str, JoinStrategy] = {}
            return OptimizedPlan(
                logical_plan=logical_plan,
                source_subplans=source_subplans,
                local_stage_sql=local_stage_sql,
                local_stage_dialect=local_dialect,
                join_order=join_order,
                join_strategies=join_strategies,
                pushdown_full_query=True,
                pushdown_reasons=[],
            )

        for alias, table_ref in logical_plan.tables.items():
            binding = virtual_dataset.tables[table_ref.table_key]
            projected_columns = sorted(required_columns.get(alias) or [])
            if has_unqualified and "*" not in projected_columns:
                projected_columns = ["*"]

            alias_filters = predicate_map.get(alias) or []
            source_id = table_ref.source_id
            target_dialect = source_dialects.get(source_id, input_dialect)
            capabilities = source_capabilities.get(source_id, SourceCapabilities())
            pushable_filters = (
                [
                    predicate
                    for predicate in alias_filters
                    if _can_push_filter(
                        predicate=predicate,
                        input_dialect=input_dialect,
                        target_dialect=target_dialect,
                    )
                ]
                if capabilities.pushdown_filter
                else []
            )
            rejected_filters = [
                predicate
                for predicate in alias_filters
                if predicate not in pushable_filters
            ]
            pushed_limit = (
                logical_plan.limit
                if _can_push_limit_to_scan(
                    logical_plan=logical_plan,
                    expression=expression,
                    source_capabilities=capabilities,
                    rejected_filters=rejected_filters,
                )
                else None
            )
            pushed_projection = capabilities.pushdown_projection and not (
                not projected_columns or "*" in projected_columns
            )
            sql = _build_scan_sql(
                alias=alias,
                binding=binding,
                projected_columns=projected_columns if capabilities.pushdown_projection else ["*"],
                pushed_filters=pushable_filters,
                pushed_limit=pushed_limit,
                dialect=target_dialect,
            )
            stats = stats_by_table.get(table_ref.table_key) or binding.stats or TableStatistics()
            estimated_rows = stats.row_count_estimate
            estimated_bytes = estimate_bytes(rows=estimated_rows, bytes_per_row=stats.bytes_per_row)
            pushed_filter_sql = [
                predicate.sql(dialect=target_dialect)
                for predicate in pushable_filters
            ]
            source_subplans.append(
                SourceSubplan(
                    stage_id=f"scan_{alias}",
                    source_id=source_id,
                    alias=alias,
                    table_key=table_ref.table_key,
                    sql=sql,
                    projected_columns=(projected_columns if capabilities.pushdown_projection else []),
                    pushed_filters=pushed_filter_sql,
                    pushed_limit=pushed_limit,
                    estimated_rows=estimated_rows,
                    estimated_bytes=estimated_bytes,
                    pushdown=PushdownDiagnostics(
                        full_query=PushdownDecision(
                            pushed=False,
                            supported=True,
                            reason=_first_reason(
                                pushdown_reasons,
                                fallback="Local compute is required for this stage.",
                            ),
                        ),
                        filter=PushdownDecision(
                            pushed=bool(pushed_filter_sql),
                            supported=capabilities.pushdown_filter,
                            reason=_filter_pushdown_reason(
                                has_alias_filters=bool(alias_filters),
                                supports_filter=capabilities.pushdown_filter,
                                rejected_filters=rejected_filters,
                            ),
                            details=pushed_filter_sql,
                        ),
                        projection=PushdownDecision(
                            pushed=pushed_projection,
                            supported=capabilities.pushdown_projection,
                            reason=_projection_pushdown_reason(
                                projected_columns=projected_columns,
                                supports_projection=capabilities.pushdown_projection,
                            ),
                            details=projected_columns if pushed_projection else [],
                        ),
                        aggregation=PushdownDecision(
                            pushed=False,
                            supported=capabilities.pushdown_aggregation if has_aggregation else None,
                            reason=(
                                _stage_pushdown_reason(
                                    supports_feature=capabilities.pushdown_aggregation,
                                    full_query_reasons=pushdown_reasons,
                                    unsupported_reason=(
                                        "Unsupported connector/source capability: aggregation pushdown is unavailable."
                                    ),
                                )
                                if has_aggregation
                                else None
                            ),
                        ),
                        limit=PushdownDecision(
                            pushed=pushed_limit is not None,
                            supported=capabilities.pushdown_limit if has_limit else None,
                            reason=(
                                _scan_limit_pushdown_reason(
                                    logical_plan=logical_plan,
                                    expression=expression,
                                    source_capabilities=capabilities,
                                    rejected_filters=rejected_filters,
                                    full_query_reasons=pushdown_reasons,
                                )
                                if has_limit
                                else None
                            ),
                            details=[str(pushed_limit)] if pushed_limit is not None else [],
                        ),
                        join=PushdownDecision(
                            pushed=False,
                            supported=capabilities.pushdown_join if has_join else None,
                            reason=(
                                _stage_pushdown_reason(
                                    supports_feature=capabilities.pushdown_join,
                                    full_query_reasons=pushdown_reasons,
                                    unsupported_reason=(
                                        "Unsupported connector/source capability: join pushdown is unavailable."
                                    ),
                                )
                                if has_join
                                else None
                            ),
                            details=[],
                        ),
                    ),
                )
            )

        join_order = _choose_join_order(logical_plan=logical_plan, stats_by_table=stats_by_table)
        join_strategies = _choose_join_strategies(
            logical_plan=logical_plan,
            stats_by_table=stats_by_table,
            broadcast_threshold_bytes=self._broadcast_threshold_bytes,
        )

        stage_table_map = {alias: f"scan_{alias}" for alias in logical_plan.tables}
        local_stage_sql = render_local_stage_sql(
            expression,
            stage_tables=stage_table_map,
            source_dialect=input_dialect,
            target_dialect=local_dialect,
        )

        return OptimizedPlan(
            logical_plan=logical_plan,
            source_subplans=source_subplans,
            local_stage_sql=local_stage_sql,
            local_stage_dialect=local_dialect,
            join_order=join_order,
            join_strategies=join_strategies,
            pushdown_full_query=False,
            pushdown_reasons=pushdown_reasons,
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

_SAFE_UNQUOTED_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
        if isinstance(rewritten.this, exp.Identifier):
            rewritten.set(
                "this",
                _normalize_scan_identifier(
                    identifier=rewritten.this,
                    binding=binding,
                ),
            )
        rewritten.set("db", None)
        rewritten.set("catalog", None)
        return rewritten

    return expression.transform(_replace)


def _normalize_scan_identifier(
    *,
    identifier: exp.Identifier,
    binding,
) -> exp.Identifier:
    metadata = binding.metadata if isinstance(getattr(binding, "metadata", None), dict) else {}
    if not bool(metadata.get("physical_sql")):
        return identifier
    if not bool(getattr(identifier, "quoted", False)):
        return identifier

    raw_identifier = str(getattr(identifier, "this", "") or "").strip()
    if not raw_identifier:
        return identifier
    if raw_identifier != raw_identifier.lower():
        return identifier
    if _SAFE_UNQUOTED_IDENTIFIER_RE.fullmatch(raw_identifier) is None:
        return identifier

    return exp.Identifier(this=raw_identifier, quoted=False)


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


def _binding_can_support_full_query_rewrite(binding) -> bool:
    metadata = binding.metadata if isinstance(getattr(binding, "metadata", None), dict) else {}
    physical_sql = metadata.get("physical_sql")
    if isinstance(physical_sql, str) and physical_sql.strip():
        return False
    return True


def _rewrite_expression_for_full_query_pushdown(
    *,
    expression: exp.Expression,
    logical_plan: LogicalPlan,
    virtual_dataset: VirtualDataset,
) -> exp.Expression:
    bindings_by_alias = {
        alias: virtual_dataset.tables[table.table_key]
        for alias, table in logical_plan.tables.items()
    }

    def _column_alias(node: exp.Column) -> str | None:
        table_name = str(node.table or "").strip().lower()
        if not table_name:
            return None
        schema_name = str(node.db or "").strip().lower() or None
        catalog_name = str(node.catalog or "").strip().lower() or None

        for alias, binding in bindings_by_alias.items():
            metadata = binding.metadata if isinstance(getattr(binding, "metadata", None), dict) else {}
            physical_catalog = metadata.get("physical_catalog", binding.catalog)
            physical_schema = metadata.get("physical_schema", binding.schema_name)
            physical_table = metadata.get("physical_table", binding.table)

            valid_tables = {
                str(alias).strip().lower(),
                str(binding.table_key).strip().lower(),
                str(binding.table).strip().lower(),
                str(physical_table).strip().lower(),
                str(metadata.get("dataset_alias") or "").strip().lower(),
            }
            if table_name not in {value for value in valid_tables if value}:
                continue

            valid_schemas = {
                str(schema).strip().lower()
                for schema in (binding.schema_name, physical_schema)
                if str(schema).strip()
            }
            if schema_name is not None and valid_schemas and schema_name not in valid_schemas:
                continue

            valid_catalogs = {
                str(catalog).strip().lower()
                for catalog in (binding.catalog, physical_catalog)
                if str(catalog).strip()
            }
            if catalog_name is not None and valid_catalogs and catalog_name not in valid_catalogs:
                continue

            return alias
        return None

    def _replace(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column):
            alias = _column_alias(node)
            if alias is None:
                return node
            rewritten = node.copy()
            rewritten.set("table", exp.Identifier(this=alias, quoted=False))
            rewritten.set("db", None)
            rewritten.set("catalog", None)
            return rewritten

        if not isinstance(node, exp.Table):
            return node

        alias = str(node.alias_or_name or "").strip()
        binding = bindings_by_alias.get(alias)
        if binding is None:
            return node

        metadata = binding.metadata if isinstance(getattr(binding, "metadata", None), dict) else {}
        physical_sql = metadata.get("physical_sql")
        if isinstance(physical_sql, str) and physical_sql.strip():
            return node

        physical_catalog = metadata.get("physical_catalog", binding.catalog)
        physical_schema = metadata.get("physical_schema", binding.schema_name)
        physical_table = metadata.get("physical_table", binding.table)
        if bool(metadata.get("skip_catalog_in_pushdown")):
            physical_catalog = None

        rewritten = exp.table_(
            physical_table,
            db=physical_schema or None,
            catalog=physical_catalog or None,
            alias=alias or None,
            quoted=False,
        )
        return rewritten

    return expression.copy().transform(_replace)


def _full_query_pushdown_reasons(
    *,
    distinct_source_count: int,
    has_cte: bool,
    requires_physical_name_rewrite: bool,
    requires_logical_name_rewrite: bool,
    cross_dialect_block_reason: str | None,
    has_join: bool,
    has_filter: bool,
    has_aggregation: bool,
    has_limit: bool,
    source_capabilities: SourceCapabilities | None,
) -> list[str]:
    reasons: list[str] = []
    if distinct_source_count != 1:
        reasons.append("Cross-source query requires local federation stages.")
    if has_cte:
        reasons.append("Local rewrite is required because the query contains a CTE.")
    if requires_physical_name_rewrite:
        reasons.append("Local rewrite is required to map logical tables to physical relations.")
    if requires_logical_name_rewrite:
        reasons.append("Local rewrite is required to preserve runtime dataset aliases.")
    if cross_dialect_block_reason:
        reasons.append(cross_dialect_block_reason)
    if source_capabilities is None:
        return _dedupe_reasons(reasons)
    if has_filter and not source_capabilities.pushdown_filter:
        reasons.append("Unsupported connector/source capability: filter pushdown is unavailable.")
    if not source_capabilities.pushdown_projection:
        reasons.append("Unsupported connector/source capability: projection pushdown is unavailable.")
    if has_aggregation and not source_capabilities.pushdown_aggregation:
        reasons.append("Unsupported connector/source capability: aggregation pushdown is unavailable.")
    if has_limit and not source_capabilities.pushdown_limit:
        reasons.append("Unsupported connector/source capability: limit pushdown is unavailable.")
    if has_join and not source_capabilities.pushdown_join:
        reasons.append("Unsupported connector/source capability: join pushdown is unavailable.")
    return _dedupe_reasons(reasons)


def _cross_dialect_full_query_block_reason(
    *,
    expression: exp.Expression,
    input_dialect: str,
    target_dialect: str,
) -> str | None:
    if _normalize_dialect(input_dialect) == _normalize_dialect(target_dialect):
        return None

    anonymous_functions = sorted(
        {
            str(function.name or "").strip().upper()
            for function in expression.find_all(exp.Anonymous)
            if str(function.name or "").strip()
        }
    )
    if not anonymous_functions:
        return None

    functions = ", ".join(anonymous_functions)
    return (
        "Local rewrite is required because cross-dialect full-query pushdown contains "
        f"unrecognized function(s) that may not run on the source dialect: {functions}."
    )


def _dedupe_reasons(reasons: list[str]) -> list[str]:
    deduped: list[str] = []
    for reason in reasons:
        if reason not in deduped:
            deduped.append(reason)
    return deduped


def _first_reason(reasons: list[str], *, fallback: str | None = None) -> str | None:
    if reasons:
        return reasons[0]
    return fallback


def _filter_pushdown_reason(
    *,
    has_alias_filters: bool,
    supports_filter: bool,
    rejected_filters: list[exp.Expression],
) -> str | None:
    if not has_alias_filters:
        return None
    if not supports_filter:
        return "Unsupported connector/source capability: filter pushdown is unavailable."
    if rejected_filters:
        return "Some filters remained local because they required dialect-specific rewrite."
    return None


def _projection_pushdown_reason(
    *,
    projected_columns: list[str],
    supports_projection: bool,
) -> str | None:
    if not supports_projection:
        return "Unsupported connector/source capability: projection pushdown is unavailable."
    if not projected_columns or "*" in projected_columns:
        return "Wildcard selection requires the full row shape for this stage."
    return None


def _stage_pushdown_reason(
    *,
    supports_feature: bool,
    full_query_reasons: list[str],
    unsupported_reason: str,
) -> str | None:
    if not supports_feature:
        return unsupported_reason
    return _first_reason(
        full_query_reasons,
        fallback="Local compute is required for this stage.",
    )


def _can_push_limit_to_scan(
    *,
    logical_plan: LogicalPlan,
    expression: exp.Expression,
    source_capabilities: SourceCapabilities,
    rejected_filters: list[exp.Expression],
) -> bool:
    if logical_plan.limit is None:
        return False
    if not source_capabilities.pushdown_limit:
        return False
    if len(logical_plan.tables) != 1:
        return False
    if logical_plan.has_cte or logical_plan.joins:
        return False
    if logical_plan.group_by_sql or logical_plan.having_sql:
        return False
    if logical_plan.order_by_sql or logical_plan.offset is not None:
        return False
    if rejected_filters:
        return False
    if expression.find(exp.AggFunc) is not None:
        return False
    if expression.find(exp.Distinct) is not None:
        return False
    if expression.find(exp.Qualify) is not None:
        return False
    if expression.find(exp.Union) is not None:
        return False
    if expression.find(exp.Window) is not None:
        return False
    return True


def _scan_limit_pushdown_reason(
    *,
    logical_plan: LogicalPlan,
    expression: exp.Expression,
    source_capabilities: SourceCapabilities,
    rejected_filters: list[exp.Expression],
    full_query_reasons: list[str],
) -> str | None:
    if logical_plan.limit is None:
        return None
    if _can_push_limit_to_scan(
        logical_plan=logical_plan,
        expression=expression,
        source_capabilities=source_capabilities,
        rejected_filters=rejected_filters,
    ):
        return None
    if not source_capabilities.pushdown_limit:
        return "Unsupported connector/source capability: limit pushdown is unavailable."
    if len(logical_plan.tables) == 1:
        return "Local limit remains after the remote scan because applying it before local computation could change results."
    return _first_reason(
        full_query_reasons,
        fallback="Local compute is required for this stage.",
    )


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
