import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import sqlglot
from sqlglot import exp

from langbridge.semantic.errors import SemanticModelError, SemanticQueryError
from .join_planner import JoinPlanner
from langbridge.semantic.model import SemanticModel
from langbridge.semantic.loader import load_semantic_model
from .query_model import FilterItem, SemanticQuery
from .resolver import DimensionRef, MeasureRef, MetricRef, SemanticModelResolver, SegmentRef
from .tsql import DATE_TYPES, build_date_range_condition, date_trunc, format_literal


@dataclass(frozen=True)
class TimeDimensionRef:
    dimension: DimensionRef
    granularity: Optional[str]
    date_range: Optional[Any]


@dataclass(frozen=True)
class FilterTarget:
    kind: str
    expression: exp.Expression
    data_type: Optional[str]
    datasets: Set[str]


@dataclass(frozen=True)
class OrderItem:
    member: str
    direction: str


class TsqlSemanticTranslator:
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._dialect = "tsql"

    def translate(
        self,
        query: SemanticQuery | Dict[str, Any],
        model: SemanticModel,
        dialect: str = "tsql",
    ) -> exp.Select:
        self._dialect = (dialect or "tsql").lower()
        if isinstance(query, SemanticQuery):
            parsed = query
        else:
            parsed = SemanticQuery.model_validate(query)

        resolver = SemanticModelResolver(model)
        dimensions = [resolver.resolve_dimension(member) for member in parsed.dimensions]
        time_dimensions = [
            TimeDimensionRef(
                dimension=resolver.resolve_dimension(item.dimension),
                granularity=item.granularity,
                date_range=item.date_range,
            )
            for item in parsed.time_dimensions
        ]

        measures: List[MeasureRef] = []
        metrics: List[MetricRef] = []
        for member in parsed.measures:
            resolved = resolver.resolve_measure_or_metric(member)
            if isinstance(resolved, MetricRef):
                metrics.append(resolved)
            else:
                measures.append(resolved)

        filter_targets: List[FilterTarget] = []
        for item in parsed.filters:
            filter_targets.append(self._resolve_filter_target(resolver, item))

        segments = [resolver.resolve_segment(segment) for segment in parsed.segments]

        required_datasets = self._collect_required_datasets(
            resolver,
            dimensions,
            time_dimensions,
            measures,
            metrics,
            filter_targets,
            segments,
        )
        base_dataset = self._choose_base_dataset(
            dimensions,
            time_dimensions,
            measures,
            metrics,
            filter_targets,
            segments,
        )
        if base_dataset not in required_datasets:
            required_datasets.add(base_dataset)

        join_steps = JoinPlanner(model.relationships).plan(base_dataset, required_datasets)
        alias_map = self._build_alias_map(base_dataset, join_steps)

        #TODO: implement ctes. Currently we just inline everything which works but can lead to duplicated expressions and subqueries. We can start by implementing CTEs for joined tables and then expand to more complex expressions if needed.
        # cte_clauses = self._build_ctes

        select_clauses, group_by_expressions, order_aliases = self._build_selects(
            alias_map, dimensions, time_dimensions, measures, metrics, resolver
        )
        where_conditions = self._build_where_conditions(
            alias_map, filter_targets, time_dimensions, segments
        )
        having_conditions = self._build_having_conditions(alias_map, filter_targets)

        order_items = self._normalize_order(parsed.order)
        order_clause = self._build_order_clause(
            order_items,
            order_aliases,
            alias_map,
            resolver,
            dimensions,
            time_dimensions,
            measures,
            metrics,
        )

        query_expr = exp.select(*select_clauses)
        query_expr = self._apply_from(query_expr, model, base_dataset, alias_map, join_steps)

        if where_conditions:
            query_expr = query_expr.where(self._combine_conditions(where_conditions))

        if group_by_expressions:
            query_expr = query_expr.group_by(*group_by_expressions)

        if having_conditions:
            query_expr = query_expr.having(self._combine_conditions(having_conditions))

        if order_clause:
            query_expr = query_expr.order_by(*order_clause)

        query_expr = self._apply_limit(query_expr, parsed.limit, parsed.offset)

        return query_expr

    def load_semantic_model(self, yaml_text: str) -> SemanticModel:
        return load_semantic_model(yaml_text)

    def _collect_required_datasets(
        self,
        resolver: SemanticModelResolver,
        dimensions: Sequence[DimensionRef],
        time_dimensions: Sequence[TimeDimensionRef],
        measures: Sequence[MeasureRef],
        metrics: Sequence[MetricRef],
        filter_targets: Sequence[FilterTarget],
        segments: Sequence[SegmentRef],
    ) -> Set[str]:
        required_datasets: Set[str] = set()

        for dimension in dimensions:
            required_datasets.add(dimension.dataset)
        for time_dimension in time_dimensions:
            required_datasets.add(time_dimension.dimension.dataset)
        for measure in measures:
            required_datasets.add(measure.dataset)
        for metric in metrics:
            required_datasets.update(resolver.extract_datasets_from_expression(metric.expression))
        for target in filter_targets:
            required_datasets.update(target.datasets)
        for segment in segments:
            required_datasets.add(segment.dataset)

        return required_datasets

    def _choose_base_dataset(
        self,
        dimensions: Sequence[DimensionRef],
        time_dimensions: Sequence[TimeDimensionRef],
        measures: Sequence[MeasureRef],
        metrics: Sequence[MetricRef],
        filter_targets: Sequence[FilterTarget],
        segments: Sequence[SegmentRef],
    ) -> str:
        if measures:
            return measures[0].dataset
        if metrics:
            for dataset in self._datasets_from_expression(metrics[0].key):
                return dataset
        if time_dimensions:
            return time_dimensions[0].dimension.dataset
        if dimensions:
            return dimensions[0].dataset
        if filter_targets:
            return next(iter(filter_targets[0].datasets))
        if segments:
            return segments[0].dataset
        raise SemanticQueryError(f"Semantic query did not reference any tables in {dimensions}, {time_dimensions}, {measures}, {metrics}, {filter_targets}, {segments}.")

    def _datasets_from_expression(self, expression: str) -> List[str]:
        matches = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\.", expression)
        return matches

    def _build_alias_map(self, base_dataset: str, join_steps: Sequence[Any]) -> Dict[str, str]:
        alias_map: Dict[str, str] = {base_dataset: "t0"}
        counter = 1
        for step in join_steps:
            if step.right_dataset not in alias_map:
                alias_map[step.right_dataset] = f"t{counter}"
                counter += 1
            if step.left_dataset not in alias_map:
                alias_map[step.left_dataset] = f"t{counter}"
                counter += 1
        return alias_map

    def _build_selects(
        self,
        alias_map: Dict[str, str],
        dimensions: Sequence[DimensionRef],
        time_dimensions: Sequence[TimeDimensionRef],
        measures: Sequence[MeasureRef],
        metrics: Sequence[MetricRef],
        resolver: SemanticModelResolver,
    ) -> Tuple[List[exp.Expression], List[exp.Expression], Dict[str, str]]:
        select_clauses: List[exp.Expression] = []
        group_by_expressions: List[exp.Expression] = []
        order_aliases: Dict[str, str] = {}

        for dimension in dimensions:
            expr = self._column_expression(alias_map, dimension.dataset, dimension.column, dimension.expression, data_type=dimension.data_type)
            alias = self._alias_for_member(f"{dimension.dataset}.{dimension.column}")
            select_clauses.append(exp.alias_(expr, alias, quoted=False))
            group_by_expressions.append(expr)
            self._register_column_order_aliases(
                order_aliases=order_aliases,
                alias=alias,
                resolver=resolver,
                dataset=dimension.dataset,
                column=dimension.column,
            )

        for time_dimension in time_dimensions:
            base_expr = self._column_expression(
                alias_map,
                time_dimension.dimension.dataset,
                time_dimension.dimension.column,
                data_type=time_dimension.dimension.data_type,
            )
            expr = base_expr
            if time_dimension.granularity:
                expr = date_trunc(time_dimension.granularity, base_expr, dialect=self._dialect)
            alias = self._alias_for_time_dimension(
                time_dimension.dimension.dataset,
                time_dimension.dimension.column,
                time_dimension.granularity,
            )
            select_clauses.append(exp.alias_(expr, alias, quoted=False))
            group_by_expressions.append(expr)
            self._register_column_order_aliases(
                order_aliases=order_aliases,
                alias=alias,
                resolver=resolver,
                dataset=time_dimension.dimension.dataset,
                column=time_dimension.dimension.column,
                granularity=time_dimension.granularity,
            )

        for measure in measures:
            expr = self._measure_expression(alias_map, measure)
            alias = self._alias_for_member(f"{measure.dataset}.{measure.column}")
            select_clauses.append(exp.alias_(expr, alias, quoted=False))
            self._register_column_order_aliases(
                order_aliases=order_aliases,
                alias=alias,
                resolver=resolver,
                dataset=measure.dataset,
                column=measure.column,
            )

        for metric in metrics:
            expr = self._replace_table_refs(metric.expression, alias_map)
            alias = self._alias_for_member(metric.key)
            select_clauses.append(exp.alias_(expr, alias, quoted=False))
            order_aliases[alias] = alias
            order_aliases[metric.key] = alias

        if not select_clauses:
            raise SemanticQueryError("Semantic query did not include any dimensions, measures, or metrics.")

        return select_clauses, group_by_expressions, order_aliases

    def _build_where_conditions(
        self,
        alias_map: Dict[str, str],
        filter_targets: Sequence[FilterTarget],
        time_dimensions: Sequence[TimeDimensionRef],
        segments: Sequence[SegmentRef],
    ) -> List[exp.Expression]:
        conditions: List[exp.Expression] = []
        for target in filter_targets:
            if target.kind == "measure" or target.kind == "metric":
                continue
            conditions.append(self._replace_table_refs(target.expression, alias_map))

        for time_dimension in time_dimensions:
            if not time_dimension.date_range:
                continue
            dimension_expression: Optional[str] = time_dimension.dimension.expression
            if dimension_expression and dimension_expression.strip() == time_dimension.dimension.column:
                dimension_expression = None
            column_expr = self._column_expression(
                alias_map,
                time_dimension.dimension.dataset,
                time_dimension.dimension.column,
                dimension_expression,
                data_type=time_dimension.dimension.data_type,
            )
            conditions.append(
                build_date_range_condition(
                    column_expr,
                    time_dimension.date_range,
                    time_dimension.dimension.data_type,
                    dialect=self._dialect,
                )
            )

        for segment in segments:
            condition = self._replace_table_refs(segment.condition, alias_map)
            conditions.append(condition)

        return conditions

    def _build_having_conditions(
        self,
        alias_map: Dict[str, str],
        filter_targets: Sequence[FilterTarget],
    ) -> List[exp.Expression]:
        conditions: List[exp.Expression] = []
        for target in filter_targets:
            if target.kind in {"measure", "metric"}:
                conditions.append(self._replace_table_refs(target.expression, alias_map))
        return conditions

    def _build_order_clause(
        self,
        order_items: Sequence[OrderItem],
        order_aliases: Dict[str, str],
        alias_map: Dict[str, str],
        resolver: SemanticModelResolver,
        dimensions: Sequence[DimensionRef],
        time_dimensions: Sequence[TimeDimensionRef],
        measures: Sequence[MeasureRef],
        metrics: Sequence[MetricRef],
    ) -> List[exp.Expression]:
        if not order_items:
            return []

        clauses: List[exp.Expression] = []
        for item in order_items:
            key = item.member
            alias = order_aliases.get(key)
            if alias:
                clauses.append(
                    exp.Ordered(
                        this=exp.Identifier(this=alias, quoted=False),
                        desc=item.direction == "DESC",
                    )
                )
                continue

            resolved = self._resolve_order_member(
                key, alias_map, resolver, dimensions, time_dimensions, measures, metrics
            )
            clauses.append(exp.Ordered(this=resolved, desc=item.direction == "DESC"))

        return clauses

    def _resolve_order_member(
        self,
        member: str,
        alias_map: Dict[str, str],
        resolver: SemanticModelResolver,
        dimensions: Sequence[DimensionRef],
        time_dimensions: Sequence[TimeDimensionRef],
        measures: Sequence[MeasureRef],
        metrics: Sequence[MetricRef],
    ) -> exp.Expression:
        for time_dimension in time_dimensions:
            if member == f"{time_dimension.dimension.dataset}.{time_dimension.dimension.column}":
                expr = self._column_expression(
                    alias_map,
                    time_dimension.dimension.dataset,
                    time_dimension.dimension.column,
                    time_dimension.dimension.expression,
                )
                if time_dimension.granularity:
                    return date_trunc(time_dimension.granularity, expr, dialect=self._dialect)
                return expr

        matching_time_dimension = self._resolve_matching_time_dimension(member, resolver, time_dimensions)
        if matching_time_dimension is not None:
            expr = self._column_expression(
                alias_map,
                matching_time_dimension.dimension.dataset,
                matching_time_dimension.dimension.column,
                matching_time_dimension.dimension.expression,
            )
            if matching_time_dimension.granularity:
                return date_trunc(matching_time_dimension.granularity, expr, dialect=self._dialect)
            return expr

        try:
            dimension = resolver.resolve_dimension(member)
            return self._column_expression(alias_map, dimension.dataset, dimension.column, dimension.expression)
        except SemanticModelError:
            pass

        try:
            measure = resolver.resolve_measure(member)
            return self._measure_expression(alias_map, measure)
        except SemanticModelError:
            pass

        if member in {metric.key for metric in metrics}:
            metric = next(metric for metric in metrics if metric.key == member)
            return self._replace_table_refs(metric.expression, alias_map)

        raise SemanticQueryError(f"Unable to resolve order member '{member}'.")

    def _apply_limit(self, query: exp.Select, limit: Optional[int], offset: Optional[int]) -> exp.Select:
        if limit is None and offset is None:
            return query

        safe_limit = limit if limit is not None else 2147483647
        safe_offset = offset or 0
        return query.limit(safe_limit).offset(safe_offset)

    def _combine_conditions(self, conditions: Sequence[exp.Expression]) -> exp.Expression:
        return exp.and_(*conditions)

    def _ensure_expression(self, expression: exp.Expression | str) -> exp.Expression:
        if isinstance(expression, exp.Expression):
            return expression
        try:
            return sqlglot.parse_one(expression, read=self._dialect)
        except sqlglot.ParseError:
            return sqlglot.parse_one(expression, read="tsql")

    def _apply_from(
        self,
        query: exp.Select,
        model: SemanticModel,
        base_dataset: str,
        alias_map: Dict[str, str],
        join_steps: Sequence[Any],
    ) -> exp.Select:
        base_ref = self._dataset_ref(model, base_dataset, alias=alias_map[base_dataset])
        query = query.from_(base_ref)

        for step in join_steps:
            right_ref = self._dataset_ref(model, step.right_dataset, alias=alias_map[step.right_dataset])
            join_on = self._replace_table_refs(step.relationship.join_condition, alias_map)
            join_type = self._join_type(step.relationship.type).lower()
            query = query.join(right_ref, on=join_on, join_type=join_type)

        return query

    def _resolve_filter_target(self, resolver: SemanticModelResolver, item: FilterItem) -> FilterTarget:
        member = item.member or item.dimension or item.measure or item.time_dimension
        if not member:
            raise SemanticQueryError("Filter is missing member information.")

        operator = item.operator.strip().lower()
        values = item.values or []

        if item.dimension or item.time_dimension:
            dimension = resolver.resolve_dimension(member)
            expr = self._column_expression({}, dimension.dataset, dimension.column, dimension.expression, allow_placeholder=True)
            condition = self._build_filter_expression(expr, operator, values, dimension.data_type)
            return FilterTarget(
                kind="dimension",
                expression=condition,
                data_type=dimension.data_type,
                datasets={dimension.dataset},
            )

        if item.measure:
            resolved = resolver.resolve_measure_or_metric(member)
            if isinstance(resolved, MetricRef):
                expr = resolved.expression
                condition = self._build_filter_expression(expr, operator, values, None)
                return FilterTarget(
                    kind="metric",
                    expression=condition,
                    data_type=None,
                    datasets=resolver.extract_datasets_from_expression(expr),
                )
            expr = self._measure_expression({}, resolved, allow_placeholder=True)
            condition = self._build_filter_expression(expr, operator, values, resolved.data_type)
            return FilterTarget(
                kind="measure",
                expression=condition,
                data_type=resolved.data_type,
                datasets={resolved.dataset},
            )

        if member in (resolver.model.metrics or {}):
            metric = resolver.resolve_metric(member)
            expr = metric.expression
            condition = self._build_filter_expression(expr, operator, values, None)
            return FilterTarget(
                kind="metric",
                expression=condition,
                data_type=None,
                datasets=resolver.extract_datasets_from_expression(expr),
            )

        try:
            dimension = resolver.resolve_dimension(member)
            expr = self._column_expression({}, dimension.dataset, dimension.column, dimension.expression, allow_placeholder=True)
            condition = self._build_filter_expression(expr, operator, values, dimension.data_type)
            return FilterTarget(
                kind="dimension",
                expression=condition,
                data_type=dimension.data_type,
                datasets={dimension.dataset},
            )
        except SemanticModelError:
            pass

        resolved = resolver.resolve_measure_or_metric(member)
        if isinstance(resolved, MetricRef):
            expr = resolved.expression
            condition = self._build_filter_expression(expr, operator, values, None)
            return FilterTarget(
                kind="metric",
                expression=condition,
                data_type=None,
                datasets=resolver.extract_datasets_from_expression(expr),
            )
        expr = self._measure_expression({}, resolved, allow_placeholder=True)
        condition = self._build_filter_expression(expr, operator, values, resolved.data_type)
        return FilterTarget(
            kind="measure",
            expression=condition,
            data_type=resolved.data_type,
            datasets={resolved.dataset},
        )

    def _build_filter_expression(
        self,
        expression: exp.Expression | str,
        operator: str,
        values: Sequence[Any],
        data_type: Optional[str],
    ) -> exp.Expression:
        op = operator.strip().lower()
        expr = self._ensure_expression(expression)
        formatted_values = [format_literal(value, data_type, dialect=self._dialect) for value in values]

        if op in {"equals", "equal", "eq"}:
            if len(formatted_values) == 1:
                return exp.EQ(this=expr, expression=formatted_values[0])
            return exp.In(this=expr, expressions=formatted_values)
        if op in {"notequals", "not_equals", "ne"}:
            if len(formatted_values) == 1:
                return exp.NEQ(this=expr, expression=formatted_values[0])
            return exp.Not(this=exp.In(this=expr, expressions=formatted_values))
        if op == "contains":
            return exp.Like(
                this=expr,
                expression=format_literal(f"%{values[0]}%", None, dialect=self._dialect),
            )
        if op == "notcontains":
            return exp.Not(
                this=exp.Like(
                    this=expr,
                    expression=format_literal(f"%{values[0]}%", None, dialect=self._dialect),
                )
            )
        if op == "startswith":
            return exp.Like(
                this=expr,
                expression=format_literal(f"{values[0]}%", None, dialect=self._dialect),
            )
        if op == "endswith":
            return exp.Like(
                this=expr,
                expression=format_literal(f"%{values[0]}", None, dialect=self._dialect),
            )
        if op in {"gt", "greater"}:
            return exp.GT(this=expr, expression=formatted_values[0])
        if op in {"gte", "gteq", "greater_or_equal"}:
            return exp.GTE(this=expr, expression=formatted_values[0])
        if op in {"lt", "less"}:
            return exp.LT(this=expr, expression=formatted_values[0])
        if op in {"lte", "lteq", "less_or_equal"}:
            return exp.LTE(this=expr, expression=formatted_values[0])
        if op == "beforedate":
            return exp.LT(this=expr, expression=formatted_values[0])
        if op == "afterdate":
            return exp.GT(this=expr, expression=formatted_values[0])
        if op == "indaterange":
            if len(values) == 1:
                date_range = values[0]
            else:
                date_range = list(values)
            return build_date_range_condition(expr, date_range, data_type, dialect=self._dialect)
        if op == "notindaterange":
            if len(values) == 1:
                date_range = values[0]
            else:
                date_range = list(values)
            return exp.Not(
                this=build_date_range_condition(expr, date_range, data_type, dialect=self._dialect)
            )
        if op == "set":
            return exp.Not(this=exp.Is(this=expr, expression=exp.Null()))
        if op == "notset":
            return exp.Is(this=expr, expression=exp.Null())
        if op == "in":
            return exp.In(this=expr, expressions=formatted_values)
        if op == "notin":
            return exp.Not(this=exp.In(this=expr, expressions=formatted_values))

        raise SemanticQueryError(f"Unsupported filter operator '{operator}'.")

    def _measure_expression(
        self, alias_map: Dict[str, str], measure: MeasureRef, allow_placeholder: bool = False
    ) -> exp.Expression:
        column_expr = self._column_expression(
            alias_map,
            measure.dataset,
            measure.column,
            expression=measure.expression,
            allow_placeholder=allow_placeholder,
        )
        aggregation = (measure.aggregation or "").strip().lower()
        if not aggregation:
            aggregation = "sum" if (measure.data_type or "").lower() in {"integer", "decimal", "float", "number"} else "count"

        if aggregation in {"count_distinct", "countdistinct"}:
            return exp.Count(this=column_expr, distinct=True)
        if aggregation == "count":
            return exp.Count(this=column_expr)

        aggregator = aggregation.lower()
        if aggregator == "sum":
            return exp.Sum(this=column_expr)
        if aggregator == "avg":
            return exp.Avg(this=column_expr)
        if aggregator == "min":
            return exp.Min(this=column_expr)
        if aggregator == "max":
            return exp.Max(this=column_expr)
        return exp.func(aggregator.upper(), column_expr)

    def _column_expression(
        self,
        alias_map: Dict[str, str],
        dataset: str,
        column: str,
        expression: Optional[str] = None,
        allow_placeholder: bool = False,
        data_type: Optional[str] = None,
    ) -> exp.Expression:
        if not alias_map:
            if not allow_placeholder:
                raise SemanticQueryError("Column expression requested before aliases are available.")
            alias = dataset
        else:
            alias = alias_map[dataset]
        if expression:
            expr = self._ensure_expression(expression)
            if isinstance(expr, exp.Column):
                if expr.table:
                    base_expr = self._replace_table_refs(expr, alias_map)
                else:
                    source_column = str(expr.name or column).strip() or column
                    base_expr = exp.Column(
                        this=exp.Identifier(this=source_column, quoted=False),
                        table=exp.Identifier(this=alias, quoted=False),
                    )
            else:
                base_expr = self._replace_table_refs(expr, alias_map)
        else:
            base_expr = exp.Column(
                this=exp.Identifier(this=column, quoted=False),
                table=exp.Identifier(this=alias, quoted=False),
            )
        return self._coerce_column_type(base_expr, data_type=data_type)

    @staticmethod
    def _coerce_column_type(
        expression: exp.Expression,
        *,
        data_type: Optional[str],
    ) -> exp.Expression:
        normalized_type = str(data_type or "").strip().lower()
        if normalized_type not in DATE_TYPES:
            return expression

        target_type = "TIMESTAMP" if normalized_type in {"datetime", "timestamp", "time"} else "DATE"
        return exp.Cast(
            this=expression,
            to=exp.DataType(this=target_type),
        )

    def _replace_table_refs(
        self, expression: exp.Expression | str, alias_map: Dict[str, str]
    ) -> exp.Expression:
        expr = self._ensure_expression(expression)

        def _replace(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Column):
                table = node.table
                if table in alias_map:
                    return exp.Column(
                        this=node.this.copy() if isinstance(node.this, exp.Identifier) else node.this,
                        table=exp.Identifier(this=alias_map[table], quoted=False),
                    )
            return node

        return expr.transform(_replace)

    def _dataset_ref(self, model: SemanticModel, dataset_key: str, alias: Optional[str] = None) -> exp.Expression:
        dataset = model.datasets.get(dataset_key)
        if dataset is None:
            raise SemanticQueryError(f"Unknown dataset '{dataset_key}'.")
        catalog = dataset.catalog_name
        schema = dataset.schema_name

        if not catalog and schema and "." in schema:
            first, remainder = schema.split(".", 1)
            catalog = first or None
            schema = remainder

        return exp.table_(
            dataset.get_relation_name(dataset_key),
            db=schema or None,
            catalog=catalog or None,
            quoted=False,
            alias=alias,
        )

    def _alias_for_member(self, member: str) -> str:
        alias = member.replace(".", "__").replace(" ", "_")
        return re.sub(r"[^A-Za-z0-9_]+", "_", alias)

    def _alias_for_time_dimension(self, dataset: str, column: str, granularity: Optional[str]) -> str:
        base = self._alias_for_member(f"{dataset}.{column}")
        if not granularity:
            return base
        return f"{base}_{granularity}"

    def _normalize_order(self, order: Any) -> List[OrderItem]:
        if order is None:
            return []

        items: List[OrderItem] = []
        if isinstance(order, dict):
            for key, direction in order.items():
                items.append(OrderItem(member=key, direction=self._normalize_direction(direction)))
            return items

        if isinstance(order, list):
            for entry in order:
                if isinstance(entry, dict):
                    for key, direction in entry.items():
                        items.append(OrderItem(member=key, direction=self._normalize_direction(direction)))
                elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                    items.append(OrderItem(member=str(entry[0]), direction=self._normalize_direction(entry[1])))
            return items

        raise SemanticQueryError("Unsupported order format.")

    def _normalize_direction(self, direction: Any) -> str:
        value = str(direction or "asc").strip().lower()
        return "DESC" if value == "desc" else "ASC"

    def _join_type(self, relationship_type: Optional[str]) -> str:
        if relationship_type in {"left", "right", "full", "inner"}:
            return relationship_type.upper()
        if relationship_type in {"one_to_many", "many_to_one", "one_to_one"}:
            return "LEFT"
        return "INNER"

    def _register_column_order_aliases(
        self,
        *,
        order_aliases: Dict[str, str],
        alias: str,
        resolver: SemanticModelResolver,
        dataset: str,
        column: str,
        granularity: Optional[str] = None,
    ) -> None:
        order_aliases[alias] = alias
        for key in self._build_member_candidates(resolver, dataset, column):
            order_aliases[key] = alias
            if granularity:
                order_aliases[f"{key}.{granularity}"] = alias

    def _build_member_candidates(
        self,
        resolver: SemanticModelResolver,
        dataset: str,
        column: str,
    ) -> List[str]:
        candidates = [f"{dataset}.{column}"]
        dataset_meta = resolver.model.datasets.get(dataset)
        if dataset_meta:
            relation_name = dataset_meta.get_relation_name(dataset)
            if relation_name:
                candidates.append(f"{relation_name}.{column}")
            schema_relation = ".".join(part for part in [dataset_meta.schema_name, relation_name] if part)
            if schema_relation:
                candidates.append(f"{schema_relation}.{column}")
                if dataset_meta.catalog_name:
                    candidates.append(f"{dataset_meta.catalog_name}.{schema_relation}.{column}")
        deduped: List[str] = []
        seen: Set[str] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        return deduped

    def _resolve_matching_time_dimension(
        self,
        member: str,
        resolver: SemanticModelResolver,
        time_dimensions: Sequence[TimeDimensionRef],
    ) -> Optional[TimeDimensionRef]:
        try:
            resolved_member = resolver.resolve_dimension(member)
        except SemanticModelError:
            return None

        for time_dimension in time_dimensions:
            if (
                time_dimension.dimension.dataset == resolved_member.dataset
                and time_dimension.dimension.column == resolved_member.column
            ):
                return time_dimension
        return None
