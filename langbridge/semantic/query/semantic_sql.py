from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any, Iterable

import sqlglot
from sqlglot import exp

from langbridge.semantic.errors import (
    SemanticModelError,
    SemanticSqlAmbiguousMemberError,
    SemanticSqlInvalidFilterError,
    SemanticSqlInvalidGroupingError,
    SemanticSqlInvalidMemberError,
    SemanticSqlInvalidTimeBucketError,
    SemanticSqlParseError,
    SemanticSqlUnsupportedConstructError,
    SemanticSqlUnsupportedExpressionError,
)
from langbridge.semantic.model import SemanticModel

from .query_model import SemanticQuery
from .resolver import MetricRef, SemanticModelResolver

_YEAR_PATTERN = re.compile(r"^\d{4}$")
_YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")
_ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TIME_DATA_TYPES = {"date", "datetime", "timestamp", "time"}


@dataclass(frozen=True)
class ParsedSemanticSqlQuery:
    statement: exp.Select
    rendered_query: str
    query_dialect: str
    semantic_model_ref: str
    relation_name: str
    relation_alias: str | None


@dataclass(frozen=True)
class SemanticSqlProjection:
    kind: str
    member: str
    source_key: str
    output_name: str
    granularity: str | None = None


@dataclass(frozen=True)
class SemanticSqlQueryPlan:
    semantic_model_ref: str
    semantic_query: SemanticQuery
    projections: list[SemanticSqlProjection]
    rendered_query: str


@dataclass(frozen=True)
class ResolvedSemanticMember:
    kind: str
    member: str
    source_key: str
    data_type: str | None = None


class SemanticSqlFrontend:
    _TIME_GRAIN_GUIDANCE = (
        "Semantic SQL time buckets only support minute, hour, day, week, month, quarter, "
        "or year over a semantic time dimension."
    )
    _SELECT_GUIDANCE = (
        "Semantic SQL SELECT only supports semantic members, semantic metrics, and "
        "DATE_TRUNC/TIMESTAMP_TRUNC time buckets."
    )
    _DATASET_SQL_GUIDANCE = "Use dataset SQL scope if you need free-form SQL."

    def parse_query(
        self,
        *,
        query: str,
        query_dialect: str,
    ) -> ParsedSemanticSqlQuery:
        rendered_query = str(query or "").strip()
        if not rendered_query:
            raise SemanticSqlParseError(
                "Semantic SQL query is empty. Provide one SELECT over a semantic model, "
                "for example `SELECT region, net_sales FROM commerce_performance`."
            )

        try:
            statements = sqlglot.parse(rendered_query, read=query_dialect)
        except sqlglot.ParseError as exc:
            raise SemanticSqlParseError(
                "Semantic SQL could not parse this query. "
                "Semantic SQL only accepts one governed SELECT over a single semantic model. "
                f"Parser detail: {exc}. {self._SELECT_GUIDANCE} {self._DATASET_SQL_GUIDANCE}"
            ) from exc

        if not statements:
            raise SemanticSqlParseError(
                "Semantic SQL query is empty. Provide one SELECT over a semantic model, "
                "for example `SELECT region, net_sales FROM commerce_performance`."
            )
        if len(statements) != 1:
            raise SemanticSqlUnsupportedConstructError(
                "Semantic SQL only supports a single SELECT statement over one semantic model. "
                "Split the request into separate queries or use dataset SQL scope for multi-statement SQL.",
                construct="multiple_statements",
            )

        statement = statements[0]
        if not isinstance(statement, exp.Select):
            raise SemanticSqlUnsupportedConstructError(
                "Semantic SQL only supports SELECT queries because it is a governed semantic frontend, "
                "not a general SQL execution surface. Select semantic members or metrics from "
                "`FROM <semantic_model>`, or use dataset SQL scope for other SQL statements.",
                construct=type(statement).__name__.lower(),
            )
        if statement.args.get("distinct") is not None:
            raise SemanticSqlUnsupportedConstructError(
                "Semantic SQL does not support DISTINCT because result grain is governed by the "
                "selected semantic dimensions and time buckets. Select the semantic dimensions or "
                "time buckets you need, model count-distinct logic as a semantic metric, or use "
                "dataset SQL scope for free-form DISTINCT.",
                construct="distinct",
            )
        if statement.args.get("joins"):
            raise SemanticSqlUnsupportedConstructError(
                "Semantic SQL does not support JOIN clauses because the semantic model owns governed "
                "relationships. Query a single semantic model in `FROM` and select semantic members "
                "or metrics that already encode those joins. Use dataset SQL scope for ad hoc joins.",
                construct="join",
            )
        if statement.args.get("having") is not None:
            raise SemanticSqlUnsupportedConstructError(
                "Semantic SQL does not support HAVING because semantic metrics and governed query "
                "grain belong in the semantic model, not ad hoc SQL aggregate filters. Add the logic "
                "as a semantic metric or use dataset SQL scope for free-form SQL.",
                construct="having",
            )
        if statement.args.get("with") is not None:
            raise SemanticSqlUnsupportedConstructError(
                "Semantic SQL does not support CTEs because semantic scope is intentionally constrained "
                "to one governed SELECT over a semantic model. Move the query to dataset SQL scope if "
                "you need CTEs or intermediate SQL shaping.",
                construct="cte",
            )

        from_clause = statement.args.get("from")
        if from_clause is None or not isinstance(from_clause.this, exp.Table):
            raise SemanticSqlUnsupportedConstructError(
                "Semantic SQL requires `FROM <semantic_model_name>` because the semantic model defines "
                "the governed query surface. Query one runtime semantic model, not a subquery or raw "
                "table reference.",
                construct="from_clause",
            )

        table = from_clause.this
        if table.db or table.catalog:
            raise SemanticSqlUnsupportedConstructError(
                "Semantic SQL only supports an unqualified semantic model name in the FROM clause. "
                "Use `FROM commerce_performance`, not catalog- or schema-qualified source objects.",
                construct="qualified_from",
            )

        semantic_model_ref = str(table.name or "").strip()
        if not semantic_model_ref:
            raise SemanticSqlParseError(
                "Semantic SQL could not resolve a semantic model name from the FROM clause. "
                "Use `FROM <semantic_model_name>`."
            )

        return ParsedSemanticSqlQuery(
            statement=statement,
            rendered_query=rendered_query,
            query_dialect=query_dialect,
            semantic_model_ref=semantic_model_ref,
            relation_name=semantic_model_ref,
            relation_alias=(str(table.alias or "").strip() or None),
        )

    def build_query_plan(
        self,
        *,
        parsed_query: ParsedSemanticSqlQuery,
        semantic_model: SemanticModel,
        requested_limit: int | None = None,
    ) -> SemanticSqlQueryPlan:
        resolver = SemanticModelResolver(semantic_model)
        relation_names = {
            name
            for name in (parsed_query.relation_name, parsed_query.relation_alias)
            if str(name or "").strip()
        }

        projections: list[SemanticSqlProjection] = []
        alias_to_projection: dict[str, SemanticSqlProjection] = {}
        member_to_projection: dict[str, SemanticSqlProjection] = {}
        source_key_to_projection: dict[str, SemanticSqlProjection] = {}
        dimensions: list[str] = []
        measures: list[str] = []
        time_dimensions: list[dict[str, Any]] = []

        for expression in parsed_query.statement.expressions:
            projection = self._build_projection(
                expression=expression,
                resolver=resolver,
                relation_names=relation_names,
            )
            if projection.output_name in alias_to_projection:
                raise SemanticSqlUnsupportedExpressionError(
                    f"Semantic SQL projection alias '{projection.output_name}' is duplicated. "
                    "Each selected semantic member or time bucket must have a unique output name.",
                    construct="duplicate_alias",
                )
            projections.append(projection)
            alias_to_projection[projection.output_name] = projection
            member_to_projection.setdefault(projection.member, projection)
            source_key_to_projection.setdefault(projection.source_key, projection)
            if projection.kind == "time_dimension":
                time_dimensions.append(
                    {
                        "dimension": projection.member,
                        "granularity": projection.granularity,
                    }
                )
            elif projection.kind == "dimension":
                dimensions.append(projection.member)
            else:
                measures.append(projection.member)

        self._validate_group_by(
            parsed_query.statement.args.get("group"),
            projections=projections,
            alias_to_projection=alias_to_projection,
            member_to_projection=member_to_projection,
            relation_names=relation_names,
            resolver=resolver,
        )

        query_limit = self._limit_value(parsed_query.statement.args.get("limit"))
        if requested_limit is not None:
            query_limit = min(query_limit, requested_limit) if query_limit is not None else requested_limit

        semantic_query = SemanticQuery(
            measures=self._dedupe(measures),
            dimensions=self._dedupe(dimensions),
            timeDimensions=self._dedupe_time_dimensions(time_dimensions),
            filters=self._build_filters(
                where_clause=parsed_query.statement.args.get("where"),
                resolver=resolver,
                relation_names=relation_names,
            ),
            order=self._build_order(
                order_clause=parsed_query.statement.args.get("order"),
                alias_to_projection=alias_to_projection,
                member_to_projection=member_to_projection,
                source_key_to_projection=source_key_to_projection,
                relation_names=relation_names,
                resolver=resolver,
            ),
            limit=query_limit,
        )

        return SemanticSqlQueryPlan(
            semantic_model_ref=parsed_query.semantic_model_ref,
            semantic_query=semantic_query,
            projections=projections,
            rendered_query=parsed_query.rendered_query,
        )

    def _build_projection(
        self,
        *,
        expression: exp.Expression,
        resolver: SemanticModelResolver,
        relation_names: set[str],
    ) -> SemanticSqlProjection:
        alias = str(getattr(expression, "alias", "") or "").strip() or None
        inner = expression.this if isinstance(expression, exp.Alias) else expression

        if isinstance(inner, exp.Star):
            raise SemanticSqlUnsupportedExpressionError(
                "Semantic SQL does not support `SELECT *` because the projection must stay explicit "
                "and governed. Select the semantic members or metrics you need by name.",
                construct="select_star",
            )

        if isinstance(inner, exp.Column):
            member = self._member_reference(
                column=inner,
                relation_names=relation_names,
            )
            return self._member_projection(
                member=member,
                output_name=alias or str(getattr(expression, "alias_or_name", "") or member.split(".")[-1]),
                resolver=resolver,
            )

        if self._is_time_trunc_expression(inner):
            if not isinstance(inner.this, exp.Column):
                raise SemanticSqlInvalidTimeBucketError(
                    "Semantic SQL time buckets must reference a semantic time dimension column directly. "
                    "Use `DATE_TRUNC('month', order_date)` or `TIMESTAMP_TRUNC(order_date, MONTH)`, "
                    "not a raw SQL expression.",
                    construct="time_bucket_expression",
                )
            member = self._member_reference(
                column=inner.this,
                relation_names=relation_names,
            )
            resolved = self._resolve_member(
                member=member,
                resolver=resolver,
            )
            self._require_time_dimension(resolved=resolved, member=member)
            granularity = self._time_granularity(inner)
            output_name = alias or resolved.source_key.rsplit(".", 1)[-1]
            return SemanticSqlProjection(
                kind="time_dimension",
                member=resolved.member,
                source_key=f"{resolved.source_key.rsplit('.', 1)[-1]}_{granularity}",
                output_name=output_name,
                granularity=granularity,
            )

        expression_sql = self._expression_sql(inner)
        if self._contains_aggregate_expression(inner):
            raise SemanticSqlUnsupportedExpressionError(
                f"Semantic SQL does not allow free-form aggregate expressions in SELECT such as "
                f"`{expression_sql}`. {self._SELECT_GUIDANCE} Add a semantic metric to the model for "
                "this calculation, or use dataset SQL scope for free-form SQL.",
                construct="aggregate_select_expression",
            )

        raise SemanticSqlUnsupportedExpressionError(
            f"Semantic SQL does not allow raw SQL expressions in SELECT such as `{expression_sql}`. "
            f"{self._SELECT_GUIDANCE} Model the calculation as a semantic metric or use dataset SQL "
            "scope for free-form SQL.",
            construct="select_expression",
        )

    @staticmethod
    def _member_reference(*, column: exp.Column, relation_names: set[str]) -> str:
        parts = [str(part.name) for part in column.parts]
        if not parts:
            raise SemanticSqlInvalidMemberError(
                "Semantic SQL could not resolve a semantic member from this column reference. "
                "Select semantic members by name, for example `orders.region` or `net_sales`."
            )
        if parts[0] in relation_names and len(parts) > 1:
            return ".".join(parts[1:])
        return ".".join(parts)

    def _member_projection(
        self,
        *,
        member: str,
        output_name: str,
        resolver: SemanticModelResolver,
    ) -> SemanticSqlProjection:
        resolved = self._resolve_member(
            member=member,
            resolver=resolver,
        )
        return SemanticSqlProjection(
            kind=resolved.kind,
            member=resolved.member,
            source_key=resolved.source_key,
            output_name=str(output_name or "").strip() or resolved.source_key.rsplit(".", 1)[-1],
        )

    def _resolve_member(
        self,
        *,
        member: str,
        resolver: SemanticModelResolver,
    ) -> ResolvedSemanticMember:
        dimension_ref = None
        measure_ref = None

        try:
            dimension_ref = resolver.resolve_dimension(member)
        except SemanticModelError:
            dimension_ref = None

        try:
            measure_ref = resolver.resolve_measure_or_metric(member)
        except SemanticModelError:
            measure_ref = None

        if dimension_ref is not None and measure_ref is not None:
            suggestions = self._member_suggestions(member=member, resolver=resolver)
            suggestion_text = ", ".join(f"`{item}`" for item in suggestions[:4])
            raise SemanticSqlAmbiguousMemberError(
                f"Semantic member `{member}` is ambiguous in this semantic model. "
                f"Qualify it with the dataset name, for example {suggestion_text}.",
                construct="ambiguous_member",
            )
        if dimension_ref is not None:
            canonical = f"{dimension_ref.dataset}.{dimension_ref.column}"
            return ResolvedSemanticMember(
                kind="dimension",
                member=canonical,
                source_key=canonical,
                data_type=dimension_ref.data_type,
            )
        if isinstance(measure_ref, MetricRef):
            return ResolvedSemanticMember(
                kind="metric",
                member=measure_ref.key,
                source_key=measure_ref.key,
            )
        if measure_ref is not None:
            canonical = f"{measure_ref.dataset}.{measure_ref.column}"
            return ResolvedSemanticMember(
                kind="measure",
                member=canonical,
                source_key=canonical,
                data_type=measure_ref.data_type,
            )

        suggestions = self._member_suggestions(member=member, resolver=resolver)
        suffix = ""
        if "." not in member and len(suggestions) > 1:
            suggestion_text = ", ".join(f"`{item}`" for item in suggestions[:4])
            raise SemanticSqlAmbiguousMemberError(
                f"Semantic member `{member}` is ambiguous in this semantic model. "
                f"Qualify it with the dataset name, for example {suggestion_text}.",
                construct="ambiguous_member",
            )
        if suggestions:
            suffix = " Try one of: " + ", ".join(f"`{item}`" for item in suggestions[:4]) + "."
        raise SemanticSqlInvalidMemberError(
            f"Unknown semantic member `{member}`. Semantic SQL only accepts semantic members or "
            f"metrics defined in the semantic model, not raw source columns or ad hoc SQL expressions."
            f"{suffix} Add the member or metric to the semantic model, or use dataset SQL scope for "
            "free-form SQL.",
            construct="unknown_member",
        )

    def _build_filters(
        self,
        *,
        where_clause: exp.Where | None,
        resolver: SemanticModelResolver,
        relation_names: set[str],
    ) -> list[dict[str, Any]]:
        if where_clause is None:
            return []
        predicates = self._flatten_and(where_clause.this)
        filters: list[dict[str, Any]] = []
        for predicate in predicates:
            filters.append(
                self._predicate_to_filter(
                    predicate=predicate,
                    resolver=resolver,
                    relation_names=relation_names,
                )
            )
        return filters

    def _flatten_and(self, expression: exp.Expression) -> list[exp.Expression]:
        if isinstance(expression, exp.Paren):
            return self._flatten_and(expression.this)
        if isinstance(expression, exp.And):
            return self._flatten_and(expression.left) + self._flatten_and(expression.right)
        if isinstance(expression, exp.Or):
            raise SemanticSqlInvalidFilterError(
                "Semantic SQL WHERE only supports AND-combined predicates over semantic members. "
                "Model more complex boolean logic in the semantic layer or use dataset SQL scope "
                "for arbitrary SQL predicates.",
                construct="or_predicate",
            )
        return [expression]

    def _predicate_to_filter(
        self,
        *,
        predicate: exp.Expression,
        resolver: SemanticModelResolver,
        relation_names: set[str],
    ) -> dict[str, Any]:
        if isinstance(predicate, exp.Not):
            inner = predicate.this
            if isinstance(inner, exp.In):
                member = self._member_reference(column=self._require_column(inner.this), relation_names=relation_names)
                return {
                    "member": self._resolve_member(member=member, resolver=resolver).member,
                    "operator": "notin",
                    "values": [self._literal_value(item) for item in inner.expressions],
                }
            if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
                member = self._member_reference(column=self._require_column(inner.this), relation_names=relation_names)
                return {
                    "member": self._resolve_member(member=member, resolver=resolver).member,
                    "operator": "set",
                    "values": [],
                }
            if isinstance(inner, exp.Like):
                return self._like_filter(
                    like_expression=inner,
                    operator="notcontains",
                    resolver=resolver,
                    relation_names=relation_names,
                )
            if isinstance(inner, exp.ILike):
                return self._ilike_filter(
                    like_expression=inner,
                    operator="notilike",
                    resolver=resolver,
                    relation_names=relation_names,
                )
            raise SemanticSqlInvalidFilterError(
                "Semantic SQL WHERE only supports `NOT IN`, `IS NOT NULL`, `NOT LIKE`, and `NOT ILIKE` "
                "over semantic members. Use dataset SQL scope for more complex NOT predicates.",
                construct="not_predicate",
            )

        if isinstance(predicate, exp.In):
            member = self._member_reference(column=self._require_column(predicate.this), relation_names=relation_names)
            return {
                "member": self._resolve_member(member=member, resolver=resolver).member,
                "operator": "in",
                "values": [self._literal_value(item) for item in predicate.expressions],
            }

        if isinstance(predicate, exp.Is) and isinstance(predicate.expression, exp.Null):
            member = self._member_reference(column=self._require_column(predicate.this), relation_names=relation_names)
            return {
                "member": self._resolve_member(member=member, resolver=resolver).member,
                "operator": "notset",
                "values": [],
            }

        if isinstance(predicate, exp.Like):
            return self._like_filter(
                like_expression=predicate,
                operator=None,
                resolver=resolver,
                relation_names=relation_names,
            )
        if isinstance(predicate, exp.ILike):
            return self._ilike_filter(
                like_expression=predicate,
                operator=None,
                resolver=resolver,
                relation_names=relation_names,
            )

        operator_map = {
            exp.EQ: "equals",
            exp.NEQ: "not_equals",
            exp.GT: "gt",
            exp.GTE: "gte",
            exp.LT: "lt",
            exp.LTE: "lte",
        }
        for expression_type, operator in operator_map.items():
            if isinstance(predicate, expression_type):
                member, value, normalized_operator = self._comparison_parts(
                    predicate=predicate,
                    relation_names=relation_names,
                    default_operator=operator,
                )
                resolved_member = self._resolve_member(member=member, resolver=resolver)
                normalized_operator, normalized_values = self._normalize_filter_values(
                    operator=normalized_operator,
                    values=[value],
                    data_type=resolved_member.data_type,
                )
                return {
                    "member": resolved_member.member,
                    "operator": normalized_operator,
                    "values": normalized_values,
                }

        raise SemanticSqlInvalidFilterError(
            "Semantic SQL WHERE only supports simple comparisons, IN lists, NULL checks, and "
            "restricted LIKE/ILIKE filters over semantic members. Use dataset SQL scope for raw SQL predicates.",
            construct="where_predicate",
        )

    def _comparison_parts(
        self,
        *,
        predicate: exp.Expression,
        relation_names: set[str],
        default_operator: str,
    ) -> tuple[str, Any, str]:
        if isinstance(predicate.this, exp.Column):
            return (
                self._member_reference(column=predicate.this, relation_names=relation_names),
                self._literal_value(predicate.expression),
                default_operator,
            )
        if isinstance(predicate.expression, exp.Column):
            inverted = {
                "gt": "lt",
                "gte": "lte",
                "lt": "gt",
                "lte": "gte",
            }
            return (
                self._member_reference(column=predicate.expression, relation_names=relation_names),
                self._literal_value(predicate.this),
                inverted.get(default_operator, default_operator),
            )
        raise SemanticSqlInvalidFilterError(
            "Semantic SQL comparisons must compare one semantic member to one literal value. "
            "Member-to-member comparisons and raw SQL expressions are not supported in semantic scope.",
            construct="comparison_expression",
        )

    def _like_filter(
        self,
        *,
        like_expression: exp.Like,
        operator: str | None,
        resolver: SemanticModelResolver,
        relation_names: set[str],
    ) -> dict[str, Any]:
        member = self._member_reference(
            column=self._require_column(like_expression.this),
            relation_names=relation_names,
        )
        value = self._literal_value(like_expression.expression)
        if not isinstance(value, str):
            raise SemanticSqlInvalidFilterError(
                "Semantic SQL LIKE patterns must be string literals. "
                "Use patterns like `'foo%'`, `'%foo'`, or `'%foo%'`.",
                construct="like_literal",
            )
        resolved_operator, resolved_value = self._pattern_operator(value=value, negated=operator is not None)
        return {
            "member": self._resolve_member(member=member, resolver=resolver).member,
            "operator": operator or resolved_operator,
            "values": [resolved_value],
        }

    def _ilike_filter(
        self,
        *,
        like_expression: exp.ILike,
        operator: str | None,
        resolver: SemanticModelResolver,
        relation_names: set[str],
    ) -> dict[str, Any]:
        member = self._member_reference(
            column=self._require_column(like_expression.this),
            relation_names=relation_names,
        )
        value = self._literal_value(like_expression.expression)
        if not isinstance(value, str):
            raise SemanticSqlInvalidFilterError(
                "Semantic SQL ILIKE patterns must be string literals. "
                "Use patterns like `'foo%'`, `'%foo'`, or `'%foo%'`.",
                construct="ilike_literal",
            )
        self._validate_ilike_pattern(value=value)
        return {
            "member": self._resolve_member(member=member, resolver=resolver).member,
            "operator": operator or "ilike",
            "values": [value],
        }

    @classmethod
    def _pattern_operator(cls, *, value: str, negated: bool) -> tuple[str, str]:
        if "_" in value:
            raise SemanticSqlInvalidFilterError(
                "Semantic SQL LIKE only supports exact, prefix, suffix, or contains patterns. "
                "Single-character `_` wildcards are not supported. Use a simpler pattern or dataset SQL scope.",
                construct="like_pattern",
            )
        if value.startswith("%") and value.endswith("%") and value.count("%") == 2:
            return ("notcontains" if negated else "contains", value[1:-1])
        if value.endswith("%") and value.count("%") == 1:
            return ("notcontains" if negated else "startswith", value[:-1])
        if value.startswith("%") and value.count("%") == 1:
            return ("notcontains" if negated else "endswith", value[1:])
        if "%" not in value:
            return ("not_equals" if negated else "equals", value)
        raise SemanticSqlInvalidFilterError(
            "Semantic SQL LIKE only supports exact, prefix, suffix, or contains patterns such as "
            "`foo`, `foo%`, `%foo`, or `%foo%`. Use dataset SQL scope for more complex wildcard patterns.",
            construct="like_pattern",
        )

    @classmethod
    def _validate_ilike_pattern(cls, *, value: str) -> None:
        if "_" in value:
            raise SemanticSqlInvalidFilterError(
                "Semantic SQL ILIKE only supports exact, prefix, suffix, or contains patterns. "
                "Single-character `_` wildcards are not supported. Use a simpler pattern or dataset SQL scope.",
                construct="ilike_pattern",
            )
        if "%" not in value:
            return
        if value.startswith("%") and value.endswith("%") and value.count("%") == 2:
            return
        if value.endswith("%") and value.count("%") == 1:
            return
        if value.startswith("%") and value.count("%") == 1:
            return
        raise SemanticSqlInvalidFilterError(
            "Semantic SQL ILIKE only supports exact, prefix, suffix, or contains patterns such as "
            "`foo`, `foo%`, `%foo`, or `%foo%`. Use dataset SQL scope for more complex wildcard patterns.",
            construct="ilike_pattern",
        )

    def _build_order(
        self,
        *,
        order_clause: exp.Order | None,
        alias_to_projection: dict[str, SemanticSqlProjection],
        member_to_projection: dict[str, SemanticSqlProjection],
        source_key_to_projection: dict[str, SemanticSqlProjection],
        relation_names: set[str],
        resolver: SemanticModelResolver,
    ) -> dict[str, str] | list[dict[str, str]] | None:
        if order_clause is None or not order_clause.expressions:
            return None
        entries: list[dict[str, str]] = []
        for position, ordered in enumerate(order_clause.expressions, start=1):
            projection = self._order_projection(
                ordered_expression=ordered.this,
                ordinal=position,
                alias_to_projection=alias_to_projection,
                member_to_projection=member_to_projection,
                source_key_to_projection=source_key_to_projection,
                relation_names=relation_names,
                resolver=resolver,
            )
            entries.append(
                {
                    projection.member: "desc" if bool(ordered.args.get("desc")) else "asc",
                }
            )
        if len(entries) == 1:
            return entries[0]
        return entries

    def _order_projection(
        self,
        *,
        ordered_expression: exp.Expression,
        ordinal: int,
        alias_to_projection: dict[str, SemanticSqlProjection],
        member_to_projection: dict[str, SemanticSqlProjection],
        source_key_to_projection: dict[str, SemanticSqlProjection],
        relation_names: set[str],
        resolver: SemanticModelResolver,
    ) -> SemanticSqlProjection:
        if isinstance(ordered_expression, exp.Literal) and not ordered_expression.is_string:
            index = int(str(ordered_expression.this))
            if index < 1 or index > len(alias_to_projection):
                raise SemanticSqlInvalidGroupingError(
                    f"Semantic SQL ORDER BY ordinal {ordinal} points to position {index}, but only "
                    f"{len(alias_to_projection)} select items are available. Use a valid select position "
                    "or order by a semantic member alias.",
                    construct="order_by_ordinal",
                )
            return list(alias_to_projection.values())[index - 1]

        if isinstance(ordered_expression, exp.Column):
            reference = self._member_reference(
                column=ordered_expression,
                relation_names=relation_names,
            )
            if reference in alias_to_projection:
                return alias_to_projection[reference]
            resolved = self._resolve_member(member=reference, resolver=resolver)
            projection = member_to_projection.get(resolved.member) or source_key_to_projection.get(resolved.source_key)
            if projection is not None:
                return projection
            return SemanticSqlProjection(
                kind=resolved.kind,
                member=resolved.member,
                source_key=resolved.source_key,
                output_name=reference.split(".")[-1],
            )

        if self._is_time_trunc_expression(ordered_expression):
            if not isinstance(ordered_expression.this, exp.Column):
                raise SemanticSqlInvalidTimeBucketError(
                    "Semantic SQL ORDER BY time buckets must reference a semantic time dimension column "
                    "directly. Use `ORDER BY DATE_TRUNC('month', order_date)` or the selected alias.",
                    construct="order_by_time_bucket",
                )
            reference = self._member_reference(
                column=ordered_expression.this,
                relation_names=relation_names,
            )
            resolved = self._resolve_member(member=reference, resolver=resolver)
            self._require_time_dimension(resolved=resolved, member=reference)
            granularity = self._time_granularity(ordered_expression)
            projection = source_key_to_projection.get(f"{resolved.source_key.rsplit('.', 1)[-1]}_{granularity}")
            if projection is not None:
                return projection
            return SemanticSqlProjection(
                kind="time_dimension",
                member=resolved.member,
                source_key=f"{resolved.source_key.rsplit('.', 1)[-1]}_{granularity}",
                output_name=f"{resolved.source_key.rsplit('.', 1)[-1]}_{granularity}",
                granularity=granularity,
            )

        raise SemanticSqlInvalidGroupingError(
            "Semantic SQL ORDER BY only supports semantic members, selected aliases or ordinals, "
            "and semantic time buckets. Do not use raw SQL functions or scalar expressions in semantic scope.",
            construct="order_by_expression",
        )

    def _validate_group_by(
        self,
        group_clause: exp.Group | None,
        *,
        projections: list[SemanticSqlProjection],
        alias_to_projection: dict[str, SemanticSqlProjection],
        member_to_projection: dict[str, SemanticSqlProjection],
        relation_names: set[str],
        resolver: SemanticModelResolver,
    ) -> None:
        if group_clause is None or not group_clause.expressions:
            return

        selected_dimension_keys = {
            projection.source_key
            for projection in projections
            if projection.kind in {"dimension", "time_dimension"}
        }
        grouped_dimension_keys: set[str] = set()
        grouped_projections: dict[str, SemanticSqlProjection] = {}

        for expression in group_clause.expressions:
            projection = self._group_projection(
                expression=expression,
                projections=projections,
                alias_to_projection=alias_to_projection,
                member_to_projection=member_to_projection,
                relation_names=relation_names,
                resolver=resolver,
            )
            if projection.kind not in {"dimension", "time_dimension"}:
                raise SemanticSqlInvalidGroupingError(
                    "Semantic SQL GROUP BY can only reference semantic dimensions or semantic time buckets. "
                    "Semantic metrics and measures define the aggregated values and do not belong in GROUP BY.",
                    construct="group_by_member_kind",
                )
            grouped_dimension_keys.add(projection.source_key)
            grouped_projections[projection.source_key] = projection

        if grouped_dimension_keys != selected_dimension_keys:
            missing = [
                projection.output_name
                for projection in projections
                if projection.kind in {"dimension", "time_dimension"}
                and projection.source_key in (selected_dimension_keys - grouped_dimension_keys)
            ]
            extra = [
                grouped_projections[key].output_name
                for key in (grouped_dimension_keys - selected_dimension_keys)
                if key in grouped_projections
            ]
            detail_parts: list[str] = []
            if missing:
                detail_parts.append("missing: " + ", ".join(f"`{item}`" for item in missing))
            if extra:
                detail_parts.append("extra: " + ", ".join(f"`{item}`" for item in extra))
            detail = f" ({'; '.join(detail_parts)})" if detail_parts else ""
            raise SemanticSqlInvalidGroupingError(
                "Semantic SQL GROUP BY must match the selected semantic dimensions and time buckets exactly "
                f"because they define the governed result grain{detail}. Group by each selected dimension or "
                "time bucket once, and do not group by semantic metrics.",
                construct="group_by_mismatch",
            )

    def _group_projection(
        self,
        *,
        expression: exp.Expression,
        projections: list[SemanticSqlProjection],
        alias_to_projection: dict[str, SemanticSqlProjection],
        member_to_projection: dict[str, SemanticSqlProjection],
        relation_names: set[str],
        resolver: SemanticModelResolver,
    ) -> SemanticSqlProjection:
        if isinstance(expression, exp.Literal) and not expression.is_string:
            index = int(str(expression.this))
            if index < 1 or index > len(projections):
                raise SemanticSqlInvalidGroupingError(
                    f"Semantic SQL GROUP BY ordinal {index} is out of range for {len(projections)} select items. "
                    "Use a valid select position or group by a selected semantic alias.",
                    construct="group_by_ordinal",
                )
            return projections[index - 1]

        if isinstance(expression, exp.Column):
            reference = self._member_reference(column=expression, relation_names=relation_names)
            if reference in alias_to_projection:
                return alias_to_projection[reference]
            resolved = self._resolve_member(member=reference, resolver=resolver)
            projection = member_to_projection.get(resolved.member)
            if projection is None:
                raise SemanticSqlInvalidGroupingError(
                    f"Semantic SQL GROUP BY member `{reference}` is not selected. "
                    "Select the semantic dimension or time bucket first, then GROUP BY the selected alias, "
                    "member, or ordinal.",
                    construct="group_by_unselected_member",
                )
            if projection.source_key != resolved.source_key and projection.kind != "time_dimension":
                raise SemanticSqlInvalidGroupingError(
                    f"Semantic SQL GROUP BY member `{reference}` does not match the selected projection. "
                    "Group by the exact selected semantic member or time bucket.",
                    construct="group_by_projection_mismatch",
                )
            return projection

        if self._is_time_trunc_expression(expression):
            if not isinstance(expression.this, exp.Column):
                raise SemanticSqlInvalidTimeBucketError(
                    "Semantic SQL GROUP BY time buckets must reference a semantic time dimension column "
                    "directly. Use the same time bucket expression selected in the projection, or its alias.",
                    construct="group_by_time_bucket",
                )
            reference = self._member_reference(column=expression.this, relation_names=relation_names)
            resolved = self._resolve_member(member=reference, resolver=resolver)
            self._require_time_dimension(resolved=resolved, member=reference)
            granularity = self._time_granularity(expression)
            for projection in projections:
                if (
                    projection.kind == "time_dimension"
                    and projection.member == resolved.member
                    and projection.granularity == granularity
                ):
                    return projection
            raise SemanticSqlInvalidGroupingError(
                "Semantic SQL GROUP BY time buckets must match a selected semantic time bucket exactly. "
                "Select the bucket in SELECT, then GROUP BY the same bucket or its alias.",
                construct="group_by_time_bucket_mismatch",
            )

        raise SemanticSqlInvalidGroupingError(
            "Semantic SQL GROUP BY only supports selected semantic members, matching time buckets, "
            "or ordinals that point at them. Do not use raw SQL functions or scalar expressions in semantic scope.",
            construct="group_by_expression",
        )

    @staticmethod
    def _limit_value(limit_expression: exp.Limit | None) -> int | None:
        if limit_expression is None or limit_expression.expression is None:
            return None
        try:
            return int(str(limit_expression.expression.this))
        except (TypeError, ValueError, AttributeError):
            raise SemanticSqlUnsupportedExpressionError(
                "Semantic SQL LIMIT must be an integer literal such as `LIMIT 100`.",
                construct="limit",
            ) from None

    @staticmethod
    def _literal_value(expression: exp.Expression) -> Any:
        if isinstance(expression, exp.Literal):
            if expression.is_string:
                return str(expression.this)
            raw = str(expression.this)
            try:
                return int(raw)
            except ValueError:
                try:
                    return float(raw)
                except ValueError:
                    return raw
        if isinstance(expression, exp.Boolean):
            return bool(expression.this)
        if isinstance(expression, exp.Null):
            return None
        if isinstance(expression, exp.Array):
            return [SemanticSqlFrontend._literal_value(item) for item in expression.expressions]
        if isinstance(expression, exp.Cast):
            return SemanticSqlFrontend._literal_value(expression.this)
        raise SemanticSqlInvalidFilterError(
            "Semantic SQL filters only support literal values such as strings, numbers, booleans, NULL, "
            "or literal lists. Raw SQL expressions are not supported in semantic filters.",
            construct="filter_literal",
        )

    @classmethod
    def _normalize_filter_values(
        cls,
        *,
        operator: str,
        values: list[Any],
        data_type: str | None,
    ) -> tuple[str, list[str]]:
        normalized_values = [cls._stringify_filter_value(value) for value in values]
        if (data_type or "").strip().lower() not in _TIME_DATA_TYPES or len(normalized_values) != 1:
            return operator, normalized_values
        normalized_operator, time_values = cls._normalize_time_filter_value(
            operator=operator,
            value=normalized_values[0],
        )
        return normalized_operator, time_values

    @staticmethod
    def _stringify_filter_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    @classmethod
    def _normalize_time_filter_value(cls, *, operator: str, value: str) -> tuple[str, list[str]]:
        op = operator.strip().lower()
        if op not in {"equals", "equal", "eq", "notequals", "not_equals", "ne"}:
            return operator, [value]
        normalized = cls._normalize_single_date_like_value(value)
        if normalized is None:
            return operator, [value]
        return ("notindaterange" if op in {"notequals", "not_equals", "ne"} else "indaterange"), normalized

    @staticmethod
    def _normalize_single_date_like_value(value: str) -> list[str] | None:
        trimmed = value.strip()
        if not trimmed:
            return None
        if _YEAR_PATTERN.match(trimmed):
            return [f"{trimmed}-01-01", f"{trimmed}-12-31"]
        year_month_match = _YEAR_MONTH_PATTERN.match(trimmed)
        if year_month_match:
            year_str, month_str = trimmed.split("-")
            year = int(year_str)
            month = int(month_str)
            if month == 12:
                next_year, next_month = year + 1, 1
            else:
                next_year, next_month = year, month + 1
            last_day = (datetime(next_year, next_month, 1) - datetime(year, month, 1)).days
            return [f"{trimmed}-01", f"{trimmed}-{last_day:02d}"]
        if _ISO_DATE_PATTERN.match(trimmed):
            return [f"on:{trimmed}"]
        return None

    @staticmethod
    def _require_column(expression: exp.Expression) -> exp.Column:
        if not isinstance(expression, exp.Column):
            raise SemanticSqlInvalidFilterError(
                "Semantic SQL predicates must reference semantic member columns directly. "
                "Raw SQL expressions like `LOWER(country)` are not supported in semantic filters.",
                construct="predicate_column",
            )
        return expression

    @staticmethod
    def _dedupe(values: Iterable[str]) -> list[str]:
        seen: list[str] = []
        for value in values:
            if value not in seen:
                seen.append(value)
        return seen

    @staticmethod
    def _dedupe_time_dimensions(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: list[dict[str, Any]] = []
        for item in items:
            if item not in seen:
                seen.append(item)
        return seen

    @staticmethod
    def _is_time_trunc_expression(expression: exp.Expression) -> bool:
        supported = tuple(
            candidate
            for candidate in (
                getattr(exp, "TimestampTrunc", None),
                getattr(exp, "DateTrunc", None),
            )
            if candidate is not None
        )
        return isinstance(expression, supported)

    @classmethod
    def _time_granularity(cls, expression: exp.Expression) -> str:
        unit = expression.args.get("unit")
        if unit is None:
            raise SemanticSqlInvalidTimeBucketError(
                "Semantic SQL time buckets require an explicit granularity such as month or day. "
                "Use `DATE_TRUNC('month', order_date)` or `TIMESTAMP_TRUNC(order_date, MONTH)`.",
                construct="time_bucket_granularity",
            )
        raw = str(getattr(unit, "this", unit) or "").strip().lower()
        supported = {"minute", "hour", "day", "week", "month", "quarter", "year"}
        if raw not in supported:
            raise SemanticSqlInvalidTimeBucketError(
                f"{cls._TIME_GRAIN_GUIDANCE} Received `{raw or 'unknown'}`. Use a supported granularity or "
                "move the query to dataset SQL scope for free-form SQL date logic.",
                construct="time_bucket_granularity",
            )
        return raw

    @staticmethod
    def _expression_sql(expression: exp.Expression) -> str:
        try:
            return expression.sql()
        except Exception:
            return str(expression)

    @staticmethod
    def _contains_aggregate_expression(expression: exp.Expression) -> bool:
        aggregate_types = tuple(
            candidate
            for candidate in (
                getattr(exp, "AggFunc", None),
                getattr(exp, "Count", None),
                getattr(exp, "Min", None),
                getattr(exp, "Max", None),
                getattr(exp, "Sum", None),
                getattr(exp, "Avg", None),
            )
            if candidate is not None
        )
        if aggregate_types and isinstance(expression, aggregate_types):
            return True
        finder = getattr(expression, "find", None)
        if callable(finder) and getattr(exp, "AggFunc", None) is not None:
            return finder(exp.AggFunc) is not None
        return False

    @staticmethod
    def _member_suggestions(*, member: str, resolver: SemanticModelResolver) -> list[str]:
        normalized_member = str(member or "").strip().lower()
        suffix = normalized_member.rsplit(".", 1)[-1]
        suggestions: list[str] = []
        for dataset_name, dataset in resolver.model.datasets.items():
            for dimension in dataset.dimensions or []:
                candidate = f"{dataset_name}.{dimension.name}"
                if normalized_member in candidate.lower() or suffix == str(dimension.name).lower():
                    suggestions.append(candidate)
            for measure in dataset.measures or []:
                candidate = f"{dataset_name}.{measure.name}"
                if normalized_member in candidate.lower() or suffix == str(measure.name).lower():
                    suggestions.append(candidate)
        for metric_name in (resolver.model.metrics or {}):
            candidate = str(metric_name)
            if normalized_member in candidate.lower() or suffix == candidate.lower():
                suggestions.append(candidate)

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in suggestions:
            if candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        return deduped

    @classmethod
    def _require_time_dimension(
        cls,
        *,
        resolved: ResolvedSemanticMember,
        member: str,
    ) -> None:
        normalized_type = str(resolved.data_type or "").strip().lower()
        if resolved.kind != "dimension" or normalized_type not in {"date", "datetime", "timestamp", "time"}:
            raise SemanticSqlInvalidTimeBucketError(
                f"Semantic SQL time buckets only support semantic time dimensions, but `{member}` is not a "
                f"semantic time dimension. {cls._TIME_GRAIN_GUIDANCE} Add a semantic time dimension to the "
                "model or use dataset SQL scope for raw SQL date logic.",
                construct="time_bucket_member",
            )
