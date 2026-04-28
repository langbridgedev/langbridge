import json
import uuid
from typing import Any

import sqlglot
from sqlglot import exp

from langbridge.runtime.models import (
    CreateDatasetPreviewJobRequest,
    DatasetColumnMetadata,
    DatasetPolicyMetadata,
)
from langbridge.runtime.services.errors import ExecutionValidationError
from langbridge.runtime.utils.sql import render_sql_with_params


class DatasetQuerySqlBuilder:
    """Builds safe preview and profiling SQL for dataset query jobs."""

    def build_preview_sql(
        self,
        *,
        table_key: str,
        columns: list[DatasetColumnMetadata],
        policy: DatasetPolicyMetadata,
        request: CreateDatasetPreviewJobRequest,
        effective_limit: int,
        dialect: str,
    ) -> str:
        select_expr = exp.select()
        allowed_columns = [column for column in columns if column.is_allowed]

        if not allowed_columns:
            select_expr = select_expr.select(exp.Star())
        else:
            projections: list[exp.Expression] = []
            for column in allowed_columns:
                if column.is_computed and column.expression:
                    try:
                        parsed_expression = sqlglot.parse_one(column.expression, read=dialect)
                        projections.append(exp.alias_(parsed_expression, column.name, quoted=True))
                    except sqlglot.ParseError:
                        continue
                    continue
                projections.append(exp.Column(this=exp.Identifier(this=column.name, quoted=True)))
            select_expr = select_expr.select(*(projections or [exp.Star()]))

        select_expr = select_expr.from_(exp.table_(table_key, quoted=False))

        filter_expressions = self.build_filter_expressions(
            filters=request.filters,
            allowed_columns=allowed_columns,
            dialect=dialect,
        )
        filter_expressions.extend(
            self.build_row_filter_expressions(
                policy=policy,
                request_context=request.user_context,
                workspace_id=request.workspace_id,
                actor_id=request.actor_id,
                dialect=dialect,
            )
        )
        if filter_expressions:
            select_expr = select_expr.where(exp.and_(*filter_expressions))

        order_items: list[exp.Ordered] = []
        allowed_names = {column.name.lower() for column in allowed_columns}
        for item in request.sort:
            column = str(item.get("column") or "").strip()
            direction = str(item.get("direction") or "asc").strip().lower()
            if not column:
                continue
            if allowed_names and column.lower() not in allowed_names:
                continue
            order_items.append(
                exp.Ordered(
                    this=exp.Column(this=exp.Identifier(this=column, quoted=True)),
                    desc=direction == "desc",
                )
            )
        if order_items:
            select_expr = select_expr.order_by(*order_items)

        select_expr = select_expr.limit(effective_limit)
        return select_expr.sql(dialect=dialect)

    def build_filter_expressions(
        self,
        *,
        filters: dict[str, Any],
        allowed_columns: list[DatasetColumnMetadata],
        dialect: str,
    ) -> list[exp.Expression]:
        if not filters:
            return []

        allowed_names = {column.name.lower() for column in allowed_columns}
        expressions: list[exp.Expression] = []
        for raw_column, raw_value in filters.items():
            column = str(raw_column or "").strip()
            if not column:
                continue
            if allowed_names and column.lower() not in allowed_names:
                continue

            column_expr = exp.Column(this=exp.Identifier(this=column, quoted=True))
            if isinstance(raw_value, dict):
                operator = str(raw_value.get("operator") or "eq").strip().lower()
                value = raw_value.get("value")
                expressions.extend(
                    self.apply_operator_filter(
                        column_expr=column_expr,
                        operator=operator,
                        value=value,
                        dialect=dialect,
                    )
                )
                continue

            if isinstance(raw_value, list):
                literals = [self.literal_expression(item, dialect=dialect) for item in raw_value]
                expressions.append(exp.In(this=column_expr, expressions=literals))
                continue

            expressions.append(
                exp.EQ(this=column_expr, expression=self.literal_expression(raw_value, dialect=dialect))
            )

        return expressions

    def apply_operator_filter(
        self,
        *,
        column_expr: exp.Column,
        operator: str,
        value: Any,
        dialect: str,
    ) -> list[exp.Expression]:
        literal = self.literal_expression(value, dialect=dialect)
        if operator in {"eq", "equals"}:
            return [exp.EQ(this=column_expr, expression=literal)]
        if operator in {"neq", "not_equals"}:
            return [exp.NEQ(this=column_expr, expression=literal)]
        if operator in {"gt", "greater_than"}:
            return [exp.GT(this=column_expr, expression=literal)]
        if operator in {"gte", "greater_than_or_equal"}:
            return [exp.GTE(this=column_expr, expression=literal)]
        if operator in {"lt", "less_than"}:
            return [exp.LT(this=column_expr, expression=literal)]
        if operator in {"lte", "less_than_or_equal"}:
            return [exp.LTE(this=column_expr, expression=literal)]
        if operator in {"contains", "like"}:
            return [
                exp.Like(
                    this=column_expr,
                    expression=self.literal_expression(f"%{value}%", dialect=dialect),
                )
            ]
        if operator == "in" and isinstance(value, list):
            return [
                exp.In(
                    this=column_expr,
                    expressions=[self.literal_expression(item, dialect=dialect) for item in value],
                )
            ]
        return [exp.EQ(this=column_expr, expression=literal)]

    def build_row_filter_expressions(
        self,
        *,
        policy: DatasetPolicyMetadata,
        request_context: dict[str, Any],
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID,
        dialect: str,
    ) -> list[exp.Expression]:
        templates = list(policy.row_filters_json or [])
        if not templates:
            return []

        render_context: dict[str, Any] = {
            "workspace_id": str(workspace_id),
            "actor_id": str(actor_id),
        }
        render_context.update(request_context or {})

        expressions: list[exp.Expression] = []
        for template in templates:
            if not isinstance(template, str) or not template.strip():
                continue
            rendered = render_sql_with_params(template, render_context)
            try:
                expressions.append(sqlglot.parse_one(rendered, read=dialect))
            except sqlglot.ParseError as exc:
                raise ExecutionValidationError(f"Invalid row filter policy expression: {exc}") from exc
        return expressions

    def build_count_sql(
        self,
        *,
        table_key: str,
        filters: list[exp.Expression],
        dialect: str,
    ) -> str:
        query = (
            exp.select(exp.alias_(exp.Count(this=exp.Star()), "row_count", quoted=True))
            .from_(exp.table_(table_key, quoted=False))
        )
        if filters:
            query = query.where(exp.and_(*filters))
        return query.sql(dialect=dialect)

    def build_column_profile_sql(
        self,
        *,
        table_key: str,
        column_name: str,
        filters: list[exp.Expression],
        dialect: str,
    ) -> str:
        column_expr = exp.Column(this=exp.Identifier(this=column_name, quoted=True))
        distinct_expr = exp.alias_(
            exp.Count(this=column_expr.copy(), distinct=True),
            "distinct_count",
            quoted=True,
        )
        null_expr = exp.alias_(
            exp.Sum(
                this=exp.Case(
                    ifs=[
                        (
                            exp.Is(this=column_expr.copy(), expression=exp.Null()),
                            exp.Literal.number(1),
                        )
                    ],
                    default=exp.Literal.number(0),
                )
            ),
            "null_count",
            quoted=True,
        )
        query = exp.select(distinct_expr, null_expr).from_(exp.table_(table_key, quoted=False))
        if filters:
            query = query.where(exp.and_(*filters))
        return query.sql(dialect=dialect)

    def literal_expression(self, value: Any, *, dialect: str) -> exp.Expression:
        if value is None:
            return exp.Null()
        if isinstance(value, bool):
            return exp.true() if value else exp.false()
        if isinstance(value, (int, float)):
            return exp.Literal.number(value)
        if isinstance(value, (dict, list)):
            return exp.Literal.string(json.dumps(value))
        return exp.Literal.string(str(value))
