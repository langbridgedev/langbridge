from dataclasses import dataclass
import re
from typing import Any, Callable, Iterable, List, Sequence

import sqlglot
from sqlglot import exp

from langbridge.semantic.model import SemanticModel
from .query_model import SemanticQuery
from .resolver import MetricRef, SemanticModelResolver
from .translator import TsqlSemanticTranslator

RewriteExpression = Callable[[sqlglot.Expression], sqlglot.Expression]


@dataclass(frozen=True)
class SemanticQueryPlan:
    sql: str
    annotations: List[dict[str, str]]
    metadata: List[dict[str, str]]


class SemanticQueryEngine:
    """
    Core semantic query compiler that is agnostic of service-layer dependencies.
    """

    def __init__(self, translator: TsqlSemanticTranslator | None = None) -> None:
        self._translator = translator or TsqlSemanticTranslator()

    def compile(
        self,
        semantic_query: SemanticQuery,
        semantic_model: SemanticModel,
        *,
        dialect: str,
        rewrite_expression: RewriteExpression | None = None,
    ) -> SemanticQueryPlan:
        tree = self._translator.translate(
            semantic_query,
            semantic_model,
            dialect=dialect,
        )
        sql = self._transpile(tree, dialect=dialect, rewrite_expression=rewrite_expression)
        return SemanticQueryPlan(
            sql=sql,
            annotations=self.build_annotations(semantic_model),
            metadata=self.build_result_metadata(semantic_query, semantic_model),
        )
        
    def compile_from_sql(
        self,
        sql: str,
        semantic_model: SemanticModel,
        *,
        dialect: str,
        rewrite_expression: RewriteExpression | None = None,
    ) -> SemanticQueryPlan:
        tree = sqlglot.parse_one(sql, read=dialect)
        return SemanticQueryPlan(
            sql=self._transpile(tree, dialect=dialect, rewrite_expression=rewrite_expression),
            annotations=self.build_annotations(semantic_model),
            metadata=[],
        )

    @staticmethod
    def build_annotations(semantic_model: SemanticModel) -> List[dict[str, str]]:
        annotations: List[dict[str, str]] = []
        for dataset_key, dataset in semantic_model.datasets.items():
            for column, name in dataset.get_annotations(dataset_key).items():
                annotations.append({"column": column, "name": name})
        return annotations

    def build_result_metadata(
        self,
        semantic_query: SemanticQuery,
        semantic_model: SemanticModel,
    ) -> List[dict[str, str]]:
        resolver = SemanticModelResolver(semantic_model)
        metadata: List[dict[str, str]] = []

        for member in semantic_query.dimensions:
            ref = resolver.resolve_dimension(member)
            alias = self._alias_for_member(f"{ref.dataset}.{ref.column}")
            label = ref.alias or ref.column
            metadata.append(
                {"column": alias, "name": label, "source": f"{ref.dataset}.{ref.column}"}
            )

        for time_dimension in semantic_query.time_dimensions:
            ref = resolver.resolve_dimension(time_dimension.dimension)
            alias = self._alias_for_time_dimension(
                ref.dataset, ref.column, time_dimension.granularity
            )
            label = ref.alias or ref.column
            if time_dimension.granularity:
                label = f"{label} ({time_dimension.granularity})"
            metadata.append(
                {"column": alias, "name": label, "source": f"{ref.dataset}.{ref.column}"}
            )

        for member in semantic_query.measures:
            resolved = resolver.resolve_measure_or_metric(member)
            if isinstance(resolved, MetricRef):
                alias = self._alias_for_member(resolved.key)
                metadata.append({"column": alias, "name": resolved.key, "source": resolved.key})
                continue
            alias = self._alias_for_member(f"{resolved.dataset}.{resolved.column}")
            metadata.append(
                {
                    "column": alias,
                    "name": resolved.column,
                    "source": f"{resolved.dataset}.{resolved.column}",
                }
            )

        return metadata

    @staticmethod
    def format_rows(
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
    ) -> List[dict[str, Any]]:
        return [dict(zip(columns, row)) for row in rows]

    @staticmethod
    def _transpile(
        tree: exp.Select,
        *,
        dialect: str,
        rewrite_expression: RewriteExpression | None = None,
    ) -> str:
        if rewrite_expression:
            rewritten = tree.transform(lambda node: rewrite_expression(node))
            return rewritten.sql(dialect=dialect)
        return tree.sql(dialect=dialect)

    @staticmethod
    def _alias_for_member(member: str) -> str:
        alias = member.replace(".", "__").replace(" ", "_")
        return re.sub(r"[^A-Za-z0-9_]+", "_", alias)

    def _alias_for_time_dimension(
        self,
        dataset: str,
        column: str,
        granularity: str | None,
    ) -> str:
        base = self._alias_for_member(f"{dataset}.{column}")
        if not granularity:
            return base
        return f"{base}_{granularity}"
