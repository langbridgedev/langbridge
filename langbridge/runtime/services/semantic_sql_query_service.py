from __future__ import annotations

from typing import Any, Iterable

from langbridge.runtime.utils.sql import normalize_sql_dialect, render_sql_with_params
from langbridge.semantic.model import SemanticModel
from langbridge.semantic.query.semantic_sql import (
    ParsedSemanticSqlQuery,
    SemanticSqlFrontend,
    SemanticSqlProjection,
    SemanticSqlQueryPlan,
)


def normalize_semantic_sql_row_key(*, key: str, dataset_names: set[str]) -> str:
    normalized_key = str(key or "")
    if not normalized_key:
        return normalized_key

    dataset_names_by_lower = {dataset_name.lower(): dataset_name for dataset_name in dataset_names}
    if normalized_key.count("__") >= 2:
        _, dataset_name, suffix = normalized_key.split("__", 2)
        resolved_dataset = dataset_names_by_lower.get(dataset_name.lower())
        if resolved_dataset:
            return f"{resolved_dataset}.{suffix}"
    elif "__" in normalized_key:
        dataset_name, suffix = normalized_key.split("__", 1)
        resolved_dataset = dataset_names_by_lower.get(dataset_name.lower())
        if resolved_dataset:
            return f"{resolved_dataset}.{suffix}"
    return normalized_key


def build_semantic_sql_metadata_columns_by_source(
    metadata: Iterable[dict[str, Any]] | None,
) -> dict[str, list[str]]:
    columns_by_source: dict[str, list[str]] = {}
    for item in metadata or []:
        if not isinstance(item, dict):
            continue
        source = str(item.get("source") or "").strip()
        column = str(item.get("column") or "").strip()
        if not source or not column:
            continue
        columns_by_source.setdefault(source, []).append(column)
    return columns_by_source


def _semantic_sql_projection_candidates(
    *,
    projection: SemanticSqlProjection,
    metadata_columns_by_source: dict[str, list[str]],
    dataset_names: set[str],
) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(candidate: str | None) -> None:
        value = str(candidate or "").strip()
        if not value:
            return
        for item in (
            value,
            normalize_semantic_sql_row_key(key=value, dataset_names=dataset_names),
            value.replace(".", "__") if "." in value else value,
            value.rsplit(".", 1)[-1],
        ):
            normalized_item = str(item or "").strip()
            if normalized_item and normalized_item not in seen:
                seen.add(normalized_item)
                candidates.append(normalized_item)

    add(projection.source_key)
    add(projection.output_name)
    add(projection.member)

    if projection.kind == "time_dimension" and projection.granularity:
        add(f"{projection.member}_{projection.granularity}")
        add(f"{projection.member.split('.')[-1]}_{projection.granularity}")

    for source_key in (projection.member, projection.source_key):
        for column in metadata_columns_by_source.get(str(source_key or ""), []):
            add(column)

    return candidates


def resolve_semantic_sql_projection_value(
    *,
    row: dict[str, Any],
    projection: SemanticSqlProjection,
    metadata_columns_by_source: dict[str, list[str]] | None = None,
    dataset_names: set[str] | None = None,
) -> Any:
    row_keys_by_lower = {
        str(key).strip().lower(): key
        for key in row.keys()
        if str(key or "").strip()
    }
    for candidate in _semantic_sql_projection_candidates(
        projection=projection,
        metadata_columns_by_source=metadata_columns_by_source or {},
        dataset_names=dataset_names or set(),
    ):
        if candidate in row:
            return row[candidate]
        resolved_key = row_keys_by_lower.get(candidate.lower())
        if resolved_key is not None:
            return row[resolved_key]
    return None


class SemanticSqlQueryService:
    def __init__(self, frontend: SemanticSqlFrontend | None = None) -> None:
        self._frontend = frontend or SemanticSqlFrontend()

    def parse_query(
        self,
        *,
        query: str,
        query_dialect: str,
        params: dict[str, Any] | None = None,
    ) -> ParsedSemanticSqlQuery:
        rendered_query = render_sql_with_params(str(query or "").strip(), params or {})
        dialect = normalize_sql_dialect(query_dialect, default="tsql")
        return self._frontend.parse_query(
            query=rendered_query,
            query_dialect=dialect,
        )

    def build_query_plan(
        self,
        *,
        parsed_query: ParsedSemanticSqlQuery,
        semantic_model: SemanticModel,
        requested_limit: int | None = None,
    ) -> SemanticSqlQueryPlan:
        return self._frontend.build_query_plan(
            parsed_query=parsed_query,
            semantic_model=semantic_model,
            requested_limit=requested_limit,
        )
