
import re
from dataclasses import dataclass
from typing import Mapping, Sequence
from uuid import UUID

from langbridge.semantic.model import Metric, Relationship, SemanticModel
from langbridge.semantic.unified_model import UnifiedSemanticRelationship


@dataclass(frozen=True)
class UnifiedSourceModel:
    model: SemanticModel
    connector_id: UUID | None
    key: str | None = None
    alias: str | None = None
    name: str | None = None
    model_id: UUID | None = None
    description: str | None = None


@dataclass(frozen=True)
class WorkspaceAwareQueryContext:
    workspace_id: UUID
    execution_connector_id: UUID


@dataclass(frozen=True)
class _ResolvedMember:
    dataset_key: str
    field_name: str


class _SourceModelResolver:
    def __init__(self, source: UnifiedSourceModel) -> None:
        self._source = source
        self._member_index: dict[str, list[_ResolvedMember]] = {}
        self._qualified_index: dict[str, _ResolvedMember] = {}
        self._build_indexes()

    def _build_indexes(self) -> None:
        for dataset_key, dataset in self._source.model.datasets.items():
            for field in list(dataset.dimensions or []) + list(dataset.measures or []):
                resolved = _ResolvedMember(dataset_key=dataset_key, field_name=field.name)
                self._member_index.setdefault(field.name, []).append(resolved)
                self._qualified_index[f"{dataset_key}.{field.name}"] = resolved

    def resolve(self, member_ref: str) -> _ResolvedMember:
        normalized = str(member_ref or "").strip()
        if not normalized:
            raise ValueError(f"Unified semantic model field reference is empty for '{_source_key(self._source)}'.")
        if normalized in self._qualified_index:
            return self._qualified_index[normalized]
        matches = self._member_index.get(normalized, [])
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError(
                f"Field '{normalized}' is ambiguous in semantic model '{_source_key(self._source)}'. "
                "Use dataset.field notation within the semantic model."
            )
        raise ValueError(f"Field '{normalized}' was not found in semantic model '{_source_key(self._source)}'.")


_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_]*"
_MODEL_MEMBER_RE = re.compile(rf"\b(?P<model>{_IDENTIFIER})\.(?P<member>{_IDENTIFIER}(?:\.{_IDENTIFIER})?)\b")


def build_unified_semantic_model(
    *,
    source_models: Sequence[UnifiedSourceModel],
    relationships: Sequence[Mapping[str, object]] | None = None,
    metrics: Mapping[str, object] | None = None,
    name: str | None = None,
    description: str | None = None,
    dialect: str | None = None,
    version: str = "1.0",
) -> tuple[SemanticModel, dict[str, UUID]]:
    if not source_models:
        raise ValueError("source_models must include at least one semantic model.")

    resolvers = {_source_key(source): _SourceModelResolver(source) for source in source_models}
    merged_datasets = {}
    merged_relationships: list[Relationship] = []
    merged_metrics: dict[str, Metric] = {}
    dataset_connector_map: dict[str, UUID] = {}

    for source in source_models:
        source_key = _source_key(source)
        for dataset_key, dataset in source.model.datasets.items():
            materialized_key = _materialized_dataset_key(source_key, dataset_key)
            if materialized_key in merged_datasets:
                raise ValueError(f"Duplicate dataset key '{materialized_key}' detected while building unified model.")
            merged_datasets[materialized_key] = dataset.model_copy(deep=True)
            if source.connector_id is not None:
                dataset_connector_map[materialized_key] = source.connector_id

        for relationship in source.model.relationships or []:
            merged_relationships.append(
                Relationship(
                    name=f"{source_key}__{relationship.name}",
                    source_dataset=_materialized_dataset_key(source_key, relationship.source_dataset),
                    source_field=relationship.source_field,
                    target_dataset=_materialized_dataset_key(source_key, relationship.target_dataset),
                    target_field=relationship.target_field,
                    operator=relationship.operator,
                    type=getattr(relationship, "type", getattr(relationship, "relationship_type", "inner")),
                )
            )

        for metric_key, metric in (source.model.metrics or {}).items():
            merged_metrics[f"{source_key}.{metric_key}"] = Metric(
                description=metric.description,
                expression=_rewrite_source_metric_expression(source, metric.expression),
            )

    for relationship_payload in relationships or []:
        relationship = UnifiedSemanticRelationship.model_validate(dict(relationship_payload))
        source = _require_source_model_by_id(source_models, relationship.source_semantic_model_id)
        target = _require_source_model_by_id(source_models, relationship.target_semantic_model_id)
        source_ref = resolvers[_source_key(source)].resolve(relationship.source_field)
        target_ref = resolvers[_source_key(target)].resolve(relationship.target_field)
        merged_relationships.append(
            Relationship(
                name=relationship.name
                or _build_relationship_name(
                    source=source,
                    source_ref=source_ref,
                    target=target,
                    target_ref=target_ref,
                ),
                source_dataset=_materialized_dataset_key(_source_key(source), source_ref.dataset_key),
                source_field=source_ref.field_name,
                target_dataset=_materialized_dataset_key(_source_key(target), target_ref.dataset_key),
                target_field=target_ref.field_name,
                operator=relationship.operator,
                type=getattr(relationship, "type", getattr(relationship, "relationship_type", "inner")),
            )
        )

    for metric_name, metric_value in (metrics or {}).items():
        metric = metric_value if isinstance(metric_value, Metric) else Metric.model_validate(metric_value)
        merged_metrics[str(metric_name)] = Metric(
            description=metric.description,
            expression=_rewrite_unified_metric_expression(metric.expression, source_models, resolvers),
        )

    unified_model = SemanticModel(
        version=version,
        name=name,
        description=description,
        dialect=dialect,
        datasets=merged_datasets,
        relationships=merged_relationships or None,
        metrics=merged_metrics or None,
    )
    return unified_model, dataset_connector_map


def apply_workspace_aware_context(
    semantic_model: SemanticModel,
    *,
    context: WorkspaceAwareQueryContext,
    table_connector_map: Mapping[str, UUID] | None = None,
) -> SemanticModel:
    model_copy = semantic_model.model_copy(deep=True)

    for dataset_key, dataset in model_copy.datasets.items():
        if dataset.catalog_name:
            continue

        schema = (dataset.schema_name or "").strip()
        if "." in schema:
            catalog, normalized_schema = schema.split(".", 1)
            dataset.catalog_name = catalog
            dataset.schema_name = normalized_schema
            continue

        connector_id = context.execution_connector_id
        if table_connector_map and dataset_key in table_connector_map:
            connector_id = table_connector_map[dataset_key]

        dataset.catalog_name = _build_catalog_token(
            workspace_id=context.workspace_id,
            connector_id=connector_id,
        )

    return model_copy


def _build_catalog_token(*, workspace_id: UUID, connector_id: UUID) -> str:
    workspace_token = workspace_id.hex[:12]
    connector_token = connector_id.hex[:12]
    return f"ws_{workspace_token}__src_{connector_token}"


def _materialized_dataset_key(model_key: str, dataset_key: str) -> str:
    return f"{model_key}__{dataset_key}"


def _rewrite_source_metric_expression(source: UnifiedSourceModel, expression: str) -> str:
    rewritten = str(expression)
    for dataset_key in sorted(source.model.datasets.keys(), key=len, reverse=True):
        rewritten = re.sub(
            rf"\b{re.escape(dataset_key)}\.",
            f"{_materialized_dataset_key(_source_key(source), dataset_key)}.",
            rewritten,
        )
    return rewritten


def _rewrite_unified_metric_expression(
    expression: str,
    source_models: Sequence[UnifiedSourceModel],
    resolvers: Mapping[str, _SourceModelResolver],
) -> str:
    source_lookup = {_source_key(source): source for source in source_models}

    def _replace(match: re.Match[str]) -> str:
        model_key = match.group("model")
        member_ref = match.group("member")
        source = source_lookup.get(model_key)
        if source is None:
            return match.group(0)
        resolved = resolvers[model_key].resolve(member_ref)
        return f"{_materialized_dataset_key(_source_key(source), resolved.dataset_key)}.{resolved.field_name}"

    return _MODEL_MEMBER_RE.sub(_replace, str(expression))


def _source_key(source: UnifiedSourceModel) -> str:
    explicit = str(source.key or source.alias or "").strip()
    if explicit:
        return explicit
    if source.name and source.name.strip():
        normalized = re.sub(r"[^0-9A-Za-z_]+", "_", source.name.strip().lower()).strip("_")
        if normalized:
            return normalized
    if source.model_id is not None:
        return f"model_{source.model_id.hex[:8]}"
    return "model"


def _require_source_model_by_id(source_models: Sequence[UnifiedSourceModel], model_id: UUID) -> UnifiedSourceModel:
    for source_model in source_models:
        if source_model.model_id == model_id:
            return source_model
    raise ValueError(f"Semantic model '{model_id}' is not available in the unified semantic model.")


def _build_relationship_name(
    *,
    source: UnifiedSourceModel,
    source_ref: _ResolvedMember,
    target: UnifiedSourceModel,
    target_ref: _ResolvedMember,
) -> str:
    return (
        f"{_source_key(source)}__{source_ref.dataset_key}_{source_ref.field_name}"
        f"__to__{_source_key(target)}__{target_ref.dataset_key}_{target_ref.field_name}"
    )
