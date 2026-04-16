
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from langbridge.semantic.errors import SemanticModelError
from langbridge.semantic.graph import (
    SemanticGraph,
    SemanticGraphRelationship,
    SemanticGraphSourceModel,
)
from langbridge.semantic.model import Metric, Relationship, SemanticModel


def load_semantic_model(source: str | Mapping[str, Any] | Path) -> SemanticModel:
    payload = _load_mapping(source)
    return parse_semantic_model_payload(payload)


def load_semantic_graph(source: str | Mapping[str, Any] | Path) -> SemanticGraph:
    payload = _load_mapping(source)
    return parse_semantic_graph_payload(payload)


def load_unified_semantic_model(source: str | Mapping[str, Any] | Path) -> SemanticGraph:
    return load_semantic_graph(source)


def parse_semantic_model_payload(payload: Mapping[str, Any]) -> SemanticModel:
    if (
        "source_models" in payload
        or "sourceModels" in payload
        or "semantic_models" in payload
    ):
        raise SemanticModelError(
            "Semantic graph payloads must be loaded with load_semantic_graph()."
        )
    if "datasets" in payload or "tables" in payload:
        return _parse_standard_payload(payload)
    raise SemanticModelError("Semantic model payload must define datasets.")


def parse_semantic_graph_payload(payload: Mapping[str, Any]) -> SemanticGraph:
    if "datasets" in payload or "tables" in payload:
        raise SemanticModelError(
            "Semantic graphs cannot define datasets, tables, dimensions, or measures."
        )
    if "joins" in payload:
        raise SemanticModelError(
            "Semantic graphs must define relationships instead of joins."
        )
    if "semantic_models" in payload:
        raise SemanticModelError(
            "Semantic graphs must reference source_models instead of embedding semantic_models."
        )

    source_models_raw = payload.get("source_models") or payload.get("sourceModels")
    if not isinstance(source_models_raw, list) or not source_models_raw:
        raise SemanticModelError("Semantic graph must define at least one source model.")

    relationships = _parse_semantic_graph_relationships(payload.get("relationships"))
    metrics = _parse_metrics(payload.get("metrics"))

    normalized = {
        "version": str(payload.get("version") or "1.0"),
        "name": payload.get("name"),
        "description": payload.get("description"),
        "source_models": _parse_source_models(source_models_raw),
        "relationships": relationships,
        "metrics": metrics,
    }
    try:
        return SemanticGraph.model_validate(normalized)
    except Exception as exc:
        raise SemanticModelError(f"Invalid semantic graph: {exc}") from exc


def parse_unified_semantic_model_payload(payload: Mapping[str, Any]) -> SemanticGraph:
    return parse_semantic_graph_payload(payload)


def _load_mapping(source: str | Mapping[str, Any] | Path) -> dict[str, Any]:
    if isinstance(source, Path):
        return _load_mapping(source.read_text(encoding="utf-8"))
    if isinstance(source, Mapping):
        payload = dict(source)
    else:
        try:
            payload = yaml.safe_load(source)
        except yaml.YAMLError as exc:
            raise SemanticModelError(f"Unable to parse semantic model payload: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise SemanticModelError("Semantic model payload must be a mapping.")
    return dict(payload)


def _parse_standard_payload(payload: Mapping[str, Any]) -> SemanticModel:
    normalized = dict(payload)
    raw_datasets = payload.get("datasets") if isinstance(payload.get("datasets"), Mapping) else payload.get("tables")
    if not isinstance(raw_datasets, Mapping) or not raw_datasets:
        raise SemanticModelError("Semantic model payload must define at least one dataset.")

    normalized["version"] = str(payload.get("version") or "1.0")
    normalized["datasets"] = _normalize_datasets(raw_datasets)
    normalized.pop("tables", None)
    normalized["relationships"] = _parse_relationships(payload.get("relationships") or payload.get("joins")) or None
    normalized["metrics"] = _parse_metrics(payload.get("metrics")) or None
    return SemanticModel.model_validate(normalized)


def _parse_source_models(source_models_raw: list[Any]) -> list[SemanticGraphSourceModel]:
    parsed: list[SemanticGraphSourceModel] = []
    for entry in source_models_raw:
        if not isinstance(entry, Mapping):
            continue
        try:
            parsed.append(SemanticGraphSourceModel.model_validate(dict(entry)))
        except Exception as exc:
            raise SemanticModelError(f"Invalid semantic graph source model: {exc}") from exc
    return parsed


def _normalize_datasets(raw_datasets: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for dataset_key, raw_value in raw_datasets.items():
        if not isinstance(raw_value, Mapping):
            continue
        value = dict(raw_value)
        value.setdefault("relation_name", str(dataset_key))
        normalized[str(dataset_key)] = value
    return normalized


def _parse_relationships(value: Any) -> list[Relationship]:
    if not isinstance(value, list):
        return []
    relationships: list[Relationship] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        try:
            relationships.append(Relationship.model_validate(dict(item)))
        except Exception as exc:
            raise SemanticModelError(f"Invalid semantic relationship: {exc}") from exc
    return relationships


def _parse_semantic_graph_relationships(value: Any) -> list[SemanticGraphRelationship]:
    if not isinstance(value, list):
        return []
    relationships: list[SemanticGraphRelationship] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        try:
            relationships.append(SemanticGraphRelationship.model_validate(dict(item)))
        except Exception as exc:
            raise SemanticModelError(f"Invalid semantic graph relationship: {exc}") from exc
    return relationships


def _parse_unified_relationships(value: Any) -> list[SemanticGraphRelationship]:
    return _parse_semantic_graph_relationships(value)


def _parse_metrics(value: Any) -> dict[str, Metric]:
    if not isinstance(value, Mapping):
        return {}
    metrics: dict[str, Metric] = {}
    for metric_key, raw_value in value.items():
        if not isinstance(raw_value, Mapping):
            continue
        try:
            metrics[str(metric_key)] = Metric.model_validate(dict(raw_value))
        except Exception as exc:
            raise SemanticModelError(f"Invalid semantic metric '{metric_key}': {exc}") from exc
    return metrics
