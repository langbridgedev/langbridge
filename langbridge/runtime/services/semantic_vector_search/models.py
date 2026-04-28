from collections.abc import Mapping
from typing import Any

import yaml

from langbridge.semantic.loader import load_semantic_model
from langbridge.semantic.model import SemanticModel


class SemanticVectorModelLoader:
    """Loads semantic model YAML and raw dataset payloads."""

    def load_model(self, content_yaml: str) -> SemanticModel:
        return load_semantic_model(content_yaml)

    def extract_raw_datasets(self, model_record: Any) -> Mapping[str, Any] | None:
        payload: Any = None
        content_json = getattr(model_record, "content_json", None)
        if isinstance(content_json, dict):
            payload = content_json
        elif isinstance(content_json, str) and content_json.strip():
            try:
                payload = yaml.safe_load(content_json)
            except Exception:
                payload = None
        if payload is None:
            try:
                payload = yaml.safe_load(getattr(model_record, "content_yaml", "") or "")
            except Exception:
                payload = None
        if not isinstance(payload, Mapping):
            return None
        datasets = payload.get("datasets")
        if isinstance(datasets, Mapping):
            return datasets
        tables = payload.get("tables")
        if isinstance(tables, Mapping):
            return tables
        return None
