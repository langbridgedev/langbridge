import re
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, PrivateAttr, model_validator


class MeasureAggregation(str, Enum):
    sum = "sum"
    avg = "avg"
    min = "min"
    max = "max"
    _count = "count"


class DimensionVectorStore(BaseModel):
    type: Literal["managed_faiss", "connector"] = "managed_faiss"
    connector_name: str | None = None
    index_name: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_shape(cls, value: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, str):
            normalized_value = value.strip()
            if not normalized_value or normalized_value == "managed_faiss":
                return {"type": "managed_faiss"}
            return {
                "type": "connector",
                "connector_name": normalized_value,
            }
        if not isinstance(value, dict):
            return value

        normalized = dict(value)
        connector_name = (
            normalized.get("connector_name")
            or normalized.get("connectorName")
            or normalized.get("connector")
            or normalized.get("name")
        )
        if connector_name is not None:
            normalized["connector_name"] = connector_name

        store_type = (
            normalized.get("type")
            or normalized.get("store_type")
            or normalized.get("storeType")
            or normalized.get("kind")
        )
        if store_type is None:
            normalized["type"] = (
                "connector"
                if normalized.get("connector_name")
                else "managed_faiss"
            )
            return normalized

        lowered = str(store_type).strip().lower()
        if lowered in {"managed", "managed_faiss", "faiss", "default"}:
            normalized["type"] = "managed_faiss"
        elif lowered in {"connector", "vector_connector", "vector_db_connector"}:
            normalized["type"] = "connector"
        return normalized

    @model_validator(mode="after")
    def _validate_shape(self) -> "DimensionVectorStore":
        if self.type == "connector" and not str(self.connector_name or "").strip():
            raise ValueError("vector.store.connector_name is required when type='connector'.")
        if self.type != "connector":
            self.connector_name = None
        return self


class DimensionVectorConfig(BaseModel):
    enabled: bool = True
    refresh_interval: str | None = None
    max_values: int | None = None
    store: DimensionVectorStore = Field(default_factory=DimensionVectorStore)

    @model_validator(mode="before")
    @classmethod
    def _normalize_shape(cls, value: Any) -> Any:
        if value is None:
            return value
        if isinstance(value, bool):
            return {"enabled": value}
        if not isinstance(value, dict):
            return value

        normalized = dict(value)
        if "enabled" not in normalized:
            normalized["enabled"] = True
        if normalized.get("refresh_interval") is None:
            refresh_interval = normalized.get("refreshInterval")
            if refresh_interval is not None:
                normalized["refresh_interval"] = refresh_interval
        if normalized.get("max_values") is None:
            max_values = normalized.get("maxValues")
            if max_values is not None:
                normalized["max_values"] = max_values
        return normalized


class Dimension(BaseModel):
    _dataset: "Dataset | None" = PrivateAttr(default=None)

    name: str
    expression: Optional[str] = None
    type: str
    primary_key: bool = False
    alias: Optional[str] = None
    description: Optional[str] = None
    synonyms: Optional[List[str]] = None
    vector: DimensionVectorConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_vector_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        normalized = dict(value)
        if normalized.get("vector") is not None:
            return normalized

        vectorized = bool(normalized.get("vectorized"))
        vector_reference = normalized.get("vector_reference")
        vector_index = normalized.get("vector_index")
        if not vectorized and vector_reference is None and not isinstance(vector_index, dict):
            return normalized

        vector_payload: dict[str, Any] = {"enabled": True}
        if isinstance(vector_index, dict):
            refresh_interval = (
                vector_index.get("refresh_interval")
                or vector_index.get("refreshInterval")
            )
            if refresh_interval is not None:
                vector_payload["refresh_interval"] = refresh_interval
            max_values = vector_index.get("max_values") or vector_index.get("maxValues")
            if max_values is not None:
                vector_payload["max_values"] = max_values

            legacy_store: dict[str, Any] = {}
            legacy_type = (
                vector_index.get("type")
                or vector_index.get("store_type")
                or vector_index.get("storeType")
            )
            if legacy_type is not None:
                legacy_store["type"] = legacy_type
            connector_name = (
                vector_index.get("connector_name")
                or vector_index.get("connectorName")
                or vector_index.get("connector")
                or vector_reference
            )
            if connector_name is not None:
                legacy_store["connector_name"] = connector_name
            index_name = (
                vector_index.get("index_name")
                or vector_index.get("indexName")
                or vector_index.get("namespace")
                or vector_index.get("collection")
            )
            if index_name is not None:
                legacy_store["index_name"] = index_name
            if legacy_store:
                vector_payload["store"] = legacy_store
        elif vector_reference is not None:
            vector_payload["store"] = {
                "type": "connector",
                "connector_name": vector_reference,
            }
        else:
            vector_payload["enabled"] = vectorized

        normalized["vector"] = vector_payload
        return normalized


class Measure(BaseModel):
    _dataset: "Dataset | None" = PrivateAttr(default=None)

    name: str
    expression: Optional[str] = None
    type: str
    description: Optional[str] = None
    aggregation: Optional[str] = None
    synonyms: Optional[List[str]] = None


class DatasetFilter(BaseModel):
    condition: str
    description: Optional[str] = None
    synonyms: Optional[List[str]] = None


class Dataset(BaseModel):
    dataset_id: str | None = None
    relation_name: Optional[str] = None
    schema_name: Optional[str] = None
    catalog_name: Optional[str] = None
    description: Optional[str] = None
    synonyms: Optional[List[str]] = None
    dimensions: Optional[List[Dimension]] = None
    measures: Optional[List[Measure]] = None
    filters: Optional[Dict[str, DatasetFilter]] = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if normalized.get("dataset_id") is None and normalized.get("datasetId") is not None:
            normalized["dataset_id"] = normalized.get("datasetId")
        if normalized.get("relation_name") is None:
            relation_name = normalized.get("relationName") or normalized.get("name")
            if relation_name is not None:
                normalized["relation_name"] = relation_name
        if normalized.get("schema_name") is None:
            schema_name = normalized.get("schemaName") or normalized.get("schema")
            if schema_name is not None:
                normalized["schema_name"] = schema_name
        if normalized.get("catalog_name") is None:
            catalog_name = normalized.get("catalogName") or normalized.get("catalog")
            if catalog_name is not None:
                normalized["catalog_name"] = catalog_name
        return normalized

    def get_relation_name(self, dataset_key: str) -> str:
        candidate = (self.relation_name or "").strip()
        return candidate or dataset_key

    def get_annotations(self, dataset_key: str) -> Dict[str, str]:
        annotations: Dict[str, str] = {}
        for dimension in self.dimensions or []:
            annotations[f"{dataset_key}.{dimension.name}"] = dimension.name
        for measure in self.measures or []:
            annotations[f"{dataset_key}.{measure.name}"] = measure.name
        return annotations

    def model_post_init(self, __context: Any) -> None:
        for dimension in self.dimensions or []:
            dimension._dataset = self
        for measure in self.measures or []:
            measure._dataset = self

    @property
    def schema(self) -> str:
        return (self.schema_name or "").strip()

    @schema.setter
    def schema(self, value: str | None) -> None:
        normalized = str(value or "").strip()
        self.schema_name = normalized or None

    @property
    def name(self) -> str:
        return self.get_relation_name("")

    @name.setter
    def name(self, value: str | None) -> None:
        normalized = str(value or "").strip()
        self.relation_name = normalized or None

    @property
    def catalog(self) -> Optional[str]:
        return self.catalog_name

    @catalog.setter
    def catalog(self, value: str | None) -> None:
        normalized = str(value or "").strip()
        self.catalog_name = normalized or None


_LEGACY_RELATIONSHIP_PATTERN = re.compile(
    r"^\s*(?P<left_dataset>[A-Za-z_][A-Za-z0-9_]*)\.(?P<left_field>[A-Za-z_][A-Za-z0-9_]*)\s*(?P<operator>=|!=|>|>=|<|<=)\s*"
    r"(?P<right_dataset>[A-Za-z_][A-Za-z0-9_]*)\.(?P<right_field>[A-Za-z_][A-Za-z0-9_]*)\s*$"
)


class Relationship(BaseModel):
    name: str
    source_dataset: str
    source_field: str
    target_dataset: str
    target_field: str
    operator: str = "="
    type: Literal[
        "one_to_many",
        "many_to_one",
        "one_to_one",
        "many_to_many",
        "inner",
        "left",
        "right",
        "full",
    ]

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if normalized.get("source_dataset") and normalized.get("target_dataset"):
            return normalized

        condition = normalized.get("join_on") or normalized.get("joinOn") or normalized.get("on") or normalized.get("condition")
        if condition:
            match = _LEGACY_RELATIONSHIP_PATTERN.match(str(condition))
            if match:
                groups = match.groupdict()
                normalized.setdefault("source_dataset", groups["left_dataset"])
                normalized.setdefault("source_field", groups["left_field"])
                normalized.setdefault("target_dataset", groups["right_dataset"])
                normalized.setdefault("target_field", groups["right_field"])
                normalized.setdefault("operator", groups["operator"])
        left_dataset = normalized.get("source_dataset") or normalized.get("from_") or normalized.get("from") or normalized.get("left")
        right_dataset = normalized.get("target_dataset") or normalized.get("to") or normalized.get("right")
        if left_dataset is not None:
            normalized["source_dataset"] = left_dataset
        if right_dataset is not None:
            normalized["target_dataset"] = right_dataset
        return normalized

    @property
    def join_condition(self) -> str:
        return (
            f"{self.source_dataset}.{self.source_field} {self.operator} "
            f"{self.target_dataset}.{self.target_field}"
        )

    @property
    def from_(self) -> str:
        return self.source_dataset

    @property
    def to(self) -> str:
        return self.target_dataset

    @property
    def join_on(self) -> str:
        return self.join_condition


class Metric(BaseModel):
    description: Optional[str] = None
    expression: str


class SemanticModel(BaseModel):
    version: str
    name: Optional[str] = None
    connector: Optional[str] = None
    dialect: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    datasets: Dict[str, Dataset] = Field(default_factory=dict)
    relationships: Optional[List[Relationship]] = None
    metrics: Optional[Dict[str, Metric]] = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if normalized.get("datasets") is None and isinstance(normalized.get("tables"), dict):
            normalized["datasets"] = normalized.get("tables")
        return normalized

    @property
    def tables(self) -> Dict[str, Dataset]:
        return self.datasets

    def yml_dump(self) -> str:
        return yaml.safe_dump(
            self.model_dump(exclude_none=True),
            sort_keys=False,
        )


Table = Dataset
TableFilter = DatasetFilter
