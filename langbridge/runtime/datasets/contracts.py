from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from langbridge.connectors.base.config import ConnectorSyncStrategy
from langbridge.runtime.scheduling import normalize_dataset_sync_cadence


class RuntimeModel(BaseModel):
    model_config = ConfigDict(
        extra="allow",
        from_attributes=True,
        populate_by_name=True,
    )


def _to_camel(value: str) -> str:
    parts = value.split("_")
    if not parts:
        return value
    head, *tail = parts
    return head + "".join(part.capitalize() for part in tail)


class RuntimeRequestModel(RuntimeModel):
    model_config = ConfigDict(
        alias_generator=_to_camel,
        extra="allow",
        from_attributes=True,
        populate_by_name=True,
    )


class DatasetSourceMode(str, Enum):
    TABLE = "table"
    SQL = "sql"
    RESOURCE = "resource"
    REQUEST = "request"
    FILE = "file"


class DatasetMaterializationMode(str, Enum):
    LIVE = "live"
    SYNCED = "synced"


class DatasetRequestConfig(RuntimeRequestModel):
    method: Literal["get", "post"] = "get"
    path: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    body: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, Any] = Field(default_factory=dict)

    @field_validator("method", mode="before")
    @classmethod
    def _validate_method(cls, value: Any) -> Literal["get", "post"]:
        normalized = str(getattr(value, "value", value) or "get").strip().lower()
        if normalized not in {"get", "post"}:
            raise ValueError("Dataset request method must be 'get' or 'post'.")
        return normalized  # type: ignore[return-value]


class DatasetExtractionConfig(RuntimeRequestModel):
    type: Literal["json", "xml", "csv", "raw"] = "raw"
    options: dict[str, Any] = Field(default_factory=dict)


class DatasetSchemaHintColumn(RuntimeRequestModel):
    name: str
    type: str
    nullable: bool = True
    description: str | None = None
    path: str | None = None
    default_value: Any = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if normalized.get("type") is None and normalized.get("data_type") is not None:
            normalized["type"] = normalized.pop("data_type")
        return normalized

    @property
    def data_type(self) -> str:
        return self.type


class DatasetSchemaHint(RuntimeRequestModel):
    columns: list[DatasetSchemaHintColumn] = Field(default_factory=list)
    dynamic: bool = False


class DatasetSyncPolicy(RuntimeRequestModel):
    strategy: ConnectorSyncStrategy | None = None
    cadence: str | None = None
    cursor_field: str | None = None
    initial_cursor: str | None = None
    lookback_window: str | None = None
    backfill_start: str | None = None
    backfill_end: str | None = None
    sync_on_start: bool = False

    @field_validator("strategy", mode="before")
    @classmethod
    def _validate_strategy(cls, value: Any) -> ConnectorSyncStrategy | None:
        if value is None or value == "":
            return None
        if isinstance(value, ConnectorSyncStrategy):
            return value
        return ConnectorSyncStrategy(str(getattr(value, "value", value)).strip().upper())

    @field_validator("cadence", mode="before")
    @classmethod
    def _validate_cadence(cls, value: Any) -> str | None:
        return normalize_dataset_sync_cadence(value)


class DatasetMaterializationConfig(RuntimeRequestModel):
    mode: DatasetMaterializationMode
    sync: DatasetSyncPolicy | None = None

    @field_validator("mode", mode="before")
    @classmethod
    def _validate_mode(cls, value: Any) -> DatasetMaterializationMode:
        if isinstance(value, DatasetMaterializationMode):
            return value
        normalized = str(getattr(value, "value", value) or "").strip().lower()
        if not normalized:
            raise ValueError("Dataset materialization_mode is required.")
        return DatasetMaterializationMode(normalized)

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        legacy_mode = normalized.pop("materialization_mode", None)
        legacy_sync = normalized.pop("sync", None)
        if normalized.get("mode") is None and legacy_mode is not None:
            normalized["mode"] = legacy_mode
        if normalized.get("sync") is None and legacy_sync is not None:
            normalized["sync"] = legacy_sync
        return normalized

    @model_validator(mode="after")
    def _validate_contract(self) -> "DatasetMaterializationConfig":
        if self.mode == DatasetMaterializationMode.SYNCED:
            if self.sync is None:
                raise ValueError("Synced datasets must declare sync config in materialization.sync.")
            return self
        if self.sync is not None:
            raise ValueError("Live datasets must not declare materialization.sync.")
        return self

    @property
    def materialization_mode(self) -> DatasetMaterializationMode:
        return self.mode


class DatasetSourceConfig(RuntimeRequestModel):
    kind: DatasetSourceMode | None = None
    table: str | None = None
    resource: str | None = None
    request: DatasetRequestConfig | None = None
    flatten: list[str] | None = None
    sql: str | None = None
    path: str | None = None
    storage_uri: str | None = None
    format: str | None = None
    file_format: str | None = None
    header: bool | None = None
    delimiter: str | None = None
    quote: str | None = None
    extraction: DatasetExtractionConfig | None = None

    @field_validator("kind", mode="before")
    @classmethod
    def _validate_kind(cls, value: Any) -> DatasetSourceMode | None:
        if value is None or value == "":
            return None
        if isinstance(value, DatasetSourceMode):
            return value
        return DatasetSourceMode(str(getattr(value, "value", value)).strip().lower())

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if normalized.get("kind") is not None:
            return normalized
        has_table = bool(str(normalized.get("table") or "").strip())
        has_resource = bool(str(normalized.get("resource") or "").strip())
        has_request = normalized.get("request") is not None
        legacy_resource = normalized.get("resource")
        if isinstance(legacy_resource, dict):
            normalized["request"] = legacy_resource
            normalized["resource"] = None
            has_resource = False
            has_request = True
        has_sql = bool(str(normalized.get("sql") or "").strip())
        has_file = bool(str(normalized.get("path") or "").strip() or str(normalized.get("storage_uri") or "").strip())
        configured_modes = sum((has_table, has_resource, has_request, has_sql, has_file))
        if configured_modes == 1:
            if has_table:
                normalized["kind"] = DatasetSourceMode.TABLE.value
            elif has_resource:
                normalized["kind"] = DatasetSourceMode.RESOURCE.value
            elif has_request:
                normalized["kind"] = DatasetSourceMode.REQUEST.value
            elif has_sql:
                normalized["kind"] = DatasetSourceMode.SQL.value
            else:
                normalized["kind"] = DatasetSourceMode.FILE.value
        return normalized

    @model_validator(mode="after")
    def _validate_source(self) -> "DatasetSourceConfig":
        kind = self.kind
        if kind is None:
            raise ValueError("Dataset source.kind is required.")
        table = str(self.table or "").strip()
        resource = str(self.resource or "").strip()
        sql = str(self.sql or "").strip()
        path = str(self.path or "").strip()
        storage_uri = str(self.storage_uri or "").strip()
        file_format = str(self.format or self.file_format or "").strip().lower() or None
        self.format = file_format
        if self.file_format is not None and self.format is not None:
            self.file_format = self.format
        if kind == DatasetSourceMode.TABLE:
            if not table:
                raise ValueError("Dataset table source requires source.table.")
            self._reject_unrelated_fields({"table"})
        elif kind == DatasetSourceMode.SQL:
            if not sql:
                raise ValueError("Dataset SQL source requires source.sql.")
            self._reject_unrelated_fields({"sql"})
        elif kind == DatasetSourceMode.RESOURCE:
            if not resource:
                raise ValueError("Dataset resource source requires source.resource.")
            self._reject_unrelated_fields({"resource", "flatten", "extraction"})
        elif kind == DatasetSourceMode.REQUEST:
            if self.request is None:
                raise ValueError("Dataset request source requires source.request.")
            if not str(self.request.path or "").strip():
                raise ValueError("Dataset request source requires source.request.path.")
            self._reject_unrelated_fields({"request", "flatten", "extraction"})
        elif kind == DatasetSourceMode.FILE:
            if not path and not storage_uri:
                raise ValueError("Dataset file source requires source.path or source.storage_uri.")
            self._reject_unrelated_fields(
                {"path", "storage_uri", "format", "file_format", "header", "delimiter", "quote"}
            )
        else:
            raise ValueError(f"Unsupported dataset source.kind '{kind}'.")
        if self.flatten and kind not in {DatasetSourceMode.RESOURCE, DatasetSourceMode.REQUEST}:
            raise ValueError("Dataset source.flatten is only valid for resource/request dataset sources.")
        if self.extraction is not None and kind not in {DatasetSourceMode.RESOURCE, DatasetSourceMode.REQUEST}:
            raise ValueError("Dataset source.extraction is only valid for resource/request dataset sources.")
        return self

    def _reject_unrelated_fields(self, allowed: set[str]) -> None:
        field_values = {
            "table": bool(str(self.table or "").strip()),
            "resource": bool(str(self.resource or "").strip()),
            "request": self.request is not None,
            "flatten": bool(self.flatten),
            "sql": bool(str(self.sql or "").strip()),
            "path": bool(str(self.path or "").strip()),
            "storage_uri": bool(str(self.storage_uri or "").strip()),
            "format": bool(str(self.format or "").strip()),
            "file_format": bool(str(self.file_format or "").strip()),
            "header": self.header is not None,
            "delimiter": self.delimiter is not None,
            "quote": self.quote is not None,
            "extraction": self.extraction is not None,
        }
        disallowed = [
            field_name
            for field_name, configured in field_values.items()
            if configured and field_name not in allowed
        ]
        if disallowed:
            joined = ", ".join(sorted(disallowed))
            raise ValueError(f"Dataset source.kind '{self.kind.value}' does not accept: {joined}.")

    @property
    def resource_request(self) -> DatasetRequestConfig | None:
        if self.kind == DatasetSourceMode.REQUEST:
            return self.request
        if self.kind == DatasetSourceMode.RESOURCE and self.resource:
            return DatasetRequestConfig(path=self.resource)
        return None

    @property
    def resolved_storage_uri(self) -> str | None:
        raw_storage_uri = str(self.storage_uri or "").strip()
        if raw_storage_uri:
            return raw_storage_uri
        raw_path = str(self.path or "").strip()
        if not raw_path:
            return None
        if "://" in raw_path:
            return raw_path
        return Path(raw_path).resolve().as_uri()
