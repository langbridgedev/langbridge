
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from langbridge.connectors.base.config import ConnectorSyncStrategy
from langbridge.runtime.models.metadata import (
    ConnectorCapabilities,
    DatasetMaterializationMode,
    SecretReference,
)


class LocalRuntimeConnectorConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    type: str
    description: str | None = None
    connection: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    secrets: dict[str, Any] = Field(default_factory=dict)
    policy: dict[str, Any] | None = None
    capabilities: ConnectorCapabilities | None = None
    managed: bool = False


class LocalRuntimeDatasetSourceConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    table: str | None = None
    resource: str | None = None
    sql: str | None = None
    path: str | None = None
    storage_uri: str | None = None
    format: str | None = None
    file_format: str | None = None
    header: bool | None = None
    delimiter: str | None = None
    quote: str | None = None

    @model_validator(mode="after")
    def _validate_source(self) -> "LocalRuntimeDatasetSourceConfig":
        has_table = bool(str(self.table or "").strip())
        has_resource = bool(str(self.resource or "").strip())
        has_sql = bool(str(self.sql or "").strip())
        has_file = bool(str(self.path or "").strip() or str(self.storage_uri or "").strip())
        configured_modes = sum((has_table, has_resource, has_sql, has_file))
        if configured_modes != 1:
            raise ValueError(
                "Dataset source must define exactly one of table, resource, sql, or path/storage_uri."
            )
        return self


class LocalRuntimeDatasetPolicyConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    max_rows_preview: int | None = Field(default=None, ge=1)
    max_export_rows: int | None = Field(default=None, ge=1)
    redaction_rules: dict[str, str] = Field(default_factory=dict)
    row_filters: list[str] = Field(default_factory=list)
    allow_dml: bool = False


class LocalRuntimeDatasetSyncConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    resource: str
    strategy: ConnectorSyncStrategy | None = None
    cadence: str | None = None
    cursor_field: str | None = None
    initial_cursor: str | None = None
    lookback_window: str | None = None
    backfill_start: str | None = None
    backfill_end: str | None = None
    sync_on_start: bool = False
    flattern_into_datasets: bool = False

    @model_validator(mode="after")
    def _validate_sync(self) -> "LocalRuntimeDatasetSyncConfig":
        resource = str(self.resource or "").strip()
        if not resource:
            raise ValueError("Dataset sync config requires resource.")
        self.resource = resource
        return self


class LocalRuntimeDatasetConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    label: str | None = None
    description: str | None = None
    connector: str | None = None
    materialization_mode: DatasetMaterializationMode
    source: LocalRuntimeDatasetSourceConfig | None = None
    sync: LocalRuntimeDatasetSyncConfig | None = None
    semantic_model: str | None = None
    default_time_dimension: str | None = None
    tags: list[str] = Field(default_factory=list)
    policy: LocalRuntimeDatasetPolicyConfig | None = None

    @model_validator(mode="after")
    def _validate_materialization_mode_source(self) -> "LocalRuntimeDatasetConfig":
        if self.materialization_mode == DatasetMaterializationMode.SYNCED:
            if self.source is not None:
                raise ValueError("Synced datasets must declare sync config, not live source config.")
            if self.sync is None:
                raise ValueError("Synced datasets must declare sync config.")
            return self

        if self.sync is not None:
            raise ValueError("Live datasets must not declare sync config.")
        if self.source is None:
            raise ValueError("Live datasets must declare source.")
        return self


class LocalRuntimeSemanticModelConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    description: str | None = None
    default: bool = False
    model: dict[str, Any] | None = None
    datasets: list[str] = Field(default_factory=list)


class LocalRuntimeLLMConnectionConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    provider: str
    model: str
    description: str | None = None
    api_key: str | None = None
    api_key_secret: SecretReference | None = None
    configuration: dict[str, Any] = Field(default_factory=dict)
    default: bool = False

    @model_validator(mode="after")
    def _validate_credentials(self) -> "LocalRuntimeLLMConnectionConfig":
        if not str(self.api_key or "").strip() and self.api_key_secret is None:
            raise ValueError("LLM connection must define api_key or api_key_secret.")
        return self


class LocalRuntimeAgentConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    description: str | None = None
    llm_connection: str | None = None
    semantic_model: str | None = None
    dataset: str | None = None
    default: bool = False
    instructions: str | None = None
    definition: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _validate_authoring_shape(self) -> "LocalRuntimeAgentConfig":
        has_definition = bool(self.definition)
        has_semantic_model = bool(str(self.semantic_model or "").strip())
        has_dataset = bool(str(self.dataset or "").strip())
        if not has_definition and not has_semantic_model and not has_dataset:
            raise ValueError(
                "Agent config must define definition or a shorthand semantic_model/dataset binding."
            )
        return self


class LocalRuntimeMetadataStoreConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["in_memory", "sqlite", "postgres"] = "sqlite"
    path: str | None = None
    url: str | None = None
    echo: bool = False
    pool_size: int | None = Field(default=None, ge=1)
    max_overflow: int | None = Field(default=None, ge=0)
    pool_timeout: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _validate_shape(self) -> "LocalRuntimeMetadataStoreConfig":
        if self.type == "in_memory":
            if str(self.path or "").strip() or str(self.url or "").strip():
                raise ValueError("runtime.metadata_store in_memory mode does not accept path or url.")
            return self
        if self.type == "sqlite":
            if str(self.url or "").strip():
                raise ValueError("runtime.metadata_store sqlite mode uses path, not url.")
            if str(self.path or "").strip() == ":memory:":
                raise ValueError(
                    "runtime.metadata_store sqlite path must not be ':memory:'; use type: in_memory for ephemeral mode."
                )
            return self
        if not str(self.url or "").strip():
            raise ValueError("runtime.metadata_store postgres mode requires url.")
        if str(self.path or "").strip():
            raise ValueError("runtime.metadata_store postgres mode uses url, not path.")
        return self


class LocalRuntimeDuckDbConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    path: str | None = None
    temp_directory: str | None = None


class LocalRuntimeExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    engine: str | None = None
    duckdb: LocalRuntimeDuckDbConfig = Field(default_factory=LocalRuntimeDuckDbConfig)


class LocalRuntimeMigrationsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    auto_apply: bool = True


class LocalRuntimeRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    mode: str | None = None
    metadata_store: LocalRuntimeMetadataStoreConfig | None = None
    migrations: LocalRuntimeMigrationsConfig = Field(default_factory=LocalRuntimeMigrationsConfig)
    execution: LocalRuntimeExecutionConfig = Field(default_factory=LocalRuntimeExecutionConfig)


class LocalRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    version: int | str = 1
    runtime: LocalRuntimeRuntimeConfig = Field(default_factory=LocalRuntimeRuntimeConfig)
    connectors: list[LocalRuntimeConnectorConfig] = Field(default_factory=list)
    datasets: list[LocalRuntimeDatasetConfig] = Field(default_factory=list)
    semantic_models: list[LocalRuntimeSemanticModelConfig] = Field(default_factory=list)
    llm_connections: list[LocalRuntimeLLMConnectionConfig] = Field(default_factory=list)
    agents: list[LocalRuntimeAgentConfig] = Field(default_factory=list)


@dataclass(slots=True, frozen=True)
class ResolvedLocalRuntimeMetadataStoreConfig:
    type: Literal["in_memory", "sqlite", "postgres"]
    path: Path | None = None
    url: str | None = None
    sync_url: str | None = None
    async_url: str | None = None
    echo: bool = False
    pool_size: int | None = None
    max_overflow: int | None = None
    pool_timeout: int | None = None


ConnectorConfig = LocalRuntimeConnectorConfig
DatasetSourceConfig = LocalRuntimeDatasetSourceConfig
DatasetSyncConfig = LocalRuntimeDatasetSyncConfig
DatasetPolicyConfig = LocalRuntimeDatasetPolicyConfig
DatasetConfig = LocalRuntimeDatasetConfig
SemanticModelConfig = LocalRuntimeSemanticModelConfig
LLMConnectionConfig = LocalRuntimeLLMConnectionConfig
AgentConfig = LocalRuntimeAgentConfig
MetadataStoreConfig = LocalRuntimeMetadataStoreConfig
RuntimeConfig = LocalRuntimeRuntimeConfig
