
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from langbridge.connectors.base.config import ConnectorSyncStrategy
from langbridge.runtime.datasets.contracts import (
    DatasetExtractionConfig,
    DatasetMaterializationConfig,
    DatasetMaterializationMode,
    DatasetRequestConfig,
    DatasetSchemaHint,
    DatasetSchemaHintColumn,
    DatasetSourceConfig,
    DatasetSyncPolicy,
)
from langbridge.runtime.models.metadata import ConnectorCapabilities, SecretReference
from langbridge.runtime.scheduling import normalize_dataset_sync_cadence


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

class LocalRuntimeDatasetSourceConfigRequestConfig(DatasetRequestConfig):
    pass


class LocalRuntimeDatasetSourceConfigExtractionConfig(DatasetExtractionConfig):
    pass


class LocalRuntimeDatasetSourceConfigSchemaHintColumnConfig(DatasetSchemaHintColumn):
    pass


class LocalRuntimeDatasetSourceConfigSchemaHintConfig(DatasetSchemaHint):
    pass


class LocalRuntimeDatasetSourceConfig(DatasetSourceConfig):
    pass


class LocalRuntimeDatasetSyncSourceConfig(DatasetSourceConfig):
    pass


class LocalRuntimeDatasetPolicyConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    max_rows_preview: int | None = Field(default=None, ge=1)
    max_export_rows: int | None = Field(default=None, ge=1)
    redaction_rules: dict[str, str] = Field(default_factory=dict)
    row_filters: list[str] = Field(default_factory=list)
    allow_dml: bool = False


class LocalRuntimeDatasetSyncConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    source: LocalRuntimeDatasetSyncSourceConfig
    strategy: ConnectorSyncStrategy | None = None
    cadence: str | None = None
    cursor_field: str | None = None
    initial_cursor: str | None = None
    lookback_window: str | None = None
    backfill_start: str | None = None
    backfill_end: str | None = None
    sync_on_start: bool = False

    @model_validator(mode="before")
    @classmethod
    def _normalize_sync_policy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        strategy = normalized.get("strategy")
        if isinstance(strategy, DatasetSyncPolicy):
            normalized.update(strategy.model_dump(mode="json", exclude_none=True))
            normalized["strategy"] = strategy.strategy
        return normalized

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


class LocalRuntimeDatasetMaterializationConfig(DatasetMaterializationConfig):
    pass


class LocalRuntimeDatasetConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    label: str | None = None
    description: str | None = None
    connector: str | None = None
    source: LocalRuntimeDatasetSourceConfig
    materialization: LocalRuntimeDatasetMaterializationConfig
    schema_hint: LocalRuntimeDatasetSourceConfigSchemaHintConfig | None = None
    tags: list[str] = Field(default_factory=list)
    policy: LocalRuntimeDatasetPolicyConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_legacy_shape(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        legacy_mode = normalized.pop("materialization_mode", None)
        legacy_sync = normalized.pop("sync", None)
        legacy_sync_source = (
            legacy_sync.get("source")
            if isinstance(legacy_sync, dict)
            else None
        )
        legacy_semantic_model = normalized.pop("semantic_model", None)
        legacy_default_time_dimension = normalized.pop("default_time_dimension", None)
        source_payload = normalized.get("source")
        if source_payload is not None and legacy_sync_source is not None:
            normalized_source = LocalRuntimeDatasetSourceConfig.model_validate(source_payload).model_dump(
                mode="json",
                exclude_none=True,
            )
            normalized_sync_source = LocalRuntimeDatasetSyncSourceConfig.model_validate(legacy_sync_source).model_dump(
                mode="json",
                exclude_none=True,
            )
            if normalized_source != normalized_sync_source:
                raise ValueError(
                    "Synced datasets must declare sync config, not live source config."
                )
        if isinstance(source_payload, dict) and "schema" in source_payload and "schema_hint" not in normalized:
            normalized["schema_hint"] = source_payload.get("schema")
            source_payload = dict(source_payload)
            source_payload.pop("schema", None)
            normalized["source"] = source_payload
        if normalized.get("materialization") is None:
            normalized["materialization"] = {
                "mode": legacy_mode,
                "sync": legacy_sync,
            }
        if normalized.get("source") is None and isinstance(legacy_sync, dict) and legacy_sync.get("source") is not None:
            normalized["source"] = legacy_sync.get("source")
        normalized.pop("semantic", None)
        if legacy_semantic_model is not None or legacy_default_time_dimension is not None:
            normalized["_legacy_semantic_binding"] = {
                "semantic_model": legacy_semantic_model,
                "default_time_dimension": legacy_default_time_dimension,
            }
        return normalized

    @model_validator(mode="after")
    def _validate_materialization_mode_source(self) -> "LocalRuntimeDatasetConfig":
        if self.materialization.mode == DatasetMaterializationMode.SYNCED and not str(self.connector or "").strip():
            raise ValueError("Dataset connector is required for synced datasets.")
        if self.source.kind.value in {"table", "sql", "resource", "request"} and not str(self.connector or "").strip():
            raise ValueError(
                "Dataset connector is required for table-backed, sql-backed, and API dataset sources."
            )
        return self

    @property
    def materialization_mode(self) -> DatasetMaterializationMode:
        return self.materialization.mode

    @property
    def sync(self) -> LocalRuntimeDatasetSyncConfig | None:
        sync_policy = self.materialization.sync
        if sync_policy is None:
            return None
        return LocalRuntimeDatasetSyncConfig.model_validate(
            {
                "source": self.source.model_dump(mode="json"),
                **sync_policy.model_dump(mode="json", exclude_none=True),
            }
        )


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
        if str(self.provider or "").strip().lower() == "ollama":
            return self
        if not str(self.api_key or "").strip() and self.api_key_secret is None:
            raise ValueError("LLM connection must define api_key or api_key_secret.")
        return self


class LocalRuntimeAiProfileAnalystScopeConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    semantic_models: list[str] = Field(
        default_factory=list,
        description="Semantic model ids available to the analyst for semantic scope querying.",
    )
    datasets: list[str] = Field(
        default_factory=list,
        description="Dataset ids available to the analyst for dataset scope querying.",
    )
    query_policy: Literal["semantic_preferred", "dataset_preferred", "semantic_only", "dataset_only"] = Field(
        default="semantic_preferred",
        description="Policy that controls whether semantic or dataset scope is attempted first, or exclusively.",
    )
    allow_source_scope: bool = Field(
        default=False,
        description="Allow explicit source-scope querying when expert/debug bindings add that scope.",
    )

    @model_validator(mode="after")
    def _validate_query_policy(self) -> "LocalRuntimeAiProfileAnalystScopeConfig":
        has_semantic_models = bool(self.semantic_models)
        has_datasets = bool(self.datasets)
        if self.query_policy in {"semantic_only", "semantic_preferred"} and not has_semantic_models and has_datasets:
            raise ValueError(
                "Analyst scope with semantic-only or semantic-preferred query policy must define semantic_models."
            )
        if self.query_policy in {"dataset_only", "dataset_preferred"} and not has_datasets and has_semantic_models:
            raise ValueError(
                "Analyst scope with dataset-only or dataset-preferred query policy must define datasets."
            )
        return self
            
class LocalRuntimeAiProfileLLMScopeConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    llm_connection: str | None = Field(
        default=None,
        description="LLM connection name to use for the profile. Must match the name of one of the defined LLM connections.",
    )
    provider: str | None = Field(default=None)
    model: str | None = Field(default=None)
    temperature: float | None = Field(default=None)
    reasoning_effort: str | None = Field(default=None)
    max_completion_tokens: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _validate_shape(self) -> "LocalRuntimeAiProfileLLMScopeConfig":
        if str(self.llm_connection or "").strip():
            return self
        if str(self.provider or "").strip() and str(self.model or "").strip():
            return self
        raise ValueError("AI profile llm scope must define llm_connection or provider + model.")

class LocalRuntimeAiProfileResearchScopeConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: bool = Field(
        default=False,
        description="Whether research capabilities are enabled in the research scope.",
    )
    extended_thinking_enabled: bool = Field(
        default=False,
        description="Whether extended thinking capabilities are enabled in the research scope. Extended thinking includes capabilities that go beyond retrieval-augmented generation, such as multi-hop reasoning, iterative retrieval, and synthesis of information from multiple sources.",
    )
    max_sources: int = Field(
        default=5,
        description="Maximum number of sources to retrieve and use for research queries.",
    )
    require_sources: bool = Field(
        default=False,
        description="Whether to require at least one source for research queries. If True, research queries that do not return any sources will be considered failed.",
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if normalized.get("extended_thinking_enabled") is None and normalized.get("extended_thinking") is not None:
            normalized["extended_thinking_enabled"] = normalized.get("extended_thinking")
        return normalized

class LocalRuntimeAiProfileWebSearchScopeConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    enabled: bool = Field(
        default=False,
        description="Whether web search capabilities are enabled in the research scope.",
    )
    provider: str | None = Field(
        default=None,
        description="Provider name to use for web search. Must match the name of one of the defined connectors that supports web search in its capabilities.",
    )
    allowed_domains: list[str] | None = Field(
        default_factory=list,
        description="List of allowed domains for web search when web_search_require_allowed_domain is True.",
    )
    require_allowed_domain: bool = Field(
        default=False,
        description="Whether to require allowed domains for web search. If True, web search will only be allowed if at least one allowed domain is specified and the search query contains at least one of the allowed domains.",
    )
    max_results: int = Field(
        default=10,
        description="Maximum number of web search results to return for research queries.",
    )
    timebox_seconds: int = Field(
        default=10,
        description="Maximum number of seconds to spend on web search for research queries.",
    )

    @model_validator(mode="after")
    def _validate_web_search_policy(self) -> "LocalRuntimeAiProfileWebSearchScopeConfig":
        if self.require_allowed_domain and not self.allowed_domains:
            raise ValueError(
                "Web search scope with require_allowed_domain must define allowed_domains."
            )
        return self

class LocalRuntimeAiProfilePromptsConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    system_prompt: str | None = Field(
        default=None,
        description="System prompt to use for the profile. This prompt will be prepended to all queries made under the profile.",
    )
    user_prompt: str | None = Field(
        default=None,
        description="User prompt to use for the profile. This prompt will be used as the initial prompt for all queries made under the profile, and can include instructions or examples for the analyst.",
    )
    response_format_prompt: str | None = Field(
        default=None,
        description="Response format prompt to use for the profile. This prompt will be used to instruct the LLM on the desired format of the response, such as JSON or a specific schema.",
    )
    planning_prompt: str | None = Field(default=None)
    presentation_prompt: str | None = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        aliases = {
            "system": "system_prompt",
            "user": "user_prompt",
            "response_format": "response_format_prompt",
            "planning": "planning_prompt",
            "presentation": "presentation_prompt",
        }
        for source, target in aliases.items():
            if normalized.get(target) is None and normalized.get(source) is not None:
                normalized[target] = normalized.get(source)
        return normalized

class LocalRuntimeAiProfileAccessConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    allowed_connectors: list[str] = Field(
        default_factory=list,
        description="List of connector names that the profile is allowed to access. If empty, the profile is allowed to access all connectors.",
    )
    denied_connectors: list[str] = Field(
        default_factory=list,
        description="List of connector names that the profile is denied access to. If empty, the profile is not denied access to any connectors.",
    )


class LocalRuntimeAiProfileExecutionConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    max_iterations: int = Field(default=3, ge=1)
    max_replans: int = Field(default=2, ge=0)
    max_step_retries: int = Field(default=1, ge=0)

class LocalRuntimeAiProfileConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    description: str | None = None
    default: bool = False
    enabled: bool = True
    mcp_enabled: bool = False
    analyst_scope: LocalRuntimeAiProfileAnalystScopeConfig = Field(default_factory=LocalRuntimeAiProfileAnalystScopeConfig)
    llm_scope: LocalRuntimeAiProfileLLMScopeConfig | None = None
    research_scope: LocalRuntimeAiProfileResearchScopeConfig = Field(default_factory=LocalRuntimeAiProfileResearchScopeConfig)
    web_search_scope: LocalRuntimeAiProfileWebSearchScopeConfig = Field(default_factory=LocalRuntimeAiProfileWebSearchScopeConfig)
    prompts: LocalRuntimeAiProfilePromptsConfig = Field(default_factory=LocalRuntimeAiProfilePromptsConfig)
    access: LocalRuntimeAiProfileAccessConfig = Field(default_factory=LocalRuntimeAiProfileAccessConfig)
    execution: LocalRuntimeAiProfileExecutionConfig = Field(default_factory=LocalRuntimeAiProfileExecutionConfig)

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        aliases = {
            "scope": "analyst_scope",
            "research": "research_scope",
            "web_search": "web_search_scope",
            "llm": "llm_scope",
        }
        for source, target in aliases.items():
            if normalized.get(target) is None and normalized.get(source) is not None:
                normalized[target] = normalized.get(source)
        exposure = normalized.get("exposure")
        if isinstance(exposure, dict):
            if normalized.get("enabled") is None and exposure.get("runtime") is not None:
                normalized["enabled"] = bool(exposure.get("runtime"))
            if normalized.get("mcp_enabled") is None and exposure.get("mcp") is not None:
                normalized["mcp_enabled"] = bool(exposure.get("mcp"))
        return normalized

    @model_validator(mode="after")
    def _validate_runtime_shape(self) -> "LocalRuntimeAiProfileConfig":
        has_data_scope = bool(self.analyst_scope.semantic_models or self.analyst_scope.datasets)
        if not has_data_scope and not self.web_search_scope.enabled:
            raise ValueError(
                "AI profile must define analyst scope datasets/semantic models or enable web search."
            )
        return self


class LocalRuntimeAiConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    profiles: list[LocalRuntimeAiProfileConfig] = Field(default_factory=list)


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
    ai: LocalRuntimeAiConfig = Field(default_factory=LocalRuntimeAiConfig)

    @model_validator(mode="before")
    @classmethod
    def _reject_legacy_agents_config(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "agents" in value:
            raise ValueError("Legacy 'agents' config path removed. Use 'ai.profiles' instead.")
        return value

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
DatasetSyncSourceConfig = LocalRuntimeDatasetSyncSourceConfig
DatasetSyncConfig = LocalRuntimeDatasetSyncConfig
DatasetPolicyConfig = LocalRuntimeDatasetPolicyConfig
DatasetConfig = LocalRuntimeDatasetConfig
SemanticModelConfig = LocalRuntimeSemanticModelConfig
LLMConnectionConfig = LocalRuntimeLLMConnectionConfig
MetadataStoreConfig = LocalRuntimeMetadataStoreConfig
RuntimeConfig = LocalRuntimeRuntimeConfig
