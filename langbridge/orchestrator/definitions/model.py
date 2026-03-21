"""Pydantic models for agent definitions.

These capture the richer contract used by the orchestrator when loading an
agent. The shape is inspired by Snowflake Cortex agent builder concepts: prompt
contract, memory strategy, tools/connectors, access policy, runtime behavior,
output schema, guardrails, and observability knobs.
"""

import uuid
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class MemoryStrategy(str, Enum):
    none = "none"
    transient = "transient"
    conversation = "conversation"
    long_term = "long_term"
    vector = "vector"
    database = "database"  # only database is supported for now


class ExecutionMode(str, Enum):
    single_step = "single_step"
    iterative = "iterative"


class ResponseMode(str, Enum):
    analyst = "analyst"
    chat = "chat"
    executive = "executive"
    explainer = "explainer"


class OutputFormat(str, Enum):
    text = "text"
    markdown = "markdown"
    json = "json"
    yaml = "yaml"


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"
    critical = "critical"
    
class PromptContract(BaseModel):
    system_prompt: str = Field(..., description="Primary system prompt/instruction.")
    user_instructions: Optional[str] = Field(
        None, description="Additional guidance for how to interpret user input."
    )
    style_guidance: Optional[str] = Field(
        None, description="Tone, persona, and response style expectations."
    )


class MemoryConfig(BaseModel):
    strategy: MemoryStrategy = Field(
        ...,
        description="Legacy knob retained for compatibility. Runtime memory storage is system-managed.",
    )
    ttl_seconds: Optional[int] = Field(
        None, description="Optional retention hint; system-managed memory may cap or ignore this value."
    )
    vector_index: Optional[str] = Field(
        None, description="Ignored by runtime; vector memory index is managed by the system."
    )
    database_table: Optional[str] = Field(
        None, description="Ignored by runtime; database memory table is managed by the system."
    )

class ToolType(str, Enum):
    sql = "sql"
    web = "web"
    doc = "doc"
    custom = "custom"

class SqlToolConfig(BaseModel):
    dataset_ids: list[uuid.UUID] = Field(
        default_factory=list,
        description="Dataset ids available to the analyst for federated execution.",
    )
    semantic_model_ids: list[uuid.UUID] = Field(
        default_factory=list,
        description="Dataset-backed semantic model ids available to the analyst.",
    )

    @model_validator(mode="after")
    def _validate_assets(self) -> "SqlToolConfig":
        has_datasets = bool(self.dataset_ids)
        has_semantic_models = bool(self.semantic_model_ids)
        if has_datasets == has_semantic_models:
            raise ValueError(
                "SQL tool config must define either dataset_ids or semantic_model_ids."
            )
        return self

class ToolBinding(BaseModel):
    name: str = Field(..., description="Registered tool/connector name.")
    tool_type: Optional[ToolType] = Field(None, description="Type of tool.")
    description: Optional[str] = Field(None, description="Short description of the tool.")
    config: Union[SqlToolConfig, Dict[str, Any]] = Field(default_factory=dict, description="Tool-specific configuration.")

    def get_sql_tool_config(self):
        if self.tool_type == ToolType.sql:
            return SqlToolConfig.model_validate(self.config)
        else:
            raise ValueError(f"Tool type {self.tool_type} is not supported.")


class ExecutionBehavior(BaseModel):
    mode: ExecutionMode = Field(..., description="Single response or iterative planning/execution.")
    response_mode: ResponseMode = Field(
        ResponseMode.analyst,
        description="Controls agent routing and response style (analyst, chat, executive, explainer).",
    )
    max_iterations: int = Field(3, description="Cap iterations when in iterative mode.")
    max_steps_per_iteration: int = Field(5, description="Max tool calls per iteration.")
    allow_parallel_tools: bool = Field(False, description="Allow running multiple tools concurrently.")


class OutputSchema(BaseModel):
    format: OutputFormat = Field(..., description="Desired output format.")
    json_schema: Optional[Dict[str, Any]] = Field(
        None, description="JSON schema to validate structured outputs when format=json."
    )
    markdown_template: Optional[str] = Field(
        None, description="Template for markdown rendering, if applicable."
    )


class DataAccessPolicy(BaseModel):
    allowed_connectors: List[uuid.UUID] = Field(
        default_factory=list, description="Connector IDs explicitly allowed for tool access."
    )
    denied_connectors: List[uuid.UUID] = Field(
        default_factory=list, description="Connector IDs explicitly denied for tool access."
    )
    pii_handling: Optional[str] = Field(
        None, description="Guidance for handling PII (e.g., mask, redact, block)."
    )
    row_level_filter: Optional[str] = Field(
        None, description="Row-level filter expression to apply to data access."
    )


class GuardrailConfig(BaseModel):
    moderation_enabled: bool = Field(True, description="Run outputs through moderation.")
    blocked_categories: List[str] = Field(
        default_factory=list, description="Content categories to block (e.g., violence, hate)."
    )
    regex_denylist: List[str] = Field(
        default_factory=list, description="Regex patterns that should not appear in outputs."
    )
    escalation_message: Optional[str] = Field(
        None, description="What to return when content is blocked."
    )


class ObservabilityConfig(BaseModel):
    log_level: LogLevel = Field(LogLevel.info, description="Minimum log level for this agent.")
    emit_traces: bool = Field(True, description="Enable tracing/telemetry for this agent.")
    capture_prompts: bool = Field(True, description="Persist prompts/responses for debugging.")
    audit_fields: List[str] = Field(
        default_factory=list,
        description="Optional list of fields to include in audit logs (e.g., actor_id, workspace_id).",
    )

class AgentFeatures(BaseModel):
    bi_copilot_enabled: bool = Field(
        False, description="Enable the bi copilot feature for this agent."
    )
    deep_research_enabled: bool = Field(
        False, description="Enable the deep research feature for this agent."
    )
    visualization_enabled: bool = Field(
        False, description="Enable the visualization feature for this agent."
    )
    mcp_enabled: bool = Field(False, description="Enable the mcp feature for this agent.")

class AgentDefinitionModel(BaseModel):
    """Complete agent definition payload stored alongside the Agent record."""

    prompt: PromptContract
    memory: MemoryConfig
    features: AgentFeatures
    tools: List[ToolBinding] = Field(default_factory=list)
    access_policy: DataAccessPolicy = Field(default_factory=DataAccessPolicy)
    execution: ExecutionBehavior
    output: OutputSchema
    guardrails: GuardrailConfig = Field(default_factory=GuardrailConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    model_config = ConfigDict(extra="ignore")
