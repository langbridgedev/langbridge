"""Langbridge AI profile contracts."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


AgentQueryPolicy = Literal["semantic_preferred", "dataset_preferred", "semantic_only", "dataset_only"]
AgentOrchestrationPolicy = Literal["balanced_governed", "fast_sql", "strict_governed", "research_heavy"]


def _scoped_agent_name(kind: str, name: str) -> str:
    clean_name = str(name or kind).strip()
    if clean_name == kind or clean_name.startswith(f"{kind}."):
        return clean_name
    return f"{kind}.{clean_name}"


class AiAgentAvailabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runtime: bool = True
    mcp: bool = False

    @model_validator(mode="after")
    def _validate_availability(self) -> "AiAgentAvailabilityConfig":
        if self.mcp and not self.runtime:
            raise ValueError("AI agent availability cannot enable mcp when runtime is disabled.")
        return self


class AiAgentDataScopeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    semantic_models: list[str] = Field(default_factory=list)
    datasets: list[str] = Field(default_factory=list)
    query_policy: AgentQueryPolicy = "semantic_preferred"

    @model_validator(mode="after")
    def _validate_scope(self) -> "AiAgentDataScopeConfig":
        has_semantic_models = bool(self.semantic_models)
        has_datasets = bool(self.datasets)
        if self.query_policy in {"semantic_only", "semantic_preferred"} and not has_semantic_models and has_datasets:
            raise ValueError(
                "AI agent data_scope with semantic-only or semantic-preferred policy must define semantic_models."
            )
        if self.query_policy in {"dataset_only", "dataset_preferred"} and not has_datasets and has_semantic_models:
            raise ValueError("AI agent data_scope with dataset-only or dataset-preferred policy must define datasets.")
        return self


class AiAgentLLMConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    llm_connection: str | None = None
    provider: str | None = None
    model: str | None = None
    temperature: float | None = None
    reasoning_effort: str | None = None
    max_completion_tokens: int | None = None

    @model_validator(mode="after")
    def _validate_shape(self) -> "AiAgentLLMConfig":
        if str(self.llm_connection or "").strip():
            return self
        if str(self.provider or "").strip() and str(self.model or "").strip():
            return self
        raise ValueError("AI agent llm must define llm_connection or provider + model.")


class AiAgentResearchCapabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    extended_thinking: bool = False
    max_sources: int = 5
    require_sources: bool = False


class AiAgentWebSearchCapabilityConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    provider: str | None = None
    allowed_domains: list[str] = Field(default_factory=list)
    require_allowed_domain: bool = False
    max_results: int = 10
    timebox_seconds: int = 10

    @model_validator(mode="after")
    def _validate_web_search(self) -> "AiAgentWebSearchCapabilityConfig":
        if self.require_allowed_domain and not self.allowed_domains:
            raise ValueError("AI agent web_search capability with require_allowed_domain must define allowed_domains.")
        return self


class AiAgentCapabilitiesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_sql: bool = False
    research: AiAgentResearchCapabilityConfig = Field(default_factory=AiAgentResearchCapabilityConfig)
    web_search: AiAgentWebSearchCapabilityConfig = Field(default_factory=AiAgentWebSearchCapabilityConfig)

    @model_validator(mode="after")
    def _validate_capabilities(self) -> "AiAgentCapabilitiesConfig":
        if self.research.require_sources and not self.web_search.enabled:
            raise ValueError(
                "AI agent capabilities: research.require_sources is true but web_search is not enabled."
            )
        return self


class AiAgentInstructionsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    system: str | None = None
    user: str | None = None
    response_format: str | None = None
    planning: str | None = None
    presentation: str | None = None


class AiAgentOrchestrationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy: AgentOrchestrationPolicy = "balanced_governed"


class AiAgentEffectiveAccessConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connectors: list[str] = Field(default_factory=list)


class ResolvedAgentOrchestrationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy: AgentOrchestrationPolicy = "balanced_governed"
    max_iterations: int = 3
    max_replans: int = 2
    max_step_retries: int = 1
    max_evidence_rounds: int = 2
    max_governed_attempts: int = 2
    max_external_augmentations: int = 1
    final_review_enabled: bool = True


_ORCHESTRATION_POLICY_BUDGETS: dict[AgentOrchestrationPolicy, ResolvedAgentOrchestrationConfig] = {
    "balanced_governed": ResolvedAgentOrchestrationConfig(
        policy="balanced_governed",
        max_iterations=3,
        max_replans=2,
        max_step_retries=1,
        max_evidence_rounds=2,
        max_governed_attempts=2,
        max_external_augmentations=1,
        final_review_enabled=True,
    ),
    "fast_sql": ResolvedAgentOrchestrationConfig(
        policy="fast_sql",
        max_iterations=2,
        max_replans=0,
        max_step_retries=0,
        max_evidence_rounds=1,
        max_governed_attempts=1,
        max_external_augmentations=0,
        final_review_enabled=True,
    ),
    "strict_governed": ResolvedAgentOrchestrationConfig(
        policy="strict_governed",
        max_iterations=3,
        max_replans=1,
        max_step_retries=1,
        max_evidence_rounds=2,
        max_governed_attempts=2,
        max_external_augmentations=0,
        final_review_enabled=True,
    ),
    "research_heavy": ResolvedAgentOrchestrationConfig(
        policy="research_heavy",
        max_iterations=6,
        max_replans=2,
        max_step_retries=1,
        max_evidence_rounds=4,
        max_governed_attempts=3,
        max_external_augmentations=3,
        final_review_enabled=True,
    ),
}


class AiAgentProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    default: bool = False
    availability: AiAgentAvailabilityConfig = Field(default_factory=AiAgentAvailabilityConfig)
    data_scope: AiAgentDataScopeConfig = Field(default_factory=AiAgentDataScopeConfig)
    capabilities: AiAgentCapabilitiesConfig = Field(default_factory=AiAgentCapabilitiesConfig)
    llm: AiAgentLLMConfig | None = None
    instructions: AiAgentInstructionsConfig = Field(default_factory=AiAgentInstructionsConfig)
    orchestration: AiAgentOrchestrationConfig = Field(default_factory=AiAgentOrchestrationConfig)
    effective_access: AiAgentEffectiveAccessConfig = Field(default_factory=AiAgentEffectiveAccessConfig)

    @model_validator(mode="after")
    def _validate_profile(self) -> "AiAgentProfile":
        has_data_scope = bool(self.data_scope.semantic_models or self.data_scope.datasets)
        if not has_data_scope and not self.capabilities.web_search.enabled:
            raise ValueError("AI agent profile must define data_scope semantic_models/datasets or enable web_search.")
        return self

    @classmethod
    def from_config(cls, config: Mapping[str, Any] | Any) -> "AiAgentProfile":
        return cls.model_validate(config)

    @classmethod
    def from_definition(
        cls,
        *,
        name: str,
        description: str | None = None,
        default: bool = False,
        definition: Mapping[str, Any],
    ) -> "AiAgentProfile":
        payload = dict(definition)
        payload.setdefault("name", name)
        payload.setdefault("description", description)
        payload.setdefault("default", default)
        return cls.from_config(payload)

    @property
    def available_via_runtime(self) -> bool:
        return self.availability.runtime

    @property
    def available_via_mcp(self) -> bool:
        return self.availability.runtime and self.availability.mcp

    def to_analyst_config(self) -> "AnalystAgentConfig":
        return AnalystAgentConfig.from_profile(self)

    def resolved_orchestration(self) -> ResolvedAgentOrchestrationConfig:
        return resolve_orchestration_policy(self.orchestration.policy)


class AnalystAgentConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    data_scope: AiAgentDataScopeConfig = Field(default_factory=AiAgentDataScopeConfig)
    capabilities: AiAgentCapabilitiesConfig = Field(default_factory=AiAgentCapabilitiesConfig)
    instructions: AiAgentInstructionsConfig = Field(default_factory=AiAgentInstructionsConfig)
    orchestration: AiAgentOrchestrationConfig = Field(default_factory=AiAgentOrchestrationConfig)
    effective_access: AiAgentEffectiveAccessConfig = Field(default_factory=AiAgentEffectiveAccessConfig)

    @classmethod
    def from_profile(cls, profile: AiAgentProfile) -> "AnalystAgentConfig":
        return cls.model_validate(
            {
                "name": profile.name,
                "description": profile.description,
                "data_scope": profile.data_scope.model_dump(mode="json"),
                "capabilities": profile.capabilities.model_dump(mode="json"),
                "instructions": profile.instructions.model_dump(mode="json"),
                "orchestration": profile.orchestration.model_dump(mode="json"),
                "effective_access": profile.effective_access.model_dump(mode="json"),
            }
        )

    @property
    def agent_name(self) -> str:
        return _scoped_agent_name("analyst", self.name)

    @property
    def semantic_model_ids(self) -> list[str]:
        return list(self.data_scope.semantic_models)

    @property
    def dataset_ids(self) -> list[str]:
        return list(self.data_scope.datasets)

    @property
    def query_policy(self) -> str:
        return self.data_scope.query_policy

    @property
    def source_sql_enabled(self) -> bool:
        return self.capabilities.source_sql

    @property
    def supports_research(self) -> bool:
        return self.capabilities.research.enabled

    @property
    def supports_extended_thinking(self) -> bool:
        return self.capabilities.research.extended_thinking

    @property
    def max_sources(self) -> int:
        return self.capabilities.research.max_sources

    @property
    def require_sources(self) -> bool:
        return self.capabilities.research.require_sources

    @property
    def web_search_enabled(self) -> bool:
        return self.capabilities.web_search.enabled

    @property
    def web_search_provider(self) -> str | None:
        return self.capabilities.web_search.provider

    @property
    def web_search_allowed_domains(self) -> list[str]:
        return list(self.capabilities.web_search.allowed_domains)

    @property
    def web_search_require_allowed_domain(self) -> bool:
        return self.capabilities.web_search.require_allowed_domain

    @property
    def web_search_max_results(self) -> int:
        return self.capabilities.web_search.max_results

    @property
    def web_search_timebox_seconds(self) -> int:
        return self.capabilities.web_search.timebox_seconds

    @property
    def max_evidence_rounds(self) -> int:
        return resolve_orchestration_policy(self.orchestration.policy).max_evidence_rounds

    @property
    def max_governed_attempts(self) -> int:
        return resolve_orchestration_policy(self.orchestration.policy).max_governed_attempts

    @property
    def max_external_augmentations(self) -> int:
        return resolve_orchestration_policy(self.orchestration.policy).max_external_augmentations

    @property
    def final_review_enabled(self) -> bool:
        return resolve_orchestration_policy(self.orchestration.policy).final_review_enabled


def resolve_orchestration_policy(policy: AgentOrchestrationPolicy | str | None) -> ResolvedAgentOrchestrationConfig:
    normalized = str(policy or "balanced_governed").strip().lower()
    if normalized not in _ORCHESTRATION_POLICY_BUDGETS:
        raise ValueError(f"Unsupported AI agent orchestration policy: {policy}")
    return _ORCHESTRATION_POLICY_BUDGETS[normalized].model_copy()


def build_ai_profiles_from_definition(
    *,
    name: str,
    description: str | None,
    definition: Mapping[str, Any],
    runtime_only: bool = True,
) -> list[AiAgentProfile]:
    payload = dict(definition or {})
    profiles_payload = payload.get("profiles")

    if isinstance(profiles_payload, list) and profiles_payload:
        profiles = [
            AiAgentProfile.from_definition(
                name=str(item.get("name") or f"{name}_{index}").strip() or f"{name}_{index}",
                description=str(item.get("description") or description or "").strip() or None,
                default=bool(item.get("default", False)),
                definition=item,
            )
            for index, item in enumerate(profiles_payload, start=1)
            if isinstance(item, Mapping)
        ]
    else:
        profiles = [
            AiAgentProfile.from_definition(
                name=name,
                description=description,
                default=bool(payload.get("default", False)),
                definition=payload,
            )
        ]

    if runtime_only:
        profiles = [profile for profile in profiles if profile.available_via_runtime]
    return profiles


def build_analyst_configs_from_definition(
    *,
    name: str,
    description: str | None,
    definition: Mapping[str, Any],
    runtime_only: bool = True,
) -> list[AnalystAgentConfig]:
    return [
        profile.to_analyst_config()
        for profile in build_ai_profiles_from_definition(
            name=name,
            description=description,
            definition=definition,
            runtime_only=runtime_only,
        )
    ]


def resolve_orchestration_from_definition(
    *,
    definition: Mapping[str, Any],
    name: str | None = None,
    description: str | None = None,
) -> ResolvedAgentOrchestrationConfig:
    try:
        profiles = build_ai_profiles_from_definition(
            name=name or "agent",
            description=description,
            definition=definition,
            runtime_only=True,
        )
    except ValueError:
        raise

    if not profiles:
        return resolve_orchestration_policy("balanced_governed")

    resolved = [profile.resolved_orchestration() for profile in profiles]
    return ResolvedAgentOrchestrationConfig(
        policy=profiles[0].orchestration.policy,
        max_iterations=max(item.max_iterations for item in resolved),
        max_replans=max(item.max_replans for item in resolved),
        max_step_retries=max(item.max_step_retries for item in resolved),
        max_evidence_rounds=max(item.max_evidence_rounds for item in resolved),
        max_governed_attempts=max(item.max_governed_attempts for item in resolved),
        max_external_augmentations=max(item.max_external_augmentations for item in resolved),
        final_review_enabled=any(item.final_review_enabled for item in resolved),
    )


__all__ = [
    "AgentOrchestrationPolicy",
    "AgentQueryPolicy",
    "AiAgentAvailabilityConfig",
    "AiAgentCapabilitiesConfig",
    "AiAgentDataScopeConfig",
    "AiAgentEffectiveAccessConfig",
    "AiAgentInstructionsConfig",
    "AiAgentLLMConfig",
    "AiAgentOrchestrationConfig",
    "AiAgentProfile",
    "AiAgentResearchCapabilityConfig",
    "AiAgentWebSearchCapabilityConfig",
    "AnalystAgentConfig",
    "ResolvedAgentOrchestrationConfig",
    "build_ai_profiles_from_definition",
    "build_analyst_configs_from_definition",
    "resolve_orchestration_from_definition",
    "resolve_orchestration_policy",
]
