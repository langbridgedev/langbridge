"""Simple Langbridge AI profile contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


def _config_value(config: Any, key: str, default: Any = None) -> Any:
    if isinstance(config, Mapping):
        return config.get(key, default)
    return getattr(config, key, default)


def _scoped_agent_name(kind: str, name: str) -> str:
    clean_name = str(name or kind).strip()
    if clean_name == kind or clean_name.startswith(f"{kind}."):
        return clean_name
    return f"{kind}.{clean_name}"


class AiAgentAnalystScopeConfig(BaseModel):
    semantic_models: list[str] = Field(default_factory=list)
    datasets: list[str] = Field(default_factory=list)
    query_policy: Literal[
        "semantic_preferred",
        "dataset_preferred",
        "semantic_only",
        "dataset_only",
    ] = "semantic_preferred"
    allow_source_scope: bool = False

    @model_validator(mode="after")
    def _validate_scope(self) -> "AiAgentAnalystScopeConfig":
        has_semantic_models = bool(self.semantic_models)
        has_datasets = bool(self.datasets)
        if self.query_policy in {"semantic_only", "semantic_preferred"} and not has_semantic_models and has_datasets:
            raise ValueError(
                "AI agent analyst scope with semantic-only or semantic-preferred policy must define semantic_models."
            )
        if self.query_policy in {"dataset_only", "dataset_preferred"} and not has_datasets and has_semantic_models:
            raise ValueError(
                "AI agent analyst scope with dataset-only or dataset-preferred policy must define datasets."
            )
        return self


class AiAgentLLMScopeConfig(BaseModel):
    llm_connection: str | None = None
    provider: str | None = None
    model: str | None = None
    temperature: float | None = None
    reasoning_effort: str | None = None
    max_completion_tokens: int | None = None

    @model_validator(mode="after")
    def _validate_shape(self) -> "AiAgentLLMScopeConfig":
        if str(self.llm_connection or "").strip():
            return self
        if str(self.provider or "").strip() and str(self.model or "").strip():
            return self
        raise ValueError("AI agent llm scope must define llm_connection or provider + model.")


class AiAgentResearchScopeConfig(BaseModel):
    enabled: bool = False
    extended_thinking_enabled: bool = False
    max_sources: int = 5
    require_sources: bool = False

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        payload = dict(value)
        if payload.get("extended_thinking_enabled") is None and payload.get("extended_thinking") is not None:
            payload["extended_thinking_enabled"] = payload.get("extended_thinking")
        return payload


class AiAgentWebSearchScopeConfig(BaseModel):
    enabled: bool = False
    provider: str | None = None
    allowed_domains: list[str] = Field(default_factory=list)
    require_allowed_domain: bool = False
    max_results: int = 10
    timebox_seconds: int = 10

    @model_validator(mode="after")
    def _validate_web_search(self) -> "AiAgentWebSearchScopeConfig":
        if self.require_allowed_domain and not self.allowed_domains:
            raise ValueError(
                "AI agent web_search_scope with require_allowed_domain must define allowed_domains."
            )
        return self


class AiAgentPromptsConfig(BaseModel):
    system_prompt: str | None = None
    user_prompt: str | None = None
    response_format_prompt: str | None = None
    planning_prompt: str | None = None
    presentation_prompt: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        payload = dict(value)
        aliases = {
            "system": "system_prompt",
            "user": "user_prompt",
            "response_format": "response_format_prompt",
            "planning": "planning_prompt",
            "presentation": "presentation_prompt",
        }
        for source, target in aliases.items():
            if payload.get(target) is None and payload.get(source) is not None:
                payload[target] = payload.get(source)
        return payload


class AiAgentAccessConfig(BaseModel):
    allowed_connectors: list[str] = Field(default_factory=list)
    denied_connectors: list[str] = Field(default_factory=list)


class AiAgentExecutionConfig(BaseModel):
    max_iterations: int = 3
    max_replans: int = 2
    max_step_retries: int = 1
    max_evidence_rounds: int = 2
    max_governed_attempts: int = 2
    max_external_augmentations: int = 3
    final_review_enabled: bool = True


class AiAgentProfile(BaseModel):
    name: str
    description: str | None = None
    default: bool = False
    enabled: bool = True
    mcp_enabled: bool = False
    analyst_scope: AiAgentAnalystScopeConfig = Field(default_factory=AiAgentAnalystScopeConfig)
    llm_scope: AiAgentLLMScopeConfig | None = None
    research_scope: AiAgentResearchScopeConfig = Field(default_factory=AiAgentResearchScopeConfig)
    web_search_scope: AiAgentWebSearchScopeConfig = Field(default_factory=AiAgentWebSearchScopeConfig)
    prompts: AiAgentPromptsConfig = Field(default_factory=AiAgentPromptsConfig)
    access: AiAgentAccessConfig = Field(default_factory=AiAgentAccessConfig)
    execution: AiAgentExecutionConfig = Field(default_factory=AiAgentExecutionConfig)

    @model_validator(mode="before")
    @classmethod
    def _normalize_aliases(cls, value: Any) -> Any:
        if isinstance(value, Mapping):
            payload = dict(value)
        elif hasattr(value, "model_dump"):
            payload = value.model_dump(mode="json", exclude_none=True)
        else:
            payload = {
                "name": _config_value(value, "name"),
                "description": _config_value(value, "description"),
                "default": _config_value(value, "default", False),
                "enabled": _config_value(value, "enabled", True),
                "mcp_enabled": _config_value(value, "mcp_enabled", False),
                "analyst_scope": _config_value(value, "analyst_scope"),
                "llm_scope": _config_value(value, "llm_scope"),
                "research_scope": _config_value(value, "research_scope"),
                "web_search_scope": _config_value(value, "web_search_scope"),
                "prompts": _config_value(value, "prompts"),
                "access": _config_value(value, "access"),
                "execution": _config_value(value, "execution"),
            }

        aliases = {
            "scope": "analyst_scope",
            "research": "research_scope",
            "web_search": "web_search_scope",
            "llm": "llm_scope",
        }
        for source, target in aliases.items():
            if payload.get(target) is None and payload.get(source) is not None:
                payload[target] = payload.get(source)

        exposure = payload.get("exposure")
        if isinstance(exposure, Mapping):
            if payload.get("enabled") is None and exposure.get("runtime") is not None:
                payload["enabled"] = bool(exposure.get("runtime"))
            if payload.get("mcp_enabled") is None and exposure.get("mcp") is not None:
                payload["mcp_enabled"] = bool(exposure.get("mcp"))

        return payload

    @model_validator(mode="after")
    def _validate_profile(self) -> "AiAgentProfile":
        has_data_scope = bool(self.analyst_scope.semantic_models or self.analyst_scope.datasets)
        if not has_data_scope and not self.web_search_scope.enabled:
            raise ValueError("AI agent profile must define analyst scope datasets/semantic models or enable web search.")
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
        return self.enabled

    @property
    def available_via_mcp(self) -> bool:
        return self.enabled and self.mcp_enabled

    def to_analyst_config(self) -> "AnalystAgentConfig":
        return AnalystAgentConfig.from_profile(self)


class AnalystAgentConfig(BaseModel):
    name: str
    description: str | None = None
    analyst_scope: AiAgentAnalystScopeConfig = Field(default_factory=AiAgentAnalystScopeConfig)
    research_scope: AiAgentResearchScopeConfig = Field(default_factory=AiAgentResearchScopeConfig)
    web_search_scope: AiAgentWebSearchScopeConfig = Field(default_factory=AiAgentWebSearchScopeConfig)
    prompts: AiAgentPromptsConfig = Field(default_factory=AiAgentPromptsConfig)
    access: AiAgentAccessConfig = Field(default_factory=AiAgentAccessConfig)
    execution: AiAgentExecutionConfig = Field(default_factory=AiAgentExecutionConfig)

    @classmethod
    def from_profile(cls, profile: AiAgentProfile) -> "AnalystAgentConfig":
        return cls.model_validate(
            {
                "name": profile.name,
                "description": profile.description,
                "analyst_scope": profile.analyst_scope.model_dump(mode="json"),
                "research_scope": profile.research_scope.model_dump(mode="json"),
                "web_search_scope": profile.web_search_scope.model_dump(mode="json"),
                "prompts": profile.prompts.model_dump(mode="json"),
                "access": profile.access.model_dump(mode="json"),
                "execution": profile.execution.model_dump(mode="json"),
            }
        )

    @property
    def agent_name(self) -> str:
        return _scoped_agent_name("analyst", self.name)

    @property
    def semantic_model_ids(self) -> list[str]:
        return list(self.analyst_scope.semantic_models)

    @property
    def dataset_ids(self) -> list[str]:
        return list(self.analyst_scope.datasets)

    @property
    def query_policy(self) -> str:
        return self.analyst_scope.query_policy

    @property
    def allow_source_scope(self) -> bool:
        return self.analyst_scope.allow_source_scope

    @property
    def supports_research(self) -> bool:
        return self.research_scope.enabled

    @property
    def supports_extended_thinking(self) -> bool:
        return self.research_scope.extended_thinking_enabled

    @property
    def max_sources(self) -> int:
        return self.research_scope.max_sources

    @property
    def require_sources(self) -> bool:
        return self.research_scope.require_sources

    @property
    def web_search_enabled(self) -> bool:
        return self.web_search_scope.enabled

    @property
    def web_search_provider(self) -> str | None:
        return self.web_search_scope.provider

    @property
    def web_search_allowed_domains(self) -> list[str]:
        return list(self.web_search_scope.allowed_domains)

    @property
    def web_search_require_allowed_domain(self) -> bool:
        return self.web_search_scope.require_allowed_domain

    @property
    def web_search_max_results(self) -> int:
        return self.web_search_scope.max_results

    @property
    def web_search_timebox_seconds(self) -> int:
        return self.web_search_scope.timebox_seconds

    @property
    def max_evidence_rounds(self) -> int:
        return self.execution.max_evidence_rounds

    @property
    def max_governed_attempts(self) -> int:
        return self.execution.max_governed_attempts

    @property
    def max_external_augmentations(self) -> int:
        return self.execution.max_external_augmentations

    @property
    def final_review_enabled(self) -> bool:
        return self.execution.final_review_enabled


def _legacy_query_policy(config: Mapping[str, Any]) -> str:
    query_policy = str(config.get("query_scope_policy") or "semantic_preferred").strip().lower()
    has_semantic_models = bool(config.get("semantic_model_ids"))
    has_datasets = bool(config.get("dataset_ids"))
    if has_datasets and not has_semantic_models and query_policy in {"semantic_only", "semantic_preferred"}:
        return "dataset_only" if query_policy == "semantic_only" else "dataset_preferred"
    if has_semantic_models and not has_datasets and query_policy in {"dataset_only", "dataset_preferred"}:
        return "semantic_only" if query_policy == "dataset_only" else "semantic_preferred"
    return query_policy


def _build_legacy_profiles(
    *,
    name: str,
    description: str | None,
    definition: Mapping[str, Any],
) -> list[AiAgentProfile]:
    payload = dict(definition)
    prompt = payload.get("prompt") if isinstance(payload.get("prompt"), Mapping) else {}
    features = payload.get("features") if isinstance(payload.get("features"), Mapping) else {}
    access_policy = payload.get("access_policy") if isinstance(payload.get("access_policy"), Mapping) else {}
    execution = payload.get("execution") if isinstance(payload.get("execution"), Mapping) else {}
    tools = payload.get("tools") if isinstance(payload.get("tools"), list) else []

    web_search_payload: dict[str, Any] | None = None
    profiles: list[AiAgentProfile] = []

    for raw_tool in tools:
        if not isinstance(raw_tool, Mapping):
            continue
        tool_type = str(raw_tool.get("tool_type") or "").strip().casefold()
        tool_config = raw_tool.get("config") if isinstance(raw_tool.get("config"), Mapping) else {}
        if tool_type in {"web", "web_search"}:
            web_search_payload = {
                "enabled": True,
                "provider": tool_config.get("provider"),
                "allowed_domains": list(tool_config.get("allowed_domains") or []),
                "require_allowed_domain": bool(tool_config.get("require_allowed_domain")),
                "max_results": int(tool_config.get("max_results") or 10),
                "timebox_seconds": int(tool_config.get("timebox_seconds") or 10),
            }
            continue
        if tool_type != "sql":
            continue
        profile_name = str(raw_tool.get("name") or f"{name}_analyst").strip() or f"{name}_analyst"
        scope_payload = {
            "semantic_models": list(tool_config.get("semantic_model_ids") or []),
            "datasets": list(tool_config.get("dataset_ids") or []),
            "query_policy": _legacy_query_policy(tool_config),
            "allow_source_scope": bool(tool_config.get("allow_source_scope")),
        }
        profiles.append(
            AiAgentProfile.model_validate(
                {
                    "name": profile_name,
                    "description": str(raw_tool.get("description") or description or "").strip() or None,
                    "analyst_scope": scope_payload,
                    "research_scope": {
                        "enabled": bool(
                            features.get("supports_deep_research")
                            or features.get("deep_research_enabled")
                        ),
                        "extended_thinking_enabled": bool(
                            features.get("supports_extended_thinking")
                            or features.get("extended_thinking_enabled")
                        ),
                    },
                    "web_search_scope": web_search_payload or {"enabled": False},
                    "prompts": {
                        "system_prompt": prompt.get("system_prompt"),
                        "user_prompt": prompt.get("user_instructions"),
                        "response_format_prompt": prompt.get("style_guidance"),
                    },
                    "access": {
                        "allowed_connectors": list(access_policy.get("allowed_connectors") or []),
                        "denied_connectors": list(access_policy.get("denied_connectors") or []),
                    },
                    "execution": execution or {},
                }
            )
        )

    if profiles:
        if web_search_payload is not None:
            for index, profile in enumerate(profiles):
                profiles[index] = profile.model_copy(
                    update={
                        "research_scope": profile.research_scope.model_copy(update={"enabled": True}),
                        "web_search_scope": AiAgentWebSearchScopeConfig.model_validate(web_search_payload),
                    }
                )
        return profiles

    if web_search_payload is not None:
        return [
            AiAgentProfile.model_validate(
                {
                    "name": name,
                    "description": description,
                    "analyst_scope": {},
                    "research_scope": {
                        "enabled": True,
                        "extended_thinking_enabled": bool(
                            features.get("supports_extended_thinking")
                            or features.get("extended_thinking_enabled")
                        ),
                    },
                    "web_search_scope": web_search_payload,
                    "prompts": {
                        "system_prompt": prompt.get("system_prompt"),
                        "user_prompt": prompt.get("user_instructions"),
                        "response_format_prompt": prompt.get("style_guidance"),
                    },
                    "access": {
                        "allowed_connectors": list(access_policy.get("allowed_connectors") or []),
                        "denied_connectors": list(access_policy.get("denied_connectors") or []),
                    },
                    "execution": execution or {},
                }
            )
        ]

    raise ValueError("Legacy Langbridge AI definition did not resolve to any analyst profile.")


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
                definition=item,
            )
            for index, item in enumerate(profiles_payload, start=1)
            if isinstance(item, Mapping)
        ]
    elif isinstance(payload.get("analyst_scope"), Mapping) or isinstance(payload.get("scope"), Mapping):
        profiles = [AiAgentProfile.from_definition(name=name, description=description, definition=payload)]
    else:
        profiles = _build_legacy_profiles(name=name, description=description, definition=payload)

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


def build_execution_from_definition(
    *,
    definition: Mapping[str, Any],
    name: str | None = None,
    description: str | None = None,
) -> AiAgentExecutionConfig:
    payload = dict(definition or {})
    execution_payload = payload.get("execution")
    if isinstance(execution_payload, Mapping):
        return AiAgentExecutionConfig.model_validate(execution_payload)

    try:
        profiles = build_ai_profiles_from_definition(
            name=name or "agent",
            description=description,
            definition=payload,
            runtime_only=True,
        )
    except ValueError:
        return AiAgentExecutionConfig()
    if not profiles:
        return AiAgentExecutionConfig()
    return AiAgentExecutionConfig(
        max_iterations=max(profile.execution.max_iterations for profile in profiles),
        max_replans=max(profile.execution.max_replans for profile in profiles),
        max_step_retries=max(profile.execution.max_step_retries for profile in profiles),
        max_evidence_rounds=max(profile.execution.max_evidence_rounds for profile in profiles),
        max_governed_attempts=max(profile.execution.max_governed_attempts for profile in profiles),
        max_external_augmentations=max(profile.execution.max_external_augmentations for profile in profiles),
        final_review_enabled=any(profile.execution.final_review_enabled for profile in profiles),
    )


__all__ = [
    "AiAgentAccessConfig",
    "AiAgentAnalystScopeConfig",
    "AiAgentExecutionConfig",
    "AiAgentLLMScopeConfig",
    "AiAgentProfile",
    "AiAgentPromptsConfig",
    "AiAgentResearchScopeConfig",
    "AiAgentWebSearchScopeConfig",
    "AnalystAgentConfig",
    "build_ai_profiles_from_definition",
    "build_analyst_configs_from_definition",
    "build_execution_from_definition",
]
