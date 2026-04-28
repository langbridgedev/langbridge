"""Factory for constructing the Langbridge AI runtime."""
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from langbridge.ai.agents.analyst import AnalystAgent
from langbridge.ai.agents.presentation import PresentationAgent
from langbridge.ai.base import BaseAgent
from langbridge.ai.events import AIEventEmitter
from langbridge.ai.llm.base import LLMProvider
from langbridge.ai.orchestration.meta_controller import MetaControllerAgent
from langbridge.ai.profiles import AiAgentExecutionConfig, AiAgentProfile, AnalystAgentConfig
from langbridge.ai.registry import AgentRegistry
from langbridge.ai.tools.charting import ChartingTool
from langbridge.ai.tools.semantic_search import SemanticSearchTool
from langbridge.ai.tools.sql import SqlAnalysisTool
from langbridge.ai.tools.web_search import (
    WebSearchPolicy,
    WebSearchProvider,
    WebSearchTool,
    create_web_search_provider,
)


@dataclass(slots=True)
class AnalystToolBundle:
    config: AnalystAgentConfig
    sql_tools: Sequence[SqlAnalysisTool] = field(default_factory=list)
    semantic_search_tools: Sequence[SemanticSearchTool] = field(default_factory=list)
    web_search_provider: WebSearchProvider | None = None


@dataclass(slots=True)
class AiProfileRuntime:
    profiles: Sequence[AiAgentProfile]
    registry: AgentRegistry
    meta_controller: MetaControllerAgent


class LangbridgeAIFactory:
    """Owns AI runtime construction so host/runtime code does not wire agents manually."""

    def __init__(self, *, llm_provider: LLMProvider, event_emitter: AIEventEmitter | None = None) -> None:
        self._llm = llm_provider
        self._event_emitter = event_emitter

    def create_meta_controller(
        self,
        *,
        analysts: Sequence[AnalystToolBundle],
        extra_agents: Sequence[BaseAgent] | None = None,
        max_iterations: int = 8,
        max_replans: int = 2,
        max_step_retries: int = 1,
        final_review_enabled: bool = True,
    ) -> MetaControllerAgent:
        agents: list[BaseAgent] = [
            AnalystAgent(
                llm_provider=self._llm,
                config=bundle.config,
                sql_analysis_tools=bundle.sql_tools,
                semantic_search_tools=bundle.semantic_search_tools,
                web_search_tool=self._web_tool_for(bundle),
                event_emitter=self._event_emitter,
            )
            for bundle in analysts
        ]
        agents.extend(extra_agents or [])
        return MetaControllerAgent(
            registry=AgentRegistry(agents),
            llm_provider=self._llm,
            presentation_agent=PresentationAgent(
                llm_provider=self._llm,
                charting_tool=ChartingTool(llm_provider=self._llm, event_emitter=self._event_emitter),
                event_emitter=self._event_emitter,
            ),
            final_review_enabled=final_review_enabled,
            max_iterations=max_iterations,
            max_replans=max_replans,
            max_step_retries=max_step_retries,
            event_emitter=self._event_emitter,
        )

    def create_profile_runtime(
        self,
        profile: AiAgentProfile | Sequence[AiAgentProfile],
        *,
        execution: AiAgentExecutionConfig | None = None,
        sql_analysis_tools: Mapping[str, list[SqlAnalysisTool]] | None = None,
        semantic_search_tools: Mapping[str, list[SemanticSearchTool]] | None = None,
        web_search_providers: Mapping[str, WebSearchProvider] | None = None,
    ) -> AiProfileRuntime:
        profiles = [profile] if isinstance(profile, AiAgentProfile) else list(profile)
        resolved_execution = execution or AiAgentExecutionConfig()
        bundles = [
            AnalystToolBundle(
                config=item.to_analyst_config(),
                sql_tools=(sql_analysis_tools or {}).get(item.name)
                or (sql_analysis_tools or {}).get(item.to_analyst_config().agent_name)
                or [],
                semantic_search_tools=(semantic_search_tools or {}).get(item.name)
                or (semantic_search_tools or {}).get(item.to_analyst_config().agent_name)
                or [],
                web_search_provider=(web_search_providers or {}).get(item.name)
                or (web_search_providers or {}).get(item.to_analyst_config().agent_name),
            )
            for item in profiles
            if item.available_via_runtime
        ]
        agents: list[BaseAgent] = [
            AnalystAgent(
                llm_provider=self._llm,
                config=bundle.config,
                sql_analysis_tools=bundle.sql_tools,
                semantic_search_tools=bundle.semantic_search_tools,
                web_search_tool=self._web_tool_for(bundle),
                event_emitter=self._event_emitter,
            )
            for bundle in bundles
        ]
        registry = AgentRegistry(agents)
        controller = MetaControllerAgent(
            registry=registry,
            llm_provider=self._llm,
            presentation_agent=PresentationAgent(
                llm_provider=self._llm,
                charting_tool=ChartingTool(llm_provider=self._llm, event_emitter=self._event_emitter),
                event_emitter=self._event_emitter,
            ),
            final_review_enabled=resolved_execution.final_review_enabled,
            max_iterations=resolved_execution.max_iterations,
            max_replans=resolved_execution.max_replans,
            max_step_retries=resolved_execution.max_step_retries,
            event_emitter=self._event_emitter,
        )
        return AiProfileRuntime(
            profiles=profiles,
            registry=registry,
            meta_controller=controller,
        )

    def _web_tool_for(self, bundle: AnalystToolBundle) -> WebSearchTool | None:
        config = bundle.config
        if not config.web_search_enabled:
            return None
        provider = bundle.web_search_provider or create_web_search_provider(config.web_search_provider)
        return WebSearchTool(
            provider=provider,
            policy=WebSearchPolicy(
                allowed_domains=list(config.web_search_allowed_domains),
                denied_domains=[],
                require_allowed_domain=config.web_search_require_allowed_domain,
                focus_terms=[],
                max_results=config.web_search_max_results,
                region=None,
                safe_search=None,
                timebox_seconds=config.web_search_timebox_seconds,
            ),
            event_emitter=self._event_emitter,
        )

__all__ = ["AiProfileRuntime", "AnalystToolBundle", "LangbridgeAIFactory"]
