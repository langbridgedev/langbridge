from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from langbridge.ai import MetaControllerRun
from langbridge.ai.llm.base import LLMProvider
from langbridge.ai.tools.semantic_search import SemanticSearchTool
from langbridge.ai.tools.sql import SqlAnalysisTool
from langbridge.ai.tools.web_search import WebSearchProvider
from langbridge.runtime.models import (
    LLMConnectionSecret,
    RuntimeAgentDefinition,
    RuntimeThread,
    RuntimeThreadMessage,
)

LLMProviderFactory = Callable[[LLMConnectionSecret], LLMProvider]


@dataclass(slots=True)
class AgentExecutionResult:
    response: dict[str, Any]
    thread: RuntimeThread
    user_message: RuntimeThreadMessage
    assistant_message: RuntimeThreadMessage
    agent_definition: RuntimeAgentDefinition
    ai_run: MetaControllerRun


@dataclass(slots=True)
class AgentExecutionServiceTooling:
    sql_analysis_tools: Mapping[str, Sequence[SqlAnalysisTool]] = field(default_factory=dict)
    semantic_search_tools: Mapping[str, Sequence[SemanticSearchTool]] = field(default_factory=dict)
    web_search_providers: Mapping[str, WebSearchProvider] = field(default_factory=dict)
