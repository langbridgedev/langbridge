"""Core contracts for Langbridge AI agents."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class AgentTaskKind(str, Enum):
    """Typed task families understood by the AI gateway."""
    orchestration = "orchestration"
    analyst = "analyst"
    presentation = "presentation"


class AgentRiskLevel(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"


class AgentCostLevel(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"


class AgentResultStatus(str, Enum):
    succeeded = "succeeded"
    failed = "failed"
    needs_clarification = "needs_clarification"
    blocked = "blocked"


class AgentIOContract(BaseModel):
    """Minimal deterministic IO contract used by routing and verification."""

    required_keys: list[str] = Field(default_factory=list)
    optional_keys: list[str] = Field(default_factory=list)


class AgentToolSpecification(BaseModel):
    """Tool surface exposed by an agent."""

    name: str
    description: str
    input_contract: AgentIOContract = Field(default_factory=AgentIOContract)
    output_contract: AgentIOContract = Field(default_factory=AgentIOContract)
    has_side_effects: bool = False
    risk_level: AgentRiskLevel = AgentRiskLevel.low
    supports_dry_run: bool = False


class AgentRoutingSpec(BaseModel):
    """Structured routing hints. Prompts are not routing contracts."""

    keywords: list[str] = Field(default_factory=list)
    phrases: list[str] = Field(default_factory=list)
    direct_threshold: int = 2
    planner_threshold: int = 1


class AgentSpecification(BaseModel):
    """Structured description the meta-controller can reason over."""

    name: str
    description: str
    version: str = "0.1"
    task_kinds: list[AgentTaskKind]
    capabilities: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    routing: AgentRoutingSpec = Field(default_factory=AgentRoutingSpec)
    input_contract: AgentIOContract = Field(default_factory=AgentIOContract)
    output_contract: AgentIOContract = Field(default_factory=AgentIOContract)
    tools: list[AgentToolSpecification] = Field(default_factory=list)
    can_execute_direct: bool = True
    has_side_effects: bool = False
    supports_dry_run: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)
    specification_prompt: str = ""

    @model_validator(mode="after")
    def _validate_task_kinds(self) -> "AgentSpecification":
        if not self.task_kinds:
            raise ValueError("AgentSpecification.task_kinds must contain at least one task kind.")
        return self

    def supports(self, task_kind: AgentTaskKind) -> bool:
        return task_kind in self.task_kinds


class AgentTask(BaseModel):
    """Executable task passed from controller/planner to an agent."""

    task_id: str
    task_kind: AgentTaskKind
    question: str
    input: dict[str, Any] = Field(default_factory=dict)
    context: dict[str, Any] = Field(default_factory=dict)
    expected_output: AgentIOContract = Field(default_factory=AgentIOContract)
    dry_run: bool = False


class AgentResult(BaseModel):
    """Common result envelope returned by every Langbridge AI agent."""

    task_id: str
    agent_name: str
    status: AgentResultStatus
    output: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, Any] = Field(default_factory=dict)
    diagnostics: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status == AgentResultStatus.succeeded


class BaseAgent(ABC):
    """Base class for specification-driven Langbridge AI agents."""

    @property
    @abstractmethod
    def specification(self) -> AgentSpecification:
        """Return the structured agent specification."""

    @abstractmethod
    async def execute(self, task: AgentTask) -> AgentResult:
        """Execute one planned task."""

    def build_result(
        self,
        *,
        task: AgentTask,
        status: AgentResultStatus,
        output: dict[str, Any] | None = None,
        artifacts: dict[str, Any] | None = None,
        diagnostics: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> AgentResult:
        return AgentResult(
            task_id=task.task_id,
            agent_name=self.specification.name,
            status=status,
            output=output or {},
            artifacts=artifacts or {},
            diagnostics=diagnostics or {},
            error=error,
        )


__all__ = [
    "AgentCostLevel",
    "AgentIOContract",
    "AgentResult",
    "AgentResultStatus",
    "AgentRiskLevel",
    "AgentRoutingSpec",
    "AgentSpecification",
    "AgentTask",
    "AgentTaskKind",
    "AgentToolSpecification",
    "BaseAgent",
]
