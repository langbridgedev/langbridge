"""Agent registry used by the Langbridge AI meta-controller."""
from collections.abc import Iterable

from langbridge.ai.base import AgentSpecification, AgentTaskKind, BaseAgent


class AgentRegistry:
    """In-memory registry of specification-driven agents."""

    def __init__(self, agents: Iterable[BaseAgent] | None = None) -> None:
        self._agents: dict[str, BaseAgent] = {}
        for agent in agents or []:
            self.register(agent)

    def register(self, agent: BaseAgent) -> None:
        name = agent.specification.name
        if name in self._agents:
            raise ValueError(f"Agent '{name}' is already registered.")
        self._agents[name] = agent

    def get(self, name: str) -> BaseAgent:
        try:
            return self._agents[name]
        except KeyError as exc:
            raise KeyError(f"Agent '{name}' is not registered.") from exc

    def agents(self) -> list[BaseAgent]:
        return list(self._agents.values())

    def specifications(self) -> list[AgentSpecification]:
        return [agent.specification for agent in self.agents()]

    def find_by_task_kind(self, task_kind: AgentTaskKind) -> list[BaseAgent]:
        return [agent for agent in self.agents() if agent.specification.supports(task_kind)]


__all__ = ["AgentRegistry"]
