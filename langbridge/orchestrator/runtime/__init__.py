from importlib import import_module

__all__ = [
    "AgentOrchestratorFactory",
    "AgentRuntime",
    "AgentToolConfig",
    "ResponseFormatter",
    "ResponsePresentation",
]


def __getattr__(name: str):
    if name in {"AgentOrchestratorFactory", "AgentRuntime", "AgentToolConfig"}:
        module = import_module(".agent_orchestrator_factory", __name__)
        return getattr(module, name)
    if name in {"ResponseFormatter", "ResponsePresentation"}:
        module = import_module(".response_formatter", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
