import pytest

from langbridge.ai import (
    AgentIOContract,
    AgentSpecification,
    AgentTaskKind,
)


def test_agent_specification_requires_task_kind() -> None:
    with pytest.raises(ValueError, match="task_kinds"):
        AgentSpecification(
            name="invalid",
            description="Invalid spec.",
            task_kinds=[],
        )


def test_agent_io_contract_defaults_are_independent() -> None:
    left = AgentIOContract()
    right = AgentIOContract()

    left.required_keys.append("answer")

    assert right.required_keys == []
