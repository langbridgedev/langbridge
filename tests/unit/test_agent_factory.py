import pathlib
import sys

import pytest

project_root = pathlib.Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from langbridge.packages.orchestrator.langbridge_orchestrator.definitions.factory import (  # noqa: E402
    AgentDefinitionFactory,
)

def get_base_valid_definition():
    return {
        "prompt": {
            "system_prompt": "You are a helpful assistant."
        },
        "memory": {
            "strategy": "none"
        },
        "features": {
            "bi_copilot_enabled": False,
            "deep_research_enabled": False,
            "visualization_enabled": False,
            "mcp_enabled": False,
        },
        "execution": {
            "mode": "single_step",
            "response_mode": "chat",
            "max_iterations": 1  # Valid for single_step
        },
        "output": {
            "format": "text"
        },
        "observability": {
            "log_level": "info",
            "emit_traces": True,
            "capture_prompts": True
        }
    }

def test_valid_creation():
    factory = AgentDefinitionFactory()
    definition = get_base_valid_definition()
    model = factory.create_agent_definition(definition)
    assert model.prompt.system_prompt == "You are a helpful assistant."
    assert model.memory.strategy == "none"

def test_invalid_memory_ttl_seconds():
    factory = AgentDefinitionFactory()
    definition = get_base_valid_definition()
    definition["memory"] = {"strategy": "transient", "ttl_seconds": 0}

    with pytest.raises(ValueError, match="memory.ttl_seconds must be > 0 when provided"):
        factory.create_agent_definition(definition)

def test_invalid_access_policy_overlap():
    factory = AgentDefinitionFactory()
    definition = get_base_valid_definition()
    duplicate_connector = "109a2755-6734-4cd7-a52b-99cbabdfe43a"
    definition["access_policy"] = {
        "allowed_connectors": [duplicate_connector],
        "denied_connectors": [duplicate_connector],
    }

    with pytest.raises(ValueError, match="Connectors cannot be both allowed and denied"):
        factory.create_agent_definition(definition)

def test_invalid_execution_single_step_iterations():
    factory = AgentDefinitionFactory()
    definition = get_base_valid_definition()
    definition["execution"]["mode"] = "single_step"
    definition["execution"]["max_iterations"] = 5
    
    with pytest.raises(ValueError, match="Execution mode 'single_step' cannot have 'max_iterations' > 1"):
        factory.create_agent_definition(definition)

def test_invalid_output_json_schema():
    factory = AgentDefinitionFactory()
    definition = get_base_valid_definition()
    definition["output"]["format"] = "json"
    # Missing json_schema
    
    with pytest.raises(ValueError, match="Output format 'json' requires 'json_schema'"):
        factory.create_agent_definition(definition)
