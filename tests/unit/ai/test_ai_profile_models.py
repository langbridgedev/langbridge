import pytest

from langbridge.ai.profiles import AiAgentProfile, AnalystAgentConfig, build_analyst_configs_from_definition
from langbridge.runtime.config.models import LocalRuntimeAiProfileConfig, LocalRuntimeConfig


def test_ai_agent_profile_from_config_parses_runtime_schema() -> None:
    profile = AiAgentProfile.from_config(
        {
            "name": "commerce",
            "description": "Commerce analyst",
            "default": True,
            "availability": {"runtime": True, "mcp": True},
            "data_scope": {
                "semantic_models": ["commerce"],
                "datasets": ["sales_orders"],
                "query_policy": "semantic_preferred",
            },
            "llm": {"llm_connection": "openai_primary"},
            "capabilities": {
                "source_sql": True,
                "research": {"enabled": True, "extended_thinking": True},
                "web_search": {
                    "enabled": True,
                    "provider": "tavily",
                    "allowed_domains": ["docs.langbridge.dev"],
                    "require_allowed_domain": True,
                },
            },
            "instructions": {"system": "You are commerce analyst."},
            "orchestration": {"policy": "strict_governed"},
            "effective_access": {"connectors": ["warehouse"]},
        }
    )

    assert profile.name == "commerce"
    assert profile.llm is not None
    assert profile.llm.llm_connection == "openai_primary"
    assert profile.available_via_runtime is True
    assert profile.available_via_mcp is True
    assert profile.data_scope.semantic_models == ["commerce"]
    assert profile.capabilities.source_sql is True
    assert profile.capabilities.research.extended_thinking is True
    assert profile.instructions.system == "You are commerce analyst."
    assert profile.orchestration.policy == "strict_governed"
    assert profile.effective_access.connectors == ["warehouse"]


def test_ai_agent_profile_rejects_invalid_web_search_policy() -> None:
    with pytest.raises(ValueError, match="allowed_domains"):
        AiAgentProfile.from_config(
            {
                "name": "research",
                "data_scope": {"datasets": ["docs_dataset"], "query_policy": "dataset_only"},
                "capabilities": {
                    "web_search": {
                        "enabled": True,
                        "require_allowed_domain": True,
                    },
                },
            }
        )


def test_ai_agent_profile_rejects_legacy_contract_keys() -> None:
    with pytest.raises(ValueError):
        AiAgentProfile.from_config(
            {
                "name": "legacy",
                "scope": {"datasets": ["tickets"], "query_policy": "dataset_only"},
            }
        )


def test_local_runtime_ai_profile_config_validates_data_scope() -> None:
    profile = LocalRuntimeAiProfileConfig.model_validate(
        {
            "name": "support",
            "data_scope": {
                "datasets": ["tickets"],
                "query_policy": "dataset_only",
            },
        }
    )

    assert profile.name == "support"
    assert profile.data_scope.datasets == ["tickets"]


def test_local_runtime_config_parses_ai_profiles_root() -> None:
    config = LocalRuntimeConfig.model_validate(
        {
            "version": 1,
            "ai": {
                "profiles": [
                    {
                        "name": "commerce",
                        "default": True,
                        "availability": {"runtime": True, "mcp": True},
                        "data_scope": {
                            "semantic_models": ["commerce"],
                            "query_policy": "semantic_only",
                        },
                        "llm": {"llm_connection": "openai_primary"},
                    }
                ]
            },
        }
    )

    assert len(config.ai.profiles) == 1
    assert config.ai.profiles[0].name == "commerce"
    assert config.ai.profiles[0].availability.mcp is True


def test_analyst_agent_config_from_profile_keeps_narrow_shape() -> None:
    profile = AiAgentProfile.from_config(
        {
            "name": "commerce",
            "description": "Commerce analyst",
            "data_scope": {
                "semantic_models": ["commerce"],
                "datasets": ["sales_orders"],
                "query_policy": "semantic_preferred",
            },
            "capabilities": {
                "research": {"enabled": True, "max_sources": 8},
                "web_search": {
                    "enabled": True,
                    "provider": "tavily",
                    "allowed_domains": ["docs.langbridge.dev"],
                },
            },
            "instructions": {"system": "You are commerce analyst."},
            "effective_access": {"connectors": ["warehouse"]},
        }
    )

    analyst_config = AnalystAgentConfig.from_profile(profile)

    assert analyst_config.name == "commerce"
    assert analyst_config.agent_name == "analyst.commerce"
    assert analyst_config.data_scope.semantic_models == ["commerce"]
    assert analyst_config.capabilities.research.max_sources == 8
    assert analyst_config.capabilities.web_search.provider == "tavily"
    assert analyst_config.instructions.system == "You are commerce analyst."
    assert analyst_config.effective_access.connectors == ["warehouse"]


def test_build_analyst_configs_from_definition_supports_runtime_ai_shape() -> None:
    configs = build_analyst_configs_from_definition(
        name="commerce",
        description="Commerce analyst",
        definition={
            "data_scope": {
                "semantic_models": ["commerce"],
                "datasets": ["orders"],
                "query_policy": "semantic_preferred",
            },
            "capabilities": {
                "research": {"enabled": True},
                "web_search": {"enabled": True, "provider": "duckduckgo"},
            },
            "instructions": {"system": "Use governed revenue definitions."},
        },
    )

    assert len(configs) == 1
    assert configs[0].agent_name == "analyst.commerce"
    assert configs[0].supports_research is True
    assert configs[0].web_search_provider == "duckduckgo"
