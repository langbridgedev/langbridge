import pytest

from langbridge.ai.profiles import AiAgentProfile, AnalystAgentConfig, build_analyst_configs_from_definition
from langbridge.runtime.config.models import LocalRuntimeConfig, LocalRuntimeAiProfileConfig


def test_ai_agent_profile_from_config_parses_simple_shape() -> None:
    profile = AiAgentProfile.from_config(
        {
            "name": "commerce",
            "description": "Commerce analyst",
            "default": True,
            "enabled": True,
            "mcp_enabled": True,
            "analyst_scope": {
                "semantic_models": ["commerce"],
                "datasets": ["sales_orders"],
                "query_policy": "semantic_preferred",
            },
            "llm_scope": {"llm_connection": "openai_primary"},
            "research_scope": {"enabled": True, "extended_thinking_enabled": True},
            "web_search_scope": {
                "enabled": True,
                "provider": "tavily",
                "allowed_domains": ["docs.langbridge.dev"],
                "require_allowed_domain": True,
            },
            "prompts": {"system_prompt": "You are commerce analyst."},
            "access": {"allowed_connectors": ["warehouse"]},
        }
    )

    assert profile.name == "commerce"
    assert profile.llm_scope is not None
    assert profile.llm_scope.llm_connection == "openai_primary"
    assert profile.available_via_runtime is True
    assert profile.available_via_mcp is True


def test_ai_agent_profile_rejects_invalid_web_search_policy() -> None:
    with pytest.raises(ValueError, match="allowed_domains"):
        AiAgentProfile.from_config(
            {
                "name": "research",
                "analyst_scope": {"datasets": ["docs_dataset"], "query_policy": "dataset_only"},
                "web_search_scope": {
                    "enabled": True,
                    "require_allowed_domain": True,
                },
            }
        )


def test_local_runtime_ai_profile_config_validates_scope() -> None:
    profile = LocalRuntimeAiProfileConfig.model_validate(
        {
            "name": "support",
            "analyst_scope": {
                "datasets": ["tickets"],
                "query_policy": "dataset_only",
            },
        }
    )

    assert profile.name == "support"
    assert profile.analyst_scope.datasets == ["tickets"]


def test_local_runtime_config_parses_ai_profiles_root() -> None:
    config = LocalRuntimeConfig.model_validate(
        {
            "version": 1,
            "ai": {
                "profiles": [
                    {
                        "name": "commerce",
                        "default": True,
                        "mcp_enabled": True,
                        "analyst_scope": {
                            "semantic_models": ["commerce"],
                            "query_policy": "semantic_only",
                        },
                        "llm_scope": {"llm_connection": "openai_primary"},
                    }
                ]
            },
        }
    )

    assert len(config.ai.profiles) == 1
    assert config.ai.profiles[0].name == "commerce"
    assert config.ai.profiles[0].mcp_enabled is True


def test_analyst_agent_config_from_profile_keeps_narrow_shape() -> None:
    profile = AiAgentProfile.from_config(
        {
            "name": "commerce",
            "description": "Commerce analyst",
            "analyst_scope": {
                "semantic_models": ["commerce"],
                "datasets": ["sales_orders"],
                "query_policy": "semantic_preferred",
            },
            "research_scope": {"enabled": True, "max_sources": 8},
            "web_search_scope": {
                "enabled": True,
                "provider": "tavily",
                "allowed_domains": ["docs.langbridge.dev"],
            },
            "prompts": {"system_prompt": "You are commerce analyst."},
            "access": {"allowed_connectors": ["warehouse"]},
        }
    )

    analyst_config = AnalystAgentConfig.from_profile(profile)

    assert analyst_config.name == "commerce"
    assert analyst_config.agent_name == "analyst.commerce"
    assert analyst_config.analyst_scope.semantic_models == ["commerce"]
    assert analyst_config.research_scope.max_sources == 8
    assert analyst_config.web_search_scope.provider == "tavily"
    assert analyst_config.prompts.system_prompt == "You are commerce analyst."
    assert analyst_config.access.allowed_connectors == ["warehouse"]


def test_build_analyst_configs_from_definition_supports_simple_ai_shape() -> None:
    configs = build_analyst_configs_from_definition(
        name="commerce",
        description="Commerce analyst",
        definition={
            "analyst_scope": {
                "semantic_models": ["commerce"],
                "datasets": ["orders"],
                "query_policy": "semantic_preferred",
            },
            "research_scope": {"enabled": True},
            "web_search_scope": {"enabled": True, "provider": "duckduckgo"},
            "prompts": {"system_prompt": "Use governed revenue definitions."},
        },
    )

    assert len(configs) == 1
    assert configs[0].agent_name == "analyst.commerce"
    assert configs[0].supports_research is True
    assert configs[0].web_search_provider == "duckduckgo"
