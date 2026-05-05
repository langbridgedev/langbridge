import asyncio
import os

import pytest
from pydantic import BaseModel

from langbridge.ai.llm import create_provider


pytestmark = pytest.mark.skipif(
    os.getenv("LANGBRIDGE_LIVE_LLM_SMOKE") != "1",
    reason="Set LANGBRIDGE_LIVE_LLM_SMOKE=1 to run live LLM provider smoke tests.",
)


class PetExtraction(BaseModel):
    name: str
    age: int


def _run(coro):
    return asyncio.run(coro)


def _prompt() -> str:
    return "Extract the pet details: Luna is a 4 year old cat. Return the pet name and age."


def _require_env(*names: str) -> dict[str, str]:
    values = {name: os.getenv(name) for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        pytest.skip(f"Missing env vars: {', '.join(missing)}")
    return {name: str(value) for name, value in values.items()}


def test_openai_responses_parse_structured_output_smoke() -> None:
    env = _require_env("OPENAI_API_KEY", "LANGBRIDGE_OPENAI_STRUCTURED_MODEL")
    provider = create_provider(
        {
            "provider": "openai",
            "api_key": env["OPENAI_API_KEY"],
            "model": env["LANGBRIDGE_OPENAI_STRUCTURED_MODEL"],
            "configuration": {"structured_outputs": "native"},
        }
    )

    result = _run(provider.acomplete_structured(_prompt(), response_model=PetExtraction))

    assert result == PetExtraction(name="Luna", age=4)


def test_azure_chat_parse_structured_output_smoke() -> None:
    env = _require_env(
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_DEPLOYMENT",
    )
    configuration = {
        "azure_endpoint": env["AZURE_OPENAI_ENDPOINT"],
        "deployment_name": env["AZURE_OPENAI_DEPLOYMENT"],
        "structured_outputs": "native",
    }
    if os.getenv("AZURE_OPENAI_API_VERSION"):
        configuration["api_version"] = str(os.getenv("AZURE_OPENAI_API_VERSION"))
    provider = create_provider(
        {
            "provider": "azure",
            "api_key": env["AZURE_OPENAI_API_KEY"],
            "model": env["AZURE_OPENAI_DEPLOYMENT"],
            "configuration": configuration,
        }
    )

    result = _run(provider.acomplete_structured(_prompt(), response_model=PetExtraction))

    assert result == PetExtraction(name="Luna", age=4)


def test_ollama_format_structured_output_smoke() -> None:
    env = _require_env("LANGBRIDGE_OLLAMA_BASE_URL", "LANGBRIDGE_OLLAMA_STRUCTURED_MODEL")
    provider = create_provider(
        {
            "provider": "ollama",
            "api_key": "",
            "model": env["LANGBRIDGE_OLLAMA_STRUCTURED_MODEL"],
            "configuration": {
                "base_url": env["LANGBRIDGE_OLLAMA_BASE_URL"],
                "structured_outputs": "native",
            },
        }
    )

    result = _run(provider.acomplete_structured(_prompt(), response_model=PetExtraction))

    assert result == PetExtraction(name="Luna", age=4)


def test_anthropic_forced_tool_structured_output_smoke() -> None:
    env = _require_env("ANTHROPIC_API_KEY", "LANGBRIDGE_ANTHROPIC_STRUCTURED_MODEL")
    provider = create_provider(
        {
            "provider": "anthropic",
            "api_key": env["ANTHROPIC_API_KEY"],
            "model": env["LANGBRIDGE_ANTHROPIC_STRUCTURED_MODEL"],
            "configuration": {"structured_outputs": "native"},
        }
    )

    result = _run(provider.acomplete_structured(_prompt(), response_model=PetExtraction))

    assert result == PetExtraction(name="Luna", age=4)
