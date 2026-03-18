from __future__ import annotations

from langbridge.packages.orchestrator.langbridge_orchestrator.llm.provider.llm_tester import (
    LLMConnectionTester,
)
from langbridge.packages.runtime.models import LLMProvider


def test_llm_connection_tester_accepts_runtime_provider_enum(monkeypatch) -> None:
    tester = LLMConnectionTester()
    monkeypatch.setattr(
        tester,
        "_test_openai",
        lambda api_key, model: {"success": True, "message": f"{api_key}:{model}"},
    )

    result = tester.test_connection(
        provider=LLMProvider.OPENAI,
        api_key="secret",
        model="gpt-4.1",
    )

    assert result == {"success": True, "message": "secret:gpt-4.1"}
