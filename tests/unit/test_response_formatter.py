import asyncio
import pathlib
import sys
from typing import Any

project_root = pathlib.Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from langbridge.orchestrator.definitions import (  # noqa: E402
    GuardrailConfig,
    OutputFormat,
    OutputSchema,
    PromptContract,
    ResponseMode,
)
from langbridge.orchestrator.llm.provider import (  # noqa: E402
    LLMConnectionConfig,
    LLMProvider,
    LLMProviderName,
)
from langbridge.orchestrator.runtime.response_formatter import (  # noqa: E402
    ResponseFormatter,
    ResponsePresentation,
)
from langbridge.orchestrator.tools.sql_analyst.interfaces import (  # noqa: E402
    AnalystExecutionOutcome,
    AnalystOutcomeStage,
    AnalystOutcomeStatus,
    AnalystQueryResponse,
)


class _FakeProvider(LLMProvider):
    name = LLMProviderName.OPENAI

    def __init__(self, response_text: str) -> None:
        super().__init__(
            LLMConnectionConfig(
                provider=LLMProviderName.OPENAI,
                api_key="test-key",
                model="test-model",
            )
        )
        self.response_text = response_text
        self.calls: list[dict[str, Any]] = []

    def create_chat_model(self, **overrides: Any) -> Any:  # pragma: no cover - unused in tests
        raise NotImplementedError

    def complete(
        self,
        prompt: str,
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:  # pragma: no cover - unused in tests
        raise NotImplementedError

    async def acomplete(
        self,
        prompt: str,
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:  # pragma: no cover - unused in tests
        raise NotImplementedError

    def invoke(
        self,
        messages: list[dict[str, Any]] | list[Any],
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> Any:  # pragma: no cover - unused in tests
        raise NotImplementedError

    async def ainvoke(
        self,
        messages: list[dict[str, Any]] | list[Any],
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> Any:
        self.calls.append(
            {
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
        )
        return self.response_text

    async def create_embeddings(
        self,
        texts: list[str],
        embedding_model: str | None = None,
    ) -> list[list[float]]:  # pragma: no cover - unused in tests
        raise NotImplementedError


def test_generate_chat_response_uses_chat_mode_prompt_and_context() -> None:
    provider = _FakeProvider("Hello back.")
    formatter = ResponseFormatter()
    presentation = ResponsePresentation(
        prompt_contract=PromptContract(
            system_prompt="System prompt",
            user_instructions="User instructions",
            style_guidance="Style guidance",
        ),
        output_schema=OutputSchema(format=OutputFormat.markdown, markdown_template="## Reply"),
        guardrails=GuardrailConfig(),
        response_mode=ResponseMode.chat,
    )

    result = asyncio.run(
        formatter.generate_chat_response(
            provider,
            "How are we doing?",
            conversation_context="User: Hi\nAssistant: Hello",
            presentation=presentation,
        )
    )

    assert result == "Hello back."
    assert len(provider.calls) == 1
    system_prompt = str(provider.calls[0]["messages"][0].content)
    human_prompt = str(provider.calls[0]["messages"][1].content)
    assert "helpful conversational assistant" in system_prompt
    assert "System prompt" in system_prompt
    assert "User instructions" in system_prompt
    assert "Style guidance" in system_prompt
    assert "Conversation so far:" in human_prompt
    assert "Markdown template:\n## Reply" in human_prompt


def test_summarize_response_uses_executive_prompt_and_guardrails() -> None:
    provider = _FakeProvider("This contains forbidden text.")
    formatter = ResponseFormatter()
    presentation = ResponsePresentation(
        prompt_contract=PromptContract(system_prompt="System prompt"),
        output_schema=OutputSchema(format=OutputFormat.text),
        guardrails=GuardrailConfig(
            moderation_enabled=True,
            regex_denylist=["forbidden"],
            escalation_message="Blocked by guardrails.",
        ),
        response_mode=ResponseMode.executive,
    )

    result = asyncio.run(
        formatter.summarize_response(
            provider,
            "Summarize revenue",
            {
                "result": {
                    "columns": ["region", "revenue"],
                    "rows": [["EMEA", 1200], ["US", 2200]],
                },
                "visualization": {"chart_type": "bar", "x": "region", "y": "revenue"},
            },
            presentation=presentation,
        )
    )

    assert result == "Blocked by guardrails."
    assert len(provider.calls) == 1
    human_prompt = str(provider.calls[0]["messages"][-1].content)
    assert "executive briefing assistant" in human_prompt
    assert "Return 3 bullet points and 1 recommended action." in human_prompt
    assert "Tabular result preview:" in human_prompt


def test_summarize_response_includes_grounded_analytical_context_for_analyst_mode() -> None:
    provider = _FakeProvider("US leads revenue.")
    formatter = ResponseFormatter()
    presentation = ResponsePresentation(
        prompt_contract=PromptContract(system_prompt="System prompt"),
        output_schema=OutputSchema(format=OutputFormat.text),
        guardrails=GuardrailConfig(),
        response_mode=ResponseMode.analyst,
    )

    result = asyncio.run(
        formatter.summarize_response(
            provider,
            "Which region had the highest revenue?",
            {
                "result": {
                    "columns": ["region", "revenue"],
                    "rows": [["US", 2200], ["EMEA", 1200], ["APAC", 800]],
                },
                "visualization": {"chart_type": "bar", "x": "region", "y": "revenue"},
            },
            presentation=presentation,
        )
    )

    assert result == "US leads revenue."
    human_prompt = str(provider.calls[0]["messages"][-1].content)
    assert "Key analytical facts:" in human_prompt
    assert "Observed facts:" in human_prompt
    assert "Interpret the result instead of restating the table." in human_prompt


def test_summarize_response_includes_structured_analyst_outcome_context() -> None:
    provider = _FakeProvider("Handled failure.")
    formatter = ResponseFormatter()
    presentation = ResponsePresentation(
        prompt_contract=PromptContract(system_prompt="System prompt"),
        output_schema=OutputSchema(format=OutputFormat.text),
        guardrails=GuardrailConfig(),
        response_mode=ResponseMode.analyst,
    )

    result = asyncio.run(
        formatter.summarize_response(
            provider,
            "Revenue by region",
            {
                "analyst_result": AnalystQueryResponse(
                    analysis_path="dataset",
                    execution_mode="federated",
                    asset_type="dataset",
                    asset_id="dataset-1",
                    asset_name="sales_dataset",
                    sql_canonical="select revenue from sales",
                    sql_executable="select revenue from sales",
                    dialect="postgres",
                    error="Canonical SQL failed to parse.",
                    outcome=AnalystExecutionOutcome(
                        status=AnalystOutcomeStatus.query_error,
                        stage=AnalystOutcomeStage.query,
                        message="Canonical SQL failed to parse.",
                        recoverable=False,
                        terminal=True,
                        retry_attempted=True,
                        retry_count=1,
                    ),
                ),
                "result": {"columns": ["region", "revenue"], "rows": []},
            },
            presentation=presentation,
        )
    )

    assert result == "Handled failure."
    human_prompt = str(provider.calls[0]["messages"][-1].content)
    assert "Analyst outcome:" in human_prompt
    assert "status=query_error" in human_prompt


def test_summarize_response_handles_access_denied_outcome_explicitly() -> None:
    provider = _FakeProvider("Access denied summary.")
    formatter = ResponseFormatter()
    presentation = ResponsePresentation(
        prompt_contract=PromptContract(system_prompt="System prompt"),
        output_schema=OutputSchema(format=OutputFormat.text),
        guardrails=GuardrailConfig(),
        response_mode=ResponseMode.analyst,
    )

    result = asyncio.run(
        formatter.summarize_response(
            provider,
            "Revenue by payroll dataset",
            {
                "analyst_result": AnalystQueryResponse(
                    analysis_path="dataset",
                    execution_mode="federated",
                    asset_type="dataset",
                    asset_id="dataset-blocked",
                    asset_name="payroll_dataset",
                    sql_canonical="",
                    sql_executable="",
                    dialect="n/a",
                    error="Access denied: payroll_dataset uses a connector that is explicitly denied for this agent.",
                    outcome=AnalystExecutionOutcome(
                        status=AnalystOutcomeStatus.access_denied,
                        stage=AnalystOutcomeStage.authorization,
                        message="Access denied: payroll_dataset uses a connector that is explicitly denied for this agent.",
                        recoverable=False,
                        terminal=True,
                        metadata={
                            "policy_rationale": "One or more backing connectors are explicitly denied.",
                            "recovery_hint": "Retry with one of the agent's in-scope analytical assets.",
                        },
                    ),
                ),
                "result": {},
            },
            presentation=presentation,
        )
    )

    assert result == "Access denied summary."
    human_prompt = str(provider.calls[0]["messages"][-1].content)
    assert "status=access_denied" in human_prompt
    assert "policy_rationale=One or more backing connectors are explicitly denied." in human_prompt
