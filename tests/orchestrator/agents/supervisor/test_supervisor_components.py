import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

if "pandas" not in sys.modules:
    pandas_stub = SimpleNamespace(
        DataFrame=type("DataFrame", (), {}),
        api=SimpleNamespace(
            types=SimpleNamespace(
                is_numeric_dtype=lambda _series: False,
                is_object_dtype=lambda _series: False,
                is_categorical_dtype=lambda _series: False,
            )
        ),
    )
    sys.modules["pandas"] = pandas_stub

REPO_ROOT = Path(__file__).resolve().parents[4]
PACKAGE_ROOT = REPO_ROOT
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from langbridge.packages.orchestrator.langbridge_orchestrator.agents.supervisor.clarification_manager import (  # noqa: E402
    ClarificationManager,
)
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.supervisor.entity_resolver import (  # noqa: E402
    EntityResolver,
)
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.supervisor.question_classifier import (  # noqa: E402
    QuestionClassifier,
)


class StubLLM:
    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        if "Will the latest Boston storms cause my sales to drop?" in prompt:
            return json.dumps(
                {
                    "intent": "web_search",
                    "route_hint": "WebSearch",
                    "confidence": 0.9,
                    "requires_clarification": False,
                    "required_context": [],
                    "clarifying_question": None,
                    "extracted_entities": {"location": "Boston"},
                    "rationale": "Needs external weather/news evidence.",
                }
            )
        return json.dumps(
            {
                "intent": "analytical",
                "route_hint": "SimpleAnalyst",
                "confidence": 0.88,
                "requires_clarification": True,
                "required_context": ["fund_id"],
                "clarifying_question": "Which fund should I use for this analysis?",
                "extracted_entities": {"metric": "performance", "time_period": "2024 Q1"},
                "rationale": "A specific fund is required for the requested metric.",
            }
        )


class BadPayloadLLM:
    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        if "Normalize the classifier output into strict JSON." in prompt:
            return json.dumps(
                {
                    "intent": "web_search",
                    "route_hint": "WebSearch",
                    "confidence": 0.72,
                    "requires_clarification": False,
                    "clarifying_question": None,
                    "required_context": [],
                    "extracted_entities": {},
                    "rationale": "Recovered via repair prompt.",
                }
            )
        return "This is not JSON output."


class AlwaysBadPayloadLLM:
    def complete(self, prompt: str, *, temperature: float = 0.0, max_tokens: int | None = None) -> str:
        return "still not json"


def test_question_classifier_routes_analytical_query_to_analyst() -> None:
    classifier = QuestionClassifier(llm=StubLLM())
    result = asyncio.run(classifier.classify_async("Fund performance by region for 2024 Q1"))

    assert result.route_hint == "SimpleAnalyst"
    assert result.intent == "analytical"


def test_entity_resolver_extracts_core_slots() -> None:
    resolver = EntityResolver(llm=StubLLM())
    entities = asyncio.run(resolver.resolve_async("Fund performance by region for 2024 Q1"))

    assert entities.region == "by region"
    assert entities.time_period == "2024 Q1"
    assert entities.metric == "performance"
    assert entities.fund is None


def test_clarification_manager_dedupes_repeated_question() -> None:
    classifier = QuestionClassifier(llm=StubLLM())
    manager = ClarificationManager(default_max_turns=2)

    question = "Fund performance by region for 2024 Q1"
    classification = asyncio.run(classifier.classify_async(question))

    first = manager.decide(
        classification=classification,
        prior_state=None,
    )
    assert first.requires_clarification is True
    assert first.clarifying_question is not None

    second = manager.decide(
        classification=classification,
        prior_state=first.updated_state,
    )

    assert second.requires_clarification is False
    assert any("best-effort assumptions" in entry for entry in second.assumptions)


def test_regression_fund_performance_query_no_repeat_and_correct_route() -> None:
    classifier = QuestionClassifier(llm=StubLLM())
    manager = ClarificationManager(default_max_turns=2)

    question = "fund performance by region for 2024 Q1"
    classification = asyncio.run(classifier.classify_async(question))

    assert classification.route_hint == "SimpleAnalyst"

    first = manager.decide(
        classification=classification,
        prior_state=None,
    )
    assert first.requires_clarification is True

    second = manager.decide(
        classification=classification,
        prior_state=first.updated_state,
    )
    assert second.requires_clarification is False
    assert second.updated_state.turn_count == first.updated_state.turn_count


def test_external_event_business_question_routes_without_fund_clarification() -> None:
    classifier = QuestionClassifier(llm=StubLLM())
    manager = ClarificationManager(default_max_turns=2)

    question = "Will the latest Boston storms cause my sales to drop?"
    classification = asyncio.run(classifier.classify_async(question))
    decision = manager.decide(
        classification=classification,
        prior_state=None,
    )

    assert classification.route_hint in {"WebSearch", "DeepResearch"}
    assert classification.requires_clarification is False
    assert decision.requires_clarification is False


def test_classifier_repairs_invalid_payload_with_llm() -> None:
    classifier = QuestionClassifier(llm=BadPayloadLLM())
    result = asyncio.run(classifier.classify_async("Will the latest Boston storms cause my sales to drop?"))

    assert result.route_hint == "WebSearch"
    assert result.intent == "web_search"


def test_classifier_degrades_gracefully_when_payload_invalid() -> None:
    classifier = QuestionClassifier(llm=AlwaysBadPayloadLLM())
    result = asyncio.run(classifier.classify_async("Will the latest Boston storms cause my sales to drop?"))

    assert result.route_hint is None
    assert result.requires_clarification is False
