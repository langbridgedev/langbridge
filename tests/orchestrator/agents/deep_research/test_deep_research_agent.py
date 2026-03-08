import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional

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

from langbridge.packages.orchestrator.langbridge_orchestrator.agents.deep_research import (  # noqa: E402
    DeepResearchAgent,
)
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.deep_research.schemas import (  # noqa: E402
    EvidenceItem,
    ResearchPlan,
    ResearchState,
)
from langbridge.packages.orchestrator.langbridge_orchestrator.agents.web_search import (  # noqa: E402
    WebSearchAgent,
    WebSearchResultItem,
)


class StubLLM:
    def __init__(
        self,
        *,
        plan_payload: Optional[str] = None,
        synth_payload: Optional[str] = None,
        refine_payload: Optional[str] = None,
    ) -> None:
        self.plan_payload = plan_payload or "{}"
        self.synth_payload = synth_payload or "not-json"
        self.refine_payload = refine_payload or json.dumps({"follow_up_subquestions": []})

    async def acomplete(
        self,
        prompt: str,
        *,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> str:
        lowered = prompt.lower()
        if "planning a deep research workflow" in lowered:
            return self.plan_payload
        if "you produce evidence-grounded research reports" in lowered:
            return self.synth_payload
        if "you refine research subquestions" in lowered:
            return self.refine_payload
        return "{}"


class StubSearchProvider:
    name = "stub-search"

    def search(
        self,
        query: str,
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> list[WebSearchResultItem]:
        return self._results(query)[:max_results]

    async def search_async(
        self,
        query: str,
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> list[WebSearchResultItem]:
        return self.search(
            query,
            max_results=max_results,
            region=region,
            safe_search=safe_search,
            timebox_seconds=timebox_seconds,
        )

    @staticmethod
    def _results(query: str) -> list[WebSearchResultItem]:
        return [
            WebSearchResultItem(
                title="Macro outlook bulletin",
                url="https://alpha.example.com/markets/outlook-2026",
                snippet=f"{query} macro outlook and key policy drivers.",
                source="alpha.example.com",
            ),
            WebSearchResultItem(
                title="Industry report and tradeoffs",
                url="https://beta.example.org/research/tradeoffs",
                snippet=f"Tradeoffs and risks related to {query}.",
                source="beta.example.org",
            ),
            WebSearchResultItem(
                title="Regulatory tracker",
                url="https://gamma.example.net/policy/tracker",
                snippet=f"Regulatory updates relevant to {query}.",
                source="gamma.example.net",
            ),
        ]


def _build_agent(*, llm: StubLLM) -> DeepResearchAgent:
    web_agent = WebSearchAgent(provider=StubSearchProvider(), llm=None)
    return DeepResearchAgent(llm=llm, web_search_agent=web_agent, default_max_steps=5)


def test_plan_async_uses_llm_plan_and_applies_timebox_cap() -> None:
    llm = StubLLM(
        plan_payload=json.dumps(
            {
                "subquestions": [
                    "What changed recently?",
                    "Which sources disagree?",
                    "What actions follow?",
                ],
                "hypotheses": ["Recent policy shifts explain divergence."],
                "tool_strategy": ["web_search"],
                "source_strategy": "prefer_primary_sources",
                "max_steps": 9,
                "target_coverage": 0.84,
            }
        )
    )
    agent = _build_agent(llm=llm)
    context: dict[str, Any] = {
        "sql_result": {"columns": ["fund", "return"], "rows": [["A", 1.2]]},
        "retrieved_memories": [{"id": "m1", "category": "fact", "content": "Use USD where possible."}],
    }
    plan = asyncio.run(agent.plan_async("Research question", context=context, timebox_seconds=18))

    assert plan.subquestions[:2] == ["What changed recently?", "Which sources disagree?"]
    assert plan.max_steps == 3
    assert "web_search" in plan.tool_strategy
    assert "sql" in plan.tool_strategy
    assert "memory" in plan.tool_strategy
    assert plan.target_coverage == 0.84


def test_rank_and_dedupe_evidence_removes_existing_near_duplicates() -> None:
    agent = _build_agent(llm=StubLLM())

    existing = [
        EvidenceItem(
            id="ev-existing",
            source_type="web",
            source="alpha.example.com",
            source_ref="https://alpha.example.com/a",
            domain="alpha.example.com",
            snippet="Growth outlook improved after policy change.",
            score=0.8,
        )
    ]
    candidates = [
        EvidenceItem(
            id="ev-dup",
            source_type="web",
            source="alpha.example.com",
            source_ref="https://alpha.example.com/a",
            domain="alpha.example.com",
            snippet="Growth outlook improved after policy change.",
        ),
        EvidenceItem(
            id="ev-near",
            source_type="web",
            source="alpha.example.com",
            source_ref="https://alpha.example.com/b",
            domain="alpha.example.com",
            snippet="Growth outlook improved after policy change.",
        ),
        EvidenceItem(
            id="ev-unique",
            source_type="web",
            source="beta.example.org",
            source_ref="https://beta.example.org/tradeoffs",
            domain="beta.example.org",
            snippet="Tradeoffs include cost inflation and execution risk.",
        ),
    ]
    ranked = agent.rank_evidence(subquestion="growth outlook policy", evidence=candidates)
    deduped = agent.dedupe_evidence(ranked, existing=existing)

    assert len(deduped) == 1
    assert deduped[0].id == "ev-unique"


def test_stop_conditions_trigger_on_coverage_and_diminishing_returns() -> None:
    agent = _build_agent(llm=StubLLM())
    plan = ResearchPlan(
        question="q",
        subquestions=["a", "b"],
        hypotheses=[],
        tool_strategy=["web_search"],
        source_strategy="prefer_diverse_sources",
        max_steps=5,
        target_coverage=0.7,
    )
    coverage_state = ResearchState(
        started_at=datetime.now(timezone.utc),
        max_steps=5,
        steps_taken=2,
        coverage_score=0.82,
        source_diversity=3,
    )
    assert agent._should_stop(plan=plan, state=coverage_state) is True
    assert coverage_state.stop_reason == "coverage_and_diversity_reached"

    diminishing_state = ResearchState(
        started_at=datetime.now(timezone.utc),
        max_steps=5,
        steps_taken=2,
        coverage_score=0.4,
        source_diversity=1,
        diminishing_returns_count=2,
    )
    assert agent._should_stop(plan=plan, state=diminishing_state) is True
    assert diminishing_state.stop_reason == "diminishing_returns"


def test_regression_multi_source_report_has_citations() -> None:
    llm = StubLLM(
        plan_payload=json.dumps(
            {
                "subquestions": [
                    "What do recent sources say?",
                    "What are risks and tradeoffs?",
                    "What actions should be prioritized?",
                ],
                "hypotheses": ["Sources disagree on risk severity."],
                "tool_strategy": ["internal_retrieval", "web_search"],
                "source_strategy": "prefer_diverse_sources",
                "max_steps": 4,
                "target_coverage": 0.68,
            }
        ),
        synth_payload="not-json",
    )
    agent = _build_agent(llm=llm)
    context: dict[str, Any] = {
        "documents": [
            {
                "doc_id": "doc-internal-1",
                "title": "Internal PM note",
                "snippet": "Portfolio committee notes improving liquidity with residual policy risk.",
                "source": "investment_memo",
            }
        ]
    }

    result = asyncio.run(
        agent.research_async(
            "Compare policy tradeoffs for global growth outlook",
            context=context,
            timebox_seconds=24,
        )
    )

    assert result.report is not None
    assert result.report.key_findings
    assert all(finding.citations for finding in result.report.key_findings)
    assert result.report.supporting_evidence
    assert result.state is not None
    assert result.state.source_diversity >= 3
    assert result.state.step_trace
    assert result.report.weak_evidence is False
