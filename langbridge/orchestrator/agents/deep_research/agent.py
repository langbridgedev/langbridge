"""Deep research agent with plan -> execute -> synthesize workflow."""


import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from langbridge.runtime.events import (
    AgentEventVisibility,
    AgentEventEmitter,
)
from langbridge.orchestrator.agents.web_search import (
    WebSearchAgent,
    WebSearchResult,
    WebSearchResultItem,
)
from langbridge.orchestrator.llm.provider import LLMProvider

from .schemas import EvidenceItem, ResearchFinding, ResearchPlan, ResearchReport, ResearchState


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


@dataclass
class DeepResearchFinding:
    """Single insight surfaced by the deep research agent."""

    insight: str
    source: str = "knowledge_base"
    confidence: str = "medium"
    detail: Optional[str] = None
    evidence_ids: List[str] = field(default_factory=list)
    citations: List[str] = field(default_factory=list)

    def to_row(self) -> List[str]:
        citation_text = ", ".join(self.citations)
        evidence_text = ", ".join(self.evidence_ids)
        return [self.insight, self.source, self.confidence, evidence_text, citation_text]


@dataclass
class DeepResearchResult:
    """Aggregated deep research output with structured report and trace."""

    question: str
    synthesis: str
    findings: List[DeepResearchFinding] = field(default_factory=list)
    follow_ups: List[str] = field(default_factory=list)
    plan: Optional[ResearchPlan] = None
    evidence: List[EvidenceItem] = field(default_factory=list)
    state: Optional[ResearchState] = None
    report: Optional[ResearchReport] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "question": self.question,
            "synthesis": self.synthesis,
            "findings": [
                {
                    "insight": finding.insight,
                    "source": finding.source,
                    "confidence": finding.confidence,
                    "detail": finding.detail,
                    "evidence_ids": list(finding.evidence_ids),
                    "citations": list(finding.citations),
                }
                for finding in self.findings
            ],
            "follow_ups": list(self.follow_ups),
            "plan": self.plan.model_dump(mode="json") if self.plan else None,
            "evidence": [item.model_dump(mode="json") for item in self.evidence],
            "state": self.state.model_dump(mode="json") if self.state else None,
            "report": self.report.model_dump(mode="json") if self.report else None,
        }

    def to_tabular(self) -> Dict[str, Any]:
        if self.report and self.report.key_findings:
            return {
                "columns": ["finding", "confidence", "evidence_ids", "citations"],
                "rows": [
                    [
                        finding.claim,
                        finding.confidence,
                        ", ".join(finding.evidence_ids),
                        ", ".join(finding.citations),
                    ]
                    for finding in self.report.key_findings
                ],
            }

        if not self.findings:
            return {"columns": ["insight"], "rows": [[self.synthesis]]}

        return {
            "columns": ["insight", "source", "confidence", "evidence_ids", "citations"],
            "rows": [finding.to_row() for finding in self.findings],
        }


class DeepResearchAgent:
    """Research workflow with iterative evidence gathering and structured synthesis."""

    def __init__(
        self,
        *,
        llm: LLMProvider,
        web_search_agent: Optional[WebSearchAgent] = None,
        logger: Optional[logging.Logger] = None,
        event_emitter: Optional[AgentEventEmitter] = None,
        default_max_steps: int = 4,
        min_source_diversity: int = 3,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.llm = llm
        self.web_search_agent = web_search_agent or WebSearchAgent(llm=llm, logger=self.logger)
        self.event_emitter = event_emitter
        self.default_max_steps = max(1, min(int(default_max_steps), 10))
        self.min_source_diversity = max(1, int(min_source_diversity))

    async def research_async(
        self,
        question: str,
        *,
        context: Optional[Dict[str, Any]] = None,
        timebox_seconds: int = 30,
    ) -> DeepResearchResult:
        clean_question = self._normalize_question(question)
        clean_context = dict(context or {})

        start = time.perf_counter()
        started_at = datetime.now(timezone.utc)
        plan = await self.plan_async(clean_question, context=clean_context, timebox_seconds=timebox_seconds)
        await self._emit_event(
            event_type="ResearchPlanCreated",
            message="Deep research plan created.",
            details={
                "question": clean_question,
                "subquestions": list(plan.subquestions),
                "tool_strategy": list(plan.tool_strategy),
                "max_steps": plan.max_steps,
                "target_coverage": plan.target_coverage,
            },
        )
        state = ResearchState(
            started_at=started_at,
            max_steps=plan.max_steps,
            open_questions=list(plan.subquestions or [clean_question]),
        )

        evidence, state = await self.execute_async(
            plan=plan,
            state=state,
            context=clean_context,
            timebox_seconds=timebox_seconds,
        )

        report = await self.synthesize_async(
            question=clean_question,
            plan=plan,
            state=state,
            evidence=evidence,
            context=clean_context,
        )

        findings = self._report_to_findings(report)
        follow_ups = list(report.next_steps)
        if report.follow_up_question:
            follow_ups.append(report.follow_up_question)
        follow_ups = self._dedupe_text(follow_ups)

        state.elapsed_ms = int((time.perf_counter() - start) * 1000)
        state.finished_at = datetime.now(timezone.utc)

        self.logger.info(
            "DeepResearch completed question=%s steps=%s evidence=%s coverage=%.2f diversity=%s stop_reason=%s",
            clean_question,
            state.steps_taken,
            len(evidence),
            state.coverage_score,
            state.source_diversity,
            state.stop_reason,
        )
        await self._emit_event(
            event_type="ResearchCompleted",
            message="Deep research workflow completed.",
            details={
                "question": clean_question,
                "steps_taken": state.steps_taken,
                "coverage_score": state.coverage_score,
                "source_diversity": state.source_diversity,
                "stop_reason": state.stop_reason,
                "source_domains": list(state.source_domains),
                "evidence_count": len(evidence),
                "weak_evidence": bool(report.weak_evidence) if report else None,
            },
        )

        return DeepResearchResult(
            question=clean_question,
            synthesis=report.executive_summary,
            findings=findings,
            follow_ups=follow_ups,
            plan=plan,
            evidence=evidence,
            state=state,
            report=report,
        )

    async def plan_async(
        self,
        question: str,
        *,
        context: Dict[str, Any],
        timebox_seconds: int,
    ) -> ResearchPlan:
        llm_plan = await self._plan_with_llm_async(question=question, context=context, timebox_seconds=timebox_seconds)
        if llm_plan:
            return llm_plan
        return self._fallback_plan(question=question, context=context, timebox_seconds=timebox_seconds)

    async def execute_async(
        self,
        *,
        plan: ResearchPlan,
        state: ResearchState,
        context: Dict[str, Any],
        timebox_seconds: int,
    ) -> Tuple[List[EvidenceItem], ResearchState]:
        evidence_pool: List[EvidenceItem] = []
        internal_evidence = self._extract_context_evidence(context)
        if internal_evidence:
            self._mark_tool_attempt(state, "internal_retrieval")
        if "memory" in plan.tool_strategy and any(item.source_type == "memory" for item in internal_evidence):
            self._mark_tool_attempt(state, "memory")

        start = time.perf_counter()
        for _ in range(plan.max_steps):
            elapsed_seconds = time.perf_counter() - start
            if elapsed_seconds >= max(1, int(timebox_seconds)):
                state.stop_reason = "time_budget_reached"
                break

            subquestion = self._next_subquestion(state, plan)
            step_start = time.perf_counter()
            await self._emit_event(
                event_type="ResearchStepStarted",
                message="Research step started.",
                details={
                    "step_index": state.steps_taken + 1,
                    "subquestion": subquestion,
                    "open_questions": list(state.open_questions),
                    "answered_questions": list(state.answered_questions),
                },
            )
            gathered: List[EvidenceItem] = []
            gathered.extend(self._filter_internal_evidence(internal_evidence, subquestion))

            if "web_search" in plan.tool_strategy:
                self._mark_tool_attempt(state, "web_search")
                web_evidence, web_error = await self._gather_web_evidence(
                    subquestion,
                    timebox_seconds=max(4, int(timebox_seconds - elapsed_seconds)),
                )
                gathered.extend(web_evidence)
                if web_error:
                    state.errors.append(web_error)
                    await self._emit_event(
                        event_type="ResearchToolError",
                        message="Web search failed during deep research.",
                        details={
                            "step_index": state.steps_taken + 1,
                            "subquestion": subquestion,
                            "tool": "web_search",
                            "error": web_error,
                        },
                    )

            if "sql" in plan.tool_strategy:
                self._mark_tool_attempt(state, "sql")
                gathered.extend(self._extract_sql_evidence(context, subquestion))

            ranked = self.rank_evidence(subquestion=subquestion, evidence=gathered)
            deduped = self.dedupe_evidence(ranked, existing=evidence_pool)
            added = deduped[:6]
            evidence_pool.extend(added)

            state.steps_taken += 1
            state.newly_added_evidence = len(added)
            if len(added) <= 1:
                state.diminishing_returns_count += 1
            else:
                state.diminishing_returns_count = 0

            self._update_coverage(state=state, plan=plan, evidence=evidence_pool)
            self._update_source_diversity(state=state, evidence=evidence_pool)
            if subquestion in state.open_questions:
                state.open_questions.remove(subquestion)
            if subquestion not in state.answered_questions:
                state.answered_questions.append(subquestion)

            should_stop = self._should_stop(plan=plan, state=state)

            if not should_stop:
                refined_questions = await self._refine_open_questions_async(
                    plan=plan,
                    state=state,
                    evidence=evidence_pool,
                )
                for item in refined_questions:
                    if item not in state.open_questions and item not in state.answered_questions:
                        state.open_questions.append(item)

            elapsed_ms = int((time.perf_counter() - step_start) * 1000)
            self._record_step_trace(
                state=state,
                subquestion=subquestion,
                gathered_count=len(gathered),
                added_count=len(added),
                elapsed_ms=elapsed_ms,
                evidence=evidence_pool,
            )
            await self._emit_event(
                event_type="ResearchStepCompleted",
                message="Research step completed.",
                details={
                    "step_index": state.steps_taken,
                    "subquestion": subquestion,
                    "gathered_count": len(gathered),
                    "added_count": len(added),
                    "coverage_score": state.coverage_score,
                    "source_diversity": state.source_diversity,
                    "diminishing_returns_count": state.diminishing_returns_count,
                    "elapsed_ms": elapsed_ms,
                    "stop_reason": state.stop_reason,
                    "sources": self._source_trace(evidence_pool),
                },
            )
            if should_stop:
                break

        if not state.stop_reason:
            if state.steps_taken >= plan.max_steps:
                state.stop_reason = "step_budget_reached"
            else:
                state.stop_reason = "completed"
        return evidence_pool, state

    async def synthesize_async(
        self,
        *,
        question: str,
        plan: ResearchPlan,
        state: ResearchState,
        evidence: Sequence[EvidenceItem],
        context: Dict[str, Any],
    ) -> ResearchReport:
        llm_report = await self._synthesize_with_llm_async(
            question=question,
            plan=plan,
            state=state,
            evidence=evidence,
        )
        if llm_report:
            return self._validate_report(llm_report, question=question, state=state, evidence=evidence)

        fallback = self._build_fallback_report(question=question, plan=plan, state=state, evidence=evidence, context=context)
        return self._validate_report(fallback, question=question, state=state, evidence=evidence)

    def rank_evidence(self, *, subquestion: str, evidence: Sequence[EvidenceItem]) -> List[EvidenceItem]:
        ranked: List[EvidenceItem] = []
        for item in evidence:
            relevance = self._relevance_score(subquestion, f"{item.source} {item.snippet}")
            quality = self._quality_score(item)
            score = (0.62 * relevance) + (0.38 * quality)
            ranked.append(
                item.model_copy(
                    update={
                        "relevance": round(max(0.0, min(1.0, relevance)), 4),
                        "quality": round(max(0.0, min(1.0, quality)), 4),
                        "score": round(max(0.0, min(1.0, score)), 4),
                    }
                )
            )
        ranked.sort(key=lambda row: row.score, reverse=True)
        return ranked

    def dedupe_evidence(
        self,
        evidence: Sequence[EvidenceItem],
        *,
        existing: Optional[Sequence[EvidenceItem]] = None,
    ) -> List[EvidenceItem]:
        deduped: List[EvidenceItem] = []
        seen_signatures: set[str] = set()

        for item in existing or []:
            seen_signatures.add(self._evidence_signature(item))

        for candidate in evidence:
            signature = self._evidence_signature(candidate)
            if signature in seen_signatures:
                continue

            near_duplicate = False
            for prior in existing or []:
                if self._near_duplicate(candidate, prior):
                    near_duplicate = True
                    break
            if near_duplicate:
                continue
            for kept in deduped:
                if self._near_duplicate(candidate, kept):
                    near_duplicate = True
                    break
            if near_duplicate:
                continue

            seen_signatures.add(signature)
            deduped.append(candidate)

        deduped.sort(key=lambda row: row.score, reverse=True)
        return deduped

    def _should_stop(self, *, plan: ResearchPlan, state: ResearchState) -> bool:
        if state.steps_taken >= plan.max_steps:
            state.stop_reason = "step_budget_reached"
            return True

        if (
            state.coverage_score >= plan.target_coverage
            and state.source_diversity >= self.min_source_diversity
            and state.steps_taken >= 2
        ):
            state.stop_reason = "coverage_and_diversity_reached"
            return True

        if state.diminishing_returns_count >= 2 and state.steps_taken >= 2:
            state.stop_reason = "diminishing_returns"
            return True

        return False

    async def _plan_with_llm_async(
        self,
        *,
        question: str,
        context: Dict[str, Any],
        timebox_seconds: int,
    ) -> Optional[ResearchPlan]:
        prompt = self._build_plan_prompt(question=question, context=context, timebox_seconds=timebox_seconds)
        try:
            response = await self.llm.acomplete(prompt, temperature=0.0, max_tokens=450)
        except Exception as exc:  # pragma: no cover
            self.logger.warning("DeepResearch planning LLM failed: %s", exc)
            return None

        payload = self._parse_llm_payload(response)
        if not payload:
            return None

        subquestions_raw = payload.get("subquestions")
        hypotheses_raw = payload.get("hypotheses")
        tool_strategy_raw = payload.get("tool_strategy")

        subquestions = self._coerce_string_list(subquestions_raw)
        hypotheses = self._coerce_string_list(hypotheses_raw)
        tool_strategy = self._coerce_tool_strategy(tool_strategy_raw, context=context)

        max_steps = self._coerce_int(payload.get("max_steps"), default=self.default_max_steps, minimum=1, maximum=10)
        max_steps = min(max_steps, max(1, int(timebox_seconds // 6) or 1))
        target_coverage = self._coerce_float(payload.get("target_coverage"), default=0.75, minimum=0.4, maximum=0.95)

        if not subquestions:
            return None

        return ResearchPlan(
            question=question,
            subquestions=subquestions,
            hypotheses=hypotheses,
            tool_strategy=tool_strategy,
            source_strategy=str(payload.get("source_strategy") or "prefer_diverse_sources"),
            max_steps=max_steps,
            target_coverage=target_coverage,
        )

    def _fallback_plan(self, *, question: str, context: Dict[str, Any], timebox_seconds: int) -> ResearchPlan:
        subquestions = [
            f"What are the primary factors explaining: {question}?",
            f"What conflicting evidence or trade-offs exist for: {question}?",
            f"What practical recommendations follow from evidence about: {question}?",
        ]
        hypotheses = [
            "Different sources may disagree on causes and recommendations.",
            "Recent sources likely contain higher-signal evidence for changing conditions.",
        ]
        max_steps = min(self.default_max_steps, max(1, int(timebox_seconds // 6) or 1))
        return ResearchPlan(
            question=question,
            subquestions=subquestions,
            hypotheses=hypotheses,
            tool_strategy=self._coerce_tool_strategy(None, context=context),
            source_strategy="prefer_diverse_sources",
            max_steps=max_steps,
            target_coverage=0.72,
        )

    async def _gather_web_evidence(
        self,
        subquestion: str,
        *,
        timebox_seconds: int,
    ) -> Tuple[List[EvidenceItem], Optional[str]]:
        try:
            result: WebSearchResult = await self.web_search_agent.search_async(
                subquestion,
                max_results=6,
                timebox_seconds=max(4, min(20, int(timebox_seconds))),
            )
        except Exception as exc:  # pragma: no cover
            self.logger.warning("DeepResearch web search failed for '%s': %s", subquestion, exc)
            return [], f"web_search_failed:{exc.__class__.__name__}"

        evidence: List[EvidenceItem] = []
        for item in result.results:
            evidence.append(self._evidence_from_web_result(item=item, subquestion=subquestion))
        return evidence, None

    def _extract_context_evidence(self, context: Dict[str, Any]) -> List[EvidenceItem]:
        evidence: List[EvidenceItem] = []
        for index, doc in enumerate(self._extract_documents(context), start=1):
            title = str(doc.get("title") or doc.get("name") or f"Internal document {index}").strip()
            snippet = str(doc.get("snippet") or doc.get("summary") or doc.get("content") or "").strip()
            if not snippet:
                continue
            source_ref = str(doc.get("doc_id") or doc.get("id") or doc.get("url") or "").strip() or None
            source = str(doc.get("source") or title).strip() or title
            domain = self._domain_from_url(source_ref) if source_ref else None
            if not domain:
                domain = f"internal:{self._slugify(source_ref or source)}"
            evidence.append(
                EvidenceItem(
                    id=f"ev-{uuid.uuid4().hex[:10]}",
                    source_type="internal",
                    source=source,
                    source_ref=source_ref,
                    domain=domain,
                    snippet=self._trim_text(snippet, 420),
                    subquestion=None,
                    metadata={"kind": "internal_document", "title": title},
                )
            )

        memories = context.get("retrieved_memories")
        if isinstance(memories, list):
            for entry in memories:
                if not isinstance(entry, dict):
                    continue
                content = str(entry.get("content") or "").strip()
                if not content:
                    continue
                source = str(entry.get("category") or "memory")
                source_ref = str(entry.get("id") or "").strip() or None
                memory_domain = f"memory:{self._slugify(source_ref or source)}"
                evidence.append(
                    EvidenceItem(
                        id=f"ev-{uuid.uuid4().hex[:10]}",
                        source_type="memory",
                        source=f"memory:{source}",
                        source_ref=source_ref,
                        domain=memory_domain,
                        snippet=self._trim_text(content, 300),
                        subquestion=None,
                        metadata={"kind": "retrieved_memory"},
                    )
                )
        return evidence

    def _extract_sql_evidence(self, context: Dict[str, Any], subquestion: str) -> List[EvidenceItem]:
        evidence: List[EvidenceItem] = []

        sql_obs = context.get("sql_observations")
        if isinstance(sql_obs, list):
            for entry in sql_obs:
                text = str(entry).strip()
                if not text:
                    continue
                evidence.append(
                    EvidenceItem(
                        id=f"ev-{uuid.uuid4().hex[:10]}",
                        source_type="sql",
                        source="sql_observation",
                        source_ref=None,
                        domain="internal-sql",
                        snippet=self._trim_text(text, 300),
                        subquestion=subquestion,
                        metadata={"kind": "sql_observation"},
                    )
                )

        sql_result = context.get("sql_result")
        if isinstance(sql_result, dict):
            columns = sql_result.get("columns") if isinstance(sql_result.get("columns"), list) else []
            rows = sql_result.get("rows") if isinstance(sql_result.get("rows"), list) else []
            if columns and rows:
                sample = rows[:3]
                sample_lines = []
                for row in sample:
                    if isinstance(row, (list, tuple)):
                        values = [f"{columns[idx]}={row[idx]}" for idx in range(min(len(columns), len(row)))]
                        sample_lines.append(", ".join(values))
                if sample_lines:
                    snippet = "; ".join(sample_lines)
                    evidence.append(
                        EvidenceItem(
                            id=f"ev-{uuid.uuid4().hex[:10]}",
                            source_type="sql",
                            source="sql_result",
                            source_ref=None,
                            domain="internal-sql",
                            snippet=self._trim_text(snippet, 360),
                            subquestion=subquestion,
                            metadata={"kind": "sql_result"},
                        )
                    )

        return evidence

    def _filter_internal_evidence(self, evidence: Sequence[EvidenceItem], subquestion: str) -> List[EvidenceItem]:
        if not evidence:
            return []
        filtered: List[EvidenceItem] = []
        for item in evidence:
            relevance = self._relevance_score(subquestion, f"{item.source} {item.snippet}")
            if relevance < 0.18:
                continue
            filtered.append(item.model_copy(update={"subquestion": subquestion}))
        return filtered

    async def _refine_open_questions_async(
        self,
        *,
        plan: ResearchPlan,
        state: ResearchState,
        evidence: Sequence[EvidenceItem],
    ) -> List[str]:
        if state.coverage_score >= 0.65:
            return []
        if len(evidence) < 2:
            return []

        prompt_sections = [
            "You refine research subquestions to close evidence gaps.",
            "Return ONLY JSON with key: follow_up_subquestions (list of max 2 strings).",
            f"Question: {plan.question}",
            f"Current open questions: {json.dumps(state.open_questions, ensure_ascii=True)}",
            f"Coverage score: {state.coverage_score}",
            "Top evidence snippets:",
        ]
        for item in list(evidence)[:6]:
            prompt_sections.append(f"- [{item.source_type}] {item.source}: {item.snippet}")

        prompt = "\n".join(prompt_sections)
        try:
            response = await self.llm.acomplete(prompt, temperature=0.0, max_tokens=220)
        except Exception as exc:  # pragma: no cover
            self.logger.warning("DeepResearch refinement LLM failed: %s", exc)
            return []

        payload = self._parse_llm_payload(response)
        if not payload:
            return []
        return self._coerce_string_list(payload.get("follow_up_subquestions"))[:2]

    async def _synthesize_with_llm_async(
        self,
        *,
        question: str,
        plan: ResearchPlan,
        state: ResearchState,
        evidence: Sequence[EvidenceItem],
    ) -> Optional[ResearchReport]:
        if not evidence:
            return None

        evidence_payload = [
            {
                "id": item.id,
                "source_type": item.source_type,
                "source": item.source,
                "source_ref": item.source_ref,
                "domain": item.domain,
                "snippet": item.snippet,
                "relevance": item.relevance,
                "quality": item.quality,
                "score": item.score,
            }
            for item in evidence[:14]
        ]

        prompt_sections = [
            "You produce evidence-grounded research reports.",
            "Return ONLY JSON with keys:",
            "executive_summary, key_findings, risks_uncertainties, next_steps, assumptions, follow_up_question.",
            "key_findings must be a list of objects with keys: id, claim, evidence_ids, confidence.",
            "Each non-trivial claim MUST include at least one evidence_id from the provided evidence list.",
            "Do not invent citations or evidence ids.",
            f"Question: {question}",
            f"Research plan: {plan.model_dump_json()}",
            f"Research state: {state.model_dump_json()}",
            f"Evidence JSON: {json.dumps(evidence_payload, ensure_ascii=True)}",
        ]
        prompt = "\n".join(prompt_sections)

        try:
            response = await self.llm.acomplete(prompt, temperature=0.1, max_tokens=950)
        except Exception as exc:  # pragma: no cover
            self.logger.warning("DeepResearch synthesis LLM failed: %s", exc)
            return None

        payload = self._parse_llm_payload(response)
        if not payload:
            return None

        findings: List[ResearchFinding] = []
        raw_findings = payload.get("key_findings")
        if isinstance(raw_findings, list):
            for raw in raw_findings:
                if not isinstance(raw, dict):
                    continue
                claim = str(raw.get("claim") or "").strip()
                if not claim:
                    continue
                evidence_ids = self._coerce_string_list(raw.get("evidence_ids"))
                findings.append(
                    ResearchFinding(
                        id=str(raw.get("id") or f"finding-{len(findings) + 1}"),
                        claim=claim,
                        evidence_ids=evidence_ids,
                        citations=[],
                        confidence=self._normalize_confidence(raw.get("confidence")),
                    )
                )

        executive_summary = str(payload.get("executive_summary") or "").strip()
        if not executive_summary:
            return None

        return ResearchReport(
            question=question,
            executive_summary=executive_summary,
            key_findings=findings,
            supporting_evidence={},
            risks_uncertainties=self._coerce_string_list(payload.get("risks_uncertainties")),
            next_steps=self._coerce_string_list(payload.get("next_steps")),
            assumptions=self._coerce_string_list(payload.get("assumptions")),
            weak_evidence=False,
            follow_up_question=self._coerce_optional_string(payload.get("follow_up_question")),
        )

    def _build_fallback_report(
        self,
        *,
        question: str,
        plan: ResearchPlan,
        state: ResearchState,
        evidence: Sequence[EvidenceItem],
        context: Dict[str, Any],
    ) -> ResearchReport:
        top = list(evidence)[:4]
        findings: List[ResearchFinding] = []
        for idx, item in enumerate(top, start=1):
            claim = self._first_sentence(item.snippet) or item.snippet
            findings.append(
                ResearchFinding(
                    id=f"finding-{idx}",
                    claim=claim,
                    evidence_ids=[item.id],
                    citations=[item.source_ref] if item.source_ref else [],
                    confidence=self._confidence_from_score(item.score),
                )
            )

        if findings:
            executive_summary = (
                f"Reviewed {len(evidence)} evidence item(s) across {state.source_diversity} source domain(s). "
                f"Top takeaway: {findings[0].claim}"
            )
        else:
            executive_summary = (
                "Evidence was limited, so findings are best-effort and may require additional sources."
            )

        risks = [
            "Some findings rely on limited source coverage.",
        ]
        if state.source_diversity < self.min_source_diversity:
            risks.append(
                f"Source diversity is below target ({state.source_diversity}/{self.min_source_diversity} domains)."
            )

        next_steps = [
            "Collect additional high-quality sources for unresolved subquestions.",
            "Validate key claims against primary data where available.",
        ]

        assumptions: List[str] = []
        if not self._extract_documents(context):
            assumptions.append("Assuming external web sources represent the current state accurately.")

        return ResearchReport(
            question=question,
            executive_summary=executive_summary,
            key_findings=findings,
            supporting_evidence={},
            risks_uncertainties=risks,
            next_steps=next_steps,
            assumptions=assumptions,
            weak_evidence=state.source_diversity < self.min_source_diversity or len(evidence) < 3,
            follow_up_question=self._targeted_follow_up(question),
        )

    def _validate_report(
        self,
        report: ResearchReport,
        *,
        question: str,
        state: ResearchState,
        evidence: Sequence[EvidenceItem],
    ) -> ResearchReport:
        evidence_by_id = {item.id: item for item in evidence}

        validated_findings: List[ResearchFinding] = []
        supporting_evidence: Dict[str, List[str]] = {}
        for finding in report.key_findings:
            valid_evidence_ids = [item_id for item_id in finding.evidence_ids if item_id in evidence_by_id]
            if not valid_evidence_ids:
                continue
            citations: List[str] = []
            for item_id in valid_evidence_ids:
                item = evidence_by_id[item_id]
                citation = self._citation_for_evidence(item)
                if citation:
                    citations.append(citation)
            validated = finding.model_copy(update={"evidence_ids": valid_evidence_ids, "citations": self._dedupe_text(citations)})
            validated_findings.append(validated)
            supporting_evidence[validated.id] = list(valid_evidence_ids)

        weak_evidence = state.source_diversity < self.min_source_diversity or len(evidence) < 3 or len(validated_findings) == 0

        risks = list(report.risks_uncertainties)
        if weak_evidence:
            risks.append("Evidence quality or diversity was insufficient for high confidence conclusions.")
            if state.source_diversity < self.min_source_diversity:
                risks.append(
                    f"Only {state.source_diversity} distinct source domain(s) were found; target is {self.min_source_diversity}."
                )

        follow_up_question = report.follow_up_question
        if weak_evidence and not follow_up_question:
            follow_up_question = self._targeted_follow_up(question)

        return report.model_copy(
            update={
                "key_findings": validated_findings,
                "supporting_evidence": supporting_evidence,
                "risks_uncertainties": self._dedupe_text(risks),
                "next_steps": self._dedupe_text(report.next_steps),
                "assumptions": self._dedupe_text(report.assumptions),
                "weak_evidence": weak_evidence,
                "follow_up_question": follow_up_question,
            }
        )

    def _report_to_findings(self, report: ResearchReport) -> List[DeepResearchFinding]:
        findings: List[DeepResearchFinding] = []
        for item in report.key_findings:
            source = item.citations[0] if item.citations else "knowledge_base"
            findings.append(
                DeepResearchFinding(
                    insight=item.claim,
                    source=source,
                    confidence=item.confidence,
                    detail=item.id,
                    evidence_ids=list(item.evidence_ids),
                    citations=list(item.citations),
                )
            )
        return findings

    def _build_plan_prompt(self, *, question: str, context: Dict[str, Any], timebox_seconds: int) -> str:
        context_summary = {
            "has_documents": bool(self._extract_documents(context)),
            "has_sql_result": bool(context.get("sql_result") or context.get("sql_observations")),
            "has_memories": bool(context.get("retrieved_memories")),
        }
        return "\n".join(
            [
                "You are planning a deep research workflow.",
                "Return ONLY JSON with keys: subquestions, hypotheses, tool_strategy, source_strategy, max_steps, target_coverage.",
                "tool_strategy should contain any of: internal_retrieval, web_search, sql, memory.",
                f"Question: {question}",
                f"Context summary: {json.dumps(context_summary, ensure_ascii=True)}",
                f"Time budget (seconds): {timebox_seconds}",
            ]
        )

    @staticmethod
    def _extract_documents(context: Dict[str, Any]) -> List[Dict[str, Any]]:
        documents = context.get("documents") or context.get("sources") or context.get("notes") or []
        if isinstance(documents, dict):
            return [documents]
        if not isinstance(documents, Sequence):
            return []
        return [item for item in documents if isinstance(item, dict)]

    def _evidence_from_web_result(self, *, item: WebSearchResultItem, subquestion: str) -> EvidenceItem:
        source_ref = str(item.url).strip() or None
        domain = self._domain_from_url(source_ref)
        return EvidenceItem(
            id=f"ev-{uuid.uuid4().hex[:10]}",
            source_type="web",
            source=item.title or item.source or (domain or "web"),
            source_ref=source_ref,
            domain=domain,
            snippet=self._trim_text(item.snippet or item.title or "", 420),
            subquestion=subquestion,
            metadata={"provider_source": item.source or "web", "rank": str(item.rank)},
        )

    def _next_subquestion(self, state: ResearchState, plan: ResearchPlan) -> str:
        for question in list(state.open_questions):
            if question not in state.answered_questions:
                return question
        return plan.question

    def _update_coverage(self, *, state: ResearchState, plan: ResearchPlan, evidence: Sequence[EvidenceItem]) -> None:
        if not plan.subquestions:
            state.coverage_score = 0.0
            return

        answered = 0
        for subquestion in plan.subquestions:
            best = 0.0
            for item in evidence:
                item_score = item.score if item.subquestion == subquestion else self._relevance_score(subquestion, item.snippet)
                best = max(best, item_score)
            if best >= 0.45:
                answered += 1
        subquestion_coverage = answered / max(len(plan.subquestions), 1)
        evidence_bonus = min(0.25, len(evidence) / 20)
        state.coverage_score = round(max(0.0, min(1.0, subquestion_coverage + evidence_bonus)), 4)

    def _update_source_diversity(self, *, state: ResearchState, evidence: Sequence[EvidenceItem]) -> None:
        domains = {
            item.domain
            for item in evidence
            if isinstance(item.domain, str) and item.domain.strip()
        }
        state.source_domains = sorted(domains)
        state.source_diversity = len(domains)

    @staticmethod
    def _mark_tool_attempt(state: ResearchState, tool_name: str) -> None:
        if tool_name not in state.attempted_tools:
            state.attempted_tools.append(tool_name)

    @staticmethod
    def _normalize_question(value: str) -> str:
        clean = str(value or "").strip()
        if not clean:
            raise ValueError("DeepResearchAgent requires a non-empty question.")
        return clean

    @staticmethod
    def _trim_text(value: str, limit: int) -> str:
        text = str(value or "").strip()
        if len(text) <= limit:
            return text
        return text[:limit].rstrip() + "..."

    @staticmethod
    def _parse_llm_payload(response: str) -> Optional[Dict[str, Any]]:
        start = response.find("{")
        if start == -1:
            return None
        depth = 0
        end = -1
        for index in range(start, len(response)):
            char = response[index]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    end = index + 1
                    break
        if end == -1:
            return None
        try:
            payload = json.loads(response[start:end])
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    @staticmethod
    def _coerce_string_list(value: Any) -> List[str]:
        if not value:
            return []
        if isinstance(value, str):
            values = [value]
        elif isinstance(value, Sequence):
            values = list(value)
        else:
            return []
        cleaned: List[str] = []
        for item in values:
            text = str(item or "").strip()
            if text:
                cleaned.append(text)
        return cleaned

    def _coerce_tool_strategy(self, value: Any, *, context: Dict[str, Any]) -> List[str]:
        allowed = {"internal_retrieval", "web_search", "sql", "memory"}
        strategy = [item for item in self._coerce_string_list(value) if item in allowed]
        if not strategy:
            strategy = ["internal_retrieval", "web_search"]
        if context.get("sql_result") or context.get("sql_observations"):
            if "sql" not in strategy:
                strategy.append("sql")
        if context.get("retrieved_memories"):
            if "memory" not in strategy:
                strategy.append("memory")
        return strategy

    @staticmethod
    def _coerce_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_float(value: Any, *, default: float, minimum: float, maximum: float) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, min(maximum, parsed))

    @staticmethod
    def _coerce_optional_string(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _normalize_confidence(value: Any) -> str:
        cleaned = str(value or "").strip().lower()
        if cleaned in {"low", "medium", "high"}:
            return cleaned
        if cleaned in {"med", "mid"}:
            return "medium"
        return "medium"

    @staticmethod
    def _confidence_from_score(score: float) -> str:
        if score >= 0.75:
            return "high"
        if score >= 0.45:
            return "medium"
        return "low"

    @staticmethod
    def _domain_from_url(url: Optional[str]) -> Optional[str]:
        if not isinstance(url, str) or not url.strip():
            return None
        try:
            return urlparse(url).netloc or None
        except ValueError:
            return None

    @staticmethod
    def _tokens(value: str) -> set[str]:
        return set(_TOKEN_RE.findall(str(value or "").lower()))

    def _relevance_score(self, query: str, text: str) -> float:
        q = self._tokens(query)
        t = self._tokens(text)
        if not q or not t:
            return 0.0
        overlap = len(q.intersection(t))
        return overlap / max(len(q), 1)

    def _quality_score(self, evidence: EvidenceItem) -> float:
        base = 0.25
        if evidence.source_type == "internal":
            base += 0.35
        elif evidence.source_type == "sql":
            base += 0.32
        elif evidence.source_type == "web":
            base += 0.22
        elif evidence.source_type == "memory":
            base += 0.12

        if evidence.source_ref:
            base += 0.12
        if evidence.domain:
            base += 0.08
        if len(evidence.snippet) >= 80:
            base += 0.1

        return max(0.0, min(1.0, base))

    def _evidence_signature(self, evidence: EvidenceItem) -> str:
        source = (evidence.source_ref or evidence.source or "").strip().lower()
        snippet = re.sub(r"\s+", " ", evidence.snippet.strip().lower())
        return f"{source}|{snippet[:200]}"

    def _near_duplicate(self, left: EvidenceItem, right: EvidenceItem) -> bool:
        if left.source_ref and right.source_ref and left.source_ref == right.source_ref:
            return True

        left_tokens = self._tokens(f"{left.source} {left.snippet}")
        right_tokens = self._tokens(f"{right.source} {right.snippet}")
        if not left_tokens or not right_tokens:
            return False

        intersection = len(left_tokens.intersection(right_tokens))
        union = len(left_tokens.union(right_tokens))
        if union == 0:
            return False
        similarity = intersection / union
        if left.domain and right.domain and left.domain == right.domain and similarity >= 0.82:
            return True
        if similarity >= 0.9:
            return True
        return False

    @staticmethod
    def _dedupe_text(values: Sequence[str]) -> List[str]:
        seen: set[str] = set()
        deduped: List[str] = []
        for value in values:
            text = str(value or "").strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(text)
        return deduped

    @staticmethod
    def _first_sentence(text: str) -> str:
        cleaned = str(text or "").strip()
        if not cleaned:
            return ""
        parts = _SENTENCE_SPLIT_RE.split(cleaned, maxsplit=1)
        return parts[0].strip() if parts else cleaned

    @staticmethod
    def _slugify(value: str) -> str:
        cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
        return cleaned or "source"

    @staticmethod
    def _source_trace(evidence: Sequence[EvidenceItem]) -> List[Dict[str, str]]:
        trace: List[Dict[str, str]] = []
        seen: set[str] = set()
        for item in evidence:
            source_ref = str(item.source_ref or "").strip()
            source_key = source_ref or f"{item.source_type}:{item.source}"
            if source_key in seen:
                continue
            seen.add(source_key)
            trace.append(
                {
                    "id": item.id,
                    "source_type": item.source_type,
                    "source": item.source,
                    "source_ref": source_ref,
                    "domain": str(item.domain or ""),
                }
            )
            if len(trace) >= 12:
                break
        return trace

    @staticmethod
    def _citation_for_evidence(item: EvidenceItem) -> Optional[str]:
        if item.source_ref:
            return item.source_ref
        if item.source_type in {"internal", "memory", "sql"}:
            return f"{item.source_type}:{item.source}"
        if item.source:
            return item.source
        return None

    @staticmethod
    def _record_step_trace(
        *,
        state: ResearchState,
        subquestion: str,
        gathered_count: int,
        added_count: int,
        elapsed_ms: int,
        evidence: Sequence[EvidenceItem],
    ) -> None:
        entry = {
            "step": state.steps_taken,
            "subquestion": subquestion,
            "gathered_count": gathered_count,
            "added_count": added_count,
            "coverage_score": state.coverage_score,
            "source_diversity": state.source_diversity,
            "diminishing_returns_count": state.diminishing_returns_count,
            "elapsed_ms": elapsed_ms,
            "source_trace": DeepResearchAgent._source_trace(evidence),
        }
        state.step_trace.append(entry)

    async def _emit_event(
        self,
        *,
        event_type: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.event_emitter:
            return
        try:
            await self.event_emitter.emit(
                event_type=event_type,
                message=message,
                visibility=AgentEventVisibility.internal,
                source="agent:DeepResearch",
                details=details,
            )
        except Exception as exc:  # pragma: no cover
            self.logger.warning("DeepResearch event emission failed (%s): %s", event_type, exc)

    def _targeted_follow_up(self, question: str) -> str:
        lowered = question.lower()
        if "market" in lowered or "outlook" in lowered:
            return "Which market, region, or asset class should I prioritize in the next pass?"
        if "policy" in lowered or "regulation" in lowered:
            return "Which jurisdiction or regulator should the next research pass focus on?"
        if "performance" in lowered:
            return "Which entity and timeframe should I focus on to gather stronger evidence?"
        return "Could you narrow scope by region, timeframe, or target entity for a higher-confidence report?"


__all__ = ["DeepResearchAgent", "DeepResearchFinding", "DeepResearchResult"]
