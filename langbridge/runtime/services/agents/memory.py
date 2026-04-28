from collections.abc import Mapping
from typing import Any

from langbridge.ai import MetaControllerRun
from langbridge.ai.orchestration.continuation import ContinuationStateBuilder
from langbridge.runtime.models import RuntimeConversationMemoryCategory, RuntimeThread
from langbridge.runtime.ports import ConversationMemoryStore


class AgentConversationMemoryWriter:
    def __init__(self, *, memory_repository: ConversationMemoryStore | None = None) -> None:
        self._memory_repository = memory_repository

    async def write(
        self,
        *,
        thread: RuntimeThread,
        user_query: str,
        response: dict[str, Any],
        ai_run: MetaControllerRun | dict[str, Any] | None = None,
    ) -> None:
        if self._memory_repository is None:
            return
        answer = response.get("answer") or response.get("summary")
        if not isinstance(answer, str) or not answer.strip():
            return
        created = []
        item = self._memory_repository.create_item(
            thread_id=thread.id,
            actor_id=thread.created_by,
            category=RuntimeConversationMemoryCategory.answer.value,
            content=f"User asked: {user_query}\nAssistant answered: {answer.strip()}",
            metadata_json={"runtime": "langbridge.ai", "kind": "final_answer"},
        )
        if item is not None:
            created.append(item)

        research = response.get("research")
        if isinstance(research, dict):
            created.extend(self._write_research_items(thread=thread, user_query=user_query, research=research))

        diagnostics = response.get("diagnostics")
        diagnostic_ai_run = diagnostics.get("ai_run") if isinstance(diagnostics, dict) else None
        route = diagnostic_ai_run.get("route") if isinstance(diagnostic_ai_run, dict) else None
        if route:
            item = self._memory_repository.create_item(
                thread_id=thread.id,
                actor_id=thread.created_by,
                category=RuntimeConversationMemoryCategory.decision.value,
                content=f"Agent route for '{user_query}': {route}",
                metadata_json={"runtime": "langbridge.ai", "kind": "route_decision"},
            )
            if item is not None:
                created.append(item)

        continuation_state = self.build_continuation_state(
            response=response,
            user_query=user_query,
            ai_run=diagnostic_ai_run or ai_run,
        )
        if continuation_state:
            item = self._memory_repository.create_item(
                thread_id=thread.id,
                actor_id=thread.created_by,
                category=RuntimeConversationMemoryCategory.tool_outcome.value,
                content=self.continuation_memory_content(continuation_state),
                metadata_json={
                    "runtime": "langbridge.ai",
                    "kind": "continuation_state",
                    "continuation_state": continuation_state,
                },
            )
            if item is not None:
                created.append(item)

        if created:
            await self._memory_repository.flush()

    def _write_research_items(
        self,
        *,
        thread: RuntimeThread,
        user_query: str,
        research: dict[str, Any],
    ) -> list[Any]:
        if self._memory_repository is None:
            return []
        created = []
        synthesis = str(research.get("synthesis") or "").strip()
        if synthesis:
            item = self._memory_repository.create_item(
                thread_id=thread.id,
                actor_id=thread.created_by,
                category=RuntimeConversationMemoryCategory.fact.value,
                content=f"Research synthesis for '{user_query}': {synthesis}",
                metadata_json={"runtime": "langbridge.ai", "kind": "research_synthesis"},
            )
            if item is not None:
                created.append(item)
        findings = research.get("findings")
        if isinstance(findings, list):
            for finding in findings[:6]:
                if not isinstance(finding, dict):
                    continue
                insight = str(finding.get("insight") or finding.get("claim") or "").strip()
                source = str(finding.get("source") or "").strip()
                if not insight:
                    continue
                content = f"{insight}" + (f" Source: {source}" if source else "")
                item = self._memory_repository.create_item(
                    thread_id=thread.id,
                    actor_id=thread.created_by,
                    category=RuntimeConversationMemoryCategory.fact.value,
                    content=content,
                    metadata_json={"runtime": "langbridge.ai", "kind": "research_finding"},
                )
                if item is not None:
                    created.append(item)
        return created

    def build_continuation_state(
        self,
        *,
        response: dict[str, Any],
        user_query: str,
        ai_run: MetaControllerRun | dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        continuation_state = ContinuationStateBuilder.from_response(
            response=response,
            user_query=user_query,
            ai_run=ai_run,
        )
        if continuation_state is not None:
            return continuation_state.compact_payload()
        return self._clarification_continuation_state(
            response=response,
            user_query=user_query,
            ai_run=ai_run,
        )

    def _clarification_continuation_state(
        self,
        *,
        response: dict[str, Any],
        user_query: str,
        ai_run: MetaControllerRun | dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        diagnostics = response.get("diagnostics")
        clarifying_question = (
            diagnostics.get("clarifying_question")
            if isinstance(diagnostics, Mapping)
            else None
        )
        answer = response.get("answer")
        summary = response.get("summary")
        if not any(isinstance(value, str) and value.strip() for value in (clarifying_question, answer, summary)):
            return None
        payload: dict[str, Any] = {
            "question": user_query,
            "summary": str(summary or "").strip() or None,
            "answer": str(answer or clarifying_question or "").strip() or None,
        }
        run_payload: Mapping[str, Any] = {}
        if isinstance(ai_run, Mapping):
            run_payload = ai_run
        elif hasattr(ai_run, "model_dump"):
            run_payload = ai_run.model_dump(mode="json")
        plan = run_payload.get("plan") if isinstance(run_payload.get("plan"), Mapping) else {}
        steps = plan.get("steps") if isinstance(plan, Mapping) else None
        if isinstance(steps, list) and steps:
            first_step = steps[0]
            if isinstance(first_step, Mapping):
                selected_agent = str(first_step.get("agent_name") or "").strip()
                if selected_agent:
                    payload["selected_agent"] = selected_agent
        return {key: value for key, value in payload.items() if value is not None}

    def continuation_memory_content(self, continuation_state: Mapping[str, Any]) -> str:
        lines = ["Analytical continuation state for follow-up reasoning."]
        question = str(continuation_state.get("question") or "").strip()
        resolved_question = str(continuation_state.get("resolved_question") or "").strip()
        summary = str(continuation_state.get("summary") or "").strip()
        if question:
            lines.append(f"Question: {question}")
        if resolved_question:
            lines.append(f"Resolved question: {resolved_question}")
        if summary:
            lines.append(f"Summary: {summary}")
        result = continuation_state.get("result")
        if isinstance(result, Mapping):
            columns = result.get("columns")
            rows = result.get("rows")
            if isinstance(columns, list) and columns:
                lines.append(f"Columns: {', '.join(str(column) for column in columns[:12])}")
            if isinstance(rows, list):
                lines.append(f"Row count: {len(rows)}")
        if continuation_state.get("chartable"):
            lines.append("Chartable: yes. Suitable for visualization follow-ups such as pie, bar, or line charts.")
        sources = continuation_state.get("sources")
        if isinstance(sources, list) and sources:
            lines.append(f"Source count: {len(sources)}")
        self._append_analysis_state(lines, continuation_state.get("analysis_state"))
        visualization_state = continuation_state.get("visualization_state")
        if isinstance(visualization_state, Mapping):
            chart_type = str(visualization_state.get("chart_type") or "").strip()
            if chart_type:
                lines.append(f"Last chart type: {chart_type}")
        return "\n".join(lines)

    def _append_analysis_state(self, lines: list[str], analysis_state: Any) -> None:
        if not isinstance(analysis_state, Mapping):
            return
        metrics = analysis_state.get("metrics")
        dimensions = analysis_state.get("dimensions")
        period = analysis_state.get("period")
        dimension_value_samples = analysis_state.get("dimension_value_samples")
        active_filters = analysis_state.get("active_filters")
        if isinstance(metrics, list) and metrics:
            lines.append(f"Metrics: {', '.join(str(metric) for metric in metrics[:8])}")
        if isinstance(dimensions, list) and dimensions:
            lines.append(f"Dimensions: {', '.join(str(dimension) for dimension in dimensions[:8])}")
        if isinstance(period, Mapping):
            label = str(period.get("label") or "").strip()
            if label:
                lines.append(f"Period: {label}")
        if isinstance(dimension_value_samples, Mapping):
            for field_name, values in list(dimension_value_samples.items())[:3]:
                if isinstance(values, list) and values:
                    lines.append(
                        f"Sample values for {field_name}: {', '.join(str(value) for value in values[:6])}"
                    )
        if isinstance(active_filters, list) and active_filters:
            for filter_payload in active_filters[:4]:
                if not isinstance(filter_payload, Mapping):
                    continue
                field_name = str(filter_payload.get("field") or "").strip()
                operator = str(filter_payload.get("operator") or "").strip()
                values = filter_payload.get("values")
                if field_name and operator and isinstance(values, list) and values:
                    lines.append(
                        f"Active filter on {field_name} ({operator}): {', '.join(str(value) for value in values[:6])}"
                    )
