from typing import Any

from langbridge.ai import MetaControllerRun


class AgentRunResponseBuilder:
    def build_response(self, ai_run: MetaControllerRun) -> dict[str, Any]:
        response = dict(ai_run.final_result or {})
        diagnostics = response.get("diagnostics")
        execution_diagnostics = self.execution_diagnostics(ai_run)
        clarifying_question = self.clarifying_question_from_run(response=response, ai_run=ai_run)
        response["diagnostics"] = {
            **(diagnostics if isinstance(diagnostics, dict) else {}),
            "execution": execution_diagnostics,
            "sql": execution_diagnostics.get("sql", []),
            **(
                {"clarifying_question": clarifying_question}
                if clarifying_question
                and not (
                    isinstance(diagnostics, dict)
                    and isinstance(diagnostics.get("clarifying_question"), str)
                    and diagnostics.get("clarifying_question", "").strip()
                )
                else {}
            ),
            "ai_run": {
                "execution_mode": ai_run.execution_mode,
                "status": ai_run.status,
                "route": ai_run.plan.route,
                "plan": ai_run.plan.model_dump(mode="json"),
                "verification": [item.model_dump(mode="json") for item in ai_run.verification],
                "review_decisions": [
                    item.model_dump(mode="json") for item in ai_run.review_decisions
                ],
                "diagnostics": dict(ai_run.diagnostics or {}),
                "step_results": execution_diagnostics.get("step_results", []),
            },
        }
        return response

    def execution_diagnostics(self, ai_run: MetaControllerRun) -> dict[str, Any]:
        run_diagnostics = dict(ai_run.diagnostics or {})
        plan_steps = self.plan_step_diagnostics(ai_run)
        step_results = self.step_result_diagnostics(ai_run)
        sql_items = self.sql_diagnostics(ai_run)
        evidence = self.evidence_diagnostics(ai_run=ai_run, sql_items=sql_items)
        selected_agent = str(run_diagnostics.get("selected_agent") or "").strip() or None
        query_scopes = self.unique_non_empty(item.get("query_scope") for item in sql_items)
        rowcounts = [
            int(item["rowcount"])
            for item in sql_items
            if isinstance(item.get("rowcount"), int)
        ]
        execution = {
            "status": ai_run.status,
            "route": ai_run.plan.route,
            "execution_mode": ai_run.execution_mode,
            "selected_agent": selected_agent,
            "stop_reason": run_diagnostics.get("stop_reason"),
            "iterations": run_diagnostics.get("iterations"),
            "replan_count": run_diagnostics.get("replan_count"),
            "plan_steps": plan_steps,
            "step_results": step_results,
            "sql": sql_items,
            "evidence": evidence,
            "reviews": {
                "verification": [item.model_dump(mode="json") for item in ai_run.verification],
                "plan_review_decisions": [
                    item.model_dump(mode="json") for item in ai_run.review_decisions
                ],
                "final_review": dict(ai_run.final_review or {}),
            },
            "query_scopes": query_scopes,
            "rowcount": rowcounts[-1] if rowcounts else None,
            "total_sql_queries": len(sql_items),
        }
        execution["summary"] = self.execution_summary(execution)
        return execution

    def plan_step_diagnostics(self, ai_run: MetaControllerRun) -> list[dict[str, Any]]:
        return [
            {
                "step_id": step.step_id,
                "agent_name": step.agent_name,
                "task_kind": step.task_kind.value,
                "question": step.question,
                "input": dict(step.input or {}),
                "depends_on": list(step.depends_on or []),
            }
            for step in ai_run.plan.steps
        ]

    def step_result_diagnostics(self, ai_run: MetaControllerRun) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for result in ai_run.step_results:
            if not isinstance(result, dict):
                continue
            output = result.get("output") if isinstance(result.get("output"), dict) else {}
            diagnostics = result.get("diagnostics") if isinstance(result.get("diagnostics"), dict) else {}
            outcome = output.get("outcome") if isinstance(output.get("outcome"), dict) else {}
            tabular = self.compact_tabular_result(output.get("result"))
            items.append(
                {
                    "task_id": result.get("task_id"),
                    "agent_name": result.get("agent_name"),
                    "status": result.get("status"),
                    "error": result.get("error"),
                    "analysis_path": output.get("analysis_path"),
                    "query_scope": output.get("query_scope"),
                    "rowcount": tabular.get("rowcount"),
                    "columns": tabular.get("columns", []),
                    "outcome_status": outcome.get("status"),
                    "outcome_stage": outcome.get("stage"),
                    "outcome_message": outcome.get("message"),
                    "diagnostics": self.compact_agent_diagnostics(diagnostics),
                }
            )
        return items

    def sql_diagnostics(self, ai_run: MetaControllerRun) -> list[dict[str, Any]]:
        sql_items: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()
        for result in ai_run.step_results:
            if not isinstance(result, dict):
                continue
            output = result.get("output") if isinstance(result.get("output"), dict) else {}
            diagnostics = result.get("diagnostics") if isinstance(result.get("diagnostics"), dict) else {}
            self.append_sql_item(
                sql_items,
                seen,
                source="step_output",
                task_id=str(result.get("task_id") or ""),
                agent_name=str(result.get("agent_name") or ""),
                output=output,
                diagnostics=diagnostics,
            )
            evidence_bundle = output.get("evidence_bundle")
            if not isinstance(evidence_bundle, dict):
                evidence = output.get("evidence") if isinstance(output.get("evidence"), dict) else {}
                evidence_bundle = evidence.get("bundle") if isinstance(evidence.get("bundle"), dict) else {}
            for index, round_payload in enumerate(evidence_bundle.get("governed_rounds") or []):
                if not isinstance(round_payload, dict):
                    continue
                round_output = round_payload.get("output")
                if not isinstance(round_output, dict):
                    continue
                round_diagnostics = round_payload.get("diagnostics")
                self.append_sql_item(
                    sql_items,
                    seen,
                    source="research_governed_round",
                    task_id=str(result.get("task_id") or ""),
                    agent_name=str(result.get("agent_name") or ""),
                    output=round_output,
                    diagnostics=round_diagnostics if isinstance(round_diagnostics, dict) else {},
                    round_index=index + 1,
                    round_question=round_payload.get("question"),
                )
        return sql_items

    def append_sql_item(
        self,
        sql_items: list[dict[str, Any]],
        seen: set[tuple[str, str, str]],
        *,
        source: str,
        task_id: str,
        agent_name: str,
        output: dict[str, Any],
        diagnostics: dict[str, Any],
        round_index: int | None = None,
        round_question: Any = None,
    ) -> None:
        sql_canonical = str(output.get("sql_canonical") or "").strip()
        sql_executable = str(output.get("sql_executable") or "").strip()
        if not sql_canonical and not sql_executable:
            return
        key = (task_id, sql_canonical, sql_executable)
        if key in seen:
            return
        seen.add(key)
        outcome = output.get("outcome") if isinstance(output.get("outcome"), dict) else {}
        tabular = self.compact_tabular_result(output.get("result"))
        evidence = output.get("evidence") if isinstance(output.get("evidence"), dict) else {}
        governed = evidence.get("governed") if isinstance(evidence.get("governed"), dict) else {}
        sql_items.append(
            {
                "source": source,
                "task_id": task_id,
                "agent_name": agent_name,
                "round_index": round_index,
                "round_question": round_question,
                "status": outcome.get("status") or output.get("status"),
                "stage": outcome.get("stage"),
                "message": outcome.get("message"),
                "analysis_path": output.get("analysis_path"),
                "query_scope": output.get("query_scope") or governed.get("query_scope"),
                "sql_canonical": sql_canonical or None,
                "sql_executable": sql_executable or None,
                "selected_datasets": output.get("selected_datasets") or [],
                "selected_semantic_models": output.get("selected_semantic_models") or [],
                "rowcount": tabular.get("rowcount"),
                "columns": tabular.get("columns", []),
                "rows_sample": tabular.get("rows_sample", []),
                "used_fallback": bool(governed.get("used_fallback")),
                "error": output.get("error") or output.get("error_message"),
                "error_taxonomy": output.get("error_taxonomy"),
                "governed_attempts": diagnostics.get("governed_attempts") or [],
                "governed_tools_tried": diagnostics.get("governed_tools_tried") or [],
            }
        )

    def compact_tabular_result(self, value: Any, *, max_rows: int = 5) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {"columns": [], "rows_sample": [], "rowcount": None}
        rows = value.get("rows")
        row_items = list(rows[:max_rows]) if isinstance(rows, list) else []
        rowcount = value.get("rowcount")
        if rowcount is None and isinstance(rows, list):
            rowcount = len(rows)
        return {
            "columns": list(value.get("columns") or []),
            "rows_sample": row_items,
            "rowcount": rowcount,
            "truncated": isinstance(rows, list) and len(rows) > max_rows,
        }

    def compact_agent_diagnostics(self, diagnostics: dict[str, Any]) -> dict[str, Any]:
        keys = (
            "agent_mode",
            "mode_decision",
            "selected_tool",
            "selected_query_scope",
            "weak_evidence",
            "governed_attempt_count",
            "governed_tools_tried",
            "evidence_bundle_assessment",
            "research_steps",
        )
        return {key: diagnostics[key] for key in keys if key in diagnostics}

    def evidence_diagnostics(
        self,
        *,
        ai_run: MetaControllerRun,
        sql_items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        external_sources = 0
        governed_rounds = 0
        evidence_plan: dict[str, Any] | None = None
        for result in ai_run.step_results:
            if not isinstance(result, dict):
                continue
            output = result.get("output") if isinstance(result.get("output"), dict) else {}
            sources = output.get("sources")
            if isinstance(sources, list):
                external_sources = max(external_sources, len(sources))
            bundle = output.get("evidence_bundle")
            if not isinstance(bundle, dict):
                evidence = output.get("evidence") if isinstance(output.get("evidence"), dict) else {}
                bundle = evidence.get("bundle") if isinstance(evidence.get("bundle"), dict) else {}
            assessment = bundle.get("assessment") if isinstance(bundle.get("assessment"), dict) else {}
            governed_rounds = max(
                governed_rounds,
                self.safe_int(assessment.get("governed_round_count")) or 0,
            )
            if evidence_plan is None and isinstance(bundle.get("evidence_plan"), dict):
                evidence_plan = dict(bundle["evidence_plan"])
        return {
            "governed_attempted": bool(sql_items),
            "governed_rounds": governed_rounds or len(sql_items),
            "external_sources": external_sources,
            "used_fallback": any(bool(item.get("used_fallback")) for item in sql_items),
            "evidence_plan": evidence_plan,
        }

    def execution_summary(self, execution: dict[str, Any]) -> str:
        status = str(execution.get("status") or "completed").replace("_", " ")
        route = str(execution.get("route") or "unknown route")
        sql_count = int(execution.get("total_sql_queries") or 0)
        rowcount = execution.get("rowcount")
        scopes = ", ".join(execution.get("query_scopes") or [])
        parts = [f"Run {status} via {route}."]
        if sql_count:
            parts.append(
                f"Generated {sql_count} SQL quer{'y' if sql_count == 1 else 'ies'}"
                + (f" across {scopes}" if scopes else "")
                + "."
            )
        else:
            parts.append("No SQL query was generated.")
        if isinstance(rowcount, int):
            parts.append(f"Latest tabular result returned {rowcount} row{'s' if rowcount != 1 else ''}.")
        if execution.get("replan_count"):
            parts.append(f"Replanned {execution['replan_count']} time(s).")
        return " ".join(parts)

    def public_completion_message(self, response: dict[str, Any]) -> str:
        clarifying_question = self.clarifying_question(response)
        if clarifying_question:
            return clarifying_question
        answer_markdown = response.get("answer_markdown")
        if isinstance(answer_markdown, str) and answer_markdown.strip():
            return answer_markdown.strip()
        answer = response.get("answer")
        if isinstance(answer, str) and answer.strip():
            return answer.strip()
        summary = response.get("summary")
        if isinstance(summary, str) and summary.strip():
            return summary.strip()
        return "Agent run completed."

    def clarifying_question(self, response: dict[str, Any]) -> str | None:
        diagnostics = response.get("diagnostics")
        if isinstance(diagnostics, dict):
            value = diagnostics.get("clarifying_question")
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def clarifying_question_from_run(
        self,
        *,
        response: dict[str, Any],
        ai_run: MetaControllerRun,
    ) -> str | None:
        diagnostics = response.get("diagnostics")
        if isinstance(diagnostics, dict):
            value = diagnostics.get("clarifying_question")
            if isinstance(value, str) and value.strip():
                return value.strip()
        stop_reason = str(ai_run.diagnostics.get("stop_reason") or "").strip().lower()
        if ai_run.status != "clarification_needed" and stop_reason != "clarification":
            return None
        answer_markdown = response.get("answer_markdown")
        if isinstance(answer_markdown, str) and answer_markdown.strip():
            return answer_markdown.strip()
        answer = response.get("answer")
        if isinstance(answer, str) and answer.strip():
            return answer.strip()
        summary = response.get("summary")
        if isinstance(summary, str) and summary.strip():
            return summary.strip()
        return None

    def unique_non_empty(self, values: Any) -> list[str]:
        unique: list[str] = []
        for value in values:
            text = str(value or "").strip()
            if text and text not in unique:
                unique.append(text)
        return unique

    def safe_int(self, value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
