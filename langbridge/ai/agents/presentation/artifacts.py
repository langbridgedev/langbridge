"""Artifact contract helpers for the presentation agent."""

from __future__ import annotations

import re
from typing import Any

from langbridge.ai.tools.charting import ChartSpec


ARTIFACT_REF_RE = re.compile(r"\{\{artifact:([A-Za-z0-9_.:-]+)\}\}")


def build_available_artifacts(
    *,
    data_payload: dict[str, Any] | None,
    visualization: ChartSpec | None,
    step_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build verified renderable artifacts from backend-owned outputs."""

    artifacts: list[dict[str, Any]] = []
    _extend_step_artifacts(artifacts=artifacts, step_results=step_results)
    has_table = _is_tabular_payload(data_payload)

    if visualization is not None:
        _append_artifact(
            artifacts,
            _artifact(
                artifact_id="primary_visualization",
                artifact_type="chart",
                role="primary_result",
                title=visualization.title,
                source="visualization",
                payload=visualization.model_dump(mode="json"),
                provenance={"source": "presentation", "source_key": "visualization"},
                data_ref=(
                    {"kind": "artifact", "artifact_id": "result_table"}
                    if has_table
                    else {"kind": "response.visualization", "path": "visualization"}
                ),
            ),
        )

    if has_table and data_payload is not None:
        rows = data_payload.get("rows") if isinstance(data_payload.get("rows"), list) else []
        _append_artifact(
            artifacts,
            _artifact(
                artifact_id="result_table",
                artifact_type="table",
                role="supporting_result" if visualization is not None else "primary_result",
                title="Verified result table",
                source="result",
                payload={
                    "columns": list(data_payload.get("columns") or []),
                    "rows": rows,
                    "row_count": _row_count(data_payload),
                },
                provenance={"source": "analyst", "source_key": "result"},
                data_ref={"kind": "response.result", "path": "result"},
                extra={"row_count": _row_count(data_payload)},
            ),
        )

    sql_payload, sql_provenance = _latest_sql_payload(step_results)
    if sql_payload:
        _append_artifact(
            artifacts,
            _artifact(
                artifact_id="generated_sql",
                artifact_type="sql",
                role="supporting_result",
                title="Generated SQL",
                source="sql",
                payload=sql_payload,
                provenance=sql_provenance,
                data_ref=(
                    {"kind": "artifact", "artifact_id": "result_table"}
                    if has_table
                    else None
                ),
            ),
        )

    diagnostics_payload, diagnostics_provenance = _latest_diagnostics_payload(step_results)
    if diagnostics_payload:
        _append_artifact(
            artifacts,
            _artifact(
                artifact_id="execution_diagnostics",
                artifact_type="diagnostics",
                role="diagnostic",
                title="Execution diagnostics",
                source="diagnostics",
                payload=diagnostics_payload,
                provenance=diagnostics_provenance,
            ),
        )

    return artifacts


def sanitize_artifact_placeholders(
    *,
    answer_markdown: str,
    available_artifacts: list[dict[str, Any]],
) -> str:
    """Remove placeholders for artifact IDs that are not in the verified registry."""

    allowed_ids = {
        str(artifact.get("id") or "").strip()
        for artifact in available_artifacts
        if str(artifact.get("id") or "").strip()
    }

    def replace(match: re.Match[str]) -> str:
        artifact_id = match.group(1)
        return match.group(0) if artifact_id in allowed_ids else ""

    sanitized = ARTIFACT_REF_RE.sub(replace, answer_markdown)
    return re.sub(r"\n{3,}", "\n\n", sanitized).strip()


def resolve_referenced_artifacts(
    *,
    parsed: dict[str, Any],
    answer_markdown: str,
    available_artifacts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return verified artifacts referenced by markdown or parsed artifact IDs."""

    by_id = {
        str(artifact.get("id") or ""): artifact
        for artifact in available_artifacts
        if str(artifact.get("id") or "")
    }
    referenced_ids: list[str] = []
    for artifact_id in _artifact_ids_from_markdown(answer_markdown):
        if artifact_id in by_id and artifact_id not in referenced_ids:
            referenced_ids.append(artifact_id)
    for artifact_id in _artifact_ids_from_parsed(parsed):
        if artifact_id in by_id and artifact_id not in referenced_ids:
            referenced_ids.append(artifact_id)
    return [by_id[artifact_id] for artifact_id in referenced_ids]


def _artifact(
    *,
    artifact_id: str,
    artifact_type: str,
    role: str,
    title: str,
    source: str,
    payload: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    data_ref: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": artifact_id,
        "type": artifact_type,
        "role": role,
        "title": title,
        "placeholder": f"{{{{artifact:{artifact_id}}}}}",
        "source": source,
        "provenance": provenance or {"source": source},
    }
    if payload is not None:
        item["payload"] = payload
    if data_ref is not None:
        item["data_ref"] = data_ref
    if extra:
        item.update(extra)
    return item


def _append_artifact(artifacts: list[dict[str, Any]], artifact: dict[str, Any]) -> None:
    artifact_id = str(artifact.get("id") or "").strip()
    if not artifact_id:
        return
    if any(str(item.get("id") or "").strip() == artifact_id for item in artifacts):
        return
    artifacts.append(artifact)


def _extend_step_artifacts(
    *,
    artifacts: list[dict[str, Any]],
    step_results: list[dict[str, Any]],
) -> None:
    for step_result in step_results:
        if not isinstance(step_result, dict):
            continue
        for artifact in _iter_step_artifacts(step_result):
            normalized = _normalize_step_artifact(artifact)
            if normalized is not None:
                _append_artifact(artifacts, normalized)


def _iter_step_artifacts(step_result: dict[str, Any]) -> list[dict[str, Any]]:
    raw_items: list[Any] = [step_result.get("artifacts")]
    output = step_result.get("output") if isinstance(step_result.get("output"), dict) else {}
    raw_items.append(output.get("artifacts"))

    artifacts: list[dict[str, Any]] = []
    for raw in raw_items:
        if isinstance(raw, dict):
            for artifact_id, artifact in raw.items():
                if isinstance(artifact, dict):
                    artifacts.append({"id": artifact_id, **artifact})
        elif isinstance(raw, list):
            artifacts.extend(item for item in raw if isinstance(item, dict))
    return artifacts


def _normalize_step_artifact(artifact: dict[str, Any]) -> dict[str, Any] | None:
    artifact_id = str(
        artifact.get("id")
        or artifact.get("artifact_id")
        or artifact.get("artifactId")
        or artifact.get("key")
        or ""
    ).strip()
    if not artifact_id:
        return None

    artifact_type = str(
        artifact.get("type")
        or artifact.get("kind")
        or artifact.get("artifact_type")
        or ""
    ).strip()
    normalized = dict(artifact)
    normalized.update(
        {
            "id": artifact_id,
            "type": artifact_type or "artifact",
            "role": str(artifact.get("role") or "supporting_result").strip(),
            "title": str(artifact.get("title") or artifact_id.replace("_", " ")).strip(),
            "placeholder": artifact.get("placeholder") or f"{{{{artifact:{artifact_id}}}}}",
            "provenance": (
                artifact.get("provenance")
                if isinstance(artifact.get("provenance"), dict)
                else {"source": str(artifact.get("source") or "analyst")}
            ),
        }
    )
    return normalized


def _artifact_ids_from_markdown(answer_markdown: str) -> list[str]:
    ids: list[str] = []
    for match in ARTIFACT_REF_RE.finditer(answer_markdown):
        artifact_id = match.group(1)
        if artifact_id not in ids:
            ids.append(artifact_id)
    return ids


def _artifact_ids_from_parsed(parsed: dict[str, Any]) -> list[str]:
    parsed_artifacts = parsed.get("artifacts")
    if not isinstance(parsed_artifacts, list):
        return []
    ids: list[str] = []
    for item in parsed_artifacts:
        if isinstance(item, str):
            artifact_id = item.strip()
        elif isinstance(item, dict):
            artifact_id = str(item.get("id") or item.get("artifact_id") or "").strip()
        else:
            artifact_id = ""
        if artifact_id and artifact_id not in ids:
            ids.append(artifact_id)
    return ids


def _latest_sql_payload(step_results: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    for item in reversed(step_results):
        output = item.get("output") if isinstance(item.get("output"), dict) else {}
        sql_canonical = _text(output.get("sql_canonical") or output.get("generated_sql"))
        sql_executable = _text(output.get("sql_executable") or output.get("query_sql"))
        if not sql_canonical and not sql_executable:
            continue
        payload = _without_empty(
            {
                "sql_canonical": sql_canonical,
                "sql_executable": sql_executable,
                "dialect": _text(output.get("dialect")),
                "query_scope": _text(output.get("query_scope")),
                "analysis_path": _text(output.get("analysis_path")),
                "selected_datasets": _list_or_none(output.get("selected_datasets")),
                "selected_semantic_models": _list_or_none(output.get("selected_semantic_models")),
            }
        )
        provenance = _without_empty(
            {
                "source": "analyst",
                "source_key": "sql",
                "agent_name": _text(item.get("agent_name")),
                "task_id": _text(item.get("task_id")),
                "query_scope": payload.get("query_scope"),
            }
        )
        return payload, provenance
    return None, None


def _latest_diagnostics_payload(
    step_results: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    for item in reversed(step_results):
        output = item.get("output") if isinstance(item.get("output"), dict) else {}
        diagnostics = item.get("diagnostics") if isinstance(item.get("diagnostics"), dict) else {}
        payload = _without_empty(
            {
                "status": _text(item.get("status")),
                "error": _text(item.get("error")),
                "analysis_path": _text(output.get("analysis_path")),
                "query_scope": _text(output.get("query_scope")),
                "outcome": _dict_or_none(output.get("outcome")),
                "evidence": _dict_or_none(output.get("evidence")),
                "review_hints": _dict_or_none(output.get("review_hints")),
                "error_taxonomy": _dict_or_none(output.get("error_taxonomy")),
                "agent_diagnostics": diagnostics or None,
            }
        )
        if not payload:
            continue
        provenance = _without_empty(
            {
                "source": "analyst",
                "source_key": "diagnostics",
                "agent_name": _text(item.get("agent_name")),
                "task_id": _text(item.get("task_id")),
            }
        )
        return payload, provenance
    return None, None


def _is_tabular_payload(data_payload: dict[str, Any] | None) -> bool:
    return bool(data_payload and {"columns", "rows"}.issubset(data_payload))


def _row_count(data_payload: dict[str, Any]) -> int:
    rowcount = data_payload.get("rowcount")
    if isinstance(rowcount, int):
        return rowcount
    rows = data_payload.get("rows")
    return len(rows) if isinstance(rows, list) else 0


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _list_or_none(value: Any) -> list[Any] | None:
    return list(value) if isinstance(value, list) and value else None


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) and value else None


def _without_empty(payload: dict[str, Any | None]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if value not in (None, "", [], {})}


__all__ = [
    "build_available_artifacts",
    "resolve_referenced_artifacts",
    "sanitize_artifact_placeholders",
]
