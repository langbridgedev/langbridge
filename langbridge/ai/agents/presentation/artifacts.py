"""Artifact contract helpers for the presentation agent."""

from __future__ import annotations

import re
from typing import Any, Mapping

from pydantic import ValidationError

from langbridge.ai.agents.presentation.contracts import PresentationArtifactAdapter
from langbridge.ai.agents.presentation.guidance import build_column_formatting
from langbridge.ai.tools.charting import ChartSpec


ARTIFACT_REF_RE = re.compile(r"\{\{artifact:([A-Za-z0-9_.:-]+)\}\}")
PRIMARY_RESULT_ARTIFACT_ID = "primary_result"
PRIMARY_VISUALIZATION_ARTIFACT_ID = "primary_visualization"
GENERATED_SQL_ARTIFACT_ID = "generated_sql"
EXECUTION_DIAGNOSTICS_ARTIFACT_ID = "execution_diagnostics"


def build_available_artifacts(
    *,
    data_payload: dict[str, Any] | None,
    visualization: ChartSpec | None,
    step_results: list[dict[str, Any]],
    presentation_guidance: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Build verified renderable artifacts from backend-owned outputs."""

    artifacts: list[dict[str, Any]] = []
    _extend_step_artifacts(
        artifacts=artifacts,
        step_results=step_results,
        presentation_guidance=presentation_guidance,
    )
    has_table = _is_tabular_payload(data_payload)
    columns = list(data_payload.get("columns") or []) if has_table and data_payload is not None else []
    formatting = build_column_formatting(
        columns=columns,
        presentation_guidance=presentation_guidance,
    )

    has_renderable_visualization = _is_renderable_chart(visualization)

    if has_renderable_visualization and visualization is not None:
        visualization_payload = visualization.model_dump(mode="json")
        if formatting:
            visualization_payload["formatting"] = formatting
        _append_artifact(
            artifacts,
            _artifact(
                artifact_id=PRIMARY_VISUALIZATION_ARTIFACT_ID,
                artifact_type="chart",
                role="primary_result",
                title=visualization.title,
                payload=visualization_payload,
                provenance={"source": "presentation", "source_key": "visualization"},
                data_ref=(
                    {"kind": "artifact", "artifact_id": PRIMARY_RESULT_ARTIFACT_ID}
                    if has_table
                    else None
                ),
            ),
        )

    if has_table and data_payload is not None:
        rows = data_payload.get("rows") if isinstance(data_payload.get("rows"), list) else []
        payload = {
            "columns": columns,
            "rows": rows,
            "row_count": _row_count(data_payload),
        }
        if formatting:
            payload["formatting"] = formatting
        _append_artifact(
            artifacts,
            _artifact(
                artifact_id=PRIMARY_RESULT_ARTIFACT_ID,
                artifact_type="table",
                role="supporting_result" if has_renderable_visualization else "primary_result",
                title="Verified analyst result",
                payload=payload,
                provenance={"source": "analyst", "source_key": "result"},
            ),
        )

    sql_payload, sql_provenance = _latest_sql_payload(step_results)
    if sql_payload:
        _append_artifact(
            artifacts,
            _artifact(
                artifact_id=GENERATED_SQL_ARTIFACT_ID,
                artifact_type="sql",
                role="supporting_result",
                title="Generated SQL",
                payload=sql_payload,
                provenance=sql_provenance,
                data_ref=(
                    {"kind": "artifact", "artifact_id": PRIMARY_RESULT_ARTIFACT_ID}
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
                artifact_id=EXECUTION_DIAGNOSTICS_ARTIFACT_ID,
                artifact_type="diagnostics",
                role="diagnostic",
                title="Execution diagnostics",
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
    for artifact_id in (PRIMARY_RESULT_ARTIFACT_ID,):
        if artifact_id in by_id and artifact_id not in referenced_ids:
            referenced_ids.append(artifact_id)
    return [by_id[artifact_id] for artifact_id in referenced_ids]


def _artifact(
    *,
    artifact_id: str,
    artifact_type: str,
    role: str,
    title: str,
    payload: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    data_ref: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    item: dict[str, Any] = {
        "id": artifact_id,
        "type": artifact_type,
        "role": role,
        "title": title,
        "provenance": provenance or {"source": "presentation"},
    }
    if payload is not None:
        item["payload"] = payload
    if data_ref is not None:
        item["data_ref"] = data_ref
    if extra:
        item.update(extra)
    return _validate_artifact(item)


def _append_artifact(artifacts: list[dict[str, Any]], artifact: dict[str, Any] | None) -> None:
    if artifact is None:
        return
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
    presentation_guidance: dict[str, Any] | None,
) -> None:
    for step_result in step_results:
        if not isinstance(step_result, dict):
            continue
        for artifact in _iter_step_artifacts(step_result):
            normalized = _normalize_step_artifact(
                artifact,
                presentation_guidance=presentation_guidance,
            )
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


def _normalize_step_artifact(
    artifact: dict[str, Any],
    *,
    presentation_guidance: dict[str, Any] | None,
) -> dict[str, Any] | None:
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
    ).strip().lower()
    if artifact_type not in {"table", "chart", "sql", "diagnostics"}:
        return None
    source = str(artifact.get("source") or "analyst").strip() or "analyst"
    normalized = dict(artifact)
    normalized.pop("placeholder", None)
    normalized.pop("source", None)
    top_level_formatting = normalized.pop("formatting", None)
    normalized.pop("row_count", None)
    normalized.pop("rowcount", None)
    normalized.update(
        {
            "id": artifact_id,
            "type": artifact_type,
            "role": _normalize_role(artifact.get("role"), artifact_type=artifact_type),
            "title": str(artifact.get("title") or artifact_id.replace("_", " ")).strip(),
            "provenance": (
                artifact.get("provenance")
                if isinstance(artifact.get("provenance"), dict)
                else {"source": source}
            ),
        }
    )
    normalized["data_ref"] = _normalize_data_ref(normalized.get("data_ref"))
    if normalized["data_ref"] is None:
        normalized.pop("data_ref", None)
    _normalize_artifact_payload(
        artifact=normalized,
        top_level_formatting=top_level_formatting,
    )
    _attach_formatting(
        artifact=normalized,
        presentation_guidance=presentation_guidance,
    )
    return _validate_artifact(normalized)


def _normalize_role(value: Any, *, artifact_type: str) -> str:
    role = str(value or "").strip()
    if role in {"primary_result", "supporting_result", "diagnostic"}:
        return role
    return "diagnostic" if artifact_type == "diagnostics" else "supporting_result"


def _normalize_data_ref(value: Any) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return None
    return {"kind": "source", "source_key": text}


def _normalize_artifact_payload(
    *,
    artifact: dict[str, Any],
    top_level_formatting: Any,
) -> None:
    artifact_type = str(artifact.get("type") or "").strip()
    payload = artifact.get("payload")
    if not isinstance(payload, dict):
        if artifact_type == "diagnostics":
            return
        artifact["payload"] = {}
        payload = artifact["payload"]
    if artifact_type == "table":
        rowcount = payload.pop("rowcount", None)
        if "row_count" not in payload and isinstance(rowcount, int):
            payload["row_count"] = rowcount
        if "row_count" not in payload and isinstance(payload.get("rows"), list):
            payload["row_count"] = len(payload["rows"])
    if isinstance(top_level_formatting, dict) and "formatting" not in payload:
        payload["formatting"] = top_level_formatting


def _validate_artifact(artifact: dict[str, Any]) -> dict[str, Any] | None:
    try:
        validated = PresentationArtifactAdapter.validate_python(artifact)
    except ValidationError:
        return None
    return validated.model_dump(mode="json", exclude_none=True)


def _attach_formatting(
    *,
    artifact: dict[str, Any],
    presentation_guidance: dict[str, Any] | None,
) -> None:
    if not presentation_guidance:
        return
    payload = artifact.get("payload")
    if not isinstance(payload, dict) or isinstance(payload.get("formatting"), dict):
        return
    artifact_type = str(artifact.get("type") or "").strip()
    if artifact_type not in {"table", "chart"}:
        return
    columns = payload.get("columns")
    if not isinstance(columns, list):
        return
    formatting = build_column_formatting(
        columns=columns,
        presentation_guidance=presentation_guidance,
    )
    if not formatting:
        return
    payload["formatting"] = formatting


def _artifact_ids_from_markdown(answer_markdown: str) -> list[str]:
    ids: list[str] = []
    for match in ARTIFACT_REF_RE.finditer(answer_markdown):
        artifact_id = match.group(1)
        if artifact_id not in ids:
            ids.append(artifact_id)
    return ids


def _artifact_ids_from_parsed(parsed: dict[str, Any]) -> list[str]:
    parsed_artifact_ids = parsed.get("artifact_ids")
    if not isinstance(parsed_artifact_ids, list):
        return []
    ids: list[str] = []
    for item in parsed_artifact_ids:
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


def _is_renderable_chart(visualization: ChartSpec | None) -> bool:
    if visualization is None:
        return False
    chart_type = str(visualization.chart_type or "").strip().lower()
    return bool(chart_type and chart_type != "table")


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
