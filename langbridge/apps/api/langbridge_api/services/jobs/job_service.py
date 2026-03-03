from __future__ import annotations

import json
import uuid
from typing import Any

from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.jobs.agent_job import (
    AgentJobStateResponse,
    CreateAgentJobRequest,
    JobEventResponse,
    JobEventVisibility,
    JobFinalResponse,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.agentic_semantic_model_job import (
    CreateAgenticSemanticModelJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.copilot_dashboard_job import (
    CreateCopilotDashboardJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.semantic_query_job import (
    CreateSemanticQueryJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.job import JobRecord
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    PermissionDeniedBusinessValidationError,
    ResourceNotFound,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository


class JobService:
    def __init__(self, job_repository: JobRepository) -> None:
        self._job_repository = job_repository

    async def get_agent_job_state(
        self,
        *,
        job_id: uuid.UUID,
        organization_id: uuid.UUID,
        current_user: UserResponse,
        include_internal: bool = False,
    ) -> AgentJobStateResponse:
        job = await self._job_repository.get_by_id(job_id)
        if job is None:
            raise ResourceNotFound("Job not found")

        self._enforce_job_access(job, organization_id, current_user)

        events, has_internal_events = self._map_events(job, include_internal=include_internal)
        final_response = self._build_final_response(job)
        thinking_breakdown = self._build_thinking_breakdown(job, include_internal=include_internal)

        return AgentJobStateResponse(
            id=job.id,
            job_type=str(job.job_type),
            status=job.status.value,
            progress=job.progress,
            error=job.error,
            created_at=job.created_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            events=events,
            final_response=final_response,
            thinking_breakdown=thinking_breakdown,
            has_internal_events=has_internal_events,
        )

    def _enforce_job_access(
        self,
        job: JobRecord,
        organization_id: uuid.UUID,
        current_user: UserResponse,
    ) -> None:
        if str(job.organisation_id) != str(organization_id):
            raise PermissionDeniedBusinessValidationError("You do not have access to this job.")

        payload = self._read_job_payload(job)
        if payload is None:
            raise PermissionDeniedBusinessValidationError("You do not have access to this job.")

        user_id: uuid.UUID | None = None
        try:
            if str(job.job_type) == JobType.AGENT.value:
                request = CreateAgentJobRequest.model_validate(payload)
                user_id = request.user_id
            elif str(job.job_type) == JobType.SEMANTIC_QUERY.value:
                request = CreateSemanticQueryJobRequest.model_validate(payload)
                user_id = request.user_id
                if request.organisation_id != organization_id:
                    raise PermissionDeniedBusinessValidationError("You do not have access to this job.")
            elif str(job.job_type) == JobType.AGENTIC_SEMANTIC_MODEL.value:
                request = CreateAgenticSemanticModelJobRequest.model_validate(payload)
                user_id = request.user_id
                if request.organisation_id != organization_id:
                    raise PermissionDeniedBusinessValidationError("You do not have access to this job.")
            elif str(job.job_type) == JobType.COPILOT_DASHBOARD.value:
                request = CreateCopilotDashboardJobRequest.model_validate(payload)
                user_id = request.user_id
                if request.organisation_id != organization_id:
                    raise PermissionDeniedBusinessValidationError("You do not have access to this job.")
            else:
                raw_user_id = payload.get("user_id")
                if raw_user_id is not None:
                    user_id = uuid.UUID(str(raw_user_id))
        except PermissionDeniedBusinessValidationError:
            raise
        except Exception as exc:
            raise PermissionDeniedBusinessValidationError("You do not have access to this job.") from exc

        if user_id != current_user.id:
            raise PermissionDeniedBusinessValidationError("You do not have access to this job.")

    @staticmethod
    def _read_job_payload(job: JobRecord) -> dict[str, Any] | None:
        payload = job.payload
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, str):
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                return None
            return parsed if isinstance(parsed, dict) else None
        return None

    def _map_events(
        self,
        job: JobRecord,
        *,
        include_internal: bool,
    ) -> tuple[list[JobEventResponse], bool]:
        mapped: list[JobEventResponse] = []
        has_internal = False

        sorted_events = sorted(
            list(job.job_events or []),
            key=lambda event: event.created_at or job.created_at,
        )
        for event in sorted_events:
            details = event.details if isinstance(event.details, dict) else {}
            event_visibility = getattr(event, "visibility", None)
            if event_visibility is None:
                visibility_raw = ""
            else:
                visibility_raw = str(getattr(event_visibility, "value", event_visibility)).lower()
            if not visibility_raw:
                visibility_raw = str(details.get("visibility", JobEventVisibility.internal.value)).lower()
            visibility = (
                JobEventVisibility.public
                if visibility_raw == JobEventVisibility.public.value
                else JobEventVisibility.internal
            )
            if visibility == JobEventVisibility.internal:
                has_internal = True
                if not include_internal:
                    continue

            mapped.append(
                JobEventResponse(
                    id=event.id,
                    event_type=event.event_type,
                    visibility=visibility,
                    message=str(details.get("message") or event.event_type),
                    source=details.get("source"),
                    details=details.get("details") if isinstance(details.get("details"), dict) else {},
                    created_at=event.created_at,
                )
            )

        return mapped, has_internal

    @staticmethod
    def _build_final_response(job: JobRecord) -> JobFinalResponse | None:
        if not isinstance(job.result, dict):
            return None
        return JobFinalResponse(
            result=job.result.get("result"),
            visualization=job.result.get("visualization"),
            summary=job.result.get("summary"),
        )

    @staticmethod
    def _build_thinking_breakdown(
        job: JobRecord,
        *,
        include_internal: bool,
    ) -> dict[str, Any] | None:
        if not include_internal or not isinstance(job.result, dict):
            return None
        diagnostics = job.result.get("diagnostics")
        tool_calls = job.result.get("tool_calls")
        sql_audit = JobService._extract_sql_audit(job)
        return {
            "diagnostics": diagnostics if isinstance(diagnostics, dict) else {},
            "tool_calls": tool_calls if isinstance(tool_calls, list) else [],
            "sql_audit": sql_audit,
        }

    @staticmethod
    def _extract_sql_audit(job: JobRecord) -> list[dict[str, Any]]:
        sql_entries: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str]] = set()

        def add_entry(source: str, kind: str, sql: str, metadata: dict[str, Any] | None = None) -> None:
            cleaned = str(sql or "").strip()
            if not cleaned:
                return
            key = (source, kind, cleaned)
            if key in seen:
                return
            seen.add(key)
            sql_entries.append(
                {
                    "source": source,
                    "kind": kind,
                    "sql": cleaned,
                    "metadata": metadata or {},
                }
            )

        result = job.result if isinstance(job.result, dict) else {}
        diagnostics = result.get("diagnostics")
        if isinstance(diagnostics, dict):
            canonical = diagnostics.get("sql_canonical")
            executable = diagnostics.get("sql_executable")
            if isinstance(canonical, str):
                add_entry("diagnostics", "canonical", canonical)
            if isinstance(executable, str):
                add_entry("diagnostics", "executable", executable)

        tool_calls = result.get("tool_calls")
        if isinstance(tool_calls, list):
            for tool_call in tool_calls:
                if not isinstance(tool_call, dict):
                    continue
                tool_name = str(tool_call.get("tool_name") or "tool")
                tool_result = tool_call.get("result")
                if isinstance(tool_result, dict):
                    canonical = tool_result.get("sql_canonical")
                    executable = tool_result.get("sql_executable")
                    if isinstance(canonical, str):
                        add_entry(
                            f"tool_call:{tool_name}",
                            "canonical",
                            canonical,
                            {"duration_ms": tool_call.get("duration_ms")},
                        )
                    if isinstance(executable, str):
                        add_entry(
                            f"tool_call:{tool_name}",
                            "executable",
                            executable,
                            {"duration_ms": tool_call.get("duration_ms")},
                        )

        for event in list(job.job_events or []):
            details = event.details if isinstance(event.details, dict) else {}
            payload = details.get("details")
            if not isinstance(payload, dict):
                continue
            source = str(details.get("source") or "event")
            canonical = payload.get("sql_canonical")
            executable = payload.get("sql_executable")
            if isinstance(canonical, str):
                add_entry(source, "canonical", canonical, {"event_type": event.event_type})
            if isinstance(executable, str):
                add_entry(source, "executable", executable, {"event_type": event.event_type})

        return sql_entries
