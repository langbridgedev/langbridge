from typing import Any
import uuid

from langbridge.contracts.base import _Base
from langbridge.contracts.jobs.type import JobType


class CreateCopilotDashboardJobRequest(_Base):
    job_type: JobType = JobType.COPILOT_DASHBOARD
    organisation_id: uuid.UUID
    project_id: uuid.UUID | None = None
    user_id: uuid.UUID
    agent_definition_id: uuid.UUID
    semantic_model_id: uuid.UUID
    instructions: str
    dashboard_name: str | None = None
    current_dashboard: dict[str, Any] | None = None
    generate_previews: bool = True
    max_widgets: int = 6


class CopilotDashboardAssistRequest(_Base):
    project_id: uuid.UUID | None = None
    semantic_model_id: uuid.UUID
    instructions: str
    dashboard_name: str | None = None
    current_dashboard: dict[str, Any] | None = None
    generate_previews: bool = True
    max_widgets: int = 6


class CopilotDashboardJobResponse(_Base):
    job_id: uuid.UUID
    job_status: str
