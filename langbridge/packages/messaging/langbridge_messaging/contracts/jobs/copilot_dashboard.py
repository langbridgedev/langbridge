import uuid

from langbridge.contracts.jobs.type import JobType

from ..base import BaseMessagePayload, register_payload


@register_payload("copilot_dashboard_request")
class CopilotDashboardRequestMessage(BaseMessagePayload):
    """Payload for requesting worker-based BI copilot dashboard generation."""

    job_id: uuid.UUID
    job_type: JobType
