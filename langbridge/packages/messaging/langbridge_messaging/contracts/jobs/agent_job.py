
import uuid
from ..base import BaseMessagePayload, register_payload
from langbridge.contracts.jobs.type import JobType

@register_payload("agent_job_request")
class AgentJobRequestMessage(BaseMessagePayload):
    """Payload for requesting an agent job."""
    job_id: uuid.UUID
    job_type: JobType
