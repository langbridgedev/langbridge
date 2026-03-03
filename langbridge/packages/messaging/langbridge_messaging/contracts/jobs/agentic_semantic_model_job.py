import uuid
from typing import Any

from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType

from ..base import BaseMessagePayload, register_payload


@register_payload("agentic_semantic_model_job_request")
class AgenticSemanticModelJobRequestMessage(BaseMessagePayload):
    """Payload for requesting worker-based agentic semantic model generation."""

    job_id: uuid.UUID
    job_type: JobType
    job_request: dict[str, Any]
