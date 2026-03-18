import uuid
from typing import Any

from langbridge.contracts.jobs.type import JobType

from ..base import BaseMessagePayload, register_payload


@register_payload("semantic_query_request")
class SemanticQueryRequestMessage(BaseMessagePayload):
    """Payload for requesting worker-based semantic query execution."""

    job_id: uuid.UUID
    job_type: JobType
    job_request: dict[str, Any] | None = None
    semantic_model_yaml: str | None = None
    connector: dict[str, Any] | None = None
