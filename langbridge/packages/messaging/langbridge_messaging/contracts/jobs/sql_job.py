import uuid
from typing import Any

from langbridge.contracts.jobs.type import JobType

from ..base import BaseMessagePayload, register_payload


@register_payload("sql_job_request")
class SqlJobRequestMessage(BaseMessagePayload):
    """Payload for requesting worker-based SQL execution."""

    sql_job_id: uuid.UUID
    job_type: JobType
    job_request: dict[str, Any]
