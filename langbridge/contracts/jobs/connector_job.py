from __future__ import annotations

import uuid
from typing import Literal

from pydantic import Field, model_validator

from langbridge.contracts.base import _Base
from langbridge.contracts.connectors import ConnectorSyncMode

from langbridge.contracts.jobs.type import JobType


class CreateConnectorSyncJobRequest(_Base):
    job_type: JobType = JobType.CONNECTOR_SYNC
    workspace_id: uuid.UUID
    actor_id: uuid.UUID
    connection_id: uuid.UUID
    resource_names: list[str] = Field(default_factory=list)
    sync_mode: ConnectorSyncMode = ConnectorSyncMode.INCREMENTAL
    force_full_refresh: bool = False
    correlation_id: str | None = None
    operation: Literal["connector_sync"] = "connector_sync"

    @model_validator(mode="after")
    def _validate_resource_names(self) -> "CreateConnectorSyncJobRequest":
        normalized = [str(value or "").strip() for value in self.resource_names if str(value or "").strip()]
        if not normalized:
            raise ValueError("Connector sync requires at least one resource.")
        self.resource_names = normalized
        return self
