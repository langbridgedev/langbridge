from __future__ import annotations

import uuid

import pytest

from langbridge.apps.runtime_worker.handlers.jobs.connector_sync_job_request_handler import (
    ConnectorSyncJobRequestHandler,
)
from langbridge.contracts.jobs.type import JobType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.connector_job import (
    ConnectorSyncJobRequestMessage,
)
from langbridge.packages.runtime.errors import BusinessValidationError


def test_parse_request_accepts_runtime_connector_sync_shape() -> None:
    message = ConnectorSyncJobRequestMessage(
        job_id=uuid.uuid4(),
        job_type=JobType.CONNECTOR_SYNC,
        job_request={
            "workspaceId": str(uuid.uuid4()),
            "projectId": str(uuid.uuid4()),
            "userId": str(uuid.uuid4()),
            "connectionId": str(uuid.uuid4()),
            "resourceNames": ["customers"],
            "syncMode": "incremental",
        },
    )

    request = ConnectorSyncJobRequestHandler._parse_request(message)

    assert request.resource_names == ["customers"]
    assert request.sync_mode == "INCREMENTAL"


def test_parse_request_rejects_invalid_connector_sync_payload() -> None:
    message = ConnectorSyncJobRequestMessage(
        job_id=uuid.uuid4(),
        job_type=JobType.CONNECTOR_SYNC,
        job_request={
            "workspaceId": str(uuid.uuid4()),
            "userId": str(uuid.uuid4()),
            "connectionId": str(uuid.uuid4()),
            "resourceNames": [],
        },
    )

    with pytest.raises(BusinessValidationError, match="Invalid connector sync request payload"):
        ConnectorSyncJobRequestHandler._parse_request(message)
