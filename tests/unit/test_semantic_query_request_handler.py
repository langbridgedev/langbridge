from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from langbridge.apps.runtime_worker.handlers.query.semantic_query_request_handler import (
    SemanticQueryRequestHandler,
)
from langbridge.packages.runtime.models import ConnectorMetadata, SecretReference


class _FakeMessageBroker:
    async def publish(self, envelope, stream=None) -> None:  # pragma: no cover - not used here
        return None


def test_parse_job_payload_uses_runtime_semantic_request_model() -> None:
    source_model_id = uuid.uuid4()
    handler = SemanticQueryRequestHandler(message_broker=_FakeMessageBroker())
    job_record = SimpleNamespace(
        id=uuid.uuid4(),
        payload={
            "organisationId": str(uuid.uuid4()),
            "projectId": str(uuid.uuid4()),
            "userId": str(uuid.uuid4()),
            "queryScope": "unified",
            "semanticModelIds": [str(source_model_id)],
            "sourceModels": [{"id": str(source_model_id), "alias": "Sales"}],
            "relationships": [
                {
                    "sourceSemanticModelId": str(source_model_id),
                    "sourceField": "Sales.customer_id",
                    "targetSemanticModelId": str(source_model_id),
                    "targetField": "Sales.customer_id",
                }
            ],
            "query": {"measures": ["Sales.revenue"]},
        },
    )

    request = handler._parse_job_payload(job_record)

    assert request.query_scope == "unified"
    assert request.semantic_model_ids == [source_model_id]
    assert request.relationships is not None
    assert request.relationships[0].source_field == "Sales.customer_id"


def test_resolve_connector_config_accepts_runtime_connector_metadata(monkeypatch) -> None:
    monkeypatch.setenv("SEMANTIC_QUERY_DB_PASSWORD", "secret")
    handler = SemanticQueryRequestHandler(message_broker=_FakeMessageBroker())
    connector_id = uuid.uuid4()
    connector = ConnectorMetadata(
        id=connector_id,
        name="warehouse",
        connector_type="POSTGRES",
        config={"config": {"host": "db.internal"}},
        connection_metadata={"database": "analytics", "extra": {"sslmode": "require"}},
        secret_references={
            "password": SecretReference(
                provider_type="env",
                identifier="SEMANTIC_QUERY_DB_PASSWORD",
            )
        },
    )

    resolved = handler._resolve_connector_config(connector)

    assert resolved["config"]["host"] == "db.internal"
    assert resolved["config"]["database"] == "analytics"
    assert resolved["config"]["sslmode"] == "require"
    assert resolved["config"]["password"] == "secret"

