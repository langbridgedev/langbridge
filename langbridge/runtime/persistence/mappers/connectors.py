from __future__ import annotations

import json
from typing import Any

from langbridge.runtime.models import (
    ConnectionMetadata,
    ConnectionPolicy,
    ConnectorMetadata,
    ConnectorSyncState,
    SecretReference,
)
from langbridge.runtime.persistence.db.connector import Connector
from langbridge.runtime.persistence.db.connector_sync import ConnectorSyncStateRecord
from langbridge.runtime.persistence.mappers.common import as_dict


def to_secret_reference(value: Any) -> SecretReference:
    if isinstance(value, SecretReference):
        return value
    return SecretReference.model_validate(value)


def to_connection_metadata(value: Any | None) -> ConnectionMetadata | None:
    if value is None:
        return None
    if isinstance(value, ConnectionMetadata):
        return value
    payload = as_dict(value)
    if not payload:
        return None
    return ConnectionMetadata.model_validate(payload)


def to_connection_policy(value: Any | None) -> ConnectionPolicy | None:
    if value is None:
        return None
    if isinstance(value, ConnectionPolicy):
        return value
    payload = as_dict(value)
    if not payload:
        return None
    return ConnectionPolicy.model_validate(payload)


def from_connector_record(value: Any | None) -> ConnectorMetadata | None:
    if value is None:
        return None
    if isinstance(value, ConnectorMetadata):
        return value
    config = getattr(value, "config", None)
    if config is None:
        config = as_dict(getattr(value, "config_json", None))
    secret_refs_raw = getattr(value, "secret_references", None)
    if secret_refs_raw is None:
        secret_refs_raw = getattr(value, "secret_references_json", None) or {}
    secret_references = {
        str(key): to_secret_reference(item)
        for key, item in dict(secret_refs_raw or {}).items()
    }
    return ConnectorMetadata(
        id=getattr(value, "id"),
        name=str(getattr(value, "name")),
        description=getattr(value, "description", None),
        version=getattr(value, "version", None),
        label=getattr(value, "label", None) or getattr(value, "name", None),
        icon=getattr(value, "icon", None),
        connector_type=getattr(value, "connector_type", None),
        workspace_id=getattr(value, "workspace_id", None),
        config=config or None,
        connection_metadata=to_connection_metadata(
            getattr(value, "connection_metadata", None)
            or getattr(value, "connection_metadata_json", None)
        ),
        secret_references=secret_references,
        connection_policy=to_connection_policy(
            getattr(value, "connection_policy", None)
            or getattr(value, "access_policy_json", None)
        ),
        is_managed=bool(getattr(value, "is_managed", False)),
    )


def to_connector_record(value: ConnectorMetadata | Connector) -> Connector:
    if isinstance(value, Connector):
        return value
    return Connector(
        id=value.id,
        workspace_id=value.workspace_id,
        name=value.name,
        description=value.description,
        connector_type=str(value.connector_type or ""),
        type=str(value.connector_type or "connector"),
        config_json=json.dumps(value.config or {}),
        connection_metadata_json=(
            None
            if value.connection_metadata is None
            else value.connection_metadata.model_dump(exclude_none=True, by_alias=True)
        ),
        secret_references_json={
            str(key): item.model_dump(exclude_none=True)
            for key, item in dict(value.secret_references or {}).items()
        },
        access_policy_json=(
            None
            if value.connection_policy is None
            else value.connection_policy.model_dump(exclude_none=True)
        ),
        is_managed=value.is_managed,
    )


def from_connector_sync_state_record(value: Any | None) -> ConnectorSyncState | None:
    if value is None:
        return None
    if isinstance(value, ConnectorSyncState):
        return value
    dataset_ids = getattr(value, "dataset_ids", None)
    if dataset_ids is None:
        dataset_ids = getattr(value, "dataset_ids_json", None) or []
    return ConnectorSyncState(
        id=getattr(value, "id"),
        workspace_id=getattr(value, "workspace_id"),
        connection_id=getattr(value, "connection_id"),
        connector_type=str(getattr(value, "connector_type")),
        resource_name=str(getattr(value, "resource_name")),
        sync_mode=str(getattr(value, "sync_mode", "INCREMENTAL")),
        last_cursor=getattr(value, "last_cursor", None),
        last_sync_at=getattr(value, "last_sync_at", None),
        state=dict(
            getattr(value, "state", None) or getattr(value, "state_json", None) or {}
        ),
        status=str(getattr(value, "status", "never_synced")),
        error_message=getattr(value, "error_message", None),
        records_synced=int(getattr(value, "records_synced", 0) or 0),
        bytes_synced=getattr(value, "bytes_synced", None),
        dataset_ids=list(dataset_ids),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_connector_sync_state_record(
    value: ConnectorSyncState | ConnectorSyncStateRecord,
) -> ConnectorSyncStateRecord:
    if isinstance(value, ConnectorSyncStateRecord):
        return value
    return ConnectorSyncStateRecord(
        id=value.id,
        workspace_id=value.workspace_id,
        connection_id=value.connection_id,
        connector_type=value.connector_type,
        resource_name=value.resource_name,
        sync_mode=value.sync_mode,
        last_cursor=value.last_cursor,
        last_sync_at=value.last_sync_at,
        state_json=value.state_json,
        dataset_ids_json=[str(item) for item in value.dataset_ids],
        status=value.status,
        error_message=value.error_message,
        records_synced=value.records_synced,
        bytes_synced=value.bytes_synced,
        created_at=value.created_at,
        updated_at=value.updated_at,
    )


__all__ = [
    "from_connector_record",
    "from_connector_sync_state_record",
    "to_connection_metadata",
    "to_connection_policy",
    "to_connector_record",
    "to_connector_sync_state_record",
    "to_secret_reference",
]
