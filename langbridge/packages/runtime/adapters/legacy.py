from __future__ import annotations

import json
from typing import Any

from langbridge.packages.common.langbridge_common.db.agent import (
    AgentDefinition,
    LLMConnection,
)
from langbridge.packages.common.langbridge_common.db.connector_sync import (
    ConnectorSyncStateRecord,
)
from langbridge.packages.common.langbridge_common.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)
from langbridge.packages.common.langbridge_common.db.lineage import LineageEdgeRecord
from langbridge.packages.common.langbridge_common.db.sql import (
    SqlJobRecord,
    SqlJobResultArtifactRecord,
)
from langbridge.packages.common.langbridge_common.db.threads import (
    ConversationMemoryItem,
    MemoryCategory,
    Role,
    Thread,
    ThreadMessage,
    ThreadState,
)
from langbridge.packages.runtime.models import (
    ConnectionMetadata,
    ConnectionPolicy,
    ConnectorMetadata,
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    LLMConnectionSecret,
    LineageEdge,
    RuntimeAgentDefinition,
    RuntimeConversationMemoryCategory,
    RuntimeConversationMemoryItem,
    RuntimeMessageRole,
    RuntimeThread,
    RuntimeThreadMessage,
    RuntimeThreadState,
    SecretReference,
    SemanticModelMetadata,
    SqlJob,
    SqlJobResultArtifact,
)


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(exclude_none=True)
        return dumped if isinstance(dumped, dict) else {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _resolve_org_id(value: Any) -> Any:
    if getattr(value, "organization_id", None) is not None:
        return value.organization_id
    organizations = getattr(value, "organizations", None) or []
    if organizations:
        return getattr(organizations[0], "id", None)
    return None


def _resolve_project_id(value: Any) -> Any:
    if getattr(value, "project_id", None) is not None:
        return value.project_id
    projects = getattr(value, "projects", None) or []
    if projects:
        return getattr(projects[0], "id", None)
    return None


def to_runtime_secret_reference(value: Any) -> SecretReference:
    if isinstance(value, SecretReference):
        return value
    return SecretReference.model_validate(value)


def to_runtime_agent_definition(value: Any | None) -> RuntimeAgentDefinition | None:
    if value is None:
        return None
    if isinstance(value, RuntimeAgentDefinition):
        return value
    return RuntimeAgentDefinition(
        id=getattr(value, "id"),
        name=str(getattr(value, "name")),
        description=getattr(value, "description", None),
        llm_connection_id=getattr(value, "llm_connection_id"),
        definition=dict(getattr(value, "definition", None) or {}),
        is_active=bool(getattr(value, "is_active", True)),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_runtime_llm_connection(value: Any | None) -> LLMConnectionSecret | None:
    if value is None:
        return None
    if isinstance(value, LLMConnectionSecret):
        return value
    return LLMConnectionSecret(
        id=getattr(value, "id"),
        name=str(getattr(value, "name")),
        provider=str(getattr(value, "provider")),
        model=str(getattr(value, "model")),
        configuration=dict(getattr(value, "configuration", None) or {}),
        api_key=str(getattr(value, "api_key")),
        description=getattr(value, "description", None),
        is_active=bool(getattr(value, "is_active", True)),
        organization_id=_resolve_org_id(value),
        project_id=_resolve_project_id(value),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_runtime_thread(value: Any | None) -> RuntimeThread | None:
    if value is None:
        return None
    if isinstance(value, RuntimeThread):
        return value
    state = getattr(value, "state", RuntimeThreadState.awaiting_user_input)
    return RuntimeThread(
        id=getattr(value, "id"),
        organization_id=getattr(value, "organization_id"),
        project_id=getattr(value, "project_id"),
        title=getattr(value, "title", None),
        state=str(getattr(state, "value", state)),
        metadata=dict(getattr(value, "metadata", None) or getattr(value, "metadata_json", None) or {}),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
        created_by=getattr(value, "created_by"),
        last_message_id=getattr(value, "last_message_id", None),
    )


def to_runtime_thread_message(value: Any | None) -> RuntimeThreadMessage | None:
    if value is None:
        return None
    if isinstance(value, RuntimeThreadMessage):
        return value
    role = getattr(value, "role", RuntimeMessageRole.user)
    return RuntimeThreadMessage(
        id=getattr(value, "id"),
        thread_id=getattr(value, "thread_id"),
        parent_message_id=getattr(value, "parent_message_id", None),
        role=str(getattr(role, "value", role)),
        content=dict(getattr(value, "content", None) or {}),
        model_snapshot=(
            getattr(value, "model_snapshot", None)
            or getattr(value, "model_snapshot_json", None)
        ),
        token_usage=(
            getattr(value, "token_usage", None)
            or getattr(value, "token_usage_json", None)
        ),
        error=getattr(value, "error", None),
        created_at=getattr(value, "created_at", None),
    )


def to_runtime_conversation_memory_item(
    value: Any | None,
) -> RuntimeConversationMemoryItem | None:
    if value is None:
        return None
    if isinstance(value, RuntimeConversationMemoryItem):
        return value
    category = getattr(value, "category", RuntimeConversationMemoryCategory.fact)
    return RuntimeConversationMemoryItem(
        id=getattr(value, "id"),
        thread_id=getattr(value, "thread_id"),
        user_id=getattr(value, "user_id", None),
        category=str(getattr(category, "value", category)),
        content=str(getattr(value, "content", "") or ""),
        metadata=dict(getattr(value, "metadata", None) or getattr(value, "metadata_json", None) or {}),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
        last_accessed_at=getattr(value, "last_accessed_at", None),
    )


def to_runtime_connection_metadata(value: Any | None) -> ConnectionMetadata | None:
    if value is None:
        return None
    if isinstance(value, ConnectionMetadata):
        return value
    payload = _as_dict(value)
    if not payload:
        return None
    return ConnectionMetadata.model_validate(payload)


def to_runtime_connection_policy(value: Any | None) -> ConnectionPolicy | None:
    if value is None:
        return None
    if isinstance(value, ConnectionPolicy):
        return value
    payload = _as_dict(value)
    if not payload:
        return None
    return ConnectionPolicy.model_validate(payload)


def to_runtime_connector(value: Any | None) -> ConnectorMetadata | None:
    if value is None:
        return None
    if isinstance(value, ConnectorMetadata):
        return value
    config = getattr(value, "config", None)
    if config is None:
        config = _as_dict(getattr(value, "config_json", None))
    secret_refs_raw = getattr(value, "secret_references", None)
    if secret_refs_raw is None:
        secret_refs_raw = getattr(value, "secret_references_json", None) or {}
    secret_references = {
        str(key): to_runtime_secret_reference(item)
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
        organization_id=_resolve_org_id(value),
        project_id=_resolve_project_id(value),
        config=config or None,
        connection_metadata=to_runtime_connection_metadata(
            getattr(value, "connection_metadata", None)
            or getattr(value, "connection_metadata_json", None)
        ),
        secret_references=secret_references,
        connection_policy=to_runtime_connection_policy(
            getattr(value, "connection_policy", None)
            or getattr(value, "access_policy_json", None)
        ),
        is_managed=bool(getattr(value, "is_managed", False)),
    )


def to_runtime_dataset_column(value: Any) -> DatasetColumnMetadata:
    if isinstance(value, DatasetColumnMetadata):
        return value
    return DatasetColumnMetadata(
        id=getattr(value, "id"),
        dataset_id=getattr(value, "dataset_id"),
        workspace_id=getattr(value, "workspace_id", None),
        name=str(getattr(value, "name")),
        data_type=str(getattr(value, "data_type")),
        nullable=bool(getattr(value, "nullable", True)),
        description=getattr(value, "description", None),
        is_allowed=bool(getattr(value, "is_allowed", True)),
        is_computed=bool(getattr(value, "is_computed", False)),
        expression=getattr(value, "expression", None),
        ordinal_position=int(getattr(value, "ordinal_position", 0) or 0),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_runtime_dataset_policy(value: Any | None) -> DatasetPolicyMetadata | None:
    if value is None:
        return None
    if isinstance(value, DatasetPolicyMetadata):
        return value
    return DatasetPolicyMetadata(
        id=getattr(value, "id", None),
        dataset_id=getattr(value, "dataset_id", None),
        workspace_id=getattr(value, "workspace_id", None),
        max_rows_preview=int(getattr(value, "max_rows_preview", 1000) or 1000),
        max_export_rows=int(getattr(value, "max_export_rows", 10000) or 10000),
        redaction_rules=dict(
            getattr(value, "redaction_rules", None)
            or getattr(value, "redaction_rules_json", None)
            or {}
        ),
        row_filters=list(
            getattr(value, "row_filters", None)
            or getattr(value, "row_filters_json", None)
            or []
        ),
        allow_dml=bool(getattr(value, "allow_dml", False)),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_runtime_dataset(value: Any | None) -> DatasetMetadata | None:
    if value is None:
        return None
    if isinstance(value, DatasetMetadata):
        return value
    columns_raw = getattr(value, "columns", None) or []
    return DatasetMetadata(
        id=getattr(value, "id"),
        workspace_id=getattr(value, "workspace_id"),
        project_id=getattr(value, "project_id", None),
        connection_id=getattr(value, "connection_id", None),
        owner_id=getattr(value, "owner_id", None) or getattr(value, "created_by", None),
        created_by=getattr(value, "created_by", None),
        updated_by=getattr(value, "updated_by", None),
        name=str(getattr(value, "name")),
        sql_alias=str(getattr(value, "sql_alias")),
        description=getattr(value, "description", None),
        tags=list(getattr(value, "tags", None) or getattr(value, "tags_json", None) or []),
        dataset_type=str(getattr(value, "dataset_type")),
        source_kind=getattr(value, "source_kind", None),
        connector_kind=getattr(value, "connector_kind", None),
        storage_kind=getattr(value, "storage_kind", None),
        dialect=getattr(value, "dialect", None),
        catalog_name=getattr(value, "catalog_name", None),
        schema_name=getattr(value, "schema_name", None),
        table_name=getattr(value, "table_name", None),
        storage_uri=getattr(value, "storage_uri", None),
        sql_text=getattr(value, "sql_text", None),
        relation_identity=(
            getattr(value, "relation_identity", None)
            or getattr(value, "relation_identity_json", None)
        ),
        execution_capabilities=(
            getattr(value, "execution_capabilities", None)
            or getattr(value, "execution_capabilities_json", None)
        ),
        referenced_dataset_ids=list(
            getattr(value, "referenced_dataset_ids", None)
            or getattr(value, "referenced_dataset_ids_json", None)
            or []
        ),
        federated_plan=(
            getattr(value, "federated_plan", None)
            or getattr(value, "federated_plan_json", None)
        ),
        file_config=getattr(value, "file_config", None) or getattr(value, "file_config_json", None),
        status=str(getattr(value, "status", "published") or "published"),
        revision_id=getattr(value, "revision_id", None),
        row_count_estimate=getattr(value, "row_count_estimate", None),
        bytes_estimate=getattr(value, "bytes_estimate", None),
        last_profiled_at=getattr(value, "last_profiled_at", None),
        columns=[to_runtime_dataset_column(column) for column in columns_raw],
        policy=to_runtime_dataset_policy(getattr(value, "policy", None)),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_runtime_semantic_model(value: Any | None) -> SemanticModelMetadata | None:
    if value is None:
        return None
    if isinstance(value, SemanticModelMetadata):
        return value
    content_json = getattr(value, "content_json", None)
    if isinstance(content_json, str):
        try:
            parsed = json.loads(content_json)
        except json.JSONDecodeError:
            parsed = content_json
        content_json = parsed
    return SemanticModelMetadata(
        id=getattr(value, "id"),
        connector_id=getattr(value, "connector_id", None),
        organization_id=getattr(value, "organization_id"),
        project_id=getattr(value, "project_id", None),
        name=str(getattr(value, "name")),
        description=getattr(value, "description", None),
        content_yaml=str(getattr(value, "content_yaml")),
        content_json=content_json,
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_runtime_sync_state(value: Any | None) -> ConnectorSyncState | None:
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
        state=dict(getattr(value, "state", None) or getattr(value, "state_json", None) or {}),
        status=str(getattr(value, "status", "never_synced")),
        error_message=getattr(value, "error_message", None),
        records_synced=int(getattr(value, "records_synced", 0) or 0),
        bytes_synced=getattr(value, "bytes_synced", None),
        dataset_ids=list(dataset_ids),
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_runtime_sql_job_result_artifact(value: Any | None) -> SqlJobResultArtifact | None:
    if value is None:
        return None
    if isinstance(value, SqlJobResultArtifact):
        return value
    return SqlJobResultArtifact(
        id=getattr(value, "id"),
        sql_job_id=getattr(value, "sql_job_id"),
        workspace_id=getattr(value, "workspace_id"),
        created_by=getattr(value, "created_by"),
        format=str(getattr(value, "format")),
        mime_type=str(getattr(value, "mime_type")),
        row_count=int(getattr(value, "row_count", 0) or 0),
        byte_size=getattr(value, "byte_size", None),
        storage_backend=str(getattr(value, "storage_backend")),
        storage_reference=str(getattr(value, "storage_reference")),
        payload=getattr(value, "payload", None) or getattr(value, "payload_json", None),
        created_at=getattr(value, "created_at", None),
    )


def to_runtime_sql_job(value: Any | None) -> SqlJob | None:
    if value is None:
        return None
    if isinstance(value, SqlJob):
        return value
    return SqlJob(
        id=getattr(value, "id"),
        workspace_id=getattr(value, "workspace_id"),
        project_id=getattr(value, "project_id", None),
        user_id=getattr(value, "user_id"),
        connection_id=getattr(value, "connection_id", None),
        workbench_mode=str(getattr(value, "workbench_mode")),
        selected_datasets_json=list(getattr(value, "selected_datasets_json", None) or []),
        execution_mode=str(getattr(value, "execution_mode", "single")),
        status=str(getattr(value, "status", "queued")),
        query_text=str(getattr(value, "query_text")),
        query_hash=str(getattr(value, "query_hash")),
        query_params_json=dict(getattr(value, "query_params_json", None) or {}),
        requested_limit=getattr(value, "requested_limit", None),
        enforced_limit=int(getattr(value, "enforced_limit", 1000) or 1000),
        requested_timeout_seconds=getattr(value, "requested_timeout_seconds", None),
        enforced_timeout_seconds=int(getattr(value, "enforced_timeout_seconds", 30) or 30),
        is_explain=bool(getattr(value, "is_explain", False)),
        is_federated=bool(getattr(value, "is_federated", False)),
        correlation_id=getattr(value, "correlation_id", None),
        policy_snapshot_json=dict(getattr(value, "policy_snapshot_json", None) or {}),
        result_columns_json=(
            None
            if getattr(value, "result_columns_json", None) is None
            else list(getattr(value, "result_columns_json"))
        ),
        result_rows_json=(
            None
            if getattr(value, "result_rows_json", None) is None
            else list(getattr(value, "result_rows_json"))
        ),
        row_count_preview=int(getattr(value, "row_count_preview", 0) or 0),
        total_rows_estimate=getattr(value, "total_rows_estimate", None),
        bytes_scanned=getattr(value, "bytes_scanned", None),
        duration_ms=getattr(value, "duration_ms", None),
        result_cursor=getattr(value, "result_cursor", None),
        redaction_applied=bool(getattr(value, "redaction_applied", False)),
        error_json=getattr(value, "error_json", None),
        warning_json=getattr(value, "warning_json", None),
        stats_json=getattr(value, "stats_json", None),
        created_at=getattr(value, "created_at", None),
        started_at=getattr(value, "started_at", None),
        finished_at=getattr(value, "finished_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_legacy_dataset(value: DatasetMetadata | DatasetRecord) -> DatasetRecord:
    if isinstance(value, DatasetRecord):
        return value
    return DatasetRecord(
        id=value.id,
        workspace_id=value.workspace_id,
        project_id=value.project_id,
        connection_id=value.connection_id,
        created_by=value.created_by,
        updated_by=value.updated_by,
        name=value.name,
        sql_alias=value.sql_alias,
        description=value.description,
        tags_json=list(value.tags),
        dataset_type=value.dataset_type,
        source_kind=value.source_kind,
        connector_kind=value.connector_kind,
        storage_kind=value.storage_kind,
        dialect=value.dialect,
        catalog_name=value.catalog_name,
        schema_name=value.schema_name,
        table_name=value.table_name,
        storage_uri=value.storage_uri,
        sql_text=value.sql_text,
        relation_identity_json=value.relation_identity_json,
        execution_capabilities_json=value.execution_capabilities_json,
        referenced_dataset_ids_json=[str(item) for item in value.referenced_dataset_ids_json],
        federated_plan_json=value.federated_plan_json,
        file_config_json=value.file_config_json,
        status=value.status,
        revision_id=value.revision_id,
        row_count_estimate=value.row_count_estimate,
        bytes_estimate=value.bytes_estimate,
        last_profiled_at=value.last_profiled_at,
        created_at=value.created_at,
        updated_at=value.updated_at,
    )


def to_legacy_dataset_column(value: DatasetColumnMetadata | DatasetColumnRecord) -> DatasetColumnRecord:
    if isinstance(value, DatasetColumnRecord):
        return value
    return DatasetColumnRecord(
        id=value.id,
        dataset_id=value.dataset_id,
        workspace_id=getattr(value, "workspace_id"),
        name=value.name,
        data_type=value.data_type,
        nullable=value.nullable,
        ordinal_position=value.ordinal_position,
        description=value.description,
        is_allowed=value.is_allowed,
        is_computed=value.is_computed,
        expression=value.expression,
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_legacy_dataset_policy(value: DatasetPolicyMetadata | DatasetPolicyRecord) -> DatasetPolicyRecord:
    if isinstance(value, DatasetPolicyRecord):
        return value
    return DatasetPolicyRecord(
        id=value.id,
        dataset_id=value.dataset_id,
        workspace_id=value.workspace_id,
        max_rows_preview=value.max_rows_preview,
        max_export_rows=value.max_export_rows,
        redaction_rules_json=value.redaction_rules_json,
        row_filters_json=value.row_filters_json,
        allow_dml=value.allow_dml,
        created_at=getattr(value, "created_at", None),
        updated_at=getattr(value, "updated_at", None),
    )


def to_legacy_dataset_revision(value: DatasetRevision | DatasetRevisionRecord) -> DatasetRevisionRecord:
    if isinstance(value, DatasetRevisionRecord):
        return value
    return DatasetRevisionRecord(
        id=value.id,
        dataset_id=value.dataset_id,
        workspace_id=value.workspace_id,
        revision_number=value.revision_number,
        revision_hash=value.revision_hash,
        change_summary=value.change_summary,
        definition_json=value.definition_json,
        schema_json=value.schema_json,
        policy_json=value.policy_json,
        source_bindings_json=value.source_bindings_json,
        execution_characteristics_json=value.execution_characteristics_json,
        status=value.status,
        snapshot_json=value.snapshot_json,
        note=value.note,
        created_by=value.created_by,
        created_at=value.created_at,
    )


def to_legacy_lineage_edge(value: LineageEdge | LineageEdgeRecord) -> LineageEdgeRecord:
    if isinstance(value, LineageEdgeRecord):
        return value
    return LineageEdgeRecord(
        id=value.id,
        workspace_id=value.workspace_id,
        source_type=value.source_type,
        source_id=value.source_id,
        target_type=value.target_type,
        target_id=value.target_id,
        edge_type=value.edge_type,
        metadata_json=value.metadata_json,
        created_at=value.created_at,
    )


def to_legacy_connector_sync_state(
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
        status=value.status,
        error_message=value.error_message,
        records_synced=value.records_synced,
        bytes_synced=value.bytes_synced,
        created_at=value.created_at,
        updated_at=value.updated_at,
    )


def to_legacy_agent_definition(
    value: RuntimeAgentDefinition | AgentDefinition,
) -> AgentDefinition:
    if isinstance(value, AgentDefinition):
        return value
    return AgentDefinition(
        id=value.id,
        name=value.name,
        description=value.description,
        llm_connection_id=value.llm_connection_id,
        definition=dict(value.definition or {}),
        is_active=value.is_active,
        created_at=value.created_at,
        updated_at=value.updated_at,
    )


def to_legacy_llm_connection(
    value: LLMConnectionSecret | LLMConnection,
) -> LLMConnection:
    if isinstance(value, LLMConnection):
        return value
    return LLMConnection(
        id=value.id,
        name=value.name,
        description=value.description,
        provider=str(getattr(value.provider, "value", value.provider)),
        api_key=value.api_key,
        model=value.model,
        configuration=dict(value.configuration or {}),
        is_active=value.is_active,
        created_at=value.created_at,
        updated_at=value.updated_at,
    )


def to_legacy_thread(value: RuntimeThread | Thread) -> Thread:
    if isinstance(value, Thread):
        return value
    return Thread(
        id=value.id,
        organization_id=value.organization_id,
        project_id=value.project_id,
        title=value.title,
        state=ThreadState(str(getattr(value.state, "value", value.state))),
        metadata_json=value.metadata_json,
        created_at=value.created_at,
        updated_at=value.updated_at,
        created_by=value.created_by,
        last_message_id=value.last_message_id,
    )


def to_legacy_thread_message(
    value: RuntimeThreadMessage | ThreadMessage,
) -> ThreadMessage:
    if isinstance(value, ThreadMessage):
        return value
    return ThreadMessage(
        id=value.id,
        thread_id=value.thread_id,
        parent_message_id=value.parent_message_id,
        role=Role(str(getattr(value.role, "value", value.role))),
        content=dict(value.content or {}),
        model_snapshot=value.model_snapshot_json,
        token_usage=value.token_usage_json,
        error=value.error,
        created_at=value.created_at,
    )


def to_legacy_conversation_memory_item(
    value: RuntimeConversationMemoryItem | ConversationMemoryItem,
) -> ConversationMemoryItem:
    if isinstance(value, ConversationMemoryItem):
        return value
    return ConversationMemoryItem(
        id=value.id,
        thread_id=value.thread_id,
        user_id=value.user_id,
        category=MemoryCategory(str(getattr(value.category, "value", value.category))),
        content=value.content,
        metadata_json=value.metadata_json,
        created_at=value.created_at,
        updated_at=value.updated_at,
        last_accessed_at=value.last_accessed_at,
    )


def to_legacy_sql_job(value: SqlJob | SqlJobRecord) -> SqlJobRecord:
    if isinstance(value, SqlJobRecord):
        return value
    return SqlJobRecord(
        id=value.id,
        workspace_id=value.workspace_id,
        project_id=value.project_id,
        user_id=value.user_id,
        connection_id=value.connection_id,
        workbench_mode=value.workbench_mode,
        selected_datasets_json=list(value.selected_datasets_json or []),
        execution_mode=value.execution_mode,
        status=value.status,
        query_text=value.query_text,
        query_hash=value.query_hash,
        query_params_json=dict(value.query_params_json or {}),
        requested_limit=value.requested_limit,
        enforced_limit=value.enforced_limit,
        requested_timeout_seconds=value.requested_timeout_seconds,
        enforced_timeout_seconds=value.enforced_timeout_seconds,
        is_explain=value.is_explain,
        is_federated=value.is_federated,
        correlation_id=value.correlation_id,
        policy_snapshot_json=dict(value.policy_snapshot_json or {}),
        result_columns_json=(
            None if value.result_columns_json is None else list(value.result_columns_json)
        ),
        result_rows_json=(
            None if value.result_rows_json is None else list(value.result_rows_json)
        ),
        row_count_preview=value.row_count_preview,
        total_rows_estimate=value.total_rows_estimate,
        bytes_scanned=value.bytes_scanned,
        duration_ms=value.duration_ms,
        result_cursor=value.result_cursor,
        redaction_applied=value.redaction_applied,
        error_json=value.error_json,
        warning_json=value.warning_json,
        stats_json=value.stats_json,
        created_at=value.created_at,
        started_at=value.started_at,
        finished_at=value.finished_at,
        updated_at=value.updated_at,
    )


def to_legacy_sql_job_result_artifact(
    value: SqlJobResultArtifact | SqlJobResultArtifactRecord,
) -> SqlJobResultArtifactRecord:
    if isinstance(value, SqlJobResultArtifactRecord):
        return value
    return SqlJobResultArtifactRecord(
        id=value.id,
        sql_job_id=value.sql_job_id,
        workspace_id=value.workspace_id,
        created_by=value.created_by,
        format=value.format,
        mime_type=value.mime_type,
        row_count=value.row_count,
        byte_size=value.byte_size,
        storage_backend=value.storage_backend,
        storage_reference=value.storage_reference,
        payload_json=value.payload_json,
        created_at=value.created_at,
    )
