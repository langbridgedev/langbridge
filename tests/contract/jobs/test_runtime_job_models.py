
import uuid

import pytest
from pydantic import ValidationError

from langbridge.runtime.embeddings import (
    DEFAULT_OPENAI_EMBED_MODEL,
    EmbeddingProvider,
)
from langbridge.runtime.models import (
    CreateConnectorSyncJobRequest,
    CreateSemanticQueryJobRequest,
    CreateDatasetBulkCreateJobRequest,
    CreateSqlJobRequest,
    LLMConnectionSecret,
    LLMProvider,
    SqlQueryRequest,
    SqlQueryScope,
    SqlSelectedDataset,
)


def test_create_sql_job_request_accepts_camel_case_payload() -> None:
    workspace_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    request = CreateSqlJobRequest.model_validate(
        {
            "sqlJobId": str(uuid.uuid4()),
            "workspaceId": str(workspace_id),
            "userId": str(uuid.uuid4()),
            "executionMode": "federated",
            "query": "select * from orders",
            "enforcedLimit": 25,
            "enforcedTimeoutSeconds": 30,
            "allowFederation": True,
            "selectedDatasets": [str(dataset_id)],
        }
    )

    assert request.workspace_id == workspace_id
    assert request.workbench_mode.value == "dataset"
    assert request.selected_datasets == [dataset_id]


def test_sql_query_request_accepts_camel_case_scope_payload() -> None:
    request = SqlQueryRequest.model_validate(
        {
            "queryScope": "source",
            "query": "SELECT 1",
            "connectionName": "commerce_demo",
            "queryDialect": "postgres",
        }
    )

    assert request.query_scope == SqlQueryScope.source
    assert request.connection_name == "commerce_demo"
    assert request.query_dialect == "postgres"


def test_create_dataset_bulk_create_request_accepts_nested_camel_case_payload() -> None:
    request = CreateDatasetBulkCreateJobRequest.model_validate(
        {
            "workspaceId": str(uuid.uuid4()),
            "projectId": str(uuid.uuid4()),
            "userId": str(uuid.uuid4()),
            "connectionId": str(uuid.uuid4()),
            "selections": [
                {
                    "schema": "public",
                    "table": "orders",
                    "columns": [
                        {"name": "id", "dataType": "uuid"},
                        {"name": "created_at", "dataType": "timestamp"},
                    ],
                }
            ],
            "policyDefaults": {
                "maxPreviewRows": 100,
                "maxExportRows": 1000,
                "allowDml": False,
                "redactionRules": {"email": "mask"},
            },
        }
    )

    assert request.policy_defaults is not None
    assert request.policy_defaults.max_preview_rows == 100
    assert request.selections[0].columns[1].data_type == "timestamp"


def test_create_dataset_bulk_create_request_rejects_duplicate_columns() -> None:
    with pytest.raises(ValidationError):
        CreateDatasetBulkCreateJobRequest.model_validate(
            {
                "workspaceId": str(uuid.uuid4()),
                "userId": str(uuid.uuid4()),
                "connectionId": str(uuid.uuid4()),
                "selections": [
                    {
                        "schema": "public",
                        "table": "orders",
                        "columns": [
                            {"name": "id"},
                            {"name": "ID"},
                        ],
                    }
                ],
            }
        )


def test_create_connector_sync_job_request_accepts_camel_case_payload() -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    request = CreateConnectorSyncJobRequest.model_validate(
        {
            "workspaceId": str(workspace_id),
            "projectId": str(uuid.uuid4()),
            "userId": str(uuid.uuid4()),
            "connectionId": str(connection_id),
            "resourceNames": [" customers ", "subscriptions"],
            "syncMode": "incremental",
            "forceFullRefresh": False,
        }
    )

    assert request.workspace_id == workspace_id
    assert request.connection_id == connection_id
    assert request.resource_names == ["customers", "subscriptions"]
    assert request.sync_mode == "INCREMENTAL"


def test_sql_selected_dataset_accepts_stringified_enum_member_names() -> None:
    dataset = SqlSelectedDataset.model_validate(
        {
            "datasetId": str(uuid.uuid4()),
            "sourceKind": "DatasetSourceKind.DATABASE",
            "storageKind": "DatasetStorageKind.TABLE",
        }
    )

    assert dataset.source_kind == "database"
    assert dataset.storage_kind == "table"


def test_create_connector_sync_job_request_requires_resources() -> None:
    with pytest.raises(ValidationError):
        CreateConnectorSyncJobRequest.model_validate(
            {
                "workspaceId": str(uuid.uuid4()),
                "userId": str(uuid.uuid4()),
                "connectionId": str(uuid.uuid4()),
                "resourceNames": [],
            }
        )


def test_create_semantic_query_request_accepts_legacy_unified_camel_case_payload() -> None:
    source_model_id = uuid.uuid4()
    target_model_id = uuid.uuid4()
    request = CreateSemanticQueryJobRequest.model_validate(
        {
            "workspaceId": str(uuid.uuid4()),
            "actorId": str(uuid.uuid4()),
            "queryScope": "unified",
            "semanticModelIds": [
                str(source_model_id),
                str(target_model_id),
            ],
            "sourceModels": [
                {"id": str(source_model_id), "alias": "Sales"},
                {"id": str(target_model_id), "alias": "Support"},
            ],
            "relationships": [
                {
                    "sourceSemanticModelId": str(source_model_id),
                    "sourceField": "Sales.customer_id",
                    "targetSemanticModelId": str(target_model_id),
                    "targetField": "Support.customer_id",
                    "relationshipType": "left",
                }
            ],
            "metrics": {
                "gross_margin": {
                    "expression": "Sales.revenue - Sales.cost",
                }
            },
            "query": {"measures": ["Sales.revenue"]},
        }
    )

    assert request.query_scope == "semantic_graph"
    assert request.relationships is not None
    assert request.relationships[0].source_semantic_model_id == source_model_id
    assert request.relationships[0].relationship_type == "left"
    assert request.metrics is not None
    assert request.metrics["gross_margin"].expression == "Sales.revenue - Sales.cost"


def test_create_semantic_query_request_requires_semantic_model_for_standard_scope() -> None:
    with pytest.raises(ValidationError):
        CreateSemanticQueryJobRequest.model_validate(
            {
                "workspaceId": str(uuid.uuid4()),
                "actorId": str(uuid.uuid4()),
                "queryScope": "semantic_model",
                "query": {"measures": ["orders.total"]},
            }
        )


def test_embedding_provider_accepts_runtime_llm_connection_shape(monkeypatch) -> None:
    monkeypatch.setattr(EmbeddingProvider, "_build_client", lambda self: object())

    provider = EmbeddingProvider.from_llm_connection(
        LLMConnectionSecret(
            id=uuid.uuid4(),
            name="openai",
            provider=LLMProvider.OPENAI,
            model="gpt-4.1",
            api_key="secret",
            default=True,
            workspace_id=uuid.uuid4(),
        )
    )

    assert provider.provider.value == "openai"
    assert provider.embedding_model == DEFAULT_OPENAI_EMBED_MODEL
