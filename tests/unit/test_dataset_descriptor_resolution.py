from __future__ import annotations

import uuid

from langbridge.contracts.datasets import (
    DatasetSourceKind,
    DatasetStorageKind,
)
from langbridge.packages.common.langbridge_common.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
    dataset_supports_structured_federation,
    resolve_dataset_connector_kind,
    resolve_dataset_source_kind,
    resolve_dataset_storage_kind,
)


def test_shopify_parquet_dataset_resolves_to_structured_saas_dataset() -> None:
    file_config = {
        "format": "parquet",
        "connector_sync": {
            "connector_type": "shopify",
            "resource_name": "orders",
        },
    }
    connector_kind = resolve_dataset_connector_kind(
        explicit_connector_kind=None,
        connection_connector_type=None,
        file_config=file_config,
        storage_uri="file:///tmp/shopify_orders.parquet",
        legacy_dataset_type="FILE",
    )
    source_kind = resolve_dataset_source_kind(
        explicit_source_kind=None,
        legacy_dataset_type="FILE",
        connector_kind=connector_kind,
        file_config=file_config,
    )
    storage_kind = resolve_dataset_storage_kind(
        explicit_storage_kind=None,
        legacy_dataset_type="FILE",
        file_config=file_config,
        storage_uri="file:///tmp/shopify_orders.parquet",
    )
    relation_identity = build_dataset_relation_identity(
        dataset_id=uuid.uuid4(),
        connector_id=None,
        dataset_name="shopify_orders",
        catalog_name=None,
        schema_name="api_connector",
        table_name="shopify_orders",
        storage_uri="file:///tmp/shopify_orders.parquet",
        source_kind=source_kind,
        storage_kind=storage_kind,
    )
    capabilities = build_dataset_execution_capabilities(
        source_kind=source_kind,
        storage_kind=storage_kind,
    )

    assert connector_kind == "shopify"
    assert source_kind == DatasetSourceKind.SAAS
    assert storage_kind == DatasetStorageKind.PARQUET
    assert relation_identity.schema_name is None
    assert relation_identity.qualified_name == "shopify_orders"
    assert relation_identity.storage_kind == DatasetStorageKind.PARQUET
    assert capabilities.supports_sql_federation is True
    assert dataset_supports_structured_federation(
        source_kind=source_kind,
        storage_kind=storage_kind,
        capabilities=capabilities,
    )


def test_legacy_table_dataset_defaults_to_database_table_capabilities() -> None:
    source_kind = resolve_dataset_source_kind(
        explicit_source_kind=None,
        legacy_dataset_type="TABLE",
        connector_kind="postgres",
        file_config=None,
    )
    storage_kind = resolve_dataset_storage_kind(
        explicit_storage_kind=None,
        legacy_dataset_type="TABLE",
        file_config=None,
        storage_uri=None,
    )
    capabilities = build_dataset_execution_capabilities(
        source_kind=source_kind,
        storage_kind=storage_kind,
    )

    assert source_kind == DatasetSourceKind.DATABASE
    assert storage_kind == DatasetStorageKind.TABLE
    assert capabilities.supports_structured_scan is True
    assert capabilities.supports_sql_federation is True
