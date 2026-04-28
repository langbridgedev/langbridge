
import uuid

import pytest

from langbridge.runtime.models.metadata import (
    DatasetMaterializationMode,
    DatasetSourceKind,
    DatasetStorageKind,
)
from langbridge.runtime.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
    dataset_supports_structured_federation,
    resolve_dataset_materialization_mode,
    resolve_dataset_connector_kind,
    resolve_dataset_source_kind,
    resolve_dataset_storage_kind,
)


def test_explicit_shopify_parquet_dataset_resolves_to_structured_api_dataset() -> None:
    connector_kind = resolve_dataset_connector_kind(
        explicit_connector_kind="shopify",
    )
    source_kind = resolve_dataset_source_kind(
        explicit_source_kind=DatasetSourceKind.API,
    )
    storage_kind = resolve_dataset_storage_kind(
        explicit_storage_kind=DatasetStorageKind.PARQUET,
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
    assert source_kind == DatasetSourceKind.API
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


def test_dataset_kind_resolvers_require_explicit_values() -> None:
    with pytest.raises(ValueError, match="source_kind"):
        resolve_dataset_source_kind(explicit_source_kind=None)

    with pytest.raises(ValueError, match="storage_kind"):
        resolve_dataset_storage_kind(explicit_storage_kind=None)


def test_resolve_dataset_materialization_mode_prefers_explicit_value() -> None:
    resolved = resolve_dataset_materialization_mode(
        explicit_materialization_mode=DatasetMaterializationMode.LIVE,
    )

    assert resolved == DatasetMaterializationMode.LIVE


def test_resolve_dataset_materialization_mode_requires_explicit_value() -> None:
    with pytest.raises(ValueError, match="materialization_mode"):
        resolve_dataset_materialization_mode(
            explicit_materialization_mode=None,
        )
