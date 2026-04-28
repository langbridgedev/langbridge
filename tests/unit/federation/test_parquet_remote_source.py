from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from langbridge.connectors.base import BaseConnectorConfig, StorageConnector
from langbridge.federation.connectors.parquet import DuckDbParquetRemoteSource
from langbridge.federation.models.plans import SourceSubplan
from langbridge.federation.models.virtual_dataset import VirtualTableBinding


@pytest.fixture
def anyio_backend():
    return "asyncio"


class RecordingStorageConnector(StorageConnector):
    def __init__(self, *, resolved_uris: list[str]) -> None:
        super().__init__(config=BaseConnectorConfig())
        self.resolved_uris = resolved_uris
        self.configured_storage_uris: list[str] = []
        self.resolve_calls: list[list[str]] = []

    async def list_buckets(self) -> list[str]:
        return []

    async def list_objects(self, bucket: str) -> list[str]:
        return []

    async def get_object(self, bucket: str, key: str) -> bytes:
        raise NotImplementedError

    async def configure_duckdb_connection(
        self,
        connection,
        *,
        storage_uris,
        options=None,
    ) -> None:
        self.configured_storage_uris = [str(storage_uri) for storage_uri in storage_uris]

    async def resolve_duckdb_scan_uris(
        self,
        storage_uris,
        *,
        options=None,
    ) -> list[str]:
        normalized_storage_uris = [str(storage_uri) for storage_uri in storage_uris]
        self.resolve_calls.append(normalized_storage_uris)
        return list(self.resolved_uris)


@pytest.mark.anyio
async def test_parquet_remote_source_executes_schema_qualified_query_across_distributed_files(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "orders_part_1.parquet"
    second_path = tmp_path / "orders_part_2.parquet"
    pq.write_table(
        pa.table(
            {
                "order_id": [1, 2],
                "amount": [10, 20],
            }
        ),
        first_path,
    )
    pq.write_table(
        pa.table(
            {
                "order_id": [3],
                "amount": [30],
            }
        ),
        second_path,
    )

    binding = VirtualTableBinding(
        table_key="commerce.orders",
        source_id="parquet_source_orders",
        connector_id=None,
        schema="commerce",
        table="orders",
        metadata={
            "source_kind": "file",
            "storage_kind": "parquet",
            "file_format": "parquet",
            "storage_uris": [
                first_path.resolve().as_uri(),
                second_path.resolve().as_uri(),
            ],
            "union_by_name": True,
        },
    )
    source = DuckDbParquetRemoteSource(
        source_id="parquet_source_orders",
        bindings=[binding],
    )

    result = await source.execute(
        SourceSubplan(
            stage_id="stage_1",
            source_id="parquet_source_orders",
            alias="t0",
            table_key="commerce.orders",
            sql=(
                'SELECT COUNT(*) AS "row_count", SUM(amount) AS "gross_amount" '
                'FROM "commerce"."orders" AS t0'
            ),
        )
    )

    assert result.table.to_pylist() == [{"row_count": 3, "gross_amount": 60}]

    stats = await source.estimate_table_stats(binding)
    assert stats.row_count_estimate == 3.0
    assert stats.bytes_per_row > 0


@pytest.mark.anyio
async def test_parquet_remote_source_uses_storage_connector_when_supplied(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "orders.parquet"
    pq.write_table(
        pa.table(
            {
                "order_id": [1, 2],
                "amount": [10, 20],
            }
        ),
        parquet_path,
    )

    storage_connector = RecordingStorageConnector(
        resolved_uris=[parquet_path.resolve().as_posix()],
    )
    binding = VirtualTableBinding(
        table_key="orders",
        source_id="parquet_source_orders",
        connector_id=None,
        table="orders",
        metadata={
            "source_kind": "file",
            "storage_kind": "parquet",
            "file_format": "parquet",
            "storage_uri": "s3://acme-bucket/orders/orders.parquet",
        },
    )
    source = DuckDbParquetRemoteSource(
        source_id="parquet_source_orders",
        bindings=[binding],
        storage_connector=storage_connector,
    )

    result = await source.execute(
        SourceSubplan(
            stage_id="stage_2",
            source_id="parquet_source_orders",
            alias="t0",
            table_key="orders",
            sql='SELECT COUNT(*) AS "row_count" FROM "orders" AS t0',
        )
    )

    assert result.table.to_pylist() == [{"row_count": 2}]
    assert storage_connector.configured_storage_uris == ["s3://acme-bucket/orders/orders.parquet"]
    assert storage_connector.resolve_calls == [["s3://acme-bucket/orders/orders.parquet"]]
