from __future__ import annotations

from pathlib import Path

import pytest

from langbridge.packages.federation.connectors.file import DuckDbFileRemoteSource
from langbridge.packages.federation.models.plans import SourceSubplan
from langbridge.packages.federation.models.virtual_dataset import VirtualTableBinding


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_file_remote_source_executes_schema_qualified_query(tmp_path) -> None:
    csv_path = tmp_path / "performance.csv"
    csv_path.write_text("SOURCE,value\ngoogle,10\nmeta,20\n", encoding="utf-8")
    binding = VirtualTableBinding(
        table_key="performance.performance",
        source_id="file_source_performance",
        connector_id=None,
        schema="performance",
        table="performance",
        metadata={
            "source_kind": "file",
            "storage_uri": csv_path.resolve().as_uri(),
            "file_format": "csv",
            "header": True,
        },
    )
    source = DuckDbFileRemoteSource(
        source_id="file_source_performance",
        bindings=[binding],
    )

    result = await source.execute(
        SourceSubplan(
            stage_id="stage_1",
            source_id="file_source_performance",
            alias="t0",
            table_key="performance.performance",
            sql='SELECT SOURCE AS "entity_1__SOURCE" FROM "performance"."performance" AS t0 GROUP BY SOURCE LIMIT 500',
        )
    )

    assert sorted(
        result.table.to_pylist(),
        key=lambda row: row["entity_1__SOURCE"],
    ) == [
        {"entity_1__SOURCE": "google"},
        {"entity_1__SOURCE": "meta"},
    ]
