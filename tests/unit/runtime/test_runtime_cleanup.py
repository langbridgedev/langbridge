import time

import pyarrow as pa
import pytest

from langbridge.federation.executor import (
    ArtifactStore,
    StageCacheDescriptor,
    StageCacheInput,
    StageCacheInputKind,
    StageCacheInputPolicy,
)
from langbridge.runtime.services.maintenance import RuntimeCleanupService


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_runtime_cleanup_service_sweeps_federation_artifacts(tmp_path) -> None:
    artifact_store = ArtifactStore(base_dir=str(tmp_path / "artifacts"))
    manifest = artifact_store.write_stage_output(
        workspace_id="workspace",
        plan_id="plan",
        stage_id="stage",
        table=pa.table({"id": [1]}),
        cache=StageCacheDescriptor.from_inputs(
            inputs=[
                StageCacheInput(
                    kind=StageCacheInputKind.DATASET,
                    cache_policy=StageCacheInputPolicy.REVISION,
                    source_id="source",
                    table_key="orders",
                    freshness_key="revision:1",
                )
            ]
        ),
    )
    assert manifest.ttl is not None
    manifest.ttl.created_at = time.time() - 10
    manifest.ttl.seconds = 1
    manifest_path = tmp_path / "artifacts" / "workspace" / "plans" / "plan" / "stage.json"
    manifest_path.write_text(manifest.model_dump_json(), encoding="utf-8")

    cleanup = RuntimeCleanupService(
        federation_artifact_store=artifact_store,
        federation_ttl_seconds=1,
    )

    result = await cleanup.cleanup_resources()

    assert result.deleted_manifests == 1
    assert not manifest_path.exists()
