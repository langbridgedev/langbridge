
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from langbridge.federation.models.plans import StageArtifact


class ArtifactStore:
    """Content-addressed stage artifact storage with idempotent stage manifests."""

    def __init__(self, *, base_dir: str) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    def get_cached_stage_output(
        self,
        *,
        workspace_id: str,
        plan_id: str,
        stage_id: str,
    ) -> StageArtifact | None:
        manifest_path = self._manifest_path(workspace_id=workspace_id, plan_id=plan_id, stage_id=stage_id)
        if not manifest_path.exists():
            return None
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        artifact = StageArtifact.model_validate(payload)
        if not self._artifact_path(artifact.artifact_key).exists():
            return None
        return artifact

    def write_stage_output(
        self,
        *,
        workspace_id: str,
        plan_id: str,
        stage_id: str,
        table: pa.Table,
    ) -> StageArtifact:
        content_hash = self._content_hash(table)
        artifact_key = f"{workspace_id}/artifacts/{content_hash}.parquet"
        artifact_path = self._artifact_path(artifact_key)
        artifact_path.parent.mkdir(parents=True, exist_ok=True)

        if not artifact_path.exists():
            pq.write_table(table, artifact_path)

        artifact = StageArtifact(
            stage_id=stage_id,
            artifact_key=artifact_key,
            rows=table.num_rows,
            bytes_written=artifact_path.stat().st_size,
            content_hash=content_hash,
        )

        manifest_path = self._manifest_path(workspace_id=workspace_id, plan_id=plan_id, stage_id=stage_id)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            artifact.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return artifact

    def read_stage_output(
        self,
        *,
        workspace_id: str,
        plan_id: str,
        stage_id: str,
    ) -> pa.Table:
        cached = self.get_cached_stage_output(workspace_id=workspace_id, plan_id=plan_id, stage_id=stage_id)
        if cached is None:
            raise FileNotFoundError(f"No artifact manifest found for stage '{stage_id}'.")
        return self.read_artifact(cached.artifact_key)

    def read_artifact(self, artifact_key: str) -> pa.Table:
        artifact_path = self._artifact_path(artifact_key)
        if not artifact_path.exists():
            raise FileNotFoundError(f"Artifact '{artifact_key}' does not exist.")
        return pq.read_table(artifact_path)

    def _manifest_path(self, *, workspace_id: str, plan_id: str, stage_id: str) -> Path:
        safe_workspace = _safe_segment(workspace_id)
        safe_plan = _safe_segment(plan_id)
        safe_stage = _safe_segment(stage_id)
        return self._base_dir / safe_workspace / "plans" / safe_plan / f"{safe_stage}.json"

    def _artifact_path(self, artifact_key: str) -> Path:
        segments = [_safe_segment(segment) for segment in artifact_key.split("/")]
        return self._base_dir.joinpath(*segments)

    @staticmethod
    def _content_hash(table: pa.Table) -> str:
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        payload = sink.getvalue().to_pybytes()
        return hashlib.sha256(payload).hexdigest()


def _safe_segment(value: str) -> str:
    return value.replace("..", "_").replace("/", "_").replace("\\", "_")
