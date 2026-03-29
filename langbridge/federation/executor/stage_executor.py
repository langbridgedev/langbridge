
import time
from dataclasses import dataclass

import duckdb
import pyarrow as pa

from langbridge.federation.connectors import RemoteSource
from langbridge.federation.executor.artifact_store import ArtifactStore
from langbridge.federation.models.plans import StageArtifact, StageDefinition, StageMetrics, StageType
from langbridge.federation.utils.sql import normalize_sql_dialect


@dataclass(slots=True)
class StageExecutionContext:
    workspace_id: str
    plan_id: str


class StageExecutor:
    def __init__(
        self,
        *,
        artifact_store: ArtifactStore,
        sources: dict[str, RemoteSource],
    ) -> None:
        self._artifact_store = artifact_store
        self._sources = sources

    async def execute_stage(
        self,
        *,
        stage: StageDefinition,
        context: StageExecutionContext,
    ) -> tuple[StageArtifact, StageMetrics]:
        started = time.perf_counter()

        cached = self._artifact_store.get_cached_stage_output(
            workspace_id=context.workspace_id,
            plan_id=context.plan_id,
            stage_id=stage.stage_id,
        )
        if cached is not None:
            runtime_ms = int((time.perf_counter() - started) * 1000)
            return cached, StageMetrics(
                stage_id=stage.stage_id,
                attempts=1,
                runtime_ms=runtime_ms,
                rows=cached.rows,
                bytes_written=cached.bytes_written,
                cached=True,
                started_at=time.time(),
                finished_at=time.time(),
            )

        if stage.stage_type in {StageType.REMOTE_SCAN, StageType.REMOTE_FULL_QUERY}:
            if stage.subplan is None:
                raise ValueError(f"Stage '{stage.stage_id}' is missing subplan payload.")
            source = self._sources.get(stage.subplan.source_id)
            if source is None:
                raise ValueError(f"No remote source registered for source_id '{stage.subplan.source_id}'.")
            remote_result = await source.execute(stage.subplan)
            artifact = self._artifact_store.write_stage_output(
                workspace_id=context.workspace_id,
                plan_id=context.plan_id,
                stage_id=stage.stage_id,
                table=remote_result.table,
            )
            runtime_ms = int((time.perf_counter() - started) * 1000)
            return artifact, StageMetrics(
                stage_id=stage.stage_id,
                attempts=1,
                runtime_ms=runtime_ms,
                rows=artifact.rows,
                bytes_written=artifact.bytes_written,
                source_elapsed_ms=remote_result.elapsed_ms,
                cached=False,
                started_at=time.time() - (runtime_ms / 1000),
                finished_at=time.time(),
            )

        if stage.stage_type == StageType.LOCAL_COMPUTE:
            if not stage.sql:
                raise ValueError(f"Local compute stage '{stage.stage_id}' is missing SQL payload.")
            sql_dialect = normalize_sql_dialect(stage.sql_dialect, default="duckdb")
            if sql_dialect != "duckdb":
                raise ValueError(
                    f"Local compute stage '{stage.stage_id}' targets unsupported dialect '{sql_dialect}'."
                )

            connection = duckdb.connect(database=":memory:")
            try:
                table_inputs = stage.metadata.get("table_inputs", {})
                for relation_name, dependency_stage_id in table_inputs.items():
                    table = self._artifact_store.read_stage_output(
                        workspace_id=context.workspace_id,
                        plan_id=context.plan_id,
                        stage_id=str(dependency_stage_id),
                    )
                    connection.register(relation_name, table)

                arrow_result = connection.execute(stage.sql).arrow()
                local_table: pa.Table
                if isinstance(arrow_result, pa.Table):
                    local_table = arrow_result
                elif hasattr(arrow_result, "read_all"):
                    local_table = arrow_result.read_all()
                elif hasattr(arrow_result, "to_arrow_table"):
                    local_table = arrow_result.to_arrow_table()
                else:  # pragma: no cover - defensive fallback for duckdb return types
                    local_table = pa.Table.from_batches(list(arrow_result))

                artifact = self._artifact_store.write_stage_output(
                    workspace_id=context.workspace_id,
                    plan_id=context.plan_id,
                    stage_id=stage.stage_id,
                    table=local_table,
                )
                runtime_ms = int((time.perf_counter() - started) * 1000)
                return artifact, StageMetrics(
                    stage_id=stage.stage_id,
                    attempts=1,
                    runtime_ms=runtime_ms,
                    rows=artifact.rows,
                    bytes_written=artifact.bytes_written,
                    cached=False,
                    started_at=time.time() - (runtime_ms / 1000),
                    finished_at=time.time(),
                )
            finally:
                connection.close()

        raise ValueError(f"Unsupported stage type '{stage.stage_type}'.")
