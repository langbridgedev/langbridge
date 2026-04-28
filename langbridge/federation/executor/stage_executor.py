
import time
from dataclasses import dataclass

import duckdb
import pyarrow as pa

from langbridge.federation.connectors import RemoteSource
from langbridge.federation.executor.artifact_store import ArtifactStore, StageArtifactManifest
from langbridge.federation.executor.cache_context import StageCacheDescriptor, StageCacheResolver
from langbridge.federation.executor.offload import (
    FederationExecutionOffloader,
    run_federation_blocking,
)
from langbridge.federation.models.plans import (
    StageArtifact,
    StageCacheInputSnapshot,
    StageCacheStatus,
    StageDefinition,
    StageMetrics,
    StageType,
)
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
        cache_resolver: StageCacheResolver,
        sources: dict[str, RemoteSource],
        blocking_executor: FederationExecutionOffloader | None = None,
    ) -> None:
        self._artifact_store = artifact_store
        self._cache_resolver = cache_resolver
        self._sources = sources
        self._blocking_executor = blocking_executor

    async def execute_stage(
        self,
        *,
        stage: StageDefinition,
        context: StageExecutionContext,
    ) -> tuple[StageArtifact, StageMetrics]:
        started_at = time.time()
        started = time.perf_counter()
        cache_descriptor: StageCacheDescriptor = self._cache_resolver.describe_stage(
            stage=stage,
            dependency_caches=await self._dependency_caches(stage=stage, context=context),
        )
        cache_inputs = [
            _cache_input_snapshot(item)
            for item in cache_descriptor.inputs
        ]

        cached: StageArtifactManifest | None = await run_federation_blocking(
            self._blocking_executor,
            self._artifact_store.get_cached_stage_output,
            workspace_id=context.workspace_id,
            plan_id=context.plan_id,
            stage_id=stage.stage_id,
            expected_cache=cache_descriptor,
        )
        if cached is not None and cached.ttl and not cached.ttl.is_expired():
            runtime_ms = int((time.perf_counter() - started) * 1000)
            finished_at = time.time()
            return cached.artifact, StageMetrics(
                stage_id=stage.stage_id,
                stage_type=stage.stage_type,
                source_id=_stage_source_id(stage),
                attempts=1,
                runtime_ms=runtime_ms,
                rows=cached.artifact.rows,
                bytes_written=cached.artifact.bytes_written,
                cached=True,
                cacheable=cache_descriptor.cacheable,
                cache_status=StageCacheStatus.HIT,
                cache_reason=_cache_hit_reason(cache_descriptor),
                cache_inputs=cache_inputs,
                started_at=started_at,
                finished_at=finished_at,
            )

        if stage.stage_type in {StageType.REMOTE_SCAN, StageType.REMOTE_FULL_QUERY}:
            if stage.subplan is None:
                raise ValueError(f"Stage '{stage.stage_id}' is missing subplan payload.")
            source = self._sources.get(stage.subplan.source_id)
            if source is None:
                raise ValueError(f"No remote source registered for source_id '{stage.subplan.source_id}'.")
            remote_result = await source.execute(stage.subplan)
            artifact_manifest: StageArtifactManifest = await run_federation_blocking(
                self._blocking_executor,
                self._artifact_store.write_stage_output,
                workspace_id=context.workspace_id,
                plan_id=context.plan_id,
                stage_id=stage.stage_id,
                table=remote_result.table,
                cache=cache_descriptor,
            )
            runtime_ms = int((time.perf_counter() - started) * 1000)
            finished_at = time.time()
            return artifact_manifest.artifact, StageMetrics(
                stage_id=stage.stage_id,
                stage_type=stage.stage_type,
                source_id=_stage_source_id(stage),
                attempts=1,
                runtime_ms=runtime_ms,
                rows=artifact_manifest.artifact.rows,
                bytes_written=artifact_manifest.artifact.bytes_written,
                source_elapsed_ms=remote_result.elapsed_ms,
                cached=False,
                cacheable=cache_descriptor.cacheable,
                cache_status=_cache_status_for_descriptor(cache_descriptor),
                cache_reason=_cache_miss_or_bypass_reason(cache_descriptor),
                cache_inputs=cache_inputs,
                started_at=started_at,
                finished_at=finished_at,
            )

        if stage.stage_type == StageType.LOCAL_COMPUTE:
            if not stage.sql:
                raise ValueError(f"Local compute stage '{stage.stage_id}' is missing SQL payload.")
            sql_dialect = normalize_sql_dialect(stage.sql_dialect, default="duckdb")
            if sql_dialect != "duckdb":
                raise ValueError(
                    f"Local compute stage '{stage.stage_id}' targets unsupported dialect '{sql_dialect}'."
                )

            artifact_manifest = await run_federation_blocking(
                self._blocking_executor,
                self._execute_local_compute_stage_blocking,
                stage,
                context,
                cache_descriptor,
            )
            runtime_ms = int((time.perf_counter() - started) * 1000)
            finished_at = time.time()
            return artifact_manifest.artifact, StageMetrics(
                stage_id=stage.stage_id,
                stage_type=stage.stage_type,
                source_id=_stage_source_id(stage),
                attempts=1,
                runtime_ms=runtime_ms,
                rows=artifact_manifest.artifact.rows,
                bytes_written=artifact_manifest.artifact.bytes_written,
                cached=False,
                cacheable=cache_descriptor.cacheable,
                cache_status=_cache_status_for_descriptor(cache_descriptor),
                cache_reason=_cache_miss_or_bypass_reason(cache_descriptor),
                cache_inputs=cache_inputs,
                started_at=started_at,
                finished_at=finished_at,
            )

        raise ValueError(f"Unsupported stage type '{stage.stage_type}'.")

    def _execute_local_compute_stage_blocking(
        self,
        stage: StageDefinition,
        context: StageExecutionContext,
        cache_descriptor: StageCacheDescriptor,
    ) -> StageArtifactManifest:
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

            return self._artifact_store.write_stage_output(
                workspace_id=context.workspace_id,
                plan_id=context.plan_id,
                stage_id=stage.stage_id,
                table=local_table,
                cache=cache_descriptor,
            )
        finally:
            connection.close()

    async def _dependency_caches(
        self,
        *,
        stage: StageDefinition,
        context: StageExecutionContext,
    ) -> dict[str, StageCacheDescriptor | None]:
        dependency_caches: dict[str, StageCacheDescriptor | None] = {}
        for dependency_stage_id in stage.dependencies:
            manifest = await run_federation_blocking(
                self._blocking_executor,
                self._artifact_store.get_stage_output_manifest,
                workspace_id=context.workspace_id,
                plan_id=context.plan_id,
                stage_id=dependency_stage_id,
            )
            dependency_caches[dependency_stage_id] = None if manifest is None else manifest.cache
        return dependency_caches


def _stage_source_id(stage: StageDefinition) -> str | None:
    if stage.source_id:
        return stage.source_id
    if stage.subplan is not None:
        return stage.subplan.source_id
    return None


def _cache_status_for_descriptor(cache_descriptor: StageCacheDescriptor) -> StageCacheStatus:
    if cache_descriptor.cacheable:
        return StageCacheStatus.MISS
    return StageCacheStatus.BYPASS


def _cache_hit_reason(cache_descriptor: StageCacheDescriptor) -> str:
    if any(item.cache_policy.value == "revision" for item in cache_descriptor.inputs):
        return "Cache hit because dataset revision freshness matched."
    if any(item.cache_policy.value == "dependency" for item in cache_descriptor.inputs):
        return "Cache hit because dependency freshness matched."
    return "Cache hit because the stage freshness inputs matched a stored artifact."


def _cache_miss_or_bypass_reason(cache_descriptor: StageCacheDescriptor) -> str | None:
    if not cache_descriptor.cacheable:
        return cache_descriptor.reason or "Cache bypassed for this stage."
    if any(item.cache_policy.value == "revision" for item in cache_descriptor.inputs):
        return "Cache miss because no stored artifact matched the current dataset revision freshness."
    if any(item.cache_policy.value == "dependency" for item in cache_descriptor.inputs):
        return "Cache miss because no stored artifact matched the current dependency freshness."
    return "Cache miss because no stored artifact matched the current stage freshness inputs."


def _cache_input_snapshot(item) -> StageCacheInputSnapshot:
    return StageCacheInputSnapshot(
        kind=item.kind.value,
        cache_policy=item.cache_policy.value,
        source_id=item.source_id,
        table_key=item.table_key,
        dataset_id=item.dataset_id,
        dataset_name=item.dataset_name,
        canonical_reference=item.canonical_reference,
        materialization_mode=item.materialization_mode,
        revision_id=item.revision_id,
        dependency_stage_id=item.dependency_stage_id,
        freshness_key_present=bool(str(item.freshness_key or "").strip()),
        reason=item.reason,
    )
