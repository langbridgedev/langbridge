from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any

from langbridge.federation.executor.cache_context import StageCacheDescriptor, StageCacheResolver
from langbridge.federation.models import ExecutionSummary, FederationWorkflow, LogicalPlan, PhysicalPlan
from langbridge.runtime.models import (
    FederationCacheStageSurface,
    FederationCacheSurface,
    FederationDiagnosticsSummary,
    FederationLogicalJoin,
    FederationLogicalPlanSurface,
    FederationLogicalTable,
    FederationPhysicalPlanSurface,
    FederationPhysicalStageSurface,
    FederationPushdownOperation,
    FederationPushdownStageSurface,
    FederationPushdownSurface,
    FederationSourceDiagnosticsSurface,
    FederationStageCacheInputSurface,
    FederationStageCacheSurface,
    FederationStageDiagnosticsSurface,
    FederationStageMovementSurface,
    FederationStagePushdownSurface,
    RuntimeFederationDiagnostics,
)


def build_runtime_federation_diagnostics(
    *,
    workflow: FederationWorkflow | None,
    logical_plan: LogicalPlan,
    physical_plan: PhysicalPlan,
    execution: ExecutionSummary | None = None,
) -> RuntimeFederationDiagnostics:
    bindings_by_key = _workflow_bindings(workflow)
    metrics_by_stage = {
        metric.stage_id: metric
        for metric in (execution.stage_metrics if execution is not None else [])
    }
    predicted_caches = _predict_stage_caches(workflow=workflow, plan=physical_plan)
    stage_surfaces: list[FederationStageDiagnosticsSurface] = []
    physical_stage_surfaces: list[FederationPhysicalStageSurface] = []
    cache_stage_surfaces: list[FederationCacheStageSurface] = []
    pushdown_stage_surfaces: list[FederationPushdownStageSurface] = []
    source_buckets: OrderedDict[str, dict[str, Any]] = OrderedDict()

    for stage in physical_plan.stages:
        metric = metrics_by_stage.get(stage.stage_id)
        predicted_cache = predicted_caches.get(stage.stage_id)
        subplan = stage.subplan
        source_id = _stage_source_id(stage)
        binding = bindings_by_key.get(subplan.table_key) if subplan is not None else None
        dataset_name = _binding_dataset_name(binding=binding, table_key=getattr(subplan, "table_key", None))
        pushdown_surface = _map_pushdown(getattr(subplan, "pushdown", None))
        cache_surface = _map_cache(metric=metric, predicted=predicted_cache)
        movement_surface = FederationStageMovementSurface(
            rows=metric.rows if metric is not None else None,
            bytes_written=metric.bytes_written if metric is not None else None,
            estimated_rows=getattr(subplan, "estimated_rows", None),
            estimated_bytes=getattr(subplan, "estimated_bytes", None),
        )
        stage_surface = FederationStageDiagnosticsSurface(
            stage_id=stage.stage_id,
            stage_type=stage.stage_type.value,
            source_id=source_id,
            alias=getattr(subplan, "alias", None),
            dataset=dataset_name,
            table_key=getattr(subplan, "table_key", None),
            dependencies=list(stage.dependencies),
            attempts=metric.attempts if metric is not None else None,
            runtime_ms=metric.runtime_ms if metric is not None else None,
            source_elapsed_ms=metric.source_elapsed_ms if metric is not None else None,
            started_at=_to_datetime(metric.started_at if metric is not None else None),
            finished_at=_to_datetime(metric.finished_at if metric is not None else None),
            cached=bool(metric.cached) if metric is not None else False,
            local_sql=stage.sql if stage.stage_type.value == "local_compute" else None,
            remote_sql=getattr(subplan, "sql", None),
            resource=getattr(subplan, "resource", None),
            movement=movement_surface,
            cache=cache_surface,
            pushdown=pushdown_surface,
        )
        stage_surfaces.append(stage_surface)
        physical_stage_surfaces.append(
            FederationPhysicalStageSurface(
                stage_id=stage.stage_id,
                stage_type=stage.stage_type.value,
                source_id=source_id,
                alias=getattr(subplan, "alias", None),
                table_key=getattr(subplan, "table_key", None),
                dependencies=list(stage.dependencies),
                local_sql=stage.sql if stage.stage_type.value == "local_compute" else None,
                remote_sql=getattr(subplan, "sql", None),
                sql_dialect=stage.sql_dialect,
                resource=getattr(subplan, "resource", None),
                estimated_rows=getattr(subplan, "estimated_rows", None),
                estimated_bytes=getattr(subplan, "estimated_bytes", None),
            )
        )
        cache_stage_surfaces.append(
            FederationCacheStageSurface(
                stage_id=stage.stage_id,
                source_id=source_id,
                status=cache_surface.status,
                cacheable=cache_surface.cacheable,
                reason=cache_surface.reason,
            )
        )
        pushdown_stage_surfaces.append(
            FederationPushdownStageSurface(
                stage_id=stage.stage_id,
                source_id=source_id,
                alias=getattr(subplan, "alias", None),
                full_query=pushdown_surface.full_query,
                filter=pushdown_surface.filter,
                projection=pushdown_surface.projection,
                aggregation=pushdown_surface.aggregation,
                limit=pushdown_surface.limit,
                join=pushdown_surface.join,
            )
        )
        if source_id:
            bucket = source_buckets.setdefault(
                source_id,
                {
                    "datasets": [],
                    "tables": [],
                    "stage_ids": [],
                    "runtime_ms": 0,
                    "source_elapsed_ms": 0,
                    "rows": 0,
                    "bytes_written": 0,
                    "estimated_rows": 0.0,
                    "estimated_bytes": 0.0,
                    "has_runtime": False,
                    "has_source_elapsed": False,
                    "has_rows": False,
                    "has_bytes_written": False,
                    "has_estimated_rows": False,
                    "has_estimated_bytes": False,
                    "cache_hits": 0,
                    "cache_misses": 0,
                    "cache_bypasses": 0,
                    "full_query_pushdown": False,
                },
            )
            if dataset_name and dataset_name not in bucket["datasets"]:
                bucket["datasets"].append(dataset_name)
            table_name = getattr(subplan, "table_key", None)
            if table_name and table_name not in bucket["tables"]:
                bucket["tables"].append(table_name)
            bucket["stage_ids"].append(stage.stage_id)
            if stage_surface.runtime_ms is not None:
                bucket["runtime_ms"] += stage_surface.runtime_ms
                bucket["has_runtime"] = True
            if stage_surface.source_elapsed_ms is not None:
                bucket["source_elapsed_ms"] += stage_surface.source_elapsed_ms
                bucket["has_source_elapsed"] = True
            if movement_surface.rows is not None:
                bucket["rows"] += movement_surface.rows
                bucket["has_rows"] = True
            if movement_surface.bytes_written is not None:
                bucket["bytes_written"] += movement_surface.bytes_written
                bucket["has_bytes_written"] = True
            if movement_surface.estimated_rows is not None:
                bucket["estimated_rows"] += movement_surface.estimated_rows
                bucket["has_estimated_rows"] = True
            if movement_surface.estimated_bytes is not None:
                bucket["estimated_bytes"] += movement_surface.estimated_bytes
                bucket["has_estimated_bytes"] = True
            if cache_surface.status == "hit":
                bucket["cache_hits"] += 1
            elif cache_surface.status == "miss":
                bucket["cache_misses"] += 1
            elif cache_surface.status == "bypass":
                bucket["cache_bypasses"] += 1
            if pushdown_surface.full_query.pushed:
                bucket["full_query_pushdown"] = True

    source_surfaces = [
        FederationSourceDiagnosticsSurface(
            source_id=source_id,
            datasets=list(payload["datasets"]),
            tables=list(payload["tables"]),
            stage_ids=list(payload["stage_ids"]),
            stage_count=len(payload["stage_ids"]),
            total_runtime_ms=(payload["runtime_ms"] if payload["has_runtime"] else None),
            total_source_elapsed_ms=(
                payload["source_elapsed_ms"]
                if payload["has_source_elapsed"]
                else None
            ),
            total_rows=(payload["rows"] if payload["has_rows"] else None),
            total_bytes_written=(
                payload["bytes_written"]
                if payload["has_bytes_written"]
                else None
            ),
            estimated_rows=(payload["estimated_rows"] if payload["has_estimated_rows"] else None),
            estimated_bytes=(payload["estimated_bytes"] if payload["has_estimated_bytes"] else None),
            full_query_pushdown=bool(payload["full_query_pushdown"]),
            cache_hits=payload["cache_hits"],
            cache_misses=payload["cache_misses"],
            cache_bypasses=payload["cache_bypasses"],
        )
        for source_id, payload in source_buckets.items()
    ]

    logical_table_surfaces = [
        FederationLogicalTable(
            alias=alias,
            table_key=table_ref.table_key,
            dataset=_binding_dataset_name(
                binding=bindings_by_key.get(table_ref.table_key),
                table_key=table_ref.table_key,
            ),
            source_id=table_ref.source_id,
            connector_id=(str(table_ref.connector_id) if table_ref.connector_id is not None else None),
            catalog=table_ref.catalog,
            schema_name=table_ref.schema_name,
            table=table_ref.table,
        )
        for alias, table_ref in logical_plan.tables.items()
    ]
    logical_join_surfaces = [
        FederationLogicalJoin(
            left_alias=join.left_alias,
            right_alias=join.right_alias,
            join_type=join.join_type,
            on_sql=join.on_sql,
            strategy=(
                _enum_value(physical_plan.join_strategies.get(f"{join.left_alias}->{join.right_alias}"))
                if physical_plan.join_strategies
                else None
            ),
        )
        for join in logical_plan.joins
    ]

    final_metric = metrics_by_stage.get(physical_plan.result_stage_id)
    local_materialized_rows, local_materialized_bytes = _local_materialization_totals(
        plan=physical_plan,
        metrics_by_stage=metrics_by_stage,
    )
    cache_hits = sum(1 for stage in cache_stage_surfaces if stage.status == "hit")
    cache_misses = sum(1 for stage in cache_stage_surfaces if stage.status == "miss")
    cache_bypasses = sum(1 for stage in cache_stage_surfaces if stage.status == "bypass")
    summary = FederationDiagnosticsSummary(
        query_type=_query_type(logical_plan.query_type),
        plan_id=physical_plan.plan_id,
        datasets=_unique_preserve_order(item.dataset for item in logical_table_surfaces),
        tables=[item.table_key for item in logical_table_surfaces],
        sources=[item.source_id for item in source_surfaces],
        stage_count=len(physical_plan.stages),
        source_count=len(source_surfaces),
        full_query_pushdown=physical_plan.pushdown_full_query,
        pushdown_reason=physical_plan.pushdown_reasons[0] if physical_plan.pushdown_reasons else None,
        total_runtime_ms=(execution.total_runtime_ms if execution is not None else None),
        total_source_elapsed_ms=_total_source_elapsed_ms(source_surfaces),
        final_rows=(final_metric.rows if final_metric is not None else None),
        final_bytes=(final_metric.bytes_written if final_metric is not None else None),
        local_materialized_rows=local_materialized_rows,
        local_materialized_bytes=local_materialized_bytes,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
        cache_bypasses=cache_bypasses,
    )

    return RuntimeFederationDiagnostics(
        summary=summary,
        logical_plan=FederationLogicalPlanSurface(
            query_type=_query_type(logical_plan.query_type),
            sql=logical_plan.sql,
            from_alias=logical_plan.from_alias,
            tables=logical_table_surfaces,
            joins=logical_join_surfaces,
            where_sql=logical_plan.where_sql,
            having_sql=logical_plan.having_sql,
            group_by_sql=list(logical_plan.group_by_sql),
            order_by_sql=list(logical_plan.order_by_sql),
            limit=logical_plan.limit,
            offset=logical_plan.offset,
            has_cte=logical_plan.has_cte,
        ),
        physical_plan=FederationPhysicalPlanSurface(
            plan_id=physical_plan.plan_id,
            result_stage_id=physical_plan.result_stage_id,
            join_order=list(physical_plan.join_order),
            join_strategies={
                key: _enum_value(value)
                for key, value in physical_plan.join_strategies.items()
            },
            full_query_pushdown=physical_plan.pushdown_full_query,
            pushdown_reasons=list(physical_plan.pushdown_reasons),
            stages=physical_stage_surfaces,
        ),
        stages=stage_surfaces,
        sources=source_surfaces,
        cache=FederationCacheSurface(
            hits=cache_hits,
            misses=cache_misses,
            bypasses=cache_bypasses,
            cacheable_stages=sum(1 for stage in cache_stage_surfaces if stage.cacheable),
            stages=cache_stage_surfaces,
        ),
        pushdown=FederationPushdownSurface(
            full_query_pushdown=physical_plan.pushdown_full_query,
            reasons=list(physical_plan.pushdown_reasons),
            stages=pushdown_stage_surfaces,
        ),
    )


def _workflow_bindings(workflow: FederationWorkflow | None) -> dict[str, Any]:
    if workflow is None:
        return {}
    dataset = getattr(workflow, "dataset", None)
    tables = getattr(dataset, "tables", None)
    if not isinstance(tables, dict):
        return {}
    return dict(tables)


def _predict_stage_caches(
    *,
    workflow: FederationWorkflow | None,
    plan: PhysicalPlan,
) -> dict[str, StageCacheDescriptor]:
    if workflow is None:
        return {}
    resolver = StageCacheResolver(workflow=workflow, plan=plan)
    resolved: dict[str, StageCacheDescriptor] = {}
    for stage in plan.stages:
        dependency_caches = {
            dependency: resolved.get(dependency)
            for dependency in stage.dependencies
        }
        resolved[stage.stage_id] = resolver.describe_stage(
            stage=stage,
            dependency_caches=dependency_caches,
        )
    return resolved


def _map_cache(*, metric, predicted: StageCacheDescriptor | None) -> FederationStageCacheSurface:
    if metric is not None:
        return FederationStageCacheSurface(
            cacheable=bool(metric.cacheable),
            status=_enum_value(metric.cache_status),
            reason=metric.cache_reason,
            inputs=[
                FederationStageCacheInputSurface.model_validate(item.model_dump(mode="json"))
                for item in metric.cache_inputs
            ],
        )
    if predicted is None:
        return FederationStageCacheSurface()
    return FederationStageCacheSurface(
        cacheable=bool(predicted.cacheable),
        status=None,
        reason=predicted.reason,
        inputs=[
            FederationStageCacheInputSurface(
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
            for item in predicted.inputs
        ],
    )


def _map_pushdown(raw) -> FederationStagePushdownSurface:
    if raw is None:
        return FederationStagePushdownSurface()
    return FederationStagePushdownSurface(
        full_query=_map_pushdown_operation(raw.full_query),
        filter=_map_pushdown_operation(raw.filter),
        projection=_map_pushdown_operation(raw.projection),
        aggregation=_map_pushdown_operation(raw.aggregation),
        limit=_map_pushdown_operation(raw.limit),
        join=_map_pushdown_operation(raw.join),
    )


def _map_pushdown_operation(raw) -> FederationPushdownOperation:
    if raw is None:
        return FederationPushdownOperation()
    return FederationPushdownOperation(
        pushed=bool(raw.pushed),
        supported=raw.supported,
        reason=raw.reason,
        details=list(raw.details or []),
    )


def _binding_dataset_name(*, binding: Any, table_key: str | None) -> str | None:
    descriptor = getattr(binding, "dataset_descriptor", None)
    name = getattr(descriptor, "name", None)
    if str(name or "").strip():
        return str(name).strip()
    if table_key and str(table_key).strip():
        return str(table_key).strip()
    return None


def _stage_source_id(stage) -> str | None:
    if stage.source_id is not None:
        return stage.source_id
    if stage.subplan is not None:
        return stage.subplan.source_id
    return None


def _to_datetime(value: float | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc)


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value))


def _query_type(value: Any) -> str:
    normalized = str(getattr(value, "value", value)).strip().lower()
    if normalized == "smq":
        return "semantic"
    return normalized or "sql"


def _unique_preserve_order(values) -> list[str]:
    seen: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.append(normalized)
    return seen


def _local_materialization_totals(
    *,
    plan: PhysicalPlan,
    metrics_by_stage: dict[str, Any],
) -> tuple[int | None, int | None]:
    result_stage = next(
        (stage for stage in plan.stages if stage.stage_id == plan.result_stage_id),
        None,
    )
    if result_stage is None or result_stage.stage_type.value != "local_compute":
        return None, None
    rows = 0
    bytes_written = 0
    has_values = False
    for dependency in result_stage.dependencies:
        metric = metrics_by_stage.get(dependency)
        if metric is None:
            continue
        rows += int(metric.rows or 0)
        bytes_written += int(metric.bytes_written or 0)
        has_values = True
    if not has_values:
        return None, None
    return rows, bytes_written


def _total_source_elapsed_ms(sources: list[FederationSourceDiagnosticsSurface]) -> int | None:
    values = [
        item.total_source_elapsed_ms
        for item in sources
        if item.total_source_elapsed_ms is not None
    ]
    if not values:
        return None
    return sum(values)
