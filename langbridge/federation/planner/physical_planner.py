
import hashlib
import json

from langbridge.federation.models.plans import PhysicalPlan, StageDefinition, StageType
from langbridge.federation.planner.optimizer import OptimizedPlan


class PhysicalPlanner:
    def build(self, *, optimized_plan: OptimizedPlan) -> PhysicalPlan:
        stage_defs: list[StageDefinition] = []

        if optimized_plan.pushdown_full_query:
            subplan = optimized_plan.source_subplans[0]
            stage_defs.append(
                StageDefinition(
                    stage_id=subplan.stage_id,
                    stage_type=StageType.REMOTE_FULL_QUERY,
                    source_id=subplan.source_id,
                    subplan=subplan,
                    retry_limit=2,
                    metadata={"alias": subplan.alias},
                )
            )
            logical_payload = optimized_plan.logical_plan.model_dump(mode="json")
            plan_id = _plan_hash(logical_payload)
            return PhysicalPlan(
                plan_id=plan_id,
                logical_plan=optimized_plan.logical_plan,
                stages=stage_defs,
                result_stage_id=subplan.stage_id,
                join_order=optimized_plan.join_order,
                join_strategies=optimized_plan.join_strategies,
            )

        dependency_ids: list[str] = []
        table_inputs: dict[str, str] = {}
        for subplan in optimized_plan.source_subplans:
            stage_defs.append(
                StageDefinition(
                    stage_id=subplan.stage_id,
                    stage_type=StageType.REMOTE_SCAN,
                    source_id=subplan.source_id,
                    subplan=subplan,
                    retry_limit=2,
                    metadata={"alias": subplan.alias},
                )
            )
            dependency_ids.append(subplan.stage_id)
            table_inputs[f"scan_{subplan.alias}"] = subplan.stage_id

        final_stage_id = "local_compute_final"
        stage_defs.append(
            StageDefinition(
                stage_id=final_stage_id,
                stage_type=StageType.LOCAL_COMPUTE,
                dependencies=dependency_ids,
                sql=optimized_plan.local_stage_sql,
                sql_dialect=optimized_plan.local_stage_dialect,
                retry_limit=2,
                metadata={
                    "table_inputs": table_inputs,
                    "join_order": optimized_plan.join_order,
                    "join_strategies": {
                        key: value.value for key, value in optimized_plan.join_strategies.items()
                    },
                },
            )
        )

        logical_payload = optimized_plan.logical_plan.model_dump(mode="json")
        logical_payload["local_stage_sql"] = optimized_plan.local_stage_sql
        plan_id = _plan_hash(logical_payload)
        return PhysicalPlan(
            plan_id=plan_id,
            logical_plan=optimized_plan.logical_plan,
            stages=stage_defs,
            result_stage_id=final_stage_id,
            join_order=optimized_plan.join_order,
            join_strategies=optimized_plan.join_strategies,
        )


def _plan_hash(payload: dict) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
