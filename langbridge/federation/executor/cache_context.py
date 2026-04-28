import hashlib
import json
from enum import Enum
from typing import Mapping
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from langbridge.federation.models import (
    DatasetFreshnessPolicy,
    FederationWorkflow,
    PhysicalPlan,
    StageDefinition,
    StageType,
    VirtualTableBinding,
)
from langbridge.runtime.utils.util import _coerce_uuid, _string_or_none


class StageCacheInputKind(str, Enum):
    DATASET = "dataset"
    DEPENDENCY = "dependency"


class StageCacheInputPolicy(str, Enum):
    REVISION = "revision"
    DEPENDENCY = "dependency"
    VOLATILE = "volatile"
    UNKNOWN = "unknown"


class StageCacheInput(BaseModel):
    kind: StageCacheInputKind
    cache_policy: StageCacheInputPolicy
    source_id: str | None = None
    table_key: str | None = None
    dataset_id: UUID | None = None
    dataset_name: str | None = None
    canonical_reference: str | None = None
    materialization_mode: str | None = None
    freshness_key: str | None = None
    revision_id: UUID | None = None
    dependency_stage_id: str | None = None
    reason: str | None = None

    def supports_cache(self) -> bool:
        return self.cache_policy in {
            StageCacheInputPolicy.REVISION,
            StageCacheInputPolicy.DEPENDENCY,
        } and bool(str(self.freshness_key or "").strip())

class StageCacheDescriptor(BaseModel):
    cacheable: bool = False
    cache_key: str | None = None
    inputs: list[StageCacheInput] = Field(default_factory=list)
    reason: str | None = None

    @model_validator(mode="after")
    def _validate_descriptor(self) -> "StageCacheDescriptor":
        if self.cacheable and not str(self.cache_key or "").strip():
            raise ValueError("Cacheable stage descriptors require cache_key.")
        return self

    @classmethod
    def from_inputs(
        cls,
        *,
        inputs: list[StageCacheInput],
        reason: str | None = None,
    ) -> "StageCacheDescriptor":
        normalized_inputs = list(inputs)
        if not normalized_inputs:
            return cls(
                cacheable=False,
                cache_key=None,
                inputs=[],
                reason=reason or "Stage has no cacheable freshness inputs.",
            )

        first_uncacheable = next((item for item in normalized_inputs if not item.supports_cache()), None)
        if first_uncacheable is not None:
            return cls(
                cacheable=False,
                cache_key=None,
                inputs=normalized_inputs,
                reason=reason or first_uncacheable.reason or "Stage cache is bypassed for this input state.",
            )

        payload = {
            "version": 1,
            "inputs": [
                item.model_dump(mode="json", exclude_none=True)
                for item in normalized_inputs
            ],
        }
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return cls(
            cacheable=True,
            cache_key=hashlib.sha256(serialized.encode("utf-8")).hexdigest(),
            inputs=normalized_inputs,
            reason=reason,
        )


class StageCacheResolver:
    def __init__(
        self,
        *,
        workflow: FederationWorkflow,
        plan: PhysicalPlan,
    ) -> None:
        self._workflow = workflow
        self._plan = plan
        self._bindings = dict(workflow.dataset.tables)

    def describe_stage(
        self,
        *,
        stage: StageDefinition,
        dependency_caches: Mapping[str, StageCacheDescriptor | None] | None = None,
    ) -> StageCacheDescriptor:
        if stage.stage_type in {StageType.REMOTE_SCAN, StageType.REMOTE_FULL_QUERY}:
            return self._describe_remote_stage(stage=stage)
        if stage.stage_type == StageType.LOCAL_COMPUTE:
            return self._describe_local_stage(stage=stage, dependency_caches=dependency_caches or {})
        return StageCacheDescriptor(
            cacheable=False,
            cache_key=None,
            inputs=[],
            reason=f"Unsupported stage type '{stage.stage_type}' for cache resolution.",
        )

    def _describe_remote_stage(self, *, stage: StageDefinition) -> StageCacheDescriptor:
        table_keys = self._stage_table_keys(stage=stage)
        inputs = [
            self._describe_binding(self._bindings[table_key])
            for table_key in table_keys
            if table_key in self._bindings
        ]
        if not inputs:
            return StageCacheDescriptor(
                cacheable=False,
                cache_key=None,
                inputs=[],
                reason=f"Stage '{stage.stage_id}' has no dataset freshness bindings.",
            )
        return StageCacheDescriptor.from_inputs(inputs=inputs)

    def _describe_local_stage(
        self,
        *,
        stage: StageDefinition,
        dependency_caches: Mapping[str, StageCacheDescriptor | None],
    ) -> StageCacheDescriptor:
        inputs: list[StageCacheInput] = []
        for dependency_stage_id in sorted(stage.dependencies):
            dependency_cache = dependency_caches.get(dependency_stage_id)
            if dependency_cache is None:
                inputs.append(
                    StageCacheInput(
                        kind=StageCacheInputKind.DEPENDENCY,
                        cache_policy=StageCacheInputPolicy.UNKNOWN,
                        dependency_stage_id=dependency_stage_id,
                        reason="Dependency cache metadata is unavailable.",
                    )
                )
                continue
            inputs.append(
                StageCacheInput(
                    kind=StageCacheInputKind.DEPENDENCY,
                    cache_policy=(
                        StageCacheInputPolicy.DEPENDENCY
                        if dependency_cache.cacheable and dependency_cache.cache_key
                        else StageCacheInputPolicy.UNKNOWN
                    ),
                    dependency_stage_id=dependency_stage_id,
                    freshness_key=dependency_cache.cache_key,
                    reason=(
                        dependency_cache.reason
                        if not dependency_cache.cacheable
                        else None
                    ),
                )
            )
        return StageCacheDescriptor.from_inputs(inputs=inputs)

    def _stage_table_keys(self, *, stage: StageDefinition) -> list[str]:
        if stage.stage_type == StageType.REMOTE_SCAN and stage.subplan is not None:
            return [stage.subplan.table_key]
        source_id = str(stage.source_id or getattr(stage.subplan, "source_id", "") or "").strip()
        if not source_id:
            return []
        return sorted(
            {
                table_ref.table_key
                for table_ref in self._plan.logical_plan.tables.values()
                if table_ref.source_id == source_id
            }
        )

    @staticmethod
    def _describe_binding(binding: VirtualTableBinding) -> StageCacheInput:
        descriptor = getattr(binding, "dataset_descriptor", None)
        metadata = dict(binding.metadata or {})
        relation_identity = (
            dict(getattr(descriptor, "relation_identity", None) or {})
            if descriptor is not None
            else {}
        )
        freshness = getattr(descriptor, "freshness", None)
        if freshness is not None:
            return StageCacheInput(
                kind=StageCacheInputKind.DATASET,
                cache_policy=_descriptor_policy(freshness.policy),
                source_id=binding.source_id,
                table_key=binding.table_key,
                dataset_id=getattr(descriptor, "dataset_id", None),
                dataset_name=getattr(descriptor, "name", None),
                canonical_reference=_string_or_none(relation_identity.get("canonical_reference")),
                materialization_mode=_string_or_none(getattr(descriptor, "materialization_mode", None)),
                freshness_key=_string_or_none(freshness.freshness_key),
                revision_id=getattr(freshness, "revision_id", None),
                reason=_string_or_none(freshness.reason),
            )

        explicit_freshness_key = _string_or_none(metadata.get("cache_freshness_key"))
        if explicit_freshness_key:
            return StageCacheInput(
                kind=StageCacheInputKind.DATASET,
                cache_policy=StageCacheInputPolicy.REVISION,
                source_id=binding.source_id,
                table_key=binding.table_key,
                dataset_id=_coerce_uuid(metadata.get("dataset_id")),
                dataset_name=_string_or_none(metadata.get("dataset_name")),
                canonical_reference=_string_or_none(metadata.get("canonical_reference")),
                materialization_mode=_string_or_none(metadata.get("materialization_mode")),
                freshness_key=explicit_freshness_key,
                revision_id=_coerce_uuid(metadata.get("dataset_revision_id")),
                reason="Binding provided explicit cache_freshness_key metadata.",
            )

        return StageCacheInput(
            kind=StageCacheInputKind.DATASET,
            cache_policy=StageCacheInputPolicy.UNKNOWN,
            source_id=binding.source_id,
            table_key=binding.table_key,
            dataset_id=_coerce_uuid(metadata.get("dataset_id")),
            dataset_name=_string_or_none(metadata.get("dataset_name")),
            canonical_reference=_string_or_none(relation_identity.get("canonical_reference")),
            materialization_mode=_string_or_none(metadata.get("materialization_mode")),
            reason="Dataset freshness metadata is missing, so federation stage cache is bypassed.",
        )


def _descriptor_policy(value: DatasetFreshnessPolicy) -> StageCacheInputPolicy:
    if value == DatasetFreshnessPolicy.REVISION:
        return StageCacheInputPolicy.REVISION
    if value == DatasetFreshnessPolicy.VOLATILE:
        return StageCacheInputPolicy.VOLATILE
    return StageCacheInputPolicy.UNKNOWN