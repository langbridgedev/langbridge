
import copy
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import yaml

from langbridge.runtime.application.errors import ApplicationError
from langbridge.runtime.config.models import LocalRuntimeSemanticModelConfig
from langbridge.runtime.models.metadata import (
    LifecycleState,
    ManagementMode,
    SemanticModelMetadata,
)
from langbridge.runtime.persistence.mappers.semantic_models import to_semantic_model_record
from langbridge.semantic.query import SemanticQuery
from langbridge.semantic.loader import SemanticModelError, load_semantic_model, load_unified_semantic_model

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


class SemanticApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    @staticmethod
    def _require_runtime_managed_model(record) -> None:
        management_mode = str(getattr(record.management_mode, "value", record.management_mode)).lower()
        if management_mode != ManagementMode.RUNTIME_MANAGED.value:
            raise ValueError(
                f"Semantic model '{record.name}' is config_managed and read-only in the runtime UI."
            )

    async def list_semantic_models(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for name, record in self._host._semantic_models.items():
            items.append(self._host._build_semantic_model_summary(name=name, record=record))
        items.sort(key=lambda item: (not bool(item["default"]), str(item["name"]).lower()))
        return items

    async def get_semantic_model(
        self,
        *,
        model_ref: str,
    ) -> dict[str, Any]:
        record = self._host._resolve_semantic_model_record(model_ref)
        items = await self.list_semantic_models()
        summary = next((item for item in items if item["id"] == record.id), None) or {}
        return {
            **summary,
            "content_yaml": record.content_yaml,
            "content_json": copy.deepcopy(record.content_json),
        }

    async def create_semantic_model(self, *, request) -> dict[str, Any]:
        from langbridge.runtime.bootstrap.configured_runtime import (
            ConfiguredLocalRuntimeHostFactory,
            LocalRuntimeSemanticModelRecord,
        )

        normalized_request = LocalRuntimeSemanticModelConfig.model_validate(
            request.model_dump(mode="json")
        )
        model_name = str(normalized_request.name or "").strip()
        if not model_name:
            raise ValueError("Semantic model name is required.")
        if model_name in self._host._semantic_models:
            raise ValueError(f"Semantic model '{model_name}' already exists.")

        async with self._host._runtime_operation_scope() as uow:
            datasets = await self._host._dataset_repository.list_for_workspace(
                workspace_id=self._host.context.workspace_id,
            )
            datasets_by_name = {dataset.name: dataset for dataset in datasets}

            referenced_dataset_names: set[str] = {
                str(dataset_name).strip()
                for dataset_name in (normalized_request.datasets or [])
                if str(dataset_name).strip()
            }
            model_payload = copy.deepcopy(normalized_request.model or {})
            raw_datasets = (
                model_payload.get("datasets")
                if isinstance(model_payload.get("datasets"), dict)
                else model_payload.get("tables")
            )
            if isinstance(raw_datasets, dict):
                referenced_dataset_names.update(
                    str(dataset_name).strip()
                    for dataset_name in raw_datasets.keys()
                    if str(dataset_name).strip()
                )
            missing_datasets = sorted(
                dataset_name
                for dataset_name in referenced_dataset_names
                if dataset_name not in datasets_by_name
            )
            if missing_datasets:
                raise ValueError(
                    "Semantic model references unknown datasets: "
                    f"{', '.join(missing_datasets)}."
                )

            payload = ConfiguredLocalRuntimeHostFactory._materialize_semantic_model_payload(
                semantic_model=normalized_request,
                datasets=datasets_by_name,
            )

            semantic_model = None
            try:
                semantic_model = load_semantic_model(copy.deepcopy(payload))
                content_json = semantic_model.model_dump(mode="json", exclude_none=True)
                content_yaml = semantic_model.yml_dump()
            except SemanticModelError:
                try:
                    unified_model = load_unified_semantic_model(copy.deepcopy(payload))
                except SemanticModelError as exc:
                    raise ValueError(str(exc)) from exc
                known_semantic_model_ids = {record.id for record in self._host._semantic_models.values()}
                missing_source_models = sorted(
                    str(source.id)
                    for source in unified_model.source_models
                    if source.id not in known_semantic_model_ids
                )
                if missing_source_models:
                    raise ValueError(
                        "Unified semantic model references unknown source semantic model ids: "
                        f"{', '.join(missing_source_models)}."
                    )
                content_json = unified_model.model_dump(mode="json", exclude_none=True)
                content_yaml = yaml.safe_dump(content_json, sort_keys=False).strip()

            now = datetime.now(timezone.utc)
            record = LocalRuntimeSemanticModelRecord(
                id=uuid.uuid4(),
                name=model_name,
                description=(
                    normalized_request.description
                    or content_json.get("description")
                ),
                workspace_id=self._host.context.workspace_id,
                semantic_model=semantic_model,
                content_yaml=content_yaml,
                content_json=copy.deepcopy(content_json),
                management_mode=ManagementMode.RUNTIME_MANAGED,
            )
            metadata = SemanticModelMetadata(
                id=record.id,
                connector_id=None,
                workspace_id=record.workspace_id,
                created_by=self._host.context.actor_id,
                updated_by=self._host.context.actor_id,
                name=record.name,
                description=record.description,
                content_yaml=record.content_yaml,
                content_json=copy.deepcopy(record.content_json),
                created_at=now,
                updated_at=now,
                management_mode=ManagementMode.RUNTIME_MANAGED,
                lifecycle_state=LifecycleState.ACTIVE,
            )

            if uow is not None:
                uow.repository("semantic_model_repository").add(
                    to_semantic_model_record(metadata)
                )
                await uow.commit()

        self._host._upsert_runtime_semantic_model_record(record)
        return await self.get_semantic_model(model_ref=str(record.id))

    async def update_semantic_model(self, *, model_ref: str, request) -> dict[str, Any]:
        from langbridge.runtime.bootstrap.configured_runtime import (
            ConfiguredLocalRuntimeHostFactory,
            LocalRuntimeSemanticModelRecord,
        )

        existing_record = self._host._resolve_semantic_model_record(model_ref)
        self._require_runtime_managed_model(existing_record)
        fields_set = set(getattr(request, "model_fields_set", set()))
        description = request.description if "description" in fields_set else existing_record.description
        model_payload = request.model if "model" in fields_set else copy.deepcopy(existing_record.content_json)
        datasets = request.datasets if "datasets" in fields_set and request.datasets is not None else []

        normalized_request = LocalRuntimeSemanticModelConfig.model_validate(
            {
                "name": existing_record.name,
                "description": description,
                "model": model_payload,
                "datasets": datasets,
            }
        )

        async with self._host._runtime_operation_scope() as uow:
            if uow is None:
                raise ApplicationError("Runtime semantic model updates require persistence support.")
            datasets_records = await self._host._dataset_repository.list_for_workspace(
                workspace_id=self._host.context.workspace_id,
            )
            datasets_by_name = {dataset.name: dataset for dataset in datasets_records}
            referenced_dataset_names: set[str] = {
                str(dataset_name).strip()
                for dataset_name in (normalized_request.datasets or [])
                if str(dataset_name).strip()
            }
            model_json = copy.deepcopy(normalized_request.model or {})
            raw_datasets = (
                model_json.get("datasets")
                if isinstance(model_json.get("datasets"), dict)
                else model_json.get("tables")
            )
            if isinstance(raw_datasets, dict):
                referenced_dataset_names.update(
                    str(dataset_name).strip()
                    for dataset_name in raw_datasets.keys()
                    if str(dataset_name).strip()
                )
            missing_datasets = sorted(
                dataset_name
                for dataset_name in referenced_dataset_names
                if dataset_name not in datasets_by_name
            )
            if missing_datasets:
                raise ValueError(
                    "Semantic model references unknown datasets: "
                    f"{', '.join(missing_datasets)}."
                )

            payload = ConfiguredLocalRuntimeHostFactory._materialize_semantic_model_payload(
                semantic_model=normalized_request,
                datasets=datasets_by_name,
            )
            semantic_model = None
            try:
                semantic_model = load_semantic_model(copy.deepcopy(payload))
                content_json = semantic_model.model_dump(mode="json", exclude_none=True)
                content_yaml = semantic_model.yml_dump()
            except SemanticModelError:
                try:
                    unified_model = load_unified_semantic_model(copy.deepcopy(payload))
                except SemanticModelError as exc:
                    raise ValueError(str(exc)) from exc
                known_semantic_model_ids = {record.id for record in self._host._semantic_models.values()}
                missing_source_models = sorted(
                    str(source.id)
                    for source in unified_model.source_models
                    if source.id not in known_semantic_model_ids
                )
                if missing_source_models:
                    raise ValueError(
                        "Unified semantic model references unknown source semantic model ids: "
                        f"{', '.join(missing_source_models)}."
                    )
                content_json = unified_model.model_dump(mode="json", exclude_none=True)
                content_yaml = yaml.safe_dump(content_json, sort_keys=False).strip()

            now = datetime.now(timezone.utc)
            updated_record = LocalRuntimeSemanticModelRecord(
                id=existing_record.id,
                name=existing_record.name,
                description=(
                    normalized_request.description
                    or content_json.get("description")
                ),
                workspace_id=self._host.context.workspace_id,
                semantic_model=semantic_model,
                content_yaml=content_yaml,
                content_json=copy.deepcopy(content_json),
                management_mode=ManagementMode.RUNTIME_MANAGED,
            )
            repository = uow.repository("semantic_model_repository")
            persisted = await repository.get_for_workspace(
                model_id=existing_record.id,
                workspace_id=self._host.context.workspace_id,
            )
            if persisted is None:
                raise ValueError(f"Semantic model '{existing_record.name}' was not found.")
            metadata = SemanticModelMetadata(
                id=updated_record.id,
                connector_id=None,
                workspace_id=updated_record.workspace_id,
                created_by=getattr(persisted, "created_by_actor_id", None),
                updated_by=self._host.context.actor_id,
                name=updated_record.name,
                description=updated_record.description,
                content_yaml=updated_record.content_yaml,
                content_json=copy.deepcopy(updated_record.content_json),
                created_at=getattr(persisted, "created_at", now),
                updated_at=now,
                management_mode=ManagementMode.RUNTIME_MANAGED,
                lifecycle_state=LifecycleState.ACTIVE,
            )
            await repository.save(to_semantic_model_record(metadata))
            await uow.commit()

        self._host._upsert_runtime_semantic_model_record(updated_record)
        return await self.get_semantic_model(model_ref=str(updated_record.id))

    async def delete_semantic_model(self, *, model_ref: str) -> dict[str, Any]:
        record = self._host._resolve_semantic_model_record(model_ref)
        self._require_runtime_managed_model(record)
        async with self._host._runtime_operation_scope() as uow:
            if uow is None:
                raise ApplicationError("Runtime semantic model deletes require persistence support.")
            repository = uow.repository("semantic_model_repository")
            persisted = await repository.get_for_workspace(
                model_id=record.id,
                workspace_id=self._host.context.workspace_id,
            )
            if persisted is None:
                raise ValueError(f"Semantic model '{record.name}' was not found.")
            await repository.delete(persisted)
            await uow.commit()

        self._host._remove_runtime_semantic_model_record(
            model_name=record.name,
            model_id=record.id,
        )
        return {"ok": True, "deleted": True, "id": record.id, "name": record.name}

    async def query_semantic(self, *args: Any, **kwargs: Any) -> Any:
        async with self._host._runtime_operation_scope() as uow:
            result = await self._host._runtime_host.query_semantic(*args, **kwargs)
            if uow is not None:
                await uow.commit()
            return result

    async def query_semantic_models(
        self,
        *,
        semantic_models: list[str] | None = None,
        measures: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
        time_dimensions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        semantic_model_records = self._host._resolve_semantic_models(semantic_models)
        unified_records = [record for record in semantic_model_records if record.semantic_model is None]
        if unified_records:
            if len(semantic_model_records) > 1:
                raise ValueError(
                    "Configured unified semantic models cannot be combined with other semantic_models in a single request."
                )
            raise ValueError(
                f"Configured unified semantic model '{unified_records[0].name}' "
                "cannot be queried directly; select its source semantic_models instead."
            )
        semantic_query = SemanticQuery(
            measures=self._host._normalize_semantic_members(
                members=measures,
                semantic_models=semantic_model_records,
            ),
            dimensions=self._host._normalize_semantic_members(
                members=dimensions,
                semantic_models=semantic_model_records,
            ),
            filters=self._host._normalize_semantic_filters_for_models(
                semantic_models=semantic_model_records,
                filters=filters,
            ),
            timeDimensions=self._host._normalize_time_dimensions_for_models(
                semantic_models=semantic_model_records,
                time_dimensions=time_dimensions,
            ),
            order=self._host._normalize_order_for_models(
                semantic_models=semantic_model_records,
                order=order,
            ),
            limit=int(limit) if limit else None,
        )
        execution_query = semantic_query
        async with self._host._runtime_operation_scope() as uow:
            if len(semantic_model_records) == 1:
                semantic_model_record = semantic_model_records[0]
                result = await self._host._runtime_host.query_semantic(
                    workspace_id=self._host.context.workspace_id,
                    semantic_model_id=semantic_model_record.id,
                    semantic_query=execution_query,
                )
                semantic_model_ids = [semantic_model_record.id]
                semantic_model_id = semantic_model_record.id
                connector_id = None
            else:
                configured_unified = self._host._resolve_configured_unified_model(
                    semantic_models=semantic_model_records,
                )
                execution_query = self._host._rewrite_semantic_query_for_unified_execution(
                    semantic_query=semantic_query,
                    semantic_models=semantic_model_records,
                    configured_unified=configured_unified,
                )
                result = await self._host._runtime_host.query_unified_semantic(
                    workspace_id=self._host.context.workspace_id,
                    semantic_model_ids=[record.id for record in semantic_model_records],
                    semantic_query=execution_query,
                    source_models=(configured_unified.source_models if configured_unified is not None else None),
                    relationships=(configured_unified.relationships if configured_unified is not None else None),
                    metrics=(configured_unified.metrics if configured_unified is not None else None),
                )
                semantic_model_ids = list(result.response.semantic_model_ids)
                semantic_model_id = None
                connector_id = result.response.connector_id
            if uow is not None:
                await uow.commit()

        rows = self._host._normalize_semantic_rows(
            rows=result.response.data,
            semantic_models=semantic_model_records,
        )
        output_fields = [
            *self._host._display_semantic_members(dimensions or []),
            *[self._host._display_time_dimension(item) for item in (time_dimensions or [])],
            *self._host._display_semantic_members(measures or []),
        ]
        response_payload = {
            "rows": rows,
            "columns": self._host._columns_from_rows(rows, fallback_names=output_fields),
            "row_count": len(rows),
            "annotations": list(result.response.annotations or []),
            "metadata": list(result.response.metadata or []),
            "generated_sql": result.compiled_sql,
            "semantic_model_ids": semantic_model_ids,
        }
        if semantic_model_id is not None:
            response_payload["semantic_model_id"] = semantic_model_id
        if connector_id is not None:
            response_payload["connector_id"] = connector_id
        return response_payload

    async def refresh_semantic_vector_search(self, *args: Any, **kwargs: Any) -> Any:
        async with self._host._runtime_operation_scope() as uow:
            result = await self._host._runtime_host.refresh_semantic_vector_search(*args, **kwargs)
            if uow is not None:
                await uow.commit()
            return result

    async def search_semantic_vectors(self, *args: Any, **kwargs: Any) -> Any:
        async with self._host._runtime_operation_scope() as uow:
            result = await self._host._runtime_host.search_semantic_vectors(*args, **kwargs)
            if uow is not None:
                await uow.commit()
            return result
