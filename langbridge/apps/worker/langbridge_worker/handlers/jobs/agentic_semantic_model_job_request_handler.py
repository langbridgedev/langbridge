from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from langbridge.apps.worker.langbridge_worker.handlers.jobs.job_event_emitter import (
    BrokerJobEventEmitter,
)
from langbridge.apps.worker.langbridge_worker.secrets import SecretProviderRegistry
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse
from langbridge.packages.common.langbridge_common.contracts.jobs.agentic_semantic_model_job import (
    CreateAgenticSemanticModelJobRequest,
)
from langbridge.packages.common.langbridge_common.db.job import JobRecord, JobStatus
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.interfaces.agent_events import (
    AgentEventVisibility,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.common.langbridge_common.repositories.llm_connection_repository import (
    LLMConnectionRepository,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import (
    SemanticModelRepository,
)
from langbridge.packages.connectors.langbridge_connectors.api import (
    ConnectorRuntimeTypeSqlDialectMap,
    SqlConnector,
    SqlConnectorFactory,
    get_connector_config_factory,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType
from langbridge.packages.messaging.langbridge_messaging.broker.base import MessageBroker
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.agentic_semantic_model_job import (
    AgenticSemanticModelJobRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler
from langbridge.packages.orchestrator.langbridge_orchestrator.llm.provider import create_provider
from langbridge.packages.semantic.langbridge_semantic.loader import (
    SemanticModelError,
    load_semantic_model,
)

TYPE_NUMERIC = {"number", "decimal", "numeric", "int", "integer", "float", "double", "real"}
TYPE_BOOLEAN = {"boolean", "bool"}
TYPE_DATE = {"date", "datetime", "timestamp", "time"}


class AgenticSemanticModelJobRequestHandler(BaseMessageHandler):
    message_type: MessageType = MessageType.AGENTIC_SEMANTIC_MODEL_JOB_REQUEST

    def __init__(
        self,
        job_repository: JobRepository,
        semantic_model_repository: SemanticModelRepository,
        connector_repository: ConnectorRepository,
        llm_repository: LLMConnectionRepository,
        message_broker: MessageBroker,
        secret_provider_registry: SecretProviderRegistry | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._job_repository = job_repository
        self._semantic_model_repository = semantic_model_repository
        self._connector_repository = connector_repository
        self._llm_repository = llm_repository
        self._message_broker = message_broker
        self._secret_provider_registry = secret_provider_registry or SecretProviderRegistry()
        self._sql_connector_factory = SqlConnectorFactory()

    async def handle(self, payload: AgenticSemanticModelJobRequestMessage) -> None:
        self._logger.info("Received agentic semantic model job request %s", payload.job_id)
        job_record = await self._job_repository.get_by_id(payload.job_id)
        if job_record is None:
            raise BusinessValidationError(f"Job with ID {payload.job_id} does not exist.")
        if job_record.status in {JobStatus.succeeded, JobStatus.failed, JobStatus.cancelled}:
            self._logger.info("Job %s already terminal (%s).", job_record.id, job_record.status)
            return None

        event_emitter = BrokerJobEventEmitter(
            job_record=job_record,
            broker_client=self._message_broker,
            logger=self._logger,
        )
        job_record.status = JobStatus.running
        job_record.progress = 5
        job_record.status_message = "Agentic semantic model generation started."
        if job_record.started_at is None:
            job_record.started_at = datetime.now(timezone.utc)
        await event_emitter.emit(
            event_type="AgenticSemanticModelStarted",
            message="Agentic semantic model generation started.",
            visibility=AgentEventVisibility.public,
            source="worker",
            details={"job_id": str(job_record.id)},
        )

        try:
            request = self._parse_job_payload(job_record)
            connector_response, sql_connector = await self._load_sql_connector(request)

            job_record.progress = 25
            job_record.status_message = "Resolving selected tables and columns."
            table_blueprints = await self._build_selected_table_blueprints(
                request=request,
                sql_connector=sql_connector,
            )

            job_record.progress = 55
            job_record.status_message = "Building semantic YAML."
            payload_model, warnings = self._build_payload_from_table_blueprints(
                connector_name=connector_response.name,
                table_blueprints=table_blueprints,
                question_prompts=request.question_prompts,
            )
            llm_rationale, llm_warnings = await self._augment_relationships_with_llm(
                request=request,
                payload_model=payload_model,
                table_blueprints=table_blueprints,
            )
            warnings.extend(llm_warnings)
            if request.include_sample_values:
                warnings.append("include_sample_values is not currently supported by the runtime generator.")

            yaml_text = self._render_and_validate_yaml(payload_model, table_blueprints)

            semantic_model_entry = await self._semantic_model_repository.get_for_scope(
                model_id=request.semantic_model_id,
                organization_id=request.organisation_id,
            )
            if semantic_model_entry is None:
                raise BusinessValidationError("Draft semantic model not found.")
            semantic_model_entry.content_yaml = yaml_text
            semantic_model_entry.content_json = json.dumps(payload_model)
            semantic_model_entry.updated_at = datetime.now(timezone.utc)

            rationale_summary = (
                llm_rationale
                or f"Generated a draft semantic model from {len(table_blueprints)} tables and {len(request.question_prompts)} question prompts."
            )
            job_record.result = {
                "result": {
                    "semantic_model_id": str(request.semantic_model_id),
                    "yaml_text": yaml_text,
                    "rationale_summary": rationale_summary,
                    "warnings": warnings,
                },
                "summary": "Agentic semantic model draft generated.",
            }
            job_record.status = JobStatus.succeeded
            job_record.progress = 100
            job_record.status_message = "Agentic semantic model generation completed."
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = None
            await event_emitter.emit(
                event_type="AgenticSemanticModelCompleted",
                message="Agentic semantic model generation completed.",
                visibility=AgentEventVisibility.public,
                source="worker",
                details={"semantic_model_id": str(request.semantic_model_id), "warning_count": len(warnings)},
            )
        except Exception as exc:  # pragma: no cover - defensive background guard
            self._logger.exception("Agentic semantic model job %s failed: %s", job_record.id, exc)
            job_record.status = JobStatus.failed
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.status_message = "Agentic semantic model generation failed."
            job_record.error = {"message": str(exc)}
            await event_emitter.emit(
                event_type="AgenticSemanticModelFailed",
                message="Agentic semantic model generation failed.",
                visibility=AgentEventVisibility.public,
                source="worker",
                details={"job_id": str(job_record.id), "error": str(exc)},
            )
        return None

    def _parse_job_payload(self, job_record: JobRecord) -> CreateAgenticSemanticModelJobRequest:
        raw_payload = job_record.payload
        if isinstance(raw_payload, str):
            try:
                payload_data = json.loads(raw_payload)
            except json.JSONDecodeError as exc:
                raise BusinessValidationError(f"Job payload for {job_record.id} is not valid JSON.") from exc
        elif isinstance(raw_payload, dict):
            payload_data = raw_payload
        else:
            raise BusinessValidationError(
                f"Job payload for {job_record.id} must be an object or JSON string."
            )
        try:
            return CreateAgenticSemanticModelJobRequest.model_validate(payload_data)
        except Exception as exc:
            raise BusinessValidationError(
                f"Job payload for {job_record.id} is invalid for agentic semantic model generation."
            ) from exc

    async def _load_sql_connector(
        self,
        request: CreateAgenticSemanticModelJobRequest,
    ) -> tuple[ConnectorResponse, SqlConnector]:
        connector = await self._connector_repository.get_by_id(request.connector_id)
        if connector is None:
            raise BusinessValidationError("Connector not found for agentic semantic model generation.")
        connector_response = ConnectorResponse.from_connector(
            connector,
            organization_id=request.organisation_id,
            project_id=request.project_id,
        )
        if connector_response.connector_type is None:
            raise BusinessValidationError("Connector type is required for semantic model generation.")

        connector_type = ConnectorRuntimeType(connector_response.connector_type.upper())
        connector_payload = self._resolve_connector_config(connector_response)
        sql_connector = await self._create_sql_connector(
            connector_type=connector_type,
            connector_config=connector_payload,
        )
        return connector_response, sql_connector

    async def _build_selected_table_blueprints(
        self,
        *,
        request: CreateAgenticSemanticModelJobRequest,
        sql_connector: SqlConnector,
    ) -> list[dict[str, Any]]:
        schemas = await sql_connector.fetch_schemas()
        table_lookup: dict[str, tuple[str, str]] = {}
        bare_lookup: dict[str, list[tuple[str, str]]] = {}
        for schema_name in schemas:
            table_names = await sql_connector.fetch_tables(schema_name)
            for table_name in table_names:
                reference = self._table_reference(schema_name, table_name).lower()
                table_lookup[reference] = (schema_name, table_name)
                bare_lookup.setdefault(table_name.lower(), []).append((schema_name, table_name))

        normalized_columns: dict[str, list[str]] = {}
        for table_key, columns in request.selected_columns.items():
            normalized_key = str(table_key).strip().lower()
            if not normalized_key:
                continue
            normalized_columns[normalized_key] = [str(column).strip() for column in columns if str(column).strip()]

        entity_registry: set[str] = set()
        table_blueprints: list[dict[str, Any]] = []
        for selected_table in request.selected_tables:
            normalized_reference = str(selected_table).strip().lower()
            if not normalized_reference:
                continue
            if normalized_reference in table_lookup:
                schema_name, table_name = table_lookup[normalized_reference]
            else:
                bare_name = normalized_reference.split(".")[-1]
                candidates = bare_lookup.get(bare_name, [])
                if len(candidates) != 1:
                    raise BusinessValidationError(
                        f"Selected table '{selected_table}' is unknown or ambiguous."
                    )
                schema_name, table_name = candidates[0]

            columns_metadata = await sql_connector.fetch_columns(schema_name, table_name)
            column_lookup = {column.name.lower(): column for column in columns_metadata}
            selected_column_names = (
                normalized_columns.get(normalized_reference)
                or normalized_columns.get(self._table_reference(schema_name, table_name).lower())
                or normalized_columns.get(table_name.lower())
            )
            if not selected_column_names:
                selected_column_names = [column.name for column in columns_metadata]

            selected_column_metadata = []
            for column_name in selected_column_names:
                column = column_lookup.get(column_name.lower())
                if column is None:
                    raise BusinessValidationError(
                        f"Column '{column_name}' is not available on table '{schema_name}.{table_name}'."
                    )
                selected_column_metadata.append(column)

            foreign_keys = await sql_connector.fetch_foreign_keys(schema_name, table_name)
            table_blueprints.append(
                {
                    "entity_name": self._build_entity_name(
                        schema=schema_name,
                        table_name=table_name,
                        registry=entity_registry,
                    ),
                    "schema": schema_name,
                    "table_name": table_name,
                    "table_reference": self._table_reference(schema_name, table_name),
                    "columns": selected_column_metadata,
                    "foreign_keys": foreign_keys,
                }
            )

        if not table_blueprints:
            raise BusinessValidationError("No valid table selections were provided.")
        return table_blueprints

    def _build_payload_from_table_blueprints(
        self,
        *,
        connector_name: str,
        table_blueprints: list[dict[str, Any]],
        question_prompts: list[str],
    ) -> tuple[dict[str, Any], list[str]]:
        warnings: list[str] = []
        tables_payload: dict[str, Any] = {}
        relationship_payload: list[dict[str, Any]] = []
        relationship_names: set[str] = set()
        table_reference_lookup = {
            blueprint["table_reference"].lower(): blueprint
            for blueprint in table_blueprints
        }

        for blueprint in table_blueprints:
            dimensions: list[dict[str, Any]] = []
            measures: list[dict[str, Any]] = []
            for column in blueprint["columns"]:
                mapped_type = self._map_column_type(getattr(column, "data_type", "string"))
                is_primary_key = bool(getattr(column, "is_primary_key", False))
                is_identifier = column.name.lower() == "id" or column.name.lower().endswith("_id")
                if mapped_type in {"integer", "decimal", "float"} and not is_identifier and not is_primary_key:
                    measures.append(
                        {
                            "name": column.name,
                            "expression": column.name,
                            "type": mapped_type,
                            "aggregation": "sum",
                            "description": f"Aggregate {column.name} from {blueprint['table_name']}",
                        }
                    )
                else:
                    dimensions.append(
                        {
                            "name": column.name,
                            "expression": column.name,
                            "type": mapped_type,
                            "primary_key": is_primary_key,
                            "description": f"Column {column.name} from {blueprint['table_name']}",
                        }
                    )

            if not dimensions and measures:
                first_measure = measures.pop(0)
                dimensions.append(
                    {
                        "name": first_measure["name"],
                        "expression": first_measure["expression"],
                        "type": first_measure["type"],
                        "primary_key": False,
                        "description": first_measure["description"],
                    }
                )
                warnings.append(
                    f"Table '{blueprint['table_reference']}' had only numeric columns; converted one to a dimension."
                )

            tables_payload[blueprint["entity_name"]] = {
                "schema": blueprint["schema"],
                "name": blueprint["table_name"],
                "description": f"Table {blueprint['table_name']} from connector {connector_name}",
                "dimensions": dimensions or None,
                "measures": measures or None,
            }

        for blueprint in table_blueprints:
            source_entity = blueprint["entity_name"]
            for foreign_key in blueprint["foreign_keys"]:
                target_reference = self._table_reference(foreign_key.schema, foreign_key.table).lower()
                target_blueprint = table_reference_lookup.get(target_reference)
                if target_blueprint is None:
                    continue
                target_entity = target_blueprint["entity_name"]
                relationship_name = f"{source_entity}_to_{target_entity}"
                if relationship_name in relationship_names:
                    continue
                relationship_names.add(relationship_name)
                relationship_payload.append(
                    {
                        "name": relationship_name,
                        "from_": source_entity,
                        "to": target_entity,
                        "type": "many_to_one",
                        "join_on": f"{source_entity}.{foreign_key.column} = {target_entity}.{foreign_key.foreign_key}",
                    }
                )

        if not relationship_payload:
            warnings.append("No relationships were inferred from selected tables.")

        payload = {
            "version": "1.0",
            "connector": connector_name,
            "description": (
                f"Draft semantic model generated from prompts: {', '.join(question_prompts[:3])}"
                if question_prompts
                else f"Draft semantic model generated from connector {connector_name}"
            ),
            "tables": tables_payload,
            "relationships": relationship_payload or None,
        }
        return payload, warnings

    async def _augment_relationships_with_llm(
        self,
        *,
        request: CreateAgenticSemanticModelJobRequest,
        payload_model: dict[str, Any],
        table_blueprints: list[dict[str, Any]],
    ) -> tuple[str | None, list[str]]:
        warnings: list[str] = []
        connections = await self._llm_repository.get_all(
            organization_id=request.organisation_id,
            project_id=request.project_id,
        )
        active_connections = [connection for connection in connections if bool(getattr(connection, "is_active", True))]
        if not active_connections:
            return None, ["No active LLM connection was available; heuristic relationships were used."]
        try:
            provider = create_provider(active_connections[0])
            table_lines = []
            for blueprint in table_blueprints:
                columns = ", ".join(column.name for column in blueprint["columns"])
                table_lines.append(f"- {blueprint['table_reference']}: {columns}")
            prompt = (
                "Suggest up to 5 additional semantic model joins as strict JSON.\n"
                "Response shape: {\"rationale\": string, \"relationships\": [{\"from_table\": string, \"to_table\": string, \"from_column\": string, \"to_column\": string}]}.\n"
                f"Tables:\n{chr(10).join(table_lines)}\n"
                f"Question themes: {', '.join(request.question_prompts)}"
            )
            completion = await provider.acomplete(prompt, temperature=0.0, max_tokens=600)
            parsed = self._extract_json_object(completion)
            relationships = parsed.get("relationships")
            if isinstance(relationships, list):
                existing_names = {
                    relationship.get("name")
                    for relationship in payload_model.get("relationships") or []
                    if isinstance(relationship, dict)
                }
                table_lookup = {
                    blueprint["table_reference"].lower(): blueprint["entity_name"]
                    for blueprint in table_blueprints
                }
                for suggestion in relationships[:5]:
                    if not isinstance(suggestion, dict):
                        continue
                    from_table = str(suggestion.get("from_table") or "").strip().lower()
                    to_table = str(suggestion.get("to_table") or "").strip().lower()
                    from_column = str(suggestion.get("from_column") or "").strip()
                    to_column = str(suggestion.get("to_column") or "").strip()
                    from_entity = table_lookup.get(from_table)
                    to_entity = table_lookup.get(to_table)
                    if not from_entity or not to_entity or not from_column or not to_column:
                        continue
                    relationship_name = f"{from_entity}_to_{to_entity}"
                    if relationship_name in existing_names:
                        continue
                    existing_names.add(relationship_name)
                    payload_model.setdefault("relationships", [])
                    payload_model["relationships"].append(
                        {
                            "name": relationship_name,
                            "from_": from_entity,
                            "to": to_entity,
                            "type": "many_to_one",
                            "join_on": f"{from_entity}.{from_column} = {to_entity}.{to_column}",
                        }
                    )
            rationale = parsed.get("rationale")
            return (rationale.strip() if isinstance(rationale, str) and rationale.strip() else None), warnings
        except Exception as exc:
            warnings.append(f"LLM relationship enrichment failed: {exc}")
            return None, warnings

    def _render_and_validate_yaml(
        self,
        payload: dict[str, Any],
        table_blueprints: list[dict[str, Any]],
    ) -> str:
        yaml_text = self._json_to_yaml(payload)
        try:
            model = load_semantic_model(yaml_text)
        except SemanticModelError as exc:
            raise BusinessValidationError(f"Generated semantic model YAML failed validation: {exc}") from exc

        relationship_names = [relationship.name for relationship in model.relationships or []]
        if len(relationship_names) != len(set(relationship_names)):
            raise BusinessValidationError("Generated semantic model contains duplicate relationship names.")

        for blueprint in table_blueprints:
            table = model.tables.get(blueprint["entity_name"])
            if table is None:
                raise BusinessValidationError(
                    f"Generated semantic model is missing selected table '{blueprint['entity_name']}'."
                )
            selected_column_names = {column.name for column in blueprint["columns"]}
            mapped_column_names = {
                dimension.name for dimension in table.dimensions or []
            } | {
                measure.name for measure in table.measures or []
            }
            if selected_column_names != mapped_column_names:
                raise BusinessValidationError(
                    f"Generated semantic model column mapping mismatch for '{blueprint['table_reference']}'."
                )
        return yaml_text

    async def _create_sql_connector(
        self,
        *,
        connector_type: ConnectorRuntimeType,
        connector_config: dict[str, Any],
    ) -> SqlConnector:
        dialect = ConnectorRuntimeTypeSqlDialectMap.get(connector_type)
        if dialect is None:
            raise BusinessValidationError(
                f"Connector type {connector_type.value} does not support SQL operations."
            )
        config_factory = get_connector_config_factory(connector_type)
        config_instance = config_factory.create(connector_config.get("config", {}))
        sql_connector = self._sql_connector_factory.create_sql_connector(
            dialect,
            config_instance,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    def _resolve_connector_config(self, connector: ConnectorResponse) -> dict[str, Any]:
        resolved_payload = dict(connector.config or {})
        runtime_config = dict(resolved_payload.get("config") or {})
        if connector.connection_metadata is not None:
            metadata = connector.connection_metadata.model_dump(exclude_none=True)
            extra = metadata.pop("extra", {})
            for key, value in metadata.items():
                runtime_config.setdefault(key, value)
            if isinstance(extra, dict):
                for key, value in extra.items():
                    if value is not None:
                        runtime_config.setdefault(key, value)
        for secret_name, secret_ref in connector.secret_references.items():
            try:
                runtime_config[secret_name] = self._secret_provider_registry.resolve(secret_ref)
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                raise BusinessValidationError(
                    f"Unable to resolve connector secret '{secret_name}'."
                ) from exc
        resolved_payload["config"] = runtime_config
        return resolved_payload

    @staticmethod
    def _extract_json_object(content: str) -> dict[str, Any]:
        text = (content or "").strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?", "", text).strip()
            text = re.sub(r"```$", "", text).strip()
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("LLM response did not include a JSON object.")
        parsed = json.loads(text[start : end + 1])
        if not isinstance(parsed, dict):
            raise ValueError("LLM response JSON payload must be an object.")
        return parsed

    @staticmethod
    def _json_to_yaml(payload: dict[str, Any]) -> str:
        import yaml

        return yaml.safe_dump(payload, sort_keys=False)

    @staticmethod
    def _table_reference(schema: str, table_name: str) -> str:
        schema_value = (schema or "").strip()
        table_value = (table_name or "").strip()
        if schema_value:
            return f"{schema_value}.{table_value}"
        return table_value

    @staticmethod
    def _build_entity_name(*, schema: str, table_name: str, registry: set[str]) -> str:
        base_name = re.sub(r"[^a-zA-Z0-9_]+", "_", f"{schema}_{table_name}".strip("_")).lower()
        root_name = base_name or "table"
        candidate = root_name
        suffix = 2
        while candidate in registry:
            candidate = f"{root_name}_{suffix}"
            suffix += 1
        registry.add(candidate)
        return candidate

    @staticmethod
    def _map_column_type(data_type: str) -> str:
        normalized = (data_type or "").lower()
        if any(token in normalized for token in TYPE_NUMERIC):
            if "int" in normalized and "point" not in normalized:
                return "integer"
            if any(token in normalized for token in ("double", "float")):
                return "float"
            return "decimal"
        if any(token == normalized or token in normalized for token in TYPE_BOOLEAN):
            return "boolean"
        if any(token == normalized or token in normalized for token in TYPE_DATE) or any(
            token in normalized for token in ("date", "time")
        ):
            return "date"
        return "string"
