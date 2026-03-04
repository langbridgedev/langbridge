import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from langbridge.apps.worker.langbridge_worker.handlers.jobs.job_event_emitter import (
    BrokerJobEventEmitter,
)
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse
from langbridge.packages.common.langbridge_common.contracts.jobs.copilot_dashboard_job import (
    CreateCopilotDashboardJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticQueryResponse,
    UnifiedSemanticQueryResponse,
)
from langbridge.packages.common.langbridge_common.db.job import JobRecord, JobStatus
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.interfaces.agent_events import (
    AgentEventVisibility,
)
from langbridge.packages.common.langbridge_common.repositories.agent_repository import AgentRepository
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
    SqlConnector,
    SqlConnectorFactory,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType
from langbridge.packages.messaging.langbridge_messaging.broker.base import MessageBroker
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.jobs.copilot_dashboard import (
    CopilotDashboardRequestMessage,
)
from langbridge.packages.messaging.langbridge_messaging.handler import BaseMessageHandler
from langbridge.apps.worker.langbridge_worker.tools import FederatedQueryTool
from langbridge.packages.orchestrator.langbridge_orchestrator.definitions import AgentDefinitionModel
from langbridge.packages.orchestrator.langbridge_orchestrator.llm.provider import create_provider
from langbridge.packages.orchestrator.langbridge_orchestrator.llm.provider.base import LLMProvider
from langbridge.packages.semantic.langbridge_semantic.loader import SemanticModelError, load_semantic_model
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery, SemanticQueryEngine
from langbridge.apps.worker.langbridge_worker.semantic_query_execution_service import (
    SemanticQueryExecutionService,
    UnifiedModelConfig,
)


VALID_CHART_TYPES = {"table", "bar", "line", "pie"}
VALID_WIDGET_SIZES = {"small", "wide", "tall", "large"}
SQL_TOOL_NAMES = {"sql", "sql_analyst", "sql_analytics"}


@dataclass(frozen=True)
class _QueryRuntime:
    semantic_model_record: Any
    semantic_model: SemanticModel
    sql_connector: SqlConnector | None
    unified_config: UnifiedModelConfig | None


class _CopilotFilter(BaseModel):
    member: str
    operator: str = "equals"
    values: str | list[str] | None = ""


class _CopilotOrderBy(BaseModel):
    member: str
    direction: str = "desc"


class _CopilotWidgetPlan(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    title: str
    type: str = "bar"
    size: str = "small"
    dimensions: list[str] = Field(default_factory=list)
    measures: list[str] = Field(default_factory=list)
    filters: list[_CopilotFilter] = Field(default_factory=list)
    order_bys: list[_CopilotOrderBy] = Field(default_factory=list, alias="orderBys")
    limit: int = 500
    time_dimension: str = Field(default="", alias="timeDimension")
    time_grain: str = Field(default="", alias="timeGrain")
    time_range_preset: str = Field(default="", alias="timeRangePreset")
    chart_x: str = Field(default="", alias="chartX")
    chart_y: str = Field(default="", alias="chartY")


class _CopilotDashboardPlan(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    summary: str | None = None
    global_filters: list[_CopilotFilter] = Field(default_factory=list, alias="globalFilters")
    widgets: list[_CopilotWidgetPlan] = Field(default_factory=list)


class CopilotDashboardRequestHandler(BaseMessageHandler):
    message_type: MessageType = MessageType.COPILOT_DASHBOARD_REQUEST

    def __init__(
        self,
        job_repository: JobRepository,
        agent_definition_repository: AgentRepository,
        llm_repository: LLMConnectionRepository,
        semantic_model_repository: SemanticModelRepository,
        connector_repository: ConnectorRepository,
        message_broker: MessageBroker,
        federated_query_tool: FederatedQueryTool | None = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self._job_repository = job_repository
        self._agent_definition_repository = agent_definition_repository
        self._llm_repository = llm_repository
        self._semantic_model_repository = semantic_model_repository
        self._connector_repository = connector_repository
        self._message_broker = message_broker
        self._federated_query_tool = federated_query_tool
        self._engine = SemanticQueryEngine()
        self._sql_connector_factory = SqlConnectorFactory()
        self._query_execution_service = SemanticQueryExecutionService(
            semantic_model_repository=self._semantic_model_repository,
            federated_query_tool=self._federated_query_tool,
            logger=self._logger,
        )

    async def handle(self, payload: CopilotDashboardRequestMessage) -> None:
        self._logger.info("Received BI copilot dashboard job %s", payload.job_id)
        job_record = await self._job_repository.get_by_id(payload.job_id)
        if job_record is None:
            raise BusinessValidationError(f"Job with ID {payload.job_id} does not exist.")

        if job_record.status in {
            JobStatus.succeeded,
            JobStatus.failed,
            JobStatus.cancelled,
        }:
            self._logger.info(
                "Job %s already in terminal state %s; skipping.",
                job_record.id,
                job_record.status,
            )
            return None

        event_emitter = BrokerJobEventEmitter(
            job_record=job_record,
            broker_client=self._message_broker,
            logger=self._logger,
        )
        job_record.status = JobStatus.running
        job_record.progress = 5
        job_record.status_message = "BI copilot job started."
        if job_record.started_at is None:
            job_record.started_at = datetime.now(timezone.utc)

        await event_emitter.emit(
            event_type="CopilotDashboardStarted",
            message="BI copilot job started.",
            visibility=AgentEventVisibility.public,
            source="worker",
            details={"job_id": str(job_record.id)},
        )

        try:
            request = self._parse_job_payload(job_record)
            agent_definition, llm_provider = await self._load_agent_and_provider(request)
            query_runtime = await self._load_query_runtime(request)
            self._validate_agent_semantic_binding(agent_definition, request.semantic_model_id)

            job_record.progress = 25
            job_record.status_message = "Generating dashboard plan."
            await event_emitter.emit(
                event_type="CopilotDashboardPlanning",
                message="Generating dashboard plan.",
                visibility=AgentEventVisibility.public,
                source="worker",
            )

            dashboard_plan = await self._build_dashboard_plan(
                llm_provider=llm_provider,
                request=request,
                semantic_model=query_runtime.semantic_model,
            )

            global_filters = self._normalize_filters(dashboard_plan.global_filters)
            widgets = self._normalize_widgets(
                dashboard_plan.widgets,
                max_widgets=request.max_widgets,
            )
            if not widgets:
                widgets = self._fallback_widgets_from_model(query_runtime.semantic_model)

            job_record.progress = 40
            job_record.status_message = "Building widget previews."
            await event_emitter.emit(
                event_type="CopilotDashboardPreparingWidgets",
                message="Building widget previews.",
                visibility=AgentEventVisibility.public,
                source="worker",
                details={"widget_count": len(widgets)},
            )

            widget_payloads: list[dict[str, Any]] = []
            preview_descriptions: list[str] = []
            preview_enabled = bool(request.generate_previews)

            for index, widget in enumerate(widgets):
                progress = 40 + int(((index + 1) / max(len(widgets), 1)) * 50)
                job_record.progress = max(job_record.progress, min(progress, 95))
                if preview_enabled:
                    job_record.status_message = f"Running query for widget {index + 1} of {len(widgets)}."
                    await event_emitter.emit(
                        event_type="CopilotDashboardWidgetPreviewStarted",
                        message=f"Running widget {index + 1}/{len(widgets)} query.",
                        visibility=AgentEventVisibility.public,
                        source="worker",
                        details={"widget_id": widget["id"], "title": widget["title"]},
                    )
                    try:
                        query_result = await self._execute_widget_query(
                            request=request,
                            semantic_model_record_id=query_runtime.semantic_model_record.id,
                            sql_connector=query_runtime.sql_connector,
                            semantic_model=query_runtime.semantic_model,
                            unified_config=query_runtime.unified_config,
                            widget=widget,
                            global_filters=global_filters,
                        )
                        row_count = len(query_result.data)
                        preview_descriptions.append(f"{widget['title']}: {row_count} rows")
                        widget_payloads.append(
                            {
                                **widget,
                                "queryResult": query_result.model_dump(mode="json"),
                                "isLoading": False,
                                "jobId": None,
                                "jobStatus": "succeeded",
                                "progress": 100,
                                "statusMessage": f"Preview ready ({row_count} rows).",
                                "error": None,
                            }
                        )
                    except Exception as exc:
                        preview_descriptions.append(f"{widget['title']}: failed ({exc})")
                        widget_payloads.append(
                            {
                                **widget,
                                "queryResult": None,
                                "isLoading": False,
                                "jobId": None,
                                "jobStatus": "failed",
                                "progress": 100,
                                "statusMessage": "Preview failed.",
                                "error": str(exc),
                            }
                        )
                else:
                    widget_payloads.append(
                        {
                            **widget,
                            "queryResult": None,
                            "isLoading": False,
                            "jobId": None,
                            "jobStatus": None,
                            "progress": 0,
                            "statusMessage": "Prepared by copilot. Run to fetch data.",
                            "error": None,
                        }
                    )

            final_summary = self._build_summary(
                dashboard_plan.summary,
                preview_descriptions,
                preview_enabled=preview_enabled,
            )
            generated_at = datetime.now(timezone.utc).isoformat()

            job_record.result = {
                "result": {
                    "summary": final_summary,
                    "globalFilters": global_filters,
                    "widgets": widget_payloads,
                    "generatedAt": generated_at,
                },
                "summary": final_summary,
            }
            job_record.status = JobStatus.succeeded
            job_record.progress = 100
            job_record.status_message = "BI copilot dashboard completed."
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.error = None

            await event_emitter.emit(
                event_type="CopilotDashboardCompleted",
                message="BI copilot dashboard completed.",
                visibility=AgentEventVisibility.public,
                source="worker",
                details={
                    "widget_count": len(widget_payloads),
                    "generate_previews": preview_enabled,
                },
            )
        except Exception as exc:  # pragma: no cover - defensive background worker guard
            self._logger.exception("BI copilot dashboard job %s failed: %s", job_record.id, exc)
            job_record.status = JobStatus.failed
            job_record.finished_at = datetime.now(timezone.utc)
            job_record.status_message = "BI copilot dashboard failed."
            job_record.error = {"message": str(exc)}
            await event_emitter.emit(
                event_type="CopilotDashboardFailed",
                message="BI copilot dashboard failed.",
                visibility=AgentEventVisibility.public,
                source="worker",
                details={"job_id": str(job_record.id), "error": str(exc)},
            )

        return None

    def _parse_job_payload(self, job_record: JobRecord) -> CreateCopilotDashboardJobRequest:
        raw_payload = job_record.payload
        if isinstance(raw_payload, str):
            try:
                payload_data = json.loads(raw_payload)
            except json.JSONDecodeError as exc:
                raise BusinessValidationError(
                    f"Job payload for {job_record.id} is not valid JSON."
                ) from exc
        elif isinstance(raw_payload, dict):
            payload_data = raw_payload
        else:
            raise BusinessValidationError(
                f"Job payload for {job_record.id} must be an object or JSON string."
            )

        try:
            return CreateCopilotDashboardJobRequest.model_validate(payload_data)
        except ValidationError as exc:
            raise BusinessValidationError(
                f"Job payload for {job_record.id} is invalid for BI copilot execution."
            ) from exc

    async def _load_agent_and_provider(
        self,
        request: CreateCopilotDashboardJobRequest,
    ) -> tuple[AgentDefinitionModel, LLMProvider]:
        agent = await self._agent_definition_repository.get_by_id(request.agent_definition_id)
        if agent is None:
            raise BusinessValidationError("Agent definition not found.")
        if not bool(getattr(agent, "is_active", True)):
            raise BusinessValidationError("Selected agent is not active.")

        try:
            definition = AgentDefinitionModel.model_validate(agent.definition)
        except ValidationError as exc:
            raise BusinessValidationError("Agent definition is invalid for BI copilot.") from exc

        if not definition.features.bi_copilot_enabled:
            raise BusinessValidationError("Selected agent does not have BI copilot enabled.")

        llm_connection = await self._llm_repository.get_by_id(agent.llm_connection_id)
        if llm_connection is None:
            raise BusinessValidationError("LLM connection for the selected agent was not found.")

        return definition, create_provider(llm_connection)

    def _validate_agent_semantic_binding(
        self,
        definition: AgentDefinitionModel,
        semantic_model_id: uuid.UUID,
    ) -> None:
        bound_model_ids: set[str] = set()
        for tool in list(definition.tools or []):
            tool_name = str(tool.name or "").strip().lower()
            tool_type_value = str(getattr(tool, "tool_type", "") or "").strip().lower()
            is_sql_tool = tool_type_value == "sql" or tool_name in SQL_TOOL_NAMES
            if not is_sql_tool:
                continue

            config = tool.config
            definition_id: str | None = None
            if isinstance(config, dict):
                raw_definition_id = config.get("definition_id")
                if raw_definition_id is not None:
                    definition_id = str(raw_definition_id)
            elif hasattr(config, "definition_id"):
                definition_id = str(getattr(config, "definition_id"))

            if definition_id:
                bound_model_ids.add(definition_id)

        if str(semantic_model_id) not in bound_model_ids:
            raise BusinessValidationError(
                "Selected agent does not have the requested semantic model configured as a SQL data source."
            )

    async def _load_query_runtime(
        self,
        request: CreateCopilotDashboardJobRequest,
    ) -> _QueryRuntime:
        semantic_model_record = await self._semantic_model_repository.get_for_scope(
            model_id=request.semantic_model_id,
            organization_id=request.organisation_id,
        )
        if semantic_model_record is None:
            raise BusinessValidationError("Semantic model not found.")

        try:
            semantic_model = load_semantic_model(semantic_model_record.content_yaml)
        except SemanticModelError as exc:
            raise BusinessValidationError(f"Semantic model failed validation: {exc}") from exc

        unified_config = SemanticQueryExecutionService.parse_unified_model_config_from_record(
            semantic_model_record
        )
        if unified_config is not None:
            return _QueryRuntime(
                semantic_model_record=semantic_model_record,
                semantic_model=semantic_model,
                sql_connector=None,
                unified_config=unified_config,
            )

        connector = await self._connector_repository.get_by_id(semantic_model_record.connector_id)
        if connector is None:
            raise BusinessValidationError("Connector not found for semantic model.")

        connector_response = ConnectorResponse.from_connector(
            connector,
            organization_id=request.organisation_id,
            project_id=request.project_id,
        )
        connector_type_raw = connector_response.connector_type
        if connector_type_raw is None:
            raise BusinessValidationError("Connector type is required for BI copilot.")
        connector_type = ConnectorRuntimeType(connector_type_raw.upper())
        sql_connector = await self._create_sql_connector(
            connector_type=connector_type,
            connector_config=connector_response.config or {},
        )
        if not isinstance(sql_connector, SqlConnector):
            raise BusinessValidationError("Only SQL connectors are supported for BI copilot.")

        return _QueryRuntime(
            semantic_model_record=semantic_model_record,
            semantic_model=semantic_model,
            sql_connector=sql_connector,
            unified_config=None,
        )

    async def _build_dashboard_plan(
        self,
        *,
        llm_provider: LLMProvider,
        request: CreateCopilotDashboardJobRequest,
        semantic_model: SemanticModel,
    ) -> _CopilotDashboardPlan:
        system_prompt = """
You are a BI copilot that designs analytic dashboards.
Return ONLY JSON with this shape:
{
  "summary": "string",
  "globalFilters": [{"member":"schema.table.field","operator":"equals","values":"a,b"}],
  "widgets": [{
    "title":"string",
    "type":"table|bar|line|pie",
    "size":"small|wide|tall|large",
    "dimensions":["schema.table.dimension"],
    "measures":["schema.table.measure"],
    "filters":[{"member":"schema.table.field","operator":"equals","values":"x"}],
    "orderBys":[{"member":"schema.table.field","direction":"asc|desc"}],
    "limit":500,
    "timeDimension":"schema.table.date_field",
    "timeGrain":"day|week|month|quarter|year|",
    "timeRangePreset":"today|yesterday|last_7_days|last_30_days|month_to_date|year_to_date|",
    "chartX":"schema.table.dimension",
    "chartY":"schema.table.measure"
  }]
}
Always use members from the provided semantic model only.
Keep widgets practical and diverse, max 6 widgets.
""".strip()

        semantic_summary = self._render_semantic_model_summary(semantic_model)
        current_dashboard_text = ""
        if isinstance(request.current_dashboard, dict) and request.current_dashboard:
            current_dashboard_text = json.dumps(request.current_dashboard, indent=2, default=str)

        user_prompt = (
            f"Dashboard instructions:\n{request.instructions.strip()}\n\n"
            f"Dashboard name (optional): {request.dashboard_name or ''}\n\n"
            f"Semantic model:\n{semantic_summary}\n\n"
            f"Current dashboard state:\n{current_dashboard_text or '{}'}\n"
        )

        response = await llm_provider.ainvoke(
            [SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)],
            temperature=0.2,
            # max_tokens=1800,
        )

        response_text = self._extract_response_text(response)
        plan = self._parse_plan_response(response_text)
        if plan is not None:
            return plan

        self._logger.warning("Falling back to deterministic BI copilot plan.")
        return _CopilotDashboardPlan(
            summary="Generated dashboard plan from semantic model defaults.",
            global_filters=[],
            widgets=self._fallback_plan_widgets(semantic_model),
        )

    @staticmethod
    def _extract_response_text(response: Any) -> str:
        if isinstance(response, BaseMessage):
            content = response.content
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                chunks: list[str] = []
                for item in content:
                    if isinstance(item, str):
                        chunks.append(item)
                    elif isinstance(item, dict):
                        text = item.get("text")
                        if isinstance(text, str):
                            chunks.append(text)
                return "\n".join(chunks).strip()
            return str(content)
        if isinstance(response, dict):
            return json.dumps(response)
        return str(response)

    def _parse_plan_response(self, raw: str) -> _CopilotDashboardPlan | None:
        cleaned = raw.strip()
        if not cleaned:
            return None

        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].strip()

        payload: dict[str, Any] | None = None
        try:
            candidate = json.loads(cleaned)
            if isinstance(candidate, dict):
                payload = candidate
        except json.JSONDecodeError:
            start = cleaned.find("{")
            end = cleaned.rfind("}")
            if start >= 0 and end > start:
                fragment = cleaned[start : end + 1]
                try:
                    candidate = json.loads(fragment)
                    if isinstance(candidate, dict):
                        payload = candidate
                except json.JSONDecodeError:
                    payload = None

        if payload is None:
            return None

        try:
            return _CopilotDashboardPlan.model_validate(payload)
        except ValidationError:
            return None

    def _normalize_widgets(
        self,
        widgets: list[_CopilotWidgetPlan],
        *,
        max_widgets: int,
    ) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        effective_max = max(1, min(int(max_widgets), 12))
        for index, widget in enumerate(widgets[:effective_max]):
            dimensions = self._clean_string_list(widget.dimensions)
            measures = self._clean_string_list(widget.measures)
            if not dimensions and not measures:
                continue

            chart_type = widget.type if widget.type in VALID_CHART_TYPES else "table"
            widget_size = widget.size if widget.size in VALID_WIDGET_SIZES else "small"
            chart_x = widget.chart_x.strip() if widget.chart_x else (dimensions[0] if dimensions else "")
            chart_y = widget.chart_y.strip() if widget.chart_y else (measures[0] if measures else "")
            limit = max(1, min(int(widget.limit or 500), 5000))

            normalized.append(
                {
                    "id": str(uuid.uuid4()),
                    "title": widget.title.strip() or f"Analysis {index + 1}",
                    "type": chart_type,
                    "size": widget_size,
                    "dimensions": dimensions,
                    "measures": measures,
                    "filters": self._normalize_filters(widget.filters),
                    "orderBys": self._normalize_order_bys(widget.order_bys),
                    "limit": limit,
                    "timeDimension": widget.time_dimension.strip() if widget.time_dimension else "",
                    "timeGrain": widget.time_grain.strip() if widget.time_grain else "",
                    "timeRangePreset": widget.time_range_preset.strip() if widget.time_range_preset else "",
                    "chartX": chart_x,
                    "chartY": chart_y,
                }
            )
        return normalized

    @staticmethod
    def _clean_string_list(values: list[str]) -> list[str]:
        seen: set[str] = set()
        cleaned: list[str] = []
        for value in values:
            item = str(value or "").strip()
            if not item or item in seen:
                continue
            seen.add(item)
            cleaned.append(item)
        return cleaned

    def _normalize_filters(self, filters: list[_CopilotFilter]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for index, filter_entry in enumerate(filters):
            member = str(filter_entry.member or "").strip()
            if not member:
                continue
            operator = str(filter_entry.operator or "equals").strip().lower() or "equals"
            values_raw = filter_entry.values
            if isinstance(values_raw, list):
                values = ",".join(str(value).strip() for value in values_raw if str(value).strip())
            elif isinstance(values_raw, str):
                values = values_raw.strip()
            else:
                values = ""
            normalized.append(
                {
                    "id": f"{member}-{index}",
                    "member": member,
                    "operator": operator,
                    "values": values,
                }
            )
        return normalized

    @staticmethod
    def _normalize_order_bys(order_bys: list[_CopilotOrderBy]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for index, order in enumerate(order_bys):
            member = str(order.member or "").strip()
            if not member:
                continue
            direction = str(order.direction or "desc").strip().lower()
            normalized.append(
                {
                    "id": f"order-{index}",
                    "member": member,
                    "direction": "asc" if direction == "asc" else "desc",
                }
            )
        return normalized

    async def _execute_widget_query(
        self,
        *,
        request: CreateCopilotDashboardJobRequest,
        semantic_model_record_id: uuid.UUID,
        sql_connector: SqlConnector | None,
        semantic_model: SemanticModel,
        unified_config: UnifiedModelConfig | None,
        widget: dict[str, Any],
        global_filters: list[dict[str, Any]],
    ) -> SemanticQueryResponse | UnifiedSemanticQueryResponse:
        query_payload = SemanticQueryExecutionService.build_widget_query_payload(
            widget=widget,
            global_filters=global_filters,
        )
        semantic_query = SemanticQuery.model_validate(query_payload)

        if unified_config is not None:
            execution = await self._query_execution_service.execute_unified_query(
                organization_id=request.organisation_id,
                project_id=request.project_id,
                semantic_query=semantic_query,
                semantic_model_ids=unified_config.semantic_model_ids,
                joins=unified_config.joins,
                metrics=unified_config.metrics,
            )
            return execution.response

        if sql_connector is None:
            raise BusinessValidationError(
                "A SQL connector is required for non-unified copilot preview execution."
            )

        rewrite_expression = None
        if sql_connector.EXPRESSION_REWRITE:
            rewrite_expression = getattr(sql_connector, "rewrite_expression", None)
            if rewrite_expression is None:
                raise BusinessValidationError(
                    "Semantic query translation failed: connector expression rewriter missing."
                )

        try:
            plan = self._engine.compile(
                semantic_query,
                semantic_model,
                dialect=sql_connector.DIALECT.value.lower(),
                rewrite_expression=rewrite_expression,
            )
        except Exception as exc:
            raise BusinessValidationError(f"Semantic query translation failed: {exc}") from exc

        query_result = await sql_connector.execute(plan.sql)
        data = self._engine.format_rows(query_result.columns, query_result.rows)
        return SemanticQueryResponse(
            id=uuid.uuid4(),
            organization_id=request.organisation_id,
            project_id=request.project_id,
            semantic_model_id=semantic_model_record_id,
            data=data,
            annotations=plan.annotations,
            metadata=plan.metadata,
        )

    async def _create_sql_connector(
        self,
        *,
        connector_type: ConnectorRuntimeType,
        connector_config: dict[str, Any],
    ) -> SqlConnector:
        return await self._query_execution_service.create_sql_connector(
            connector_type=connector_type,
            connector_config=connector_config,
            sql_connector_factory=self._sql_connector_factory,
            logger=self._logger,
        )

    @staticmethod
    def _build_summary(
        generated_summary: str | None,
        preview_descriptions: list[str],
        *,
        preview_enabled: bool,
    ) -> str:
        summary = (generated_summary or "").strip()
        if not summary:
            summary = "BI copilot generated a dashboard draft."
        if not preview_enabled:
            return summary
        if not preview_descriptions:
            return f"{summary} Previews were not available."
        return f"{summary} Preview results: {'; '.join(preview_descriptions)}."

    def _render_semantic_model_summary(self, semantic_model: SemanticModel) -> str:
        lines: list[str] = []
        for table_key, table in semantic_model.tables.items():
            prefix = f"{table.schema}.{table.name}"
            dimensions = [f"{prefix}.{dimension.name}" for dimension in list(table.dimensions or [])]
            measures = [f"{prefix}.{measure.name}" for measure in list(table.measures or [])]
            filters = [f"{prefix}.{name}" for name in (table.filters or {}).keys()]
            lines.append(f"Table: {table_key} ({prefix})")
            lines.append(f"- Dimensions: {', '.join(dimensions[:40]) or '(none)'}")
            lines.append(f"- Measures: {', '.join(measures[:40]) or '(none)'}")
            lines.append(f"- Filters: {', '.join(filters[:20]) or '(none)'}")

        if semantic_model.metrics:
            metric_names = sorted(semantic_model.metrics.keys())
            lines.append(f"Metrics: {', '.join(metric_names[:40])}")

        return "\n".join(lines)

    def _fallback_widgets_from_model(self, semantic_model: SemanticModel) -> list[dict[str, Any]]:
        return self._normalize_widgets(self._fallback_plan_widgets(semantic_model), max_widgets=2)

    @staticmethod
    def _fallback_plan_widgets(semantic_model: SemanticModel) -> list[_CopilotWidgetPlan]:
        for table in semantic_model.tables.values():
            prefix = f"{table.schema}.{table.name}"
            dimensions = [f"{prefix}.{dimension.name}" for dimension in list(table.dimensions or [])]
            measures = [f"{prefix}.{measure.name}" for measure in list(table.measures or [])]
            if dimensions or measures:
                return [
                    _CopilotWidgetPlan(
                        title="Overview",
                        type="table",
                        size="wide",
                        dimensions=dimensions[:2],
                        measures=measures[:2],
                        limit=500,
                        chartX=dimensions[0] if dimensions else "",
                        chartY=measures[0] if measures else "",
                    )
                ]
        return []
