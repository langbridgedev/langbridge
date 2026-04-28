import asyncio
import uuid
from types import SimpleNamespace

import pytest

from langbridge.runtime.events import CollectingAgentEventEmitter, normalize_agent_stream_stage
from langbridge.runtime.models import (
    CreateAgentJobRequest,
    DatasetColumnMetadata,
    DatasetMetadata,
    JobType,
    LLMConnectionSecret,
    LLMProvider as RuntimeLLMProvider,
    RuntimeAgentDefinition,
    RuntimeConversationMemoryCategory,
    RuntimeConversationMemoryItem,
    RuntimeMessageRole,
    RuntimeThread,
    RuntimeThreadMessage,
    RuntimeThreadState,
)
from langbridge.runtime.models.metadata import LifecycleState, ManagementMode
from langbridge.runtime.services.agents import AgentExecutionService


def _run(coro):
    return asyncio.run(coro)


class _FakeLLMProvider:
    def __init__(self) -> None:
        self.prompts = []

    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Create a chart specification for verified tabular data" in prompt:
            return (
                '{"chart_type":"pie","title":"Revenue by region","x":"region","y":"revenue",'
                '"series":null,"encoding":{},"rationale":"Pie chart requested for the verified result."}'
            )
        if "Decide Langbridge agent route" in prompt:
            return (
                '{"action":"direct","rationale":"Configured analyst can answer from thread context.",'
                '"agent_name":"analyst.commerce_sql","task_kind":"analyst","input":{},'
                '"clarification_question":null,"plan_guidance":null}'
            )
        if "Choose the next execution mode" in prompt:
            return '{"mode":"context_analysis","reason":"result context is available"}'
        if "Analyze verified Langbridge result data" in prompt:
            return '{"analysis":"Revenue is highest in US.","result":{"columns":["region","revenue"],"rows":[["US",2200]]}}'
        if "Review the final Langbridge answer package" in prompt:
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Answer is grounded in the supplied evidence.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        if "Compose the final Langbridge response" in prompt:
            return (
                '{"summary":"Revenue is highest in US.","result":{"columns":["region","revenue"],'
                '"rows":[["US",2200]]},"visualization":null,"research":{},'
                '"answer":"Revenue is highest in US.","diagnostics":{"mode":"test"}}'
            )
        raise AssertionError(f"Unexpected prompt: {prompt[:120]}")

    async def create_embeddings(self, texts, embedding_model=None):
        return [[1.0] for _ in texts]


class _FailingLLMProvider:
    async def acomplete(self, prompt: str, **kwargs):
        raise RuntimeError("llm down")

    async def create_embeddings(self, texts, embedding_model=None):
        return [[1.0] for _ in texts]


class _ArtifactMarkdownLLMProvider(_FakeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Compose the final Langbridge response" in prompt:
            return (
                '{"summary":"Revenue is highest in US.",'
                '"result":{"columns":["region","revenue"],"rows":[["US",2200]]},'
                '"visualization":null,"research":{},'
                '"answer":"Revenue is highest in US.\\n\\n{{artifact:primary_result}}",'
                '"answer_markdown":"Revenue is highest in US.\\n\\n{{artifact:primary_result}}",'
                '"artifacts":[{"id":"primary_result"}],'
                '"diagnostics":{"mode":"test"}}'
            )
        return await super().acomplete(prompt, **kwargs)


class _SqlLLMProvider:
    def __init__(self) -> None:
        self.prompts = []

    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Create a chart specification for verified tabular data" in prompt:
            return (
                '{"chart_type":"table","title":"Tabular result","x":null,"y":null,'
                '"series":null,"encoding":{},"rationale":"No chart requested."}'
            )
        if "Decide Langbridge agent route" in prompt:
            return (
                '{"action":"direct","rationale":"Configured analyst can query dataset SQL tool.",'
                '"agent_name":"analyst.commerce_sql","task_kind":"analyst","input":{},'
                '"clarification_question":null,"plan_guidance":null}'
            )
        if "Choose the next execution mode" in prompt:
            return '{"mode":"sql","reason":"dataset SQL tool is available"}'
        if "You are generating dataset-scope SQL" in prompt:
            return "SELECT orders.region, orders.revenue FROM orders"
        if "Summarize verified SQL analysis" in prompt:
            return '{"analysis":"US revenue is 2200."}'
        if "Review the final Langbridge answer package" in prompt:
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Answer is grounded in the supplied evidence.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        if "Compose the final Langbridge response" in prompt:
            return (
                '{"summary":"US revenue is 2200.","result":{"columns":["region","revenue"],'
                '"rows":[["US",2200]]},"visualization":null,"research":{},'
                '"answer":"US revenue is 2200.","diagnostics":{"mode":"test"}}'
            )
        raise AssertionError(f"Unexpected prompt: {prompt[:120]}")

    async def create_embeddings(self, texts, embedding_model=None):
        return [[1.0] for _ in texts]


class _SqlQ4FollowUpLLMProvider(_SqlLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Decide Langbridge agent route" in prompt and "Make that Q4" in prompt:
            raise AssertionError("Route LLM should be bypassed for Q4 follow-up rewrites.")
        if "You are generating dataset-scope SQL" in prompt:
            assert "Which order channels drove the highest net revenue and gross margin in Q4 2025?" in prompt
            return (
                "SELECT orders.order_channel, orders.net_revenue, orders.gross_margin "
                "FROM orders WHERE orders.quarter = 'Q4-2025'"
            )
        if "Review governed SQL evidence" in prompt:
            return '{"decision":"answer","reason":"Governed SQL answered the refined question.","sufficiency":"sufficient"}'
        if "Summarize verified SQL analysis" in prompt:
            return '{"analysis":"Q4 2025 order channels are summarized from the governed result."}'
        if "Review the final Langbridge answer package" in prompt:
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Answer is grounded in the supplied evidence.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        if "Compose the final Langbridge response" in prompt:
            return (
                '{"summary":"Q4 2025 order channel answer.",'
                '"result":{"columns":["order_channel","net_revenue","gross_margin"],'
                '"rows":[["Online",140000,47000]]},"visualization":null,"research":{},'
                '"answer":"Q4 2025 order channel answer.","diagnostics":{"mode":"test"}}'
            )
        return await super().acomplete(prompt, **kwargs)


class _SqlExcludeRetailFollowUpLLMProvider(_SqlLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Decide Langbridge agent route" in prompt and "Exclude retail" in prompt:
            raise AssertionError("Route LLM should be bypassed for deterministic filter follow-ups.")
        if "You are generating dataset-scope SQL" in prompt:
            assert "Exclude Retail from order channel." in prompt
            return (
                "SELECT orders.order_channel, orders.net_revenue, orders.gross_margin "
                "FROM orders WHERE orders.order_channel <> 'Retail'"
            )
        if "Review governed SQL evidence" in prompt:
            return '{"decision":"answer","reason":"Governed SQL answered the filtered question.","sufficiency":"sufficient"}'
        if "Summarize verified SQL analysis" in prompt:
            return '{"analysis":"Retail was excluded from the governed result."}'
        if "Review the final Langbridge answer package" in prompt:
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Answer is grounded in the supplied evidence.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        if "Compose the final Langbridge response" in prompt:
            return (
                '{"summary":"Retail-excluded order channel answer.",'
                '"result":{"columns":["order_channel","net_revenue","gross_margin"],'
                '"rows":[["Online",125000,42000]]},"visualization":null,"research":{},'
                '"answer":"Retail-excluded order channel answer.","diagnostics":{"mode":"test"}}'
            )
        return await super().acomplete(prompt, **kwargs)


class _SqlExcludeRetailWholesaleFollowUpLLMProvider(_SqlLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Decide Langbridge agent route" in prompt and "Exclude retail and wholesale" in prompt:
            raise AssertionError("Route LLM should be bypassed for deterministic multi-filter follow-ups.")
        if "You are generating dataset-scope SQL" in prompt:
            assert "Exclude Retail and Wholesale from order channel." in prompt
            return (
                "SELECT orders.order_channel, orders.net_revenue, orders.gross_margin "
                "FROM orders WHERE orders.order_channel NOT IN ('Retail', 'Wholesale')"
            )
        if "Review governed SQL evidence" in prompt:
            return '{"decision":"answer","reason":"Governed SQL answered the multi-filtered question.","sufficiency":"sufficient"}'
        if "Summarize verified SQL analysis" in prompt:
            return '{"analysis":"Retail and wholesale were excluded from the governed result."}'
        if "Review the final Langbridge answer package" in prompt:
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Answer is grounded in the supplied evidence.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        if "Compose the final Langbridge response" in prompt:
            return (
                '{"summary":"Retail and wholesale excluded answer.",'
                '"result":{"columns":["order_channel","net_revenue","gross_margin"],'
                '"rows":[["Online",125000,42000]]},"visualization":null,"research":{},'
                '"answer":"Retail and wholesale excluded answer.","diagnostics":{"mode":"test"}}'
            )
        return await super().acomplete(prompt, **kwargs)


class _ClarificationLLMProvider:
    def __init__(self) -> None:
        self.prompts = []

    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Decide Langbridge agent route" in prompt:
            return (
                '{"action":"direct","rationale":"Analyst should ask a targeted clarification.",'
                '"agent_name":"analyst.commerce_sql","task_kind":"analyst","input":{},'
                '"clarification_question":null,"plan_guidance":null}'
            )
        if "Choose the next execution mode" in prompt:
            return (
                '{"mode":"clarify","agent_mode":"clarify","reason":"Time period and ranking metric are missing.",'
                '"clarification_question":"Which time period should I use, and should I rank product categories by total gross margin dollars or by gross margin percentage?"}'
            )
        if "Compose the final Langbridge response" in prompt:
            return (
                '{"summary":"I need one clarification before I can answer.",'
                '"result":{},"visualization":null,"research":{},'
                '"answer":"Which time period should I use, and should I rank product categories by total gross margin dollars or by gross margin percentage?",'
                '"diagnostics":{"mode":"clarification"}}'
            )
        raise AssertionError(f"Unexpected prompt: {prompt[:120]}")

    async def create_embeddings(self, texts, embedding_model=None):
        return [[1.0] for _ in texts]


class _ClarificationThenResearchLLMProvider:
    def __init__(self) -> None:
        self.prompts = []

    async def acomplete(self, prompt: str, **kwargs):
        self.prompts.append(prompt)
        if "Decide Langbridge agent route" in prompt:
            if "cost per signup and in the last 12 months" in prompt:
                return (
                    '{"action":"direct","rationale":"The clarified follow-up can proceed as governed-first research.",'
                    '"agent_name":"analyst.commerce_sql","task_kind":"analyst",'
                    '"input":{"agent_mode":"research"},'
                    '"clarification_question":null,"plan_guidance":null}'
                )
            return (
                '{"action":"direct","rationale":"Analyst should ask a targeted clarification.",'
                '"agent_name":"analyst.commerce_sql","task_kind":"analyst","input":{},'
                '"clarification_question":null,"plan_guidance":null}'
            )
        if "Build Langbridge execution plan" in prompt:
            return (
                '{"route":"planned:analyst.commerce_sql","rationale":"Run the clarified request in research mode.",'
                '"steps":[{"step_id":"step-1","agent_name":"analyst.commerce_sql",'
                '"task_kind":"analyst","question":"cost per signup and in the last 12 months",'
                '"input":{"agent_mode":"research"},"depends_on":[]}]}'
            )
        if "Choose the next execution mode" in prompt:
            if "cost per signup and in the last 12 months" in prompt:
                return '{"mode":"research","agent_mode":"research","reason":"Clarified research request."}'
            return (
                '{"mode":"clarify","agent_mode":"clarify","reason":"Need a metric and timeframe.",'
                '"clarification_question":"Which time period should I analyze, and how do you want to define marketing efficiency for regions (for example ROAS, CAC, cost per signup, or revenue per marketing dollar)?"}'
            )
        if "You are generating dataset-scope SQL" in prompt:
            if "cost per signup and in the last 12 months" not in prompt:
                return "SELECT growth.region FROM growth"
            return (
                "SELECT growth.region, growth.cost_per_signup "
                "FROM growth WHERE growth.window = 'last_12_months'"
            )
        if "Review governed SQL evidence" in prompt:
            if "cost per signup and in the last 12 months" not in prompt:
                return (
                    '{"decision":"clarify","reason":"Need a metric and timeframe.",'
                    '"sufficiency":"insufficient",'
                    '"clarification_question":"Which time period should I analyze, and how do you want to define marketing efficiency for regions (for example ROAS, CAC, cost per signup, or revenue per marketing dollar)?"}'
                )
            return '{"decision":"answer","reason":"Governed SQL answered the clarified request.","sufficiency":"sufficient"}'
        if "Summarize verified SQL analysis" in prompt:
            return '{"analysis":"Regional cost per signup is summarized from governed SQL evidence."}'
        if "Synthesize source-backed research" in prompt:
            return (
                '{"synthesis":"Governed evidence shows cost per signup by region over the last 12 months.",'
                '"findings":[{"insight":"North has the lowest cost per signup in the governed result.","source":"governed_result"}],'
                '"follow_ups":[]}'
            )
        if "Review the final Langbridge answer package" in prompt:
            return (
                '{"action":"approve","reason_code":"grounded_complete",'
                '"rationale":"Answer is grounded in the supplied evidence.",'
                '"issues":[],"updated_context":{},"clarification_question":null}'
            )
        if "Compose the final Langbridge response" in prompt:
            if "cost per signup and in the last 12 months" in prompt:
                return (
                    '{"summary":"Regional cost per signup over the last 12 months is ready.",'
                    '"result":{"columns":["region","revenue"],"rows":[["US",2200]]},'
                    '"visualization":null,"research":{},'
                    '"answer":"Regional cost per signup over the last 12 months is ready.",'
                    '"diagnostics":{"mode":"research"}}'
                )
            return (
                '{"summary":"I need one clarification before I can answer.",'
                '"result":{},"visualization":null,"research":{},'
                '"answer":"Which time period should I analyze, and how do you want to define marketing efficiency for regions (for example ROAS, CAC, cost per signup, or revenue per marketing dollar)?",'
                '"diagnostics":{"mode":"clarification"}}'
            )
        raise AssertionError(f"Unexpected prompt: {prompt[:120]}")

    async def create_embeddings(self, texts, embedding_model=None):
        return [[1.0] for _ in texts]


class _SimpleProfileLLMProvider(_FakeLLMProvider):
    async def acomplete(self, prompt: str, **kwargs):
        if "Decide Langbridge agent route" in prompt:
            return (
                '{"action":"direct","rationale":"Configured analyst can answer from thread context.",'
                '"agent_name":"analyst.commerce_agent","task_kind":"analyst","input":{},'
                '"clarification_question":null,"plan_guidance":null}'
            )
        return await super().acomplete(prompt, **kwargs)


class _ObjectStore:
    def __init__(self, value):
        self.value = value

    async def get_by_id(self, id_):
        return self.value if self.value.id == id_ else None


class _ThreadStore(_ObjectStore):
    async def save(self, instance):
        self.value = instance
        return instance


class _ThreadMessageStore:
    def __init__(self, messages):
        self.messages = list(messages)
        self.added = []

    def add(self, instance):
        self.added.append(instance)
        self.messages.append(instance)
        return instance

    async def list_for_thread(self, thread_id):
        return [message for message in self.messages if message.thread_id == thread_id]


class _MemoryStore:
    def __init__(self, items):
        self.items = list(items)
        self.created = []
        self.touched = []
        self.flush_count = 0

    async def list_for_thread(self, thread_id, *, limit=200):
        return [item for item in self.items if item.thread_id == thread_id][:limit]

    def create_item(self, *, thread_id, actor_id, category, content, metadata_json=None):
        item = RuntimeConversationMemoryItem(
            id=uuid.uuid4(),
            thread_id=thread_id,
            actor_id=actor_id,
            category=category,
            content=content,
            metadata=metadata_json or {},
        )
        self.created.append(item)
        self.items.append(item)
        return item

    async def touch_items(self, item_ids):
        self.touched.extend(item_ids)

    async def flush(self):
        self.flush_count += 1


class _DatasetStore:
    def __init__(self, datasets):
        self.datasets = {dataset.id: dataset for dataset in datasets}

    async def get_by_ids(self, dataset_ids):
        return [self.datasets[dataset_id] for dataset_id in dataset_ids if dataset_id in self.datasets]

    async def get_by_ids_for_workspace(self, *, workspace_id, dataset_ids):
        return [
            dataset
            for dataset_id in dataset_ids
            if (dataset := self.datasets.get(dataset_id)) is not None and dataset.workspace_id == workspace_id
        ]


class _DatasetColumnStore:
    def __init__(self, columns_by_dataset):
        self.columns_by_dataset = columns_by_dataset

    async def list_for_dataset(self, *, dataset_id):
        return list(self.columns_by_dataset.get(dataset_id, []))


class _FederatedQueryTool:
    def __init__(self):
        self.calls = []

    async def execute_federated_query(self, payload):
        self.calls.append(payload)
        return {
            "columns": ["region", "revenue"],
            "rows": [{"region": "US", "revenue": 2200}],
            "execution": {"total_runtime_ms": 7},
        }


def _ids():
    return SimpleNamespace(
        workspace_id=uuid.uuid4(),
        actor_id=uuid.uuid4(),
        thread_id=uuid.uuid4(),
        message_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        llm_id=uuid.uuid4(),
        job_id=uuid.uuid4(),
    )


def _agent_definition(ids) -> RuntimeAgentDefinition:
    return RuntimeAgentDefinition(
        id=ids.agent_id,
        name="commerce_agent",
        description="Commerce analyst.",
        llm_connection_id=ids.llm_id,
        definition={
            "features": {"visualization_enabled": True, "supports_deep_research": True},
            "tools": [
                {
                    "name": "commerce_sql",
                    "tool_type": "sql",
                    "description": "Commerce dataset.",
                    "config": {"dataset_ids": ["commerce_orders"]},
                }
            ],
            "execution": {"max_iterations": 4},
        },
        is_active=True,
        management_mode=ManagementMode.RUNTIME_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )


def _dataset_agent_definition(ids, dataset_id: uuid.UUID) -> RuntimeAgentDefinition:
    return RuntimeAgentDefinition(
        id=ids.agent_id,
        name="commerce_agent",
        description="Commerce analyst.",
        llm_connection_id=ids.llm_id,
        definition={
            "features": {"visualization_enabled": True, "supports_deep_research": True},
            "tools": [
                {
                    "name": "commerce_sql",
                    "tool_type": "sql",
                    "description": "Commerce dataset.",
                    "config": {"dataset_ids": [str(dataset_id)]},
                }
            ],
            "execution": {"max_iterations": 4},
        },
        is_active=True,
        management_mode=ManagementMode.RUNTIME_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )


def _simple_ai_agent_definition(ids) -> RuntimeAgentDefinition:
    return RuntimeAgentDefinition(
        id=ids.agent_id,
        name="commerce_agent",
        description="Commerce analyst.",
        llm_connection_id=ids.llm_id,
        definition={
            "analyst_scope": {
                "datasets": ["commerce_orders"],
                "query_policy": "dataset_only",
            },
            "prompts": {"system_prompt": "Answer from verified commerce context."},
        },
        is_active=True,
        management_mode=ManagementMode.RUNTIME_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )


def _llm_connection(ids) -> LLMConnectionSecret:
    return LLMConnectionSecret(
        id=ids.llm_id,
        name="test-llm",
        provider=RuntimeLLMProvider.OPENAI,
        model="test-model",
        api_key="test",
        workspace_id=ids.workspace_id,
    )


def _thread(ids) -> RuntimeThread:
    return RuntimeThread(
        id=ids.thread_id,
        workspace_id=ids.workspace_id,
        created_by=ids.actor_id,
        last_message_id=ids.message_id,
        state=RuntimeThreadState.processing,
    )


def _user_message(ids) -> RuntimeThreadMessage:
    return RuntimeThreadMessage(
        id=ids.message_id,
        thread_id=ids.thread_id,
        role=RuntimeMessageRole.user,
        content={
            "text": "Show revenue by region",
            "context": {
                "result": {
                    "columns": ["region", "revenue"],
                    "rows": [["US", 2200]],
                }
            },
        },
    )


def _plain_user_message(ids) -> RuntimeThreadMessage:
    return RuntimeThreadMessage(
        id=ids.message_id,
        thread_id=ids.thread_id,
        role=RuntimeMessageRole.user,
        content={"text": "Show revenue by region"},
    )


def _request(ids) -> CreateAgentJobRequest:
    return CreateAgentJobRequest(
        job_type=JobType.AGENT,
        agent_definition_id=ids.agent_id,
        workspace_id=ids.workspace_id,
        actor_id=ids.actor_id,
        thread_id=ids.thread_id,
    )


def _service(ids, provider) -> tuple[AgentExecutionService, _ThreadMessageStore]:
    message_store = _ThreadMessageStore([_user_message(ids)])
    return (
        AgentExecutionService(
            agent_definition_repository=_ObjectStore(_agent_definition(ids)),
            llm_repository=_ObjectStore(_llm_connection(ids)),
            thread_repository=_ThreadStore(_thread(ids)),
            thread_message_repository=message_store,
            llm_provider_factory=lambda connection: provider,
        ),
        message_store,
    )


def test_agent_execution_service_runs_new_ai_flow_and_persists_message() -> None:
    ids = _ids()
    service, message_store = _service(ids, _FakeLLMProvider())
    emitter = CollectingAgentEventEmitter()

    result = _run(
        service.execute(
            job_id=ids.job_id,
            request=_request(ids),
            event_emitter=emitter,
        )
    )

    assert result.response["summary"] == "Revenue is highest in US."
    assert result.ai_run.execution_mode == "direct"
    assert result.ai_run.status == "completed"
    assert result.thread.state == RuntimeThreadState.awaiting_user_input
    assert result.assistant_message in message_store.added
    assert result.assistant_message.model_snapshot["runtime"] == "langbridge.ai"
    assert result.response["diagnostics"]["ai_run"]["route"] == "direct:analyst.commerce_sql"
    assert result.response["diagnostics"]["ai_run"]["execution_mode"] == "direct"
    assert result.response["diagnostics"]["ai_run"]["status"] == "completed"
    event_types = [event["event_type"] for event in emitter.events]
    assert event_types[0] == "AgentRunStarted"
    assert "MetaControllerStarted" in event_types
    assert "AgentRouteSelected" in event_types
    assert "PlanStepStarted" in event_types
    assert "AnalystContextAnalysisStarted" in event_types
    assert "PresentationCompleted" in event_types
    assert event_types[-1] == "AgentRunCompleted"
    assert result.thread.metadata["continuation_state"]["chartable"] is True
    assert result.thread.metadata["continuation_state"]["selected_agent"] == "analyst.commerce_sql"
    assert result.thread.metadata["continuation_state"]["resolved_question"] == "Show revenue by region"
    assert result.thread.metadata["continuation_state"]["analysis_state"]["metrics"] == ["revenue"]
    assert result.thread.metadata["continuation_state"]["analysis_state"]["dimensions"] == ["region"]
    assert result.thread.metadata["continuation_state"]["analysis_state"]["dimension_value_samples"] == {"region": ["US"]}
    assert result.assistant_message.content["continuation_state"]["result"]["rows"] == [["US", 2200]]


def test_agent_execution_service_persists_answer_markdown_and_artifacts() -> None:
    ids = _ids()
    service, message_store = _service(ids, _ArtifactMarkdownLLMProvider())

    result = _run(
        service.execute(
            job_id=ids.job_id,
            request=_request(ids),
            event_emitter=CollectingAgentEventEmitter(),
        )
    )

    assert result.response["answer_markdown"] == "Revenue is highest in US.\n\n{{artifact:primary_result}}"
    assert result.response["artifacts"][0]["id"] == "primary_result"
    assert result.response["artifacts"][0]["payload"]["rows"] == [["US", 2200]]
    assert result.assistant_message in message_store.added
    assert result.assistant_message.content["answer_markdown"] == result.response["answer_markdown"]
    assert result.assistant_message.content["artifacts"][0]["id"] == "primary_result"
    assert result.assistant_message.content["artifacts"][0]["payload"]["rows"] == [["US", 2200]]


def test_agent_execution_service_accepts_simple_ai_profile_definition_shape() -> None:
    ids = _ids()
    provider = _SimpleProfileLLMProvider()
    message_store = _ThreadMessageStore([_user_message(ids)])
    service = AgentExecutionService(
        agent_definition_repository=_ObjectStore(_simple_ai_agent_definition(ids)),
        llm_repository=_ObjectStore(_llm_connection(ids)),
        thread_repository=_ThreadStore(_thread(ids)),
        thread_message_repository=message_store,
        llm_provider_factory=lambda connection: provider,
    )

    result = _run(service.execute(job_id=ids.job_id, request=_request(ids)))

    assert result.response["summary"] == "Revenue is highest in US."
    assert result.response["diagnostics"]["ai_run"]["route"] == "direct:analyst.commerce_agent"


def test_agent_execution_service_restores_and_writes_conversation_memory() -> None:
    ids = _ids()
    provider = _FakeLLMProvider()
    memory_item = RuntimeConversationMemoryItem(
        id=uuid.uuid4(),
        thread_id=ids.thread_id,
        actor_id=ids.actor_id,
        category=RuntimeConversationMemoryCategory.preference,
        content="Prefer gross revenue when user asks for revenue.",
        metadata={"source": "test"},
    )
    memory_store = _MemoryStore([memory_item])
    message_store = _ThreadMessageStore([_user_message(ids)])
    service = AgentExecutionService(
        agent_definition_repository=_ObjectStore(_agent_definition(ids)),
        llm_repository=_ObjectStore(_llm_connection(ids)),
        thread_repository=_ThreadStore(_thread(ids)),
        thread_message_repository=message_store,
        memory_repository=memory_store,
        llm_provider_factory=lambda connection: provider,
    )

    result = _run(service.execute(job_id=ids.job_id, request=_request(ids)))

    assert result.response["summary"] == "Revenue is highest in US."
    assert memory_item.id in memory_store.touched
    assert any(
        "Prefer gross revenue" in prompt and "Decide Langbridge agent route" in prompt
        for prompt in provider.prompts
    )
    assert any(
        "Prefer gross revenue" in prompt and "Analyze verified Langbridge result data" in prompt
        for prompt in provider.prompts
    )
    created_categories = [item.category for item in memory_store.created]
    assert RuntimeConversationMemoryCategory.answer.value in created_categories
    assert RuntimeConversationMemoryCategory.decision.value in created_categories
    assert RuntimeConversationMemoryCategory.tool_outcome.value in created_categories
    continuation_items = [
        item for item in memory_store.created if item.category == RuntimeConversationMemoryCategory.tool_outcome.value
    ]
    assert continuation_items[0].metadata["kind"] == "continuation_state"
    assert continuation_items[0].metadata["continuation_state"]["chartable"] is True
    assert continuation_items[0].metadata["continuation_state"]["resolved_question"] == "Show revenue by region"
    assert continuation_items[0].metadata["continuation_state"]["analysis_state"]["metrics"] == ["revenue"]
    assert continuation_items[0].metadata["continuation_state"]["analysis_state"]["dimensions"] == ["region"]
    assert continuation_items[0].metadata["continuation_state"]["analysis_state"]["dimension_value_samples"] == {
        "region": ["US"]
    }
    assert memory_store.flush_count == 1


def test_agent_execution_service_passes_requested_agent_mode_to_meta_flow() -> None:
    ids = _ids()
    provider = _FakeLLMProvider()
    service, _ = _service(ids, provider)
    request = _request(ids).model_copy(update={"agent_mode": "context_analysis"})

    result = _run(service.execute(job_id=ids.job_id, request=request))

    assert result.response["summary"] == "Revenue is highest in US."
    assert any("Requested agent mode: context_analysis" in prompt for prompt in provider.prompts)
    assert not any("Choose the next execution mode" in prompt for prompt in provider.prompts)


def test_agent_execution_service_surfaces_specific_clarification_question() -> None:
    ids = _ids()
    provider = _ClarificationLLMProvider()
    message_store = _ThreadMessageStore([_plain_user_message(ids)])
    emitter = CollectingAgentEventEmitter()
    service = AgentExecutionService(
        agent_definition_repository=_ObjectStore(_agent_definition(ids)),
        llm_repository=_ObjectStore(_llm_connection(ids)),
        thread_repository=_ThreadStore(_thread(ids)),
        thread_message_repository=message_store,
        llm_provider_factory=lambda connection: provider,
    )

    result = _run(service.execute(job_id=ids.job_id, request=_request(ids), event_emitter=emitter))

    question = (
        "Which time period should I use, and should I rank product categories by total gross margin "
        "dollars or by gross margin percentage?"
    )
    assert result.response["summary"] == "I need one clarification before I can answer."
    assert result.response["answer"] == question
    assert result.response["result"] is None
    assert result.response["diagnostics"]["clarifying_question"] == question
    assert result.assistant_message.content["answer"] == question
    assert result.assistant_message.content["result"] is None
    assert result.assistant_message.content["diagnostics"]["clarifying_question"] == question
    assert "result" not in result.thread.metadata["continuation_state"]
    assert emitter.events[-1]["event_type"] == "AgentRunCompleted"
    assert emitter.events[-1]["message"] == question


def test_agent_execution_service_clarification_follow_up_still_runs_governed_seed_in_research_mode() -> None:
    ids = _ids()
    dataset_id = uuid.uuid4()
    dataset = DatasetMetadata(
        id=dataset_id,
        workspace_id=ids.workspace_id,
        connection_id=uuid.uuid4(),
        name="Growth",
        sql_alias="growth",
        dataset_type="TABLE",
        materialization_mode="live",
        source={"table": "growth"},
        source_kind="database",
        storage_kind="table",
        dialect="postgres",
        schema_name="public",
        table_name="growth",
        relation_identity={
            "canonical_reference": f"dataset:{dataset_id}",
            "relation_name": "growth",
            "source_kind": "database",
            "storage_kind": "table",
        },
        execution_capabilities={
            "supports_structured_scan": True,
            "supports_sql_federation": True,
        },
        status="published",
        management_mode=ManagementMode.RUNTIME_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )
    columns = [
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="region",
            data_type="text",
        ),
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="cost_per_signup",
            data_type="integer",
        ),
    ]
    provider = _ClarificationThenResearchLLMProvider()
    initial_user_message = RuntimeThreadMessage(
        id=ids.message_id,
        thread_id=ids.thread_id,
        role=RuntimeMessageRole.user,
        content={"text": "Do regions with higher support load also underperform on marketing efficiency?"},
    )
    message_store = _ThreadMessageStore([initial_user_message])
    thread = _thread(ids)
    service = AgentExecutionService(
        agent_definition_repository=_ObjectStore(_dataset_agent_definition(ids, dataset_id)),
        llm_repository=_ObjectStore(_llm_connection(ids)),
        thread_repository=_ThreadStore(thread),
        thread_message_repository=message_store,
        dataset_repository=_DatasetStore([dataset]),
        dataset_column_repository=_DatasetColumnStore({dataset_id: columns}),
        federated_query_tool=_FederatedQueryTool(),
        llm_provider_factory=lambda connection: provider,
    )

    first_result = _run(service.execute(job_id=ids.job_id, request=_request(ids)))

    assert first_result.response["summary"] == "I need one clarification before I can answer."
    assert first_result.response["result"] is None
    assert "result" not in first_result.thread.metadata["continuation_state"]

    follow_up_message = RuntimeThreadMessage(
        id=uuid.uuid4(),
        thread_id=ids.thread_id,
        role=RuntimeMessageRole.user,
        content={"text": "cost per signup and in the last 12 months"},
    )
    message_store.add(follow_up_message)
    thread.last_message_id = follow_up_message.id
    thread.state = RuntimeThreadState.processing

    second_result = _run(
        service.execute(
            job_id=uuid.uuid4(),
            request=_request(ids).model_copy(update={"agent_mode": "research"}),
        )
    )

    assert second_result.ai_run.status == "completed"
    assert second_result.ai_run.plan.steps[0].input["agent_mode"] == "research"
    assert second_result.ai_run.step_results[0]["diagnostics"]["agent_mode"] == "research"
    assert second_result.ai_run.step_results[0]["output"]["evidence"]["governed"]["attempted"] is True
    assert second_result.response["result"]["rows"] == [["US", 2200]]
    assert second_result.response["diagnostics"].get("clarifying_question") is None
    assert second_result.assistant_message.content["continuation_state"]["analysis_state"]["period"]["kind"] == (
        "rolling_window"
    )
    assert second_result.assistant_message.content["continuation_state"]["analysis_state"]["period"]["label"] == (
        "last 12 months"
    )
    assert any("You are generating dataset-scope SQL" in prompt for prompt in provider.prompts)


def test_agent_execution_service_rehydrates_prior_result_for_chart_follow_up() -> None:
    ids = _ids()
    provider = _FakeLLMProvider()
    thread = _thread(ids)
    message_store = _ThreadMessageStore([_user_message(ids)])
    thread_store = _ThreadStore(thread)
    service = AgentExecutionService(
        agent_definition_repository=_ObjectStore(_agent_definition(ids)),
        llm_repository=_ObjectStore(_llm_connection(ids)),
        thread_repository=thread_store,
        thread_message_repository=message_store,
        llm_provider_factory=lambda connection: provider,
    )

    first_result = _run(service.execute(job_id=ids.job_id, request=_request(ids)))
    assert first_result.thread.metadata["continuation_state"]["chartable"] is True

    thread.metadata.pop("continuation_state", None)
    follow_up_message = RuntimeThreadMessage(
        id=uuid.uuid4(),
        thread_id=ids.thread_id,
        role=RuntimeMessageRole.user,
        content={"text": "Show me in a pie chart"},
    )
    message_store.add(follow_up_message)
    thread.last_message_id = follow_up_message.id
    thread.state = RuntimeThreadState.processing

    emitter = CollectingAgentEventEmitter()
    second_result = _run(
        service.execute(
            job_id=uuid.uuid4(),
            request=_request(ids),
            event_emitter=emitter,
        )
    )

    assert second_result.ai_run.status == "completed"
    assert second_result.ai_run.step_results[0]["diagnostics"]["agent_mode"] == "context_analysis"
    assert second_result.response["visualization"]["chart_type"] == "pie"
    assert second_result.assistant_message.content["continuation_state"]["visualization"]["chart_type"] == "pie"
    assert second_result.assistant_message.content["continuation_state"]["visualization_state"]["chart_type"] == "pie"
    assert second_result.response["diagnostics"]["ai_run"]["status"] == "completed"
    assert second_result.response["diagnostics"].get("clarifying_question") is None
    assert any(
        "Structured result context available:\nTrue" in prompt and "Choose the next execution mode" in prompt
        for prompt in provider.prompts
    )
    event_types = [event["event_type"] for event in emitter.events]
    assert "AnalystContextAnalysisStarted" in event_types
    assert "ChartingStarted" in event_types


def test_agent_execution_service_rewrites_q4_follow_up_from_continuation_state() -> None:
    ids = _ids()
    dataset_id = uuid.uuid4()
    dataset = DatasetMetadata(
        id=dataset_id,
        workspace_id=ids.workspace_id,
        connection_id=uuid.uuid4(),
        name="Orders",
        sql_alias="orders",
        dataset_type="TABLE",
        materialization_mode="live",
        source={"table": "orders"},
        source_kind="database",
        storage_kind="table",
        dialect="postgres",
        schema_name="public",
        table_name="orders",
        relation_identity={
            "canonical_reference": f"dataset:{dataset_id}",
            "relation_name": "orders",
            "source_kind": "database",
            "storage_kind": "table",
        },
        execution_capabilities={
            "supports_structured_scan": True,
            "supports_sql_federation": True,
        },
        status="published",
        management_mode=ManagementMode.RUNTIME_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )
    columns = [
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="order_channel",
            data_type="text",
        ),
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="net_revenue",
            data_type="integer",
        ),
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="gross_margin",
            data_type="integer",
        ),
    ]
    thread = _thread(ids)
    thread.metadata = {
        "continuation_state": {
            "question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
            "summary": "Q3 2025 order channel performance.",
            "analysis_state": {
                "period": {"kind": "quarter", "quarter": "Q3", "year": "2025", "label": "Q3 2025"},
            },
            "selected_agent": "analyst.commerce_sql",
        }
    }
    follow_up_message = RuntimeThreadMessage(
        id=ids.message_id,
        thread_id=ids.thread_id,
        role=RuntimeMessageRole.user,
        content={"text": "Make that Q4"},
    )
    message_store = _ThreadMessageStore([follow_up_message])
    provider = _SqlQ4FollowUpLLMProvider()
    federated_query_tool = _FederatedQueryTool()
    service = AgentExecutionService(
        agent_definition_repository=_ObjectStore(_dataset_agent_definition(ids, dataset_id)),
        llm_repository=_ObjectStore(_llm_connection(ids)),
        thread_repository=_ThreadStore(thread),
        thread_message_repository=message_store,
        dataset_repository=_DatasetStore([dataset]),
        dataset_column_repository=_DatasetColumnStore({dataset_id: columns}),
        federated_query_tool=federated_query_tool,
        llm_provider_factory=lambda connection: provider,
    )

    result = _run(service.execute(job_id=ids.job_id, request=_request(ids)))

    assert result.ai_run.status == "completed"
    assert result.ai_run.plan.steps[0].input["agent_mode"] == "sql"
    assert result.ai_run.plan.steps[0].input["follow_up_period"]["label"] == "Q4 2025"
    assert result.ai_run.plan.steps[0].question == (
        "Which order channels drove the highest net revenue and gross margin in Q4 2025?"
    )
    assert result.response["answer"] == "Q4 2025 order channel answer."
    assert result.assistant_message.content["continuation_state"]["resolved_question"] == (
        "Which order channels drove the highest net revenue and gross margin in Q4 2025?"
    )
    assert result.assistant_message.content["continuation_state"]["analysis_state"]["period"]["label"] == "Q4 2025"
    assert all(
        "Decide Langbridge agent route" not in prompt or "Make that Q4" not in prompt
        for prompt in provider.prompts
    )


def test_agent_execution_service_rewrites_exclude_filter_follow_up_from_continuation_state() -> None:
    ids = _ids()
    dataset_id = uuid.uuid4()
    dataset = DatasetMetadata(
        id=dataset_id,
        workspace_id=ids.workspace_id,
        connection_id=uuid.uuid4(),
        name="Orders",
        sql_alias="orders",
        dataset_type="TABLE",
        materialization_mode="live",
        source={"table": "orders"},
        source_kind="database",
        storage_kind="table",
        dialect="postgres",
        schema_name="public",
        table_name="orders",
        relation_identity={
            "canonical_reference": f"dataset:{dataset_id}",
            "relation_name": "orders",
            "source_kind": "database",
            "storage_kind": "table",
        },
        execution_capabilities={
            "supports_structured_scan": True,
            "supports_sql_federation": True,
        },
        status="published",
        management_mode=ManagementMode.RUNTIME_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )
    columns = [
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="order_channel",
            data_type="text",
        ),
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="net_revenue",
            data_type="integer",
        ),
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="gross_margin",
            data_type="integer",
        ),
    ]
    thread = _thread(ids)
    thread.metadata = {
        "continuation_state": {
            "question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
            "summary": "Q3 2025 order channel performance.",
            "analysis_state": {
                "available_fields": ["order channel", "net revenue", "gross margin"],
                "metrics": ["net revenue", "gross margin"],
                "dimensions": ["order channel"],
                "primary_dimension": "order channel",
                "dimension_value_samples": {"order channel": ["Online", "Retail"]},
            },
            "selected_agent": "analyst.commerce_sql",
        }
    }
    follow_up_message = RuntimeThreadMessage(
        id=ids.message_id,
        thread_id=ids.thread_id,
        role=RuntimeMessageRole.user,
        content={"text": "Exclude retail"},
    )
    message_store = _ThreadMessageStore([follow_up_message])
    provider = _SqlExcludeRetailFollowUpLLMProvider()
    federated_query_tool = _FederatedQueryTool()
    service = AgentExecutionService(
        agent_definition_repository=_ObjectStore(_dataset_agent_definition(ids, dataset_id)),
        llm_repository=_ObjectStore(_llm_connection(ids)),
        thread_repository=_ThreadStore(thread),
        thread_message_repository=message_store,
        dataset_repository=_DatasetStore([dataset]),
        dataset_column_repository=_DatasetColumnStore({dataset_id: columns}),
        federated_query_tool=federated_query_tool,
        llm_provider_factory=lambda connection: provider,
    )

    result = _run(service.execute(job_id=ids.job_id, request=_request(ids)))

    assert result.ai_run.status == "completed"
    assert result.ai_run.plan.steps[0].input["agent_mode"] == "sql"
    assert result.ai_run.plan.steps[0].input["follow_up_filter"] == {
        "field": "order channel",
        "operator": "exclude",
        "value": "Retail",
    }
    assert result.assistant_message.content["continuation_state"]["resolved_question"] == (
        "Which order channels drove the highest net revenue and gross margin in Q3 2025. "
        "Exclude Retail from order channel."
    )
    assert result.assistant_message.content["continuation_state"]["analysis_state"]["active_filters"] == [
        {"field": "order channel", "operator": "exclude", "values": ["Retail"]},
    ]
    assert result.ai_run.plan.steps[0].question == (
        "Which order channels drove the highest net revenue and gross margin in Q3 2025. "
        "Exclude Retail from order channel."
    )
    assert result.response["answer"] == "Retail-excluded order channel answer."
    assert all(
        "Decide Langbridge agent route" not in prompt or "Exclude retail" not in prompt
        for prompt in provider.prompts
    )


def test_agent_execution_service_rewrites_multi_value_filter_follow_up_from_continuation_state() -> None:
    ids = _ids()
    dataset_id = uuid.uuid4()
    dataset = DatasetMetadata(
        id=dataset_id,
        workspace_id=ids.workspace_id,
        connection_id=uuid.uuid4(),
        name="Orders",
        sql_alias="orders",
        dataset_type="TABLE",
        materialization_mode="live",
        source={"table": "orders"},
        source_kind="database",
        storage_kind="table",
        dialect="postgres",
        schema_name="public",
        table_name="orders",
        relation_identity={
            "canonical_reference": f"dataset:{dataset_id}",
            "relation_name": "orders",
            "source_kind": "database",
            "storage_kind": "table",
        },
        execution_capabilities={
            "supports_structured_scan": True,
            "supports_sql_federation": True,
        },
        status="published",
        management_mode=ManagementMode.RUNTIME_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )
    columns = [
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="order_channel",
            data_type="text",
        ),
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="net_revenue",
            data_type="integer",
        ),
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="gross_margin",
            data_type="integer",
        ),
    ]
    thread = _thread(ids)
    thread.metadata = {
        "continuation_state": {
            "resolved_question": "Which order channels drove the highest net revenue and gross margin in Q3 2025?",
            "summary": "Q3 2025 order channel performance.",
            "analysis_state": {
                "available_fields": ["order channel", "net revenue", "gross margin"],
                "metrics": ["net revenue", "gross margin"],
                "dimensions": ["order channel"],
                "primary_dimension": "order channel",
                "dimension_value_samples": {"order channel": ["Online", "Retail", "Wholesale"]},
            },
            "selected_agent": "analyst.commerce_sql",
        }
    }
    follow_up_message = RuntimeThreadMessage(
        id=ids.message_id,
        thread_id=ids.thread_id,
        role=RuntimeMessageRole.user,
        content={"text": "Exclude retail and wholesale"},
    )
    message_store = _ThreadMessageStore([follow_up_message])
    provider = _SqlExcludeRetailWholesaleFollowUpLLMProvider()
    federated_query_tool = _FederatedQueryTool()
    service = AgentExecutionService(
        agent_definition_repository=_ObjectStore(_dataset_agent_definition(ids, dataset_id)),
        llm_repository=_ObjectStore(_llm_connection(ids)),
        thread_repository=_ThreadStore(thread),
        thread_message_repository=message_store,
        dataset_repository=_DatasetStore([dataset]),
        dataset_column_repository=_DatasetColumnStore({dataset_id: columns}),
        federated_query_tool=federated_query_tool,
        llm_provider_factory=lambda connection: provider,
    )

    result = _run(service.execute(job_id=ids.job_id, request=_request(ids)))

    assert result.ai_run.status == "completed"
    assert result.ai_run.plan.steps[0].input["agent_mode"] == "sql"
    assert result.ai_run.plan.steps[0].input["follow_up_filters"] == [
        {"field": "order channel", "operator": "exclude", "values": ["Retail", "Wholesale"]},
    ]
    assert result.ai_run.plan.steps[0].input["active_filters"] == [
        {"field": "order channel", "operator": "exclude", "values": ["Retail", "Wholesale"]},
    ]
    assert result.assistant_message.content["continuation_state"]["analysis_state"]["active_filters"] == [
        {"field": "order channel", "operator": "exclude", "values": ["Retail", "Wholesale"]},
    ]
    assert result.assistant_message.content["continuation_state"]["resolved_question"] == (
        "Which order channels drove the highest net revenue and gross margin in Q3 2025. "
        "Exclude Retail and Wholesale from order channel."
    )
    assert result.ai_run.plan.steps[0].question == (
        "Which order channels drove the highest net revenue and gross margin in Q3 2025. "
        "Exclude Retail and Wholesale from order channel."
    )
    assert result.response["answer"] == "Retail and wholesale excluded answer."
    assert all(
        "Decide Langbridge agent route" not in prompt or "Exclude retail and wholesale" not in prompt
        for prompt in provider.prompts
    )


def test_agent_execution_service_aborts_when_llm_provider_errors() -> None:
    ids = _ids()
    service, _ = _service(ids, _FailingLLMProvider())
    emitter = CollectingAgentEventEmitter()

    with pytest.raises(RuntimeError, match="llm down"):
        _run(
            service.execute(
                job_id=ids.job_id,
                request=_request(ids),
                event_emitter=emitter,
            )
        )

    assert emitter.events[-1]["event_type"] == "AgentRunFailed"


def test_agent_execution_service_auto_builds_sql_tool_from_runtime_catalog() -> None:
    ids = _ids()
    dataset_id = uuid.uuid4()
    dataset = DatasetMetadata(
        id=dataset_id,
        workspace_id=ids.workspace_id,
        connection_id=uuid.uuid4(),
        name="Orders",
        sql_alias="orders",
        dataset_type="TABLE",
        materialization_mode="live",
        source={"table": "orders"},
        source_kind="database",
        storage_kind="table",
        dialect="postgres",
        schema_name="public",
        table_name="orders",
        relation_identity={
            "canonical_reference": f"dataset:{dataset_id}",
            "relation_name": "orders",
            "source_kind": "database",
            "storage_kind": "table",
        },
        execution_capabilities={
            "supports_structured_scan": True,
            "supports_sql_federation": True,
        },
        status="published",
        management_mode=ManagementMode.RUNTIME_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )
    columns = [
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="region",
            data_type="text",
        ),
        DatasetColumnMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=ids.workspace_id,
            name="revenue",
            data_type="integer",
        ),
    ]
    message_store = _ThreadMessageStore([_plain_user_message(ids)])
    federated_query_tool = _FederatedQueryTool()
    emitter = CollectingAgentEventEmitter()
    service = AgentExecutionService(
        agent_definition_repository=_ObjectStore(_dataset_agent_definition(ids, dataset_id)),
        llm_repository=_ObjectStore(_llm_connection(ids)),
        thread_repository=_ThreadStore(_thread(ids)),
        thread_message_repository=message_store,
        dataset_repository=_DatasetStore([dataset]),
        dataset_column_repository=_DatasetColumnStore({dataset_id: columns}),
        federated_query_tool=federated_query_tool,
        llm_provider_factory=lambda connection: _SqlLLMProvider(),
    )

    result = _run(service.execute(job_id=ids.job_id, request=_request(ids), event_emitter=emitter))

    assert result.response["summary"] == "US revenue is 2200."
    assert result.ai_run.step_results[0]["diagnostics"]["agent_mode"] == "sql"
    assert federated_query_tool.calls[0]["query"] == "SELECT orders.region, orders.revenue FROM orders LIMIT 1000"
    event_types = [event["event_type"] for event in emitter.events]
    assert "AgentToolSelected" in event_types
    assert "SqlGenerationStarted" in event_types
    assert "SqlExecutionStarted" in event_types
    assert "SqlExecutionCompleted" in event_types


def test_ai_event_types_map_to_stream_stages() -> None:
    assert normalize_agent_stream_stage(event_type="SqlGenerationStarted") == "generating_sql"
    assert normalize_agent_stream_stage(event_type="SqlExecutionStarted") == "running_query"
    assert normalize_agent_stream_stage(event_type="WebSearchStarted") == "searching_web"
    assert normalize_agent_stream_stage(event_type="SemanticSearchStarted") == "searching_semantic"
    assert normalize_agent_stream_stage(event_type="ChartingStarted") == "rendering_chart"
    assert normalize_agent_stream_stage(event_type="PresentationStarted") == "composing_response"
