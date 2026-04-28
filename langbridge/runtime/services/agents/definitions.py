import uuid

from langbridge.ai import (
    AiAgentExecutionConfig,
    AnalystAgentConfig,
    build_analyst_configs_from_definition,
    build_execution_from_definition,
)
from langbridge.runtime.models import LLMConnectionSecret, RuntimeAgentDefinition
from langbridge.runtime.ports import AgentDefinitionStore, LLMConnectionStore
from langbridge.runtime.services.errors import ExecutionValidationError


class AgentExecutionDefinitionResolver:
    def __init__(
        self,
        *,
        agent_definition_repository: AgentDefinitionStore,
        llm_repository: LLMConnectionStore,
    ) -> None:
        self._agent_definition_repository = agent_definition_repository
        self._llm_repository = llm_repository

    async def get_agent_definition(self, agent_definition_id: uuid.UUID) -> RuntimeAgentDefinition:
        agent_definition = await self._agent_definition_repository.get_by_id(agent_definition_id)
        if agent_definition is None:
            raise ExecutionValidationError(
                f"Agent definition with ID {agent_definition_id} does not exist."
            )
        if not agent_definition.is_active:
            raise ExecutionValidationError(f"Agent definition {agent_definition_id} is not active.")
        return agent_definition

    async def get_llm_connection(self, llm_connection_id: uuid.UUID) -> LLMConnectionSecret:
        llm_connection = await self._llm_repository.get_by_id(llm_connection_id)
        if llm_connection is None:
            raise ExecutionValidationError(f"LLM connection with ID {llm_connection_id} does not exist.")
        if not llm_connection.is_active:
            raise ExecutionValidationError(f"LLM connection {llm_connection_id} is not active.")
        return llm_connection

    def build_analyst_configs(self, agent_definition: RuntimeAgentDefinition) -> list[AnalystAgentConfig]:
        return build_analyst_configs_from_definition(
            name=agent_definition.name,
            description=agent_definition.description,
            definition=agent_definition.definition or {},
        )

    def build_execution(self, agent_definition: RuntimeAgentDefinition) -> AiAgentExecutionConfig:
        definition = agent_definition.definition if isinstance(agent_definition.definition, dict) else {}
        return build_execution_from_definition(
            definition=definition,
            name=agent_definition.name,
            description=agent_definition.description,
        )
