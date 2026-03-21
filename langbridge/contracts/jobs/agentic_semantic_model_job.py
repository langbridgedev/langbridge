from __future__ import annotations

import uuid
from typing import List

from pydantic import Field, model_validator

from langbridge.contracts.base import _Base
from langbridge.contracts.jobs.type import JobType


class CreateAgenticSemanticModelJobRequest(_Base):
    job_type: JobType = JobType.AGENTIC_SEMANTIC_MODEL
    workspace_id: uuid.UUID
    actor_id: uuid.UUID
    semantic_model_id: uuid.UUID
    dataset_ids: List[uuid.UUID] = Field(default_factory=list)
    question_prompts: List[str] = Field(default_factory=list)
    include_sample_values: bool = False

    @model_validator(mode="after")
    def _validate_request(self) -> "CreateAgenticSemanticModelJobRequest":
        if not self.dataset_ids:
            raise ValueError("dataset_ids must include at least one dataset.")

        normalized_prompts = [prompt.strip() for prompt in self.question_prompts if prompt and prompt.strip()]
        if len(normalized_prompts) < 3 or len(normalized_prompts) > 10:
            raise ValueError("question_prompts must include between 3 and 10 prompts.")
        self.question_prompts = normalized_prompts
        return self
