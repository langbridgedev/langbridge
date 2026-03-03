from __future__ import annotations

import uuid
from typing import Dict, List

from pydantic import Field, model_validator

from langbridge.packages.common.langbridge_common.contracts.base import _Base

from .type import JobType


class CreateAgenticSemanticModelJobRequest(_Base):
    job_type: JobType = JobType.AGENTIC_SEMANTIC_MODEL
    organisation_id: uuid.UUID
    project_id: uuid.UUID | None = None
    user_id: uuid.UUID
    semantic_model_id: uuid.UUID
    connector_id: uuid.UUID
    selected_tables: List[str] = Field(default_factory=list)
    selected_columns: Dict[str, List[str]] = Field(default_factory=dict)
    question_prompts: List[str] = Field(default_factory=list)
    include_sample_values: bool = False

    @model_validator(mode="after")
    def _validate_request(self) -> "CreateAgenticSemanticModelJobRequest":
        if not self.selected_tables:
            raise ValueError("selected_tables must include at least one table.")

        normalized_prompts = [prompt.strip() for prompt in self.question_prompts if prompt and prompt.strip()]
        if len(normalized_prompts) < 3 or len(normalized_prompts) > 10:
            raise ValueError("question_prompts must include between 3 and 10 prompts.")
        self.question_prompts = normalized_prompts

        table_key_lookup = {table.strip().lower(): table for table in self.selected_tables if table.strip()}
        if not table_key_lookup:
            raise ValueError("selected_tables must include at least one non-empty table reference.")

        normalized_columns: Dict[str, List[str]] = {}
        for table_key, columns in self.selected_columns.items():
            normalized_table_key = str(table_key).strip().lower()
            if normalized_table_key not in table_key_lookup:
                raise ValueError(f"selected_columns contains unknown table '{table_key}'.")
            deduped_columns = []
            seen_columns: set[str] = set()
            for column in columns or []:
                candidate = str(column).strip()
                if not candidate:
                    continue
                lowered = candidate.lower()
                if lowered in seen_columns:
                    continue
                seen_columns.add(lowered)
                deduped_columns.append(candidate)
            if deduped_columns:
                normalized_columns[table_key_lookup[normalized_table_key]] = deduped_columns

        self.selected_columns = normalized_columns
        return self
