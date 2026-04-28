"""Specification-based routing helpers for Langbridge AI."""

import re
from dataclasses import dataclass

from langbridge.ai.base import AgentSpecification

_TOKEN_RE = re.compile(r"[a-z0-9_]+")
_MULTI_STEP_PHRASES = (
    " and then ",
    " then ",
    " after that ",
    " before you ",
    " use both ",
    " compare with web",
    " research and",
    " search and explain",
)


@dataclass(frozen=True)
class QuestionProfile:
    """Centralized text profile for routing decisions."""

    text: str
    tokens: frozenset[str]

    @classmethod
    def from_question(cls, question: str) -> "QuestionProfile":
        text = question.casefold()
        return cls(text=text, tokens=frozenset(_TOKEN_RE.findall(text)))

    def contains_phrase(self, phrase: str) -> bool:
        return phrase.casefold() in self.text

    def has_multi_step_cue(self) -> bool:
        padded = f" {self.text} "
        return any(phrase in padded for phrase in _MULTI_STEP_PHRASES)


@dataclass(frozen=True)
class AgentRouteMatch:
    specification: AgentSpecification
    score: int


class SpecificationRouter:
    """Scores agent specifications against a user question."""

    def rank(
        self,
        *,
        question: str,
        specifications: list[AgentSpecification],
    ) -> list[AgentRouteMatch]:
        profile = QuestionProfile.from_question(question)
        matches = [
            AgentRouteMatch(specification=specification, score=self._score(profile, specification))
            for specification in specifications
        ]
        return sorted(matches, key=lambda item: item.score, reverse=True)

    def direct_match(
        self,
        *,
        question: str,
        specifications: list[AgentSpecification],
    ) -> AgentRouteMatch | None:
        profile = QuestionProfile.from_question(question)
        if profile.has_multi_step_cue():
            return None

        for match in self.rank(question=question, specifications=specifications):
            if (
                match.specification.can_execute_direct
                and not match.specification.has_side_effects
                and match.score >= match.specification.routing.direct_threshold
            ):
                return match
        return None

    @staticmethod
    def _score(profile: QuestionProfile, specification: AgentSpecification) -> int:
        keyword_score = sum(1 for keyword in specification.routing.keywords if keyword.casefold() in profile.tokens)
        phrase_score = sum(3 for phrase in specification.routing.phrases if profile.contains_phrase(phrase))
        return keyword_score + phrase_score


__all__ = ["AgentRouteMatch", "QuestionProfile", "SpecificationRouter"]
