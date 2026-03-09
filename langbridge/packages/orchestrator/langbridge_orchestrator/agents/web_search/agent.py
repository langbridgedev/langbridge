"""
Web search agent that retrieves and normalizes search results.
"""
import asyncio
import html
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional, Protocol
from urllib.parse import parse_qs, unquote, urljoin, urlparse

try:  # pragma: no cover - optional dependency for environments that do not execute HTTP providers
    import httpx
except Exception:  # pragma: no cover
    httpx = None  # type: ignore

from langbridge.packages.orchestrator.langbridge_orchestrator.llm.provider import LLMProvider

DEFAULT_MAX_RESULTS = 6
MAX_RESULTS_CAP = 20


@dataclass
class WebSearchResultItem:
    """Single web search result."""

    title: str
    url: str
    snippet: str = ""
    source: str = ""
    rank: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "url": self.url,
            "snippet": self.snippet,
            "source": self.source,
            "rank": self.rank,
        }

    def to_row(self) -> list[str]:
        return [
            str(self.rank) if self.rank else "",
            self.title,
            self.url,
            self.snippet,
            self.source,
        ]


@dataclass
class WebSearchResult:
    """Aggregated web search output."""

    query: str
    provider: str
    results: list[WebSearchResultItem] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    answer: Optional[str] = None
    citations: list[str] = field(default_factory=list)
    weak_results: bool = False
    follow_up_question: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "provider": self.provider,
            "results": [result.to_dict() for result in self.results],
            "warnings": list(self.warnings),
            "answer": self.answer,
            "citations": list(self.citations),
            "weak_results": self.weak_results,
            "follow_up_question": self.follow_up_question,
        }

    def to_tabular(self) -> Dict[str, Any]:
        if not self.results:
            return {
                "columns": ["message"],
                "rows": [[f"No web results found for '{self.query}'."]],
            }

        return {
            "columns": ["rank", "title", "url", "snippet", "source"],
            "rows": [result.to_row() for result in self.results],
        }

    def to_documents(self) -> list[Dict[str, Any]]:
        return [
            {
                "title": result.title,
                "snippet": result.snippet,
                "url": result.url,
                "source": result.source,
            }
            for result in self.results
        ]


class WebSearchProvider(Protocol):
    """Protocol describing a web search provider implementation."""

    name: str

    def search(
        self,
        query: str,
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> list[WebSearchResultItem]:
        ...

    async def search_async(
        self,
        query: str,
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> list[WebSearchResultItem]:
        ...


class DuckDuckGoInstantAnswerProvider:
    """DuckDuckGo Instant Answer API provider."""

    name = "duckduckgo"

    _SAFE_SEARCH_MAP = {
        "off": "-1",
        "moderate": "1",
        "strict": "2",
    }

    def __init__(
        self,
        *,
        base_url: str = "https://api.duckduckgo.com/",
        html_search_url: str = "https://lite.duckduckgo.com/lite/",
        user_agent: str = "langbridge-web-search/1.0",
    ) -> None:
        self.base_url = base_url
        self.html_search_url = html_search_url
        self.user_agent = user_agent

    def search(
        self,
        query: str,
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> list[WebSearchResultItem]:
        if httpx is None:
            raise RuntimeError("httpx is required for DuckDuckGoInstantAnswerProvider.")
        params = self._build_params(query, region=region, safe_search=safe_search)
        timeout = httpx.Timeout(timebox_seconds)
        with httpx.Client(timeout=timeout, headers={"User-Agent": self.user_agent}) as client:
            response = client.get(self.base_url, params=params)
            response.raise_for_status()
            payload = response.json()
            results = self._parse_results(query, payload, max_results=max_results)
            if results:
                return results
            html_response = client.get(
                self.html_search_url,
                params=self._build_html_params(query, region=region, safe_search=safe_search),
                follow_redirects=True,
            )
            html_response.raise_for_status()
        return self._parse_html_results(html_response.text, max_results=max_results)

    async def search_async(
        self,
        query: str,
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> list[WebSearchResultItem]:
        if httpx is None:
            raise RuntimeError("httpx is required for DuckDuckGoInstantAnswerProvider.")
        params = self._build_params(query, region=region, safe_search=safe_search)
        timeout = httpx.Timeout(timebox_seconds)
        async with httpx.AsyncClient(timeout=timeout, headers={"User-Agent": self.user_agent}) as client:
            response = await client.get(self.base_url, params=params)
            response.raise_for_status()
            payload = response.json()
            results = self._parse_results(query, payload, max_results=max_results)
            if results:
                return results
            html_response = await client.get(
                self.html_search_url,
                params=self._build_html_params(query, region=region, safe_search=safe_search),
                follow_redirects=True,
            )
            html_response.raise_for_status()
        return self._parse_html_results(html_response.text, max_results=max_results)

    def _build_params(
        self,
        query: str,
        *,
        region: Optional[str],
        safe_search: Optional[str],
    ) -> Dict[str, str]:
        params = {
            "q": query,
            "format": "json",
            "no_redirect": "1",
            "no_html": "1",
            "t": "langbridge",
        }
        if region:
            params["kl"] = region
        safe_value = self._normalize_safe_search(safe_search)
        if safe_value is not None:
            params["kp"] = safe_value
        return params

    def _build_html_params(
        self,
        query: str,
        *,
        region: Optional[str],
        safe_search: Optional[str],
    ) -> Dict[str, str]:
        params = {"q": query}
        if region:
            params["kl"] = region
        safe_value = self._normalize_safe_search(safe_search)
        if safe_value is not None:
            params["kp"] = safe_value
        return params

    def _parse_results(
        self,
        query: str,
        payload: Dict[str, Any],
        *,
        max_results: int,
    ) -> list[WebSearchResultItem]:
        results: list[WebSearchResultItem] = []
        seen_urls: set[str] = set()

        def _add_result(title: str, url: str, snippet: str, source: Optional[str] = None) -> None:
            if not url or url in seen_urls:
                return
            seen_urls.add(url)
            clean_title = (title or url).strip()
            clean_snippet = (snippet or "").strip()
            resolved_source = (source or self._source_from_url(url) or self.name).strip()
            results.append(
                WebSearchResultItem(
                    title=clean_title,
                    url=url,
                    snippet=clean_snippet,
                    source=resolved_source,
                )
            )

        heading = str(payload.get("Heading") or "").strip()
        abstract_text = str(payload.get("AbstractText") or payload.get("Abstract") or "").strip()
        abstract_url = str(payload.get("AbstractURL") or "").strip()
        abstract_source = str(payload.get("AbstractSource") or "").strip()
        if abstract_text and abstract_url:
            _add_result(heading or abstract_source or query, abstract_url, abstract_text, abstract_source)

        answer = str(payload.get("Answer") or "").strip()
        answer_url = str(payload.get("AnswerURL") or "").strip()
        answer_type = str(payload.get("AnswerType") or "").strip()
        if answer and answer_url:
            _add_result(heading or answer_type or query, answer_url, answer, answer_type)

        definition = str(payload.get("Definition") or "").strip()
        definition_url = str(payload.get("DefinitionURL") or "").strip()
        definition_source = str(payload.get("DefinitionSource") or "").strip()
        if definition and definition_url:
            _add_result(heading or definition_source or query, definition_url, definition, definition_source)

        for entry in self._iter_related_topics(payload.get("RelatedTopics")):
            if len(results) >= max_results:
                break
            text = str(entry.get("Text") or "").strip()
            url = str(entry.get("FirstURL") or "").strip()
            if not text or not url:
                continue
            title = text.split(" - ", 1)[0].strip() if " - " in text else text
            _add_result(title or query, url, text, None)

        if len(results) < max_results:
            for entry in self._coerce_list(payload.get("Results")):
                if len(results) >= max_results:
                    break
                text = str(entry.get("Text") or "").strip()
                url = str(entry.get("FirstURL") or "").strip()
                if not text or not url:
                    continue
                title = text.split(" - ", 1)[0].strip() if " - " in text else text
                _add_result(title or query, url, text, None)

        return results[:max_results]

    def _parse_html_results(self, payload: str, *, max_results: int) -> list[WebSearchResultItem]:
        results: list[WebSearchResultItem] = []
        seen_urls: set[str] = set()
        pattern = re.compile(
            r"<a(?=[^>]*class=['\"]result-link['\"])(?P<attrs>[^>]*)>"
            r"(?P<title>.*?)</a>"
            r"(?P<tail>.*?)(?=<a[^>]*class=['\"]result-link['\"]|<form[^>]*>|</table>)",
            re.IGNORECASE | re.DOTALL,
        )
        for match in pattern.finditer(payload):
            if len(results) >= max_results:
                break
            href_match = re.search(
                r"href=['\"](?P<href>[^'\"]+)['\"]",
                match.group("attrs"),
                re.IGNORECASE,
            )
            if href_match is None:
                continue
            url = self._resolve_duckduckgo_result_url(href_match.group("href"))
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            title = self._clean_html_fragment(match.group("title"))
            snippet_match = re.search(
                r"<td[^>]*class=['\"]result-snippet['\"][^>]*>(?P<snippet>.*?)</td>",
                match.group("tail"),
                re.IGNORECASE | re.DOTALL,
            )
            snippet = self._clean_html_fragment(snippet_match.group("snippet")) if snippet_match else ""
            results.append(
                WebSearchResultItem(
                    title=title or url,
                    url=url,
                    snippet=snippet,
                    source=self._source_from_url(url) or self.name,
                )
            )
        return results[:max_results]

    @staticmethod
    def _iter_related_topics(raw_topics: Any) -> Iterable[Dict[str, Any]]:
        for topic in DuckDuckGoInstantAnswerProvider._coerce_list(raw_topics):
            if "Topics" in topic:
                for nested in DuckDuckGoInstantAnswerProvider._coerce_list(topic.get("Topics")):
                    yield nested
                continue
            if isinstance(topic, dict):
                yield topic

    @staticmethod
    def _coerce_list(value: Any) -> list[Dict[str, Any]]:
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def _source_from_url(url: str) -> str:
        try:
            return urlparse(url).netloc
        except ValueError:
            return ""

    @staticmethod
    def _clean_html_fragment(value: str) -> str:
        text = re.sub(r"<[^>]+>", " ", value or "")
        text = html.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _resolve_duckduckgo_result_url(value: str) -> str:
        raw_value = html.unescape(value or "").strip()
        if not raw_value:
            return ""
        if raw_value.startswith("//"):
            raw_value = f"https:{raw_value}"
        elif raw_value.startswith("/"):
            raw_value = urljoin("https://duckduckgo.com", raw_value)

        parsed = urlparse(raw_value)
        query = parse_qs(parsed.query)
        target = query.get("uddg", [])
        if target:
            return unquote(target[0]).strip()
        return raw_value

    @classmethod
    def _normalize_safe_search(cls, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        lowered = value.strip().lower()
        return cls._SAFE_SEARCH_MAP.get(lowered)


class WebSearchAgent:
    """Agent that performs query refinement, triage, and grounded synthesis."""

    def __init__(
        self,
        *,
        provider: Optional[WebSearchProvider] = None,
        llm: Optional[LLMProvider] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.provider = provider or DuckDuckGoInstantAnswerProvider()
        self.logger = logger or logging.getLogger(__name__)
        self.llm = llm

    @staticmethod
    def _tokens(text: str) -> set[str]:
        return set(re.findall(r"[a-z0-9]+", str(text or "").lower()))

    @staticmethod
    def _extract_json_blob(text: str) -> Optional[str]:
        if not text:
            return None
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        for index in range(start, len(text)):
            char = text[index]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return text[start : index + 1]
        return None

    def _parse_llm_payload(self, response: str) -> Optional[Dict[str, Any]]:
        blob = self._extract_json_blob(response)
        if not blob:
            return None
        try:
            parsed = json.loads(blob)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        return parsed

    @staticmethod
    def _normalize_alternates(value: Any, base_query: str) -> list[str]:
        if not isinstance(value, list):
            return []
        cleaned: list[str] = []
        for item in value:
            text = str(item or "").strip()
            if not text or text == base_query:
                continue
            cleaned.append(text)
        return cleaned

    @staticmethod
    def _dedupe_queries(queries: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []
        for query in queries:
            if not query:
                continue
            if query in seen:
                continue
            seen.add(query)
            ordered.append(query)
        return ordered

    def _build_query_refinement_prompt(self, query: str) -> str:
        prompt_sections = [
            "You refine web search queries for high-relevance retrieval.",
            "Return ONLY JSON with key: queries.",
            "queries must be a list with 1 to 3 concise search strings.",
            f"Original query: {query}",
        ]
        return "\n".join(prompt_sections)

    def _refine_queries_with_llm(self, query: str) -> Optional[list[str]]:
        if not self.llm:
            return None
        prompt = self._build_query_refinement_prompt(query)
        try:
            response = self.llm.complete(prompt, temperature=0.0, max_tokens=160)
        except Exception as exc:  # pragma: no cover - defensive guard
            self.logger.warning("WebSearchAgent query refinement failed: %s", exc)
            return None
        payload = self._parse_llm_payload(str(response))
        if not payload:
            return None
        raw_queries = payload.get("queries") or payload.get("search_queries")
        if isinstance(raw_queries, str):
            queries = [raw_queries]
        elif isinstance(raw_queries, list):
            queries = [str(item).strip() for item in raw_queries if str(item).strip()]
        else:
            queries = []
        deduped = self._dedupe_queries(queries)
        return deduped[:3] if deduped else None

    async def _refine_queries_with_llm_async(self, query: str) -> Optional[list[str]]:
        if not self.llm:
            return None
        return await asyncio.to_thread(self._refine_queries_with_llm, query)

    def _prepare_query_candidates(self, query: str) -> tuple[list[str], list[str]]:
        warnings: list[str] = []
        refined = self._refine_queries_with_llm(query)
        if refined:
            warnings.append(f"LLM generated {len(refined)} refined query candidate(s).")
            return self._dedupe_queries([*refined, query]), warnings
        return [query], warnings

    async def _prepare_query_candidates_async(self, query: str) -> tuple[list[str], list[str]]:
        warnings: list[str] = []
        refined = await self._refine_queries_with_llm_async(query)
        if refined:
            warnings.append(f"LLM generated {len(refined)} refined query candidate(s).")
            return self._dedupe_queries([*refined, query]), warnings
        return [query], warnings

    def _execute_query_sequence(
        self,
        queries: list[str],
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> tuple[list[WebSearchResultItem], list[str], list[str]]:
        warnings: list[str] = []
        attempts: list[str] = []
        merged: list[WebSearchResultItem] = []
        seen_urls: set[str] = set()
        per_query_limit = max(1, min(max_results, 8))
        for candidate in queries[:3]:
            attempts.append(candidate)
            try:
                results = self.provider.search(
                    candidate,
                    max_results=per_query_limit,
                    region=region,
                    safe_search=safe_search,
                    timebox_seconds=timebox_seconds,
                )
            except Exception as exc:  # pragma: no cover
                warnings.append(f"Search provider failed for query '{candidate}': {exc}")
                continue
            for result in results:
                if result.url in seen_urls:
                    continue
                seen_urls.add(result.url)
                merged.append(result)
                if len(merged) >= max_results * 2:
                    break
        return merged, attempts, warnings

    async def _execute_query_sequence_async(
        self,
        queries: list[str],
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> tuple[list[WebSearchResultItem], list[str], list[str]]:
        warnings: list[str] = []
        attempts: list[str] = []
        merged: list[WebSearchResultItem] = []
        seen_urls: set[str] = set()
        per_query_limit = max(1, min(max_results, 8))
        for candidate in queries[:3]:
            attempts.append(candidate)
            try:
                results = await self.provider.search_async(
                    candidate,
                    max_results=per_query_limit,
                    region=region,
                    safe_search=safe_search,
                    timebox_seconds=timebox_seconds,
                )
            except Exception as exc:  # pragma: no cover
                warnings.append(f"Search provider failed for query '{candidate}': {exc}")
                continue
            for result in results:
                if result.url in seen_urls:
                    continue
                seen_urls.add(result.url)
                merged.append(result)
                if len(merged) >= max_results * 2:
                    break
        return merged, attempts, warnings

    def _triage_results(
        self,
        *,
        query: str,
        results: list[WebSearchResultItem],
        max_results: int,
    ) -> tuple[list[WebSearchResultItem], bool]:
        if not results:
            return [], True

        query_tokens = self._tokens(query)
        scored: list[tuple[float, WebSearchResultItem]] = []
        for item in results:
            title_tokens = self._tokens(item.title)
            snippet_tokens = self._tokens(item.snippet)
            overlap = len(query_tokens.intersection(title_tokens.union(snippet_tokens)))
            denom = max(len(query_tokens), 1)
            score = overlap / denom
            if item.snippet:
                score += 0.1
            score = min(1.0, score)
            scored.append((score, item))

        scored.sort(key=lambda row: row[0], reverse=True)
        kept = [item for score, item in scored if score >= 0.2][:max_results]
        weak_results = len(kept) < min(2, max_results)
        return kept, weak_results

    def _synthesize_answer(
        self,
        *,
        query: str,
        triaged_results: list[WebSearchResultItem],
    ) -> tuple[Optional[str], list[str], bool, Optional[str]]:
        if not triaged_results:
            return None, [], True, self._build_follow_up_question(query)

        if self.llm:
            prompt_sections = [
                "You synthesize grounded answers from web snippets.",
                "Return ONLY JSON with keys: answer, citations, weak_results, follow_up_question.",
                "citations must be list of URLs actually used.",
                f"Question: {query}",
                "Search snippets:",
            ]
            for index, item in enumerate(triaged_results[:6], start=1):
                prompt_sections.append(
                    f"{index}. title={item.title}; url={item.url}; snippet={item.snippet}"
                )
            prompt = "\n".join(prompt_sections)
            try:
                response = self.llm.complete(prompt, temperature=0.1, max_tokens=420)
                payload = self._parse_llm_payload(str(response))
            except Exception as exc:  # pragma: no cover
                self.logger.warning("WebSearchAgent answer synthesis failed: %s", exc)
                payload = None

            if isinstance(payload, dict):
                answer = str(payload.get("answer") or "").strip() or None
                raw_citations = payload.get("citations")
                if isinstance(raw_citations, list):
                    citations = [str(item).strip() for item in raw_citations if str(item).strip()]
                else:
                    citations = []
                weak_results = bool(payload.get("weak_results"))
                follow_up = payload.get("follow_up_question")
                follow_up_question = (
                    str(follow_up).strip()
                    if isinstance(follow_up, str) and str(follow_up).strip()
                    else None
                )
                if answer:
                    return answer, citations, weak_results, follow_up_question

        top = triaged_results[:3]
        citations = [item.url for item in top]
        snippets = [item.snippet for item in top if item.snippet]
        answer = snippets[0] if snippets else f"Found {len(top)} relevant sources."
        weak = len(top) < 2
        return answer, citations, weak, self._build_follow_up_question(query) if weak else None

    @staticmethod
    def _build_follow_up_question(query: str) -> str:
        lowered = query.lower()
        if "latest" in lowered or "news" in lowered:
            return "Which company, region, or date range should I focus on?"
        if "policy" in lowered or "regulation" in lowered:
            return "Which jurisdiction or regulatory body should I prioritize?"
        return "Could you narrow this to a specific company, location, or timeframe?"

    def search(
        self,
        query: str,
        *,
        max_results: int = DEFAULT_MAX_RESULTS,
        region: Optional[str] = None,
        safe_search: Optional[str] = None,
        timebox_seconds: int = 10,
    ) -> WebSearchResult:
        clean_query = self._normalize_query(query)
        capped_max_results = self._normalize_max_results(max_results)
        query_sequence, warnings = self._prepare_query_candidates(clean_query)
        results, attempts, attempt_warnings = self._execute_query_sequence(
            query_sequence,
            max_results=capped_max_results,
            region=region,
            safe_search=safe_search,
            timebox_seconds=timebox_seconds,
        )
        warnings.extend(attempt_warnings)
        triaged_results, weak_results = self._triage_results(
            query=clean_query,
            results=results,
            max_results=capped_max_results,
        )
        answer, citations, weak_from_synthesis, follow_up = self._synthesize_answer(
            query=clean_query,
            triaged_results=triaged_results,
        )
        weak_results = weak_results or weak_from_synthesis
        if not results:
            warnings.append("No web results returned by the provider.")
        if weak_results:
            warnings.append("Search results were weak or only partially relevant.")
        self._apply_ranking(triaged_results)
        self.logger.info(
            "WebSearchAgent retrieved %d result(s) for query '%s' via %s after %d attempt(s)",
            len(triaged_results),
            clean_query,
            self.provider.name,
            len(attempts),
        )
        return WebSearchResult(
            query=clean_query,
            provider=self.provider.name,
            results=triaged_results,
            warnings=warnings,
            answer=answer,
            citations=citations,
            weak_results=weak_results,
            follow_up_question=follow_up,
        )

    async def search_async(
        self,
        query: str,
        *,
        max_results: int = DEFAULT_MAX_RESULTS,
        region: Optional[str] = None,
        safe_search: Optional[str] = None,
        timebox_seconds: int = 10,
    ) -> WebSearchResult:
        clean_query = self._normalize_query(query)
        capped_max_results = self._normalize_max_results(max_results)
        query_sequence, warnings = await self._prepare_query_candidates_async(clean_query)
        results, attempts, attempt_warnings = await self._execute_query_sequence_async(
            query_sequence,
            max_results=capped_max_results,
            region=region,
            safe_search=safe_search,
            timebox_seconds=timebox_seconds,
        )
        warnings.extend(attempt_warnings)
        triaged_results, weak_results = self._triage_results(
            query=clean_query,
            results=results,
            max_results=capped_max_results,
        )
        answer, citations, weak_from_synthesis, follow_up = self._synthesize_answer(
            query=clean_query,
            triaged_results=triaged_results,
        )
        weak_results = weak_results or weak_from_synthesis
        if not results:
            warnings.append("No web results returned by the provider.")
        if weak_results:
            warnings.append("Search results were weak or only partially relevant.")
        self._apply_ranking(triaged_results)
        self.logger.info(
            "WebSearchAgent retrieved %d result(s) for query '%s' via %s after %d attempt(s)",
            len(triaged_results),
            clean_query,
            self.provider.name,
            len(attempts),
        )
        return WebSearchResult(
            query=clean_query,
            provider=self.provider.name,
            results=triaged_results,
            warnings=warnings,
            answer=answer,
            citations=citations,
            weak_results=weak_results,
            follow_up_question=follow_up,
        )

    @staticmethod
    def _normalize_query(query: str) -> str:
        clean = str(query or "").strip()
        if not clean:
            raise ValueError("WebSearchAgent requires a non-empty query.")
        return clean

    @staticmethod
    def _normalize_max_results(max_results: int) -> int:
        if max_results < 1:
            raise ValueError("max_results must be at least 1.")
        return min(int(max_results), MAX_RESULTS_CAP)

    @staticmethod
    def _apply_ranking(results: list[WebSearchResultItem]) -> None:
        for idx, result in enumerate(results, start=1):
            result.rank = idx


__all__ = [
    "DuckDuckGoInstantAnswerProvider",
    "WebSearchAgent",
    "WebSearchProvider",
    "WebSearchResult",
    "WebSearchResultItem",
]
