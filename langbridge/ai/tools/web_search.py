"""Provider-backed web search tool for Langbridge AI."""

import html
import re
from dataclasses import dataclass
from typing import Any, Iterable, Protocol
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from langbridge.ai.events import AIEventEmitter, AIEventSource

try:  # pragma: no cover - exercised only when dependency is absent
    import httpx
except Exception:  # pragma: no cover
    httpx = None  # type: ignore[assignment]


DEFAULT_WEB_SEARCH_PROVIDER = "duckduckgo"
MAX_RESULTS_CAP = 20

_HTML_RESULT_RE = re.compile(
    r"<a(?=[^>]*class=['\"]result-link['\"])(?P<attrs>[^>]*)>"
    r"(?P<title>.*?)</a>"
    r"(?P<tail>.*?)(?=<a[^>]*class=['\"]result-link['\"]|<form[^>]*>|</table>|\Z)",
    re.IGNORECASE | re.DOTALL,
)
_HTML_SNIPPET_RE = re.compile(
    r"<td[^>]*class=['\"]result-snippet['\"][^>]*>(?P<snippet>.*?)</td>",
    re.IGNORECASE | re.DOTALL,
)
_HREF_RE = re.compile(r"href=['\"](?P<href>[^'\"]+)['\"]", re.IGNORECASE)


class WebSearchProvider(Protocol):
    name: str

    async def search_async(
        self,
        query: str,
        *,
        max_results: int,
        region: str | None = None,
        safe_search: str | None = None,
        timebox_seconds: int = 10,
    ) -> list["WebSearchResultItem"]:
        ...


@dataclass(slots=True)
class WebSearchResultItem:
    title: str
    url: str
    snippet: str = ""
    source: str = ""
    html_content_summary: str | None = None
    rank: int = 0

    def to_dict(self) -> dict[str, object]:
        return {
            "title": self.title,
            "url": self.url,
            "snippet": self.snippet,
            "source": self.source,
            "html_content_summary": self.html_content_summary,
            "rank": self.rank,
        }

    def to_row(self) -> list[str]:
        return [
            str(self.rank) if self.rank else "",
            self.title,
            self.url,
            self.snippet,
            self.source,
            self.html_content_summary or "",
        ]


@dataclass(slots=True)
class WebSearchResult:
    query: str
    provider: str
    results: list[WebSearchResultItem]
    warnings: list[str]
    answer: str | None = None
    citations: list[str] | None = None
    weak_results: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "query": self.query,
            "provider": self.provider,
            "results": [item.to_dict() for item in self.results],
            "warnings": list(self.warnings),
            "answer": self.answer,
            "citations": list(self.citations or []),
            "weak_results": self.weak_results,
        }

    def to_tabular(self) -> dict[str, object]:
        return {
            "columns": ["rank", "title", "url", "snippet", "source", "html_content_summary"],
            "rows": [item.to_row() for item in self.results],
        }


@dataclass(slots=True)
class WebSearchPolicy:
    allowed_domains: list[str]
    denied_domains: list[str]
    require_allowed_domain: bool = False
    focus_terms: list[str] | None = None
    max_results: int = 6
    region: str | None = None
    safe_search: str | None = None
    timebox_seconds: int = 10


class DuckDuckGoWebSearchProvider:
    """DuckDuckGo-backed web search provider with JSON and lite HTML fallback."""

    name = "duckduckgo"

    _SAFE_SEARCH_MAP = {
        "off": "-1",
        "moderate": "1",
        "strict": "2",
    }

    def __init__(
        self,
        *,
        api_url: str = "https://api.duckduckgo.com/",
        html_url: str = "https://lite.duckduckgo.com/lite/",
        user_agent: str = "langbridge-web-search/1.0",
        transport: Any | None = None,
    ) -> None:
        self.api_url = api_url
        self.html_url = html_url
        self.user_agent = user_agent
        self.transport = transport

    async def search_async(
        self,
        query: str,
        *,
        max_results: int,
        region: str | None = None,
        safe_search: str | None = None,
        timebox_seconds: int = 10,
    ) -> list[WebSearchResultItem]:
        if httpx is None:
            raise RuntimeError("httpx is required for DuckDuckGoWebSearchProvider.")
        clean_query = self._clean_query(query)
        capped_max_results = self._cap_max_results(max_results)
        timeout = httpx.Timeout(timebox_seconds)
        async with httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": self.user_agent},
            transport=self.transport,
            follow_redirects=True,
        ) as client:
            results = await self._search_json(
                client,
                clean_query,
                max_results=capped_max_results,
                region=region,
                safe_search=safe_search,
            )
            if not results:
                results = await self._search_html(
                    client,
                    clean_query,
                    max_results=capped_max_results,
                    region=region,
                    safe_search=safe_search,
                )
        self._apply_ranking(results)
        return results[:capped_max_results]

    async def _search_json(
        self,
        client: Any,
        query: str,
        *,
        max_results: int,
        region: str | None,
        safe_search: str | None,
    ) -> list[WebSearchResultItem]:
        response = await client.get(
            self.api_url,
            params=self._build_json_params(query, region=region, safe_search=safe_search),
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("DuckDuckGo JSON response must be an object.")
        return self._parse_json_results(query, payload, max_results=max_results)

    async def _search_html(
        self,
        client: Any,
        query: str,
        *,
        max_results: int,
        region: str | None,
        safe_search: str | None,
    ) -> list[WebSearchResultItem]:
        response = await client.get(
            self.html_url,
            params=self._build_html_params(query, region=region, safe_search=safe_search),
        )
        response.raise_for_status()
        return self._parse_html_results(response.text, max_results=max_results)

    def _build_json_params(
        self,
        query: str,
        *,
        region: str | None,
        safe_search: str | None,
    ) -> dict[str, str]:
        params = {
            "q": query,
            "format": "json",
            "no_redirect": "1",
            "no_html": "1",
            "t": "langbridge",
        }
        if region:
            params["kl"] = region
        safe_value = self._safe_search_value(safe_search)
        if safe_value is not None:
            params["kp"] = safe_value
        return params

    def _build_html_params(
        self,
        query: str,
        *,
        region: str | None,
        safe_search: str | None,
    ) -> dict[str, str]:
        params = {"q": query}
        if region:
            params["kl"] = region
        safe_value = self._safe_search_value(safe_search)
        if safe_value is not None:
            params["kp"] = safe_value
        return params

    def _parse_json_results(
        self,
        query: str,
        payload: dict[str, Any],
        *,
        max_results: int,
    ) -> list[WebSearchResultItem]:
        results: list[WebSearchResultItem] = []
        seen_urls: set[str] = set()

        def add_result(title: str, url: str, snippet: str, source: str | None = None) -> None:
            clean_url = str(url or "").strip()
            if not clean_url or clean_url in seen_urls or len(results) >= max_results:
                return
            seen_urls.add(clean_url)
            results.append(
                WebSearchResultItem(
                    title=str(title or clean_url).strip(),
                    url=clean_url,
                    snippet=str(snippet or "").strip(),
                    source=str(source or self._source_from_url(clean_url) or self.name).strip(),
                )
            )

        heading = str(payload.get("Heading") or "").strip()
        abstract_text = str(payload.get("AbstractText") or payload.get("Abstract") or "").strip()
        abstract_url = str(payload.get("AbstractURL") or "").strip()
        abstract_source = str(payload.get("AbstractSource") or "").strip()
        if abstract_text and abstract_url:
            add_result(heading or abstract_source or query, abstract_url, abstract_text, abstract_source)

        answer = str(payload.get("Answer") or "").strip()
        answer_url = str(payload.get("AnswerURL") or "").strip()
        answer_type = str(payload.get("AnswerType") or "").strip()
        if answer and answer_url:
            add_result(heading or answer_type or query, answer_url, answer, answer_type)

        definition = str(payload.get("Definition") or "").strip()
        definition_url = str(payload.get("DefinitionURL") or "").strip()
        definition_source = str(payload.get("DefinitionSource") or "").strip()
        if definition and definition_url:
            add_result(heading or definition_source or query, definition_url, definition, definition_source)

        for entry in self._iter_related_topics(payload.get("RelatedTopics")):
            text = str(entry.get("Text") or "").strip()
            url = str(entry.get("FirstURL") or "").strip()
            if not text or not url:
                continue
            title = text.split(" - ", 1)[0].strip() if " - " in text else text
            add_result(title or query, url, text)
            if len(results) >= max_results:
                break

        for entry in self._dict_list(payload.get("Results")):
            if len(results) >= max_results:
                break
            text = str(entry.get("Text") or "").strip()
            url = str(entry.get("FirstURL") or "").strip()
            if not text or not url:
                continue
            title = text.split(" - ", 1)[0].strip() if " - " in text else text
            add_result(title or query, url, text)
        return results

    def _parse_html_results(self, payload: str, *, max_results: int) -> list[WebSearchResultItem]:
        results: list[WebSearchResultItem] = []
        seen_urls: set[str] = set()
        for match in _HTML_RESULT_RE.finditer(payload or ""):
            if len(results) >= max_results:
                break
            href_match = _HREF_RE.search(match.group("attrs"))
            if href_match is None:
                continue
            url = self._resolve_duckduckgo_result_url(href_match.group("href"))
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            snippet_match = _HTML_SNIPPET_RE.search(match.group("tail"))
            snippet = self._clean_html(snippet_match.group("snippet")) if snippet_match else ""
            results.append(
                WebSearchResultItem(
                    title=self._clean_html(match.group("title")) or url,
                    url=url,
                    snippet=snippet,
                    source=self._source_from_url(url) or self.name,
                )
            )
        return results

    @classmethod
    def _safe_search_value(cls, value: str | None) -> str | None:
        if not value:
            return None
        return cls._SAFE_SEARCH_MAP.get(value.strip().casefold())

    @staticmethod
    def _clean_query(query: str) -> str:
        clean = str(query or "").strip()
        if not clean:
            raise ValueError("Web search query is required.")
        return clean

    @staticmethod
    def _cap_max_results(max_results: int) -> int:
        if max_results < 1:
            raise ValueError("max_results must be at least 1.")
        return min(int(max_results), MAX_RESULTS_CAP)

    @staticmethod
    def _iter_related_topics(raw_topics: Any) -> Iterable[dict[str, Any]]:
        for topic in DuckDuckGoWebSearchProvider._dict_list(raw_topics):
            nested = topic.get("Topics")
            if nested is not None:
                yield from DuckDuckGoWebSearchProvider._dict_list(nested)
            else:
                yield topic

    @staticmethod
    def _dict_list(value: Any) -> list[dict[str, Any]]:
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
    def _clean_html(value: str) -> str:
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
        target = parse_qs(parsed.query).get("uddg", [])
        if target:
            return unquote(target[0]).strip()
        return raw_value

    @staticmethod
    def _apply_ranking(results: list[WebSearchResultItem]) -> None:
        for index, result in enumerate(results, start=1):
            result.rank = index


def create_web_search_provider(name: str | None = None) -> WebSearchProvider:
    """Create a concrete web search provider from a provider name."""

    provider_name = (name or DEFAULT_WEB_SEARCH_PROVIDER).strip().casefold()
    if provider_name in {"duckduckgo", "ddg"}:
        return DuckDuckGoWebSearchProvider()
    raise ValueError(f"Unsupported web search provider: {name}")


class WebSearchTool(AIEventSource):
    """Runs web search through a configured provider and enforces source policy."""

    def __init__(
        self,
        *,
        provider: WebSearchProvider,
        policy: WebSearchPolicy | None = None,
        event_emitter: AIEventEmitter | None = None,
    ) -> None:
        super().__init__(event_emitter=event_emitter)
        self._provider = provider
        self._policy = policy or WebSearchPolicy(allowed_domains=[], denied_domains=[])

    @property
    def provider_name(self) -> str:
        return getattr(self._provider, "name", self._provider.__class__.__name__)

    async def search(self, query: str) -> WebSearchResult:
        focused_query = self._focused_query(query)
        await self._emit_ai_event(
            event_type="WebSearchStarted",
            message="Searching allowed web sources.",
            source="web-search",
            details={
                "provider": self.provider_name,
                "max_results": self._policy.max_results,
                "allowed_domains": list(self._policy.allowed_domains),
            },
        )
        raw_items = await self._provider.search_async(
            focused_query,
            max_results=self._policy.max_results,
            region=self._policy.region,
            safe_search=self._policy.safe_search,
            timebox_seconds=self._policy.timebox_seconds,
        )
        filtered, warnings = self._filter_items(raw_items)
        await self._emit_ai_event(
            event_type="WebSearchCompleted",
            message=f"Web search returned {len(filtered)} allowed result(s).",
            source="web-search",
            details={
                "provider": self.provider_name,
                "result_count": len(filtered),
                "warning_count": len(warnings),
            },
        )
        return WebSearchResult(
            query=focused_query,
            provider=self.provider_name,
            results=filtered,
            warnings=warnings,
            weak_results=not bool(filtered),
        )

    def _focused_query(self, query: str) -> str:
        focused = query.strip()
        query_text = focused.casefold()
        for term in self._policy.focus_terms or []:
            clean = term.strip()
            if clean and clean.casefold() not in query_text:
                focused = f"{focused} {clean}"
        return focused

    def _filter_items(
        self,
        items: list[WebSearchResultItem],
    ) -> tuple[list[WebSearchResultItem], list[str]]:
        allowed = [domain.casefold() for domain in self._policy.allowed_domains]
        denied = [domain.casefold() for domain in self._policy.denied_domains]
        accepted: list[WebSearchResultItem] = []
        warnings: list[str] = []
        for item in items:
            host = urlparse(item.url).netloc.casefold()
            denied_match = any(self._domain_matches(host, domain) for domain in denied)
            allowed_match = not allowed or any(self._domain_matches(host, domain) for domain in allowed)
            if denied_match:
                warnings.append(f"Rejected denied domain: {host}")
                continue
            if self._policy.require_allowed_domain and not allowed_match:
                warnings.append(f"Rejected non-allowlisted domain: {host}")
                continue
            accepted.append(item)
        for index, item in enumerate(accepted, start=1):
            item.rank = index
        return accepted, warnings

    @staticmethod
    def _domain_matches(host: str, domain: str) -> bool:
        return host == domain or host.endswith(f".{domain}")


__all__ = [
    "DEFAULT_WEB_SEARCH_PROVIDER",
    "DuckDuckGoWebSearchProvider",
    "MAX_RESULTS_CAP",
    "WebSearchPolicy",
    "WebSearchProvider",
    "WebSearchResult",
    "WebSearchResultItem",
    "WebSearchTool",
    "create_web_search_provider",
]
