import sys
from pathlib import Path
from typing import Optional


REPO_ROOT = Path(__file__).resolve().parents[4]
PACKAGE_ROOT = REPO_ROOT
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from langbridge.packages.orchestrator.langbridge_orchestrator.agents.web_search import (  # noqa: E402
    WebSearchAgent,
    WebSearchResultItem,
)


class StubSearchProvider:
    name = "stub-search"

    def search(
        self,
        query: str,
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> list[WebSearchResultItem]:
        return self._results()[:max_results]

    async def search_async(
        self,
        query: str,
        *,
        max_results: int,
        region: Optional[str],
        safe_search: Optional[str],
        timebox_seconds: int,
    ) -> list[WebSearchResultItem]:
        return self.search(
            query,
            max_results=max_results,
            region=region,
            safe_search=safe_search,
            timebox_seconds=timebox_seconds,
        )

    @staticmethod
    def _results() -> list[WebSearchResultItem]:
        return [
            WebSearchResultItem(
                title="Energy market digest",
                url="https://example.com/general-digest",
                snippet="Broad market coverage and commodity pricing updates.",
                source="example.com",
                html_content="""
                    <html>
                      <title>General energy market digest</title>
                      <body>
                        <p>Commodity pricing and broad market sentiment across several sectors.</p>
                        <p>This page focuses on trading conditions instead of public incentives.</p>
                      </body>
                    </html>
                """,
            ),
            WebSearchResultItem(
                title="Policy digest",
                url="https://example.org/policy-digest",
                snippet="Government support update for energy markets.",
                source="example.org",
                html_content="""
                    <html>
                      <title>Europe renewable subsidies outlook</title>
                      <body>
                        <h1>Europe renewable subsidies outlook</h1>
                        <p>Europe renewable subsidies expanded for solar and wind projects in 2026.</p>
                        <p>Several EU member states increased renewable energy grants and tax incentives.</p>
                      </body>
                    </html>
                """,
            ),
        ]


def test_search_reranks_results_using_html_summary() -> None:
    agent = WebSearchAgent(provider=StubSearchProvider(), llm=None)

    result = agent.search("europe renewable subsidies", max_results=2)

    assert [item.rank for item in result.results] == [1]
    assert result.results[0].url == "https://example.org/policy-digest"
    assert result.results[0].html_content is None
    assert result.results[0].html_content_summary is not None
    assert "<article>" in result.results[0].html_content_summary
    assert "Europe renewable subsidies outlook" in result.results[0].html_content_summary
    assert result.citations[0] == "https://example.org/policy-digest"
    assert result.answer is not None
    assert "Europe renewable subsidies expanded" in result.answer


def test_search_result_exports_include_html_summary() -> None:
    agent = WebSearchAgent(provider=StubSearchProvider(), llm=None)

    result = agent.search("europe renewable subsidies", max_results=2)
    tabular = result.to_tabular()
    documents = result.to_documents()

    assert tabular["columns"][-1] == "html_content_summary"
    assert len(tabular["rows"][0]) == len(tabular["columns"])
    assert "html_content_summary" in documents[0]
    assert documents[0]["html_content_summary"] is not None
