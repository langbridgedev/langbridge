import pathlib
import sys


REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from langbridge.orchestrator.agents.visual import VisualAgent  # noqa: E402


def test_visual_agent_generates_descriptive_title_from_axes() -> None:
    agent = VisualAgent()

    result = agent.run(
        {
            "columns": ["year", "benchmark_annualised_return"],
            "rows": [(2020, 0.051), (2021, 0.072), (2022, -0.014)],
        },
        question="Show the benchmark annualised return by year.",
    )

    assert result["chart_type"] == "line"
    assert result["title"] == "Benchmark Annualised Return by Year"


def test_visual_agent_replaces_placeholder_title_with_generated_title() -> None:
    agent = VisualAgent()

    result = agent.run(
        {
            "columns": ["region", "revenue"],
            "rows": [("US", 2200), ("EMEA", 1200), ("APAC", 900)],
        },
        title="Visualization for 'Show revenue by region'",
        question="Show revenue by region.",
    )

    assert result["title"] == "Revenue by Region"


def test_visual_agent_preserves_explicit_custom_title() -> None:
    agent = VisualAgent()

    result = agent.run(
        {
            "columns": ["year", "benchmark_annualised_return"],
            "rows": [(2020, 0.051), (2021, 0.072), (2022, -0.014)],
        },
        title="MSCI India annualised return trend",
        question="Show the benchmark annualised return by year.",
    )

    assert result["title"] == "MSCI India annualised return trend"
