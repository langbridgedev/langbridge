from __future__ import annotations

import asyncio
from pathlib import Path
import sys

EXAMPLE_DIR = Path(__file__).resolve().parent
REPO_ROOT = EXAMPLE_DIR.parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from langbridge import LangbridgeClient

from setup import setup_database

CONFIG_PATH = EXAMPLE_DIR / "langbridge_config.yml"


async def main() -> None:
    setup_database()

    client = LangbridgeClient.local(config_path=str(CONFIG_PATH))

    datasets = await client.datasets.list()
    print("Datasets")
    print(datasets.model_dump(mode="json"))

    result = await client.datasets.query(
        "shopify_orders",
        metrics=["net_sales"],
        dimensions=["country"],
        limit=5,
        order={"net_sales": "desc"},
    )
    print("Dataset query")
    print(result.model_dump(mode="json"))

    answer = await client.agents.ask("Show me top countries by net sales this quarter")
    print("Agent answer")
    print(answer.model_dump(mode="json"))


if __name__ == "__main__":
    asyncio.run(main())
