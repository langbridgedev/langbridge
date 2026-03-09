import asyncio
import pathlib
import sys

import pytest

pytest.importorskip("faiss")

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2]))

from langbridge.packages.connectors.langbridge_connectors.api.faiss.config import FaissConnectorConfig  # noqa: E402
from langbridge.packages.connectors.langbridge_connectors.api.faiss.connector import FaissConnector  # noqa: E402
from langbridge.packages.common.langbridge_common.errors.connector_errors import ConnectorError  # noqa: E402


def test_faiss_connector_upsert_and_search(tmp_path):
    async def _run() -> None:
        config = FaissConnectorConfig(location=str(tmp_path))
        connector = FaissConnector(config=config)
        await connector.test_connection()

        ids = await connector.upsert_vectors(
            [[1.0, 0.0], [0.0, 1.0]],
            metadata=[{"value": "alpha"}, {"value": "beta"}],
        )

        assert ids == [1, 2]

        matches = await connector.search([0.9, 0.1], top_k=1)
        assert matches
        assert matches[0]["metadata"]["value"] == "alpha"

    asyncio.run(_run())


def test_faiss_connector_persists_state(tmp_path):
    async def _run() -> None:
        config = FaissConnectorConfig(location=str(tmp_path))
        connector = FaissConnector(config=config)
        await connector.test_connection()
        await connector.upsert_vectors([[1.0, 0.0]], metadata=[{"label": "first"}])

        restored = FaissConnector(config=config)
        await restored.test_connection()
        matches = await restored.search([1.0, 0.0], top_k=1)

        assert matches
        assert matches[0]["metadata"] == {"label": "first"}

    asyncio.run(_run())


def test_faiss_connector_enforces_dimension(tmp_path):
    async def _run() -> None:
        config = FaissConnectorConfig(location=str(tmp_path))
        connector = FaissConnector(config=config)
        await connector.upsert_vectors([[1.0, 0.0]], metadata=[None])

        with pytest.raises(ConnectorError):
            await connector.upsert_vectors([[1.0, 0.0, 0.5]], metadata=[None])

    asyncio.run(_run())
