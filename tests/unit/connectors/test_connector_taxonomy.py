import pytest

from langbridge.connectors.base import SqlConnectorFactory, VectorDBConnectorFactory
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.builtin.postgres.connector import PostgresConnector
from langbridge.connectors.vector.faiss.connector import FaissConnector


def test_sql_connector_factory_uses_runtime_types() -> None:
    connector_class = SqlConnectorFactory.get_sql_connector_class_reference(
        ConnectorRuntimeType.POSTGRES
    )

    assert connector_class is PostgresConnector
    assert SqlConnectorFactory.get_sqlglot_dialect(ConnectorRuntimeType.POSTGRES) == "postgres"


def test_sql_connector_factory_rejects_non_sql_runtime_types() -> None:
    with pytest.raises(ValueError, match="No SQL connector found"):
        SqlConnectorFactory.get_sql_connector_class_reference(
            ConnectorRuntimeType.MONGODB
        )


def test_vector_connector_factory_uses_runtime_types() -> None:
    connector_class = VectorDBConnectorFactory.get_managed_vector_db_class_reference(
        ConnectorRuntimeType.FAISS
    )

    assert connector_class is FaissConnector
    assert ConnectorRuntimeType.FAISS in VectorDBConnectorFactory.get_all_managed_vector_dbs()
