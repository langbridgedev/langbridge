from langbridge.semantic.loader import load_semantic_model


def test_dimension_vector_block_loads_as_canonical_shape() -> None:
    semantic_model = load_semantic_model(
        """
version: "1.0"
datasets:
  shopify_orders:
    dataset_id: "11111111-1111-1111-1111-111111111111"
    relation_name: orders_enriched
    dimensions:
      - name: country
        expression: country
        type: string
        vector:
          enabled: true
          refresh_interval: 1d
          store:
            type: managed_faiss
"""
    )

    dimension = semantic_model.datasets["shopify_orders"].dimensions[0]
    assert dimension.vector is not None
    assert dimension.vector.enabled is True
    assert dimension.vector.refresh_interval == "1d"
    assert dimension.vector.store.type == "managed_faiss"
    assert "vectorized" not in dimension.model_dump(exclude_none=True)
    assert "vector_index" not in dimension.model_dump(exclude_none=True)


def test_legacy_dimension_vector_fields_are_normalized() -> None:
    semantic_model = load_semantic_model(
        """
version: "1.0"
datasets:
  shopify_orders:
    dataset_id: "11111111-1111-1111-1111-111111111111"
    dimensions:
      - name: country
        expression: country
        type: string
        vectorized: true
        vector_reference: semantic-qdrant
        vector_index:
          refresh_interval: 6h
          index_name: country-index
"""
    )

    dimension = semantic_model.datasets["shopify_orders"].dimensions[0]
    assert dimension.vector is not None
    assert dimension.vector.enabled is True
    assert dimension.vector.refresh_interval == "6h"
    assert dimension.vector.store.type == "connector"
    assert dimension.vector.store.connector_name == "semantic-qdrant"
    assert dimension.vector.store.index_name == "country-index"
