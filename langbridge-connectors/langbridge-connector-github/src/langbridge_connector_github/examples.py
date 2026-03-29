
from pathlib import Path

from langbridge.connectors.saas.declarative import (
    DeclarativeDatasetExampleSet,
    load_declarative_dataset_examples,
)


def load_dataset_examples() -> DeclarativeDatasetExampleSet:
    return load_declarative_dataset_examples(Path(__file__).resolve().parents[2])
