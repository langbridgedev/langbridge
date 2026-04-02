import logging
from dataclasses import dataclass
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False

@dataclass
class ForeignKeyMetadata:
    name: str
    column: str
    foreign_key: str
    schema: str
    table: str

@dataclass
class TableMetadata:
    schema: str
    name: str
    columns: List[ColumnMetadata] | None = None

@dataclass
class SchemaMetadata:
    name: str