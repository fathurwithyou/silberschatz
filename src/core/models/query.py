from typing import List, Optional
from dataclasses import dataclass
from enum import Enum

@dataclass
class QueryTree:
    type: str
    value: str
    children: List['QueryTree']
    parent: Optional['QueryTree'] = None


@dataclass
class ParsedQuery:
    tree: QueryTree
    query: str

class QueryNodeType(str, Enum):
    # Base operations
    TABLE = "table"
    UNKNOWN = "unknown"

    # SELECT operations
    PROJECTION = "projection"
    SELECTION = "selection"
    ORDER_BY = "order_by"
    LIMIT = "limit"

    # JOIN operations
    JOIN = "join"
    THETA_JOIN = "theta_join"
    NATURAL_JOIN = "natural_join"
    CARTESIAN_PRODUCT = "cartesian_product"

    # DML operations
    UPDATE = "update"
    DELETE = "delete"
    INSERT = "insert"

    # DDL operations
    CREATE_TABLE = "create_table"
    DROP_TABLE = "drop_table"
    CREATE_INDEX = "create_index"
    DROP_INDEX = "drop_index"

    # Transaction operations
    BEGIN_TRANSACTION = "begin_transaction"
    COMMIT = "commit"