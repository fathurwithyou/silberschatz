from typing import List, Optional
from dataclasses import dataclass


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
