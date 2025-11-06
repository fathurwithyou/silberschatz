from typing import List
from dataclasses import dataclass


@dataclass
class QueryTree:
    type: str
    value: str
    children: List['QueryTree']
    parent: 'QueryTree' = None


@dataclass
class ParsedQuery:
    tree: QueryTree
    query: str
