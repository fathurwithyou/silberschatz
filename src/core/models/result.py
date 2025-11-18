from typing import List, TypeVar, Generic, Union
from dataclasses import dataclass
from datetime import datetime
from .storage import TableSchema


T = TypeVar('T')

@dataclass
class Rows(Generic[T]):
    data: List[T]
    rows_count: int
    schema: List[TableSchema] = []

@dataclass
class ExecutionResult:
    transaction_id: int
    timestamp: datetime
    message: str
    data: Union[Rows, None]
    query: str
