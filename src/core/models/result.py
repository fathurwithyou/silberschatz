from typing import List, TypeVar, Generic, Union
from dataclasses import dataclass
from datetime import datetime


T = TypeVar('T')

@dataclass
class Rows(Generic[T]):
    data: List[T]
    rows_count: int

@dataclass
class ExecutionResult:
    transaction_id: int
    timestamp: datetime
    message: str
    data: Union[Rows, None]
    query: str
