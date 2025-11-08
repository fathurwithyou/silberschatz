from abc import ABC, abstractmethod
from .models import ExecutionResult


class IQueryProcessor(ABC):
    @abstractmethod
    def execute_query(self, query: str) -> ExecutionResult:
        raise NotImplementedError
