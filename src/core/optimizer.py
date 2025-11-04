from abc import ABC, abstractmethod
from .models import ParsedQuery


class IQueryOptimizer(ABC):
    @abstractmethod
    def parse_query(self, query: str) -> ParsedQuery:
        raise NotImplementedError

    @abstractmethod
    def optimize_query(self, query: ParsedQuery) -> ParsedQuery:
        raise NotImplementedError

    @abstractmethod
    def get_cost(self, query: ParsedQuery) -> int:
        raise NotImplementedError
