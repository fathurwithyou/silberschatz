from typing import Optional, Protocol
from src.core.models.query import QueryTree


class OptimizationRule(Protocol):

    def apply(self, node: QueryTree) -> Optional[QueryTree]:
        ...

    def is_applicable(self, node: QueryTree) -> bool:
        ...

    @property
    def name(self) -> str:
        ...
