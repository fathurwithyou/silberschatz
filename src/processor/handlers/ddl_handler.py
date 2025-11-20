from typing import TYPE_CHECKING
from src.core.models import ExecutionResult, ParsedQuery
from datetime import datetime

if TYPE_CHECKING:
    from ..processor import QueryProcessor

class DDLHandler:
    """
    Menangani query DDL (Data Definition Language) 
    """
    def __init__(self, processor: QueryProcessor):
        self.processor = processor

    def handle(self, query: ParsedQuery) -> ExecutionResult:
        raise NotImplementedError