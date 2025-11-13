from ..processor import QueryProcessor
from core.models import ExecutionResult, ParsedQuery
from datetime import datetime

class DDLHandler:
    """
    Menangani query DDL (Data Definition Language) 
    """
    def __init__(self, processor: QueryProcessor):
        self.processor = processor

    def handle(self, query: ParsedQuery) -> ExecutionResult:
        raise NotImplementedError