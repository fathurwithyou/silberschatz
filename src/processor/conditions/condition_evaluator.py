from .condition_parser import ConditionParser
from src.core.models import TableSchema
from typing import List, Dict, Any


class ConditionEvaluator:
    def __init__(self, schemas: List[TableSchema]):
        self.parser = ConditionParser.get_instance(schemas)
        
    def evaluate(self, condition_str: str, row: Dict[str, Any]) -> bool:
        condition_node = self.parser.parse(condition_str)
        return condition_node.evaluate(row)
    