from abc import ABC, abstractmethod
from typing import List, Any, Dict
from src.core.models import ComparisonOperator, TableSchema, DataType
from ..utils import get_column_type

class ConditionNode(ABC):
    @abstractmethod
    def evaluate(self, row: Dict[str, Any]) -> bool:
        """
            Evaluate the condition against a given row.
        """
        pass

class SimpleCondition(ConditionNode):
    def __init__(self, left: str, op: ComparisonOperator, right: str, schemas: List[TableSchema]):
        self.left = left
        self.op = op
        self.right = right
        self.schemas = schemas

    def evaluate(self, row: Dict[str, Any]) -> bool:
        val_left, type_left = self._parse_value_and_type(self.left, self.schemas, row)
        val_right, type_right = self._parse_value_and_type(self.right, self.schemas, row)
        
        if type_left != type_right:
            if type_left in (DataType.INTEGER, DataType.FLOAT) and type_right in (DataType.INTEGER, DataType.FLOAT):
                val_left = float(val_left)
                val_right = float(val_right)
            elif type_left in (DataType.CHAR, DataType.VARCHAR) and type_right in (DataType.CHAR, DataType.VARCHAR):
                pass
            else:
                raise ValueError("Type mismatch in condition evaluation")
        
        if self.op == ComparisonOperator.EQ: return val_left == val_right
        if self.op == ComparisonOperator.GT: return val_left > val_right
        if self.op == ComparisonOperator.LT: return val_left < val_right
        if self.op == ComparisonOperator.GE: return val_left >= val_right
        if self.op == ComparisonOperator.LE: return val_left <= val_right
        if self.op == ComparisonOperator.NE: return val_left != val_right
        
        raise ValueError(f"Unsupported operator {self.op}")

    def _parse_value_and_type(self, value: str, schemas: List[TableSchema], row: Dict[str, Any]) -> tuple[Any, DataType]:
        if value.isdigit():
            return int(value), DataType.INTEGER
        
        if value.replace('.', '', 1).isdigit() and '.' in value:
            return float(value), DataType.FLOAT
        
        if value.startswith("'") and value.endswith("'"):
            return value.strip("'\""), DataType.VARCHAR
        
        column_type = get_column_type(schemas, value)
        return row.get(value), column_type

class ComplexCondition(ConditionNode):
    def __init__(self, op: str, children: List[ConditionNode]):
        self.op = op.upper()
        self.children = children

    def evaluate(self, row: Dict[str, Any]) -> bool:
        if self.op == 'AND':
            return all(child.evaluate(row) for child in self.children)
        if self.op == 'OR':
            return any(child.evaluate(row) for child in self.children)
        return False