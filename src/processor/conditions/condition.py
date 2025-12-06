from abc import ABC, abstractmethod
from typing import List, Any, Dict
from src.core.models import ComparisonOperator, TableSchema, DataType
from ..utils import get_column_type, get_column_value, validate_column_in_schemas

class ConditionNode(ABC):
    @abstractmethod
    def evaluate(self, row: Dict[str, Any]) -> bool:
        """
            Evaluate the condition against a given row.
        """
        raise NotImplementedError
    def check_valid(self) -> tuple[Any, ComparisonOperator, Any]:
        """
            Check if the condition is valid in terms of types and columns.
            This is specifically for SimpleCondition, to parse for index selection.
        """
        raise NotImplementedError

class SimpleCondition(ConditionNode):
    def __init__(self, left: str, op: ComparisonOperator, right: str, schemas: List[TableSchema]):
        self.left = left
        self.op = op
        self.right = right
        self.schemas = schemas
        
    def check_valid(self) -> tuple[Any, ComparisonOperator, Any]:
        val_left, type_left = self._check_value_and_type(self.left, self.schemas)
        val_right, type_right = self._check_value_and_type(self.right, self.schemas)
        
        if type_left != type_right:
            if type_left in (DataType.INTEGER, DataType.FLOAT) and type_right in (DataType.INTEGER, DataType.FLOAT):
                pass
            elif type_left in (DataType.CHAR, DataType.VARCHAR) and type_right in (DataType.CHAR, DataType.VARCHAR):
                pass
            else:
                return None, self.op, None
        
        if val_left in [col.name for col in self.schemas[0].columns]:
            return val_left, self.op, val_right

        return val_right, self.op, val_left
    
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

    def _check_value_and_type(self, value: str, schemas: List[TableSchema]) -> tuple[Any, DataType]:
        if value.isdigit():
            return int(value), DataType.INTEGER
        
        if value.replace('.', '', 1).isdigit() and '.' in value:
            return float(value), DataType.FLOAT
        
        if value.startswith("'") and value.endswith("'"):
            return value.strip("'\""), DataType.VARCHAR
        
        validate_column_in_schemas(schemas, value)
        column_type = get_column_type(schemas, value)
        return value, column_type
    
    def _parse_value_and_type(self, value: str, schemas: List[TableSchema], row: Dict[str, Any]) -> tuple[Any, DataType]:
        if value.isdigit():
            return int(value), DataType.INTEGER
        
        if value.replace('.', '', 1).isdigit() and '.' in value:
            return float(value), DataType.FLOAT
        
        if value.startswith("'") and value.endswith("'"):
            return value.strip("'\""), DataType.VARCHAR
        
        validate_column_in_schemas(schemas, value)
        column_type = get_column_type(schemas, value)
        column_value = get_column_value(row, value)
        return column_value, column_type

class ComplexCondition(ConditionNode):
    def __init__(self, op: str, children: List[ConditionNode]):
        self.op = op.upper()
        self.children = children

    def check_valid(self):
        raise NotImplementedError("ComplexCondition check is not supported")
    
    def evaluate(self, row: Dict[str, Any]) -> bool:
        if self.op == 'AND':
            return all(child.evaluate(row) for child in self.children)
        if self.op == 'OR':
            return any(child.evaluate(row) for child in self.children)
        return False