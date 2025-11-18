import os
from typing import List, Any, Dict, Optional
from src.core.models import Rows, TableSchema, Condition, ComparisonOperator
from src.storage.serializer import Serializer


class DMLManager:
    
    def __init__(self, data_directory: str):
        self.data_directory = data_directory
        self.table_directory = os.path.join(data_directory, "tables")
        self.serializer = Serializer()
        
        os.makedirs(self.table_directory, exist_ok=True)
    
    def get_table_path(self, table_name: str) -> str:
        return os.path.join(self.table_directory, f"{table_name}.dat")
    
    def load_all_rows(self, table_name: str, schema: TableSchema) -> Rows:
        table_path = self.get_table_path(table_name)
        
        if not os.path.exists(table_path):
            return Rows(data=[], rows_count=0)
        
        with open(table_path, 'rb') as f:
            data = f.read()
        
        if len(data) == 0:
            return Rows(data=[], rows_count=0)
        
        return self.serializer.deserialize_rows(data, schema)
    
    def save_all_rows(self, table_name: str, rows: Rows, schema: TableSchema) -> None:
        table_path = self.get_table_path(table_name)
        serialized = self.serializer.serialize_rows(rows, schema)
        
        with open(table_path, 'wb') as f:
            f.write(serialized)
    
    def apply_conditions(self, rows: Rows, conditions: List[Condition]) -> Rows:
        if not conditions:
            return rows
        
        filtered = []
        
        for row in rows.data:
            match = True
            
            for condition in conditions:
                value = row.get(condition.column)
                
                if not self._evaluate_condition(value, condition.operator, condition.value):
                    match = False
                    break
            
            if match:
                filtered.append(row)
        
        return Rows(data=filtered, rows_count=len(filtered))
    
    def _evaluate_condition(self, left: Any, operator: ComparisonOperator, right: Any) -> bool:
        try:
            if operator == ComparisonOperator.EQ:
                return left == right
            elif operator == ComparisonOperator.NE:
                return left != right
            elif operator == ComparisonOperator.LT:
                return left < right
            elif operator == ComparisonOperator.LE:
                return left <= right
            elif operator == ComparisonOperator.GT:
                return left > right
            elif operator == ComparisonOperator.GE:
                return left >= right
            return False
        except:
            return False
    
    def project_columns(self, rows: Rows, columns: List[str]) -> Rows:
        if not columns or columns == ['*']:
            return rows
        
        projected = []
        
        for row in rows.data:
            projected_row = {col: row.get(col) for col in columns}
            projected.append(projected_row)
        
        return Rows(data=projected, rows_count=len(projected))
    
    def apply_limit_offset(self, rows: Rows, limit: Optional[int], offset: int) -> Rows:
        start = offset
        
        if limit is not None:
            end = start + limit
            sliced = rows.data[start:end]
        else:
            sliced = rows.data[start:]
        
        return Rows(data=sliced, rows_count=len(sliced))