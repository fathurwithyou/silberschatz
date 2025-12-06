import os
from typing import List, Any, Dict, Optional
from src.core.models import Rows, TableSchema, Condition, ComparisonOperator
from src.storage.serializer import Serializer


class DMLManager:
    
    def __init__(self, data_directory: str, buffer_pool=None):
        self.data_directory = data_directory
        self.table_directory = os.path.join(data_directory, "tables")
        self.serializer = Serializer()
        self.buffer_pool = buffer_pool
        
        os.makedirs(self.table_directory, exist_ok=True)
    
    def get_table_path(self, table_name: str) -> str:
        return os.path.join(self.table_directory, f"{table_name}.dat")
    
    def _load_page_data(self, table_name: str) -> bytes:
        table_path = self.get_table_path(table_name)
        if not os.path.exists(table_path):
            return b''
        with open(table_path, 'rb') as f:
            return f.read()
    
    def _write_page_data(self, table_name: str, data: bytes) -> None:
        table_path = self.get_table_path(table_name)
        with open(table_path, 'wb') as f:
            f.write(data)
    
    def _load_from_disk(self, table_name: str, schema: TableSchema) -> Rows:
        data = self._load_page_data(table_name)
        
        if len(data) == 0:
            return Rows(data=[], rows_count=0)
        
        return self.serializer.deserialize_rows(data, schema)
    
    def _save_to_disk(self, table_name: str, rows: Rows, schema: TableSchema) -> None:
        serialized = self.serializer.serialize_rows(rows, schema)
        self._write_page_data(table_name, serialized)
    
    def load_all_rows(self, table_name: str, schema: TableSchema) -> Rows:
        if self.buffer_pool is None:
            return self._load_from_disk(table_name, schema)
        
        page_id = f"table:{table_name}"
        
        data = self.buffer_pool.get_page(page_id, lambda: self._load_page_data(table_name))
        
        if len(data) == 0:
            self.buffer_pool.unpin_page(page_id)
            return Rows(data=[], rows_count=0)
        
        rows = self.serializer.deserialize_rows(data, schema)
        self.buffer_pool.unpin_page(page_id)
        
        return rows
    
    def save_all_rows(self, table_name: str, rows: Rows, schema: TableSchema) -> None:
        if self.buffer_pool is None:
            self._save_to_disk(table_name, rows, schema)
            return
        
        page_id = f"table:{table_name}"
        serialized = self.serializer.serialize_rows(rows, schema)
        
        self.buffer_pool.put_page(page_id, serialized, mark_dirty=True)
    
    def flush_table(self, table_name: str) -> None:
        if self.buffer_pool is None:
            return
        
        page_id = f"table:{table_name}"
        
        self.buffer_pool.flush_page(page_id, lambda data: self._write_page_data(table_name, data))
    
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

    def _cast_by_schema(self, row_obj: Dict[str, Any], schema: TableSchema) -> Dict[str, Any]:
        casted = {}
        for col in schema.columns:
            name = col.name

            if name not in row_obj:
                continue

            val = row_obj[name]
            t = col.data_type

            try:
                if t.value.startswith("integer"):
                    casted[name] = None if val is None else int(val)
                elif t.value.startswith("float"):
                    casted[name] = None if val is None else float(val)
                elif t.value.startswith("char") or t.value.startswith("varchar"):
                    if val is None:
                        casted[name] = None
                    else:
                        s = str(val)
                        if col.max_length:
                            casted[name] = s[: col.max_length]
                        else:
                            casted[name] = s
                else:
                    casted[name] = val
            except Exception:
                casted[name] = val

        for k, v in row_obj.items():
            if k not in casted:
                casted[k] = v
        return casted

    def row_matches(self, row: Dict[str, Any], conditions: List[Condition]) -> bool:
        if not conditions:
            return True
        for cond in conditions:
            col = cond.column
            val = row.get(col)
            if not self._evaluate_condition(val, cond.operator, cond.value):
                return False
        return True

    def _matches(self, row: Dict[str, Any], conditions: List[Condition]) -> bool:
        return self.row_matches(row, conditions)
    
    def try_read_with_index(self, table_name: str, schema: TableSchema, 
                           conditions: List[Condition], indexes_dict: Dict[tuple, Any], 
                           use_buffer: bool = True) -> Optional[Rows]:
        for condition in conditions:
            column = condition.column
            
            if (table_name, column) not in indexes_dict:
                continue
            
            index = indexes_dict[(table_name, column)]
            row_ids = []
            
            if condition.operator == ComparisonOperator.EQ:
                row_ids = index.search(condition.value)
            
            elif hasattr(index, 'range_search'):
                if condition.operator == ComparisonOperator.GT:
                    row_ids = index.range_search(condition.value, float('inf'))
                    row_ids = [rid for rid in row_ids]
                elif condition.operator == ComparisonOperator.GE:
                    row_ids = index.range_search(condition.value, float('inf'))
                elif condition.operator == ComparisonOperator.LT:
                    row_ids = index.range_search(float('-inf'), condition.value)
                elif condition.operator == ComparisonOperator.LE:
                    row_ids = index.range_search(float('-inf'), condition.value)
            
            if row_ids:
                if use_buffer:
                    all_rows = self.load_all_rows(table_name, schema)
                else:
                    all_rows = self._load_from_disk(table_name, schema)
                filtered_data = [all_rows.data[rid] for rid in row_ids if rid < len(all_rows.data)]
                
                result_rows = Rows(data=filtered_data, rows_count=len(filtered_data))
                
                remaining_conditions = [c for c in conditions if c != condition]
                if remaining_conditions:
                    result_rows = self.apply_conditions(result_rows, remaining_conditions)
                
                return result_rows
        
        return None