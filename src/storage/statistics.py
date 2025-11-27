import os
import math
from typing import Dict, Any, Set, Optional
from src.core.models import TableSchema, Rows, DataType
from src.storage.serializer import Serializer

# Block size satuan 4KB 
BLOCK_SIZE: int = 4096

class StatisticsManager:
    def __init__(self, data_directory: str = "data"):
        self.data_directory = data_directory
        self.serializer = Serializer()
    
    def get_tuple_count(self, rows: Rows) -> int:
        return rows.rows_count
    
    def get_tuple_size(self, schema: TableSchema) -> int:
        return self.serializer.calculate_row_size(schema)
    
    def get_blocking_factor(self, l_r: int) -> int:
        return BLOCK_SIZE // l_r if l_r > 0 else 0

    def get_number_of_blocks(self, n_r: int, f_r: int) -> int:
        return math.ceil(n_r / f_r) if f_r > 0 else 0
    
    def calculate_distinct_values(self, rows: Rows[Dict[str, Any]], schema: TableSchema) -> Dict[str, int]:
        # Nama kolom, set value unik
        distinct_values: Dict[str, Set[Any]] = {
            col.name: set() for col in schema.columns
            }
        
        for row in rows.data:
            for col in schema.columns:
                # Untuk kolom tidak null
                if row[col.name] is not None:
                    distinct_values[col.name].add(row[col.name])
        
        return {col_name: len(values) for col_name, values in distinct_values.items()}

    def calculate_min_max_values(self, 
                                 rows: Rows[Dict[str, Any]], 
                                 schema: TableSchema) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        min_values: Dict[str, Any] = {}
        max_values: Dict[str, Any] = {}

        for col in schema.columns:
            # Handle kolom bukan numerik
            if col.data_type not in [DataType.INTEGER, DataType.FLOAT]:
                continue

            col_values = [row[col.name] for row in rows.data if row[col.name] is not None]

            # Handle empty list
            if not col_values:
                continue
            
            min_values[col.name] = min(col_values)
            max_values[col.name] = max(col_values)
        
        # min max opsional
        if not min_values:
            return None, None
        
        return min_values, max_values

    def calculate_null_counts(self, rows: Rows[Dict[str, Any]], schema: TableSchema) -> Optional[Dict[str, int]]:
        null_counts: Dict[str, int] = {}
        
        # Find null counts for each column
        for row in rows.data:
            for col in schema.columns:
                if row.get(col.name) is None:
                    if col.name not in null_counts:
                        null_counts[col.name] = 0
                    null_counts[col.name] += 1
        
        # null count opsional kalau gada null
        if not null_counts:
            return None
        
        return null_counts
    
