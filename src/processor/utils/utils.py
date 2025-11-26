from src.core.models import TableSchema, DataType
from typing import List, Dict, Any

def get_schema_from_table_name(schemas: List[TableSchema], table_name: str) -> TableSchema:
    for schema in schemas:
        if schema.table_name == table_name:
            return schema
    raise ValueError(f"Table '{table_name}' not found in schemas")

def validate_column_in_schemas(schemas: List[TableSchema], column_name: str) -> None:
    if column_name.count('.') == 0:
        # cari jumlah kemunculan kolom di semua schema
        count = 0
        for sch in schemas:
            for col in sch.columns:
                if col.name == column_name:
                    count += 1
        if count == 0:
            raise ValueError(f"Column '{column_name}' not found in any table")
        elif count > 1:
            raise ValueError(f"Ambiguous column '{column_name}' found in multiple tables")
    
    elif column_name.count('.') == 1:
        # table_name.column_name
        table_name, col_name = column_name.split('.')
        schema = get_schema_from_table_name(schemas, table_name)
        if not any(col.name == col_name for col in schema.columns):
            raise ValueError(f"Column '{col_name}' not found in table '{table_name}'")
    
    else:
        raise ValueError(f"Invalid column name '{column_name}'")

def get_column_type(schemas: List[TableSchema], column_name: str) -> DataType:
    schema = None
    lookup_name = column_name
    
    if column_name.count('.') == 0:
        for sch in schemas:
            for col in sch.columns:
                if col.name == column_name:
                    schema = sch
    
    elif column_name.count('.') == 1:
        # table_name.column_name
        table_name, col_name = column_name.split('.')
        schema = get_schema_from_table_name(schemas, table_name)
        lookup_name = col_name
    
    if schema is None:
        raise ValueError(f"Invalid column '{column_name}'")
    
    for col in schema.columns:
        if col.name == lookup_name:
            return col.data_type
        
    raise ValueError(f"Column '{column_name}' not found")


# yang di row pasti {table_name}.{column_name}: value
# di column_name bisa aja ada table_name atau bisa juga gaada
def get_column_value(row: Dict[str, Any], column_name: str) -> Any:
    if column_name in row:
        return row.get(column_name)
    
    for key, val in row.items():
        if key.endswith(f".{column_name}"):
            return val
    
    raise ValueError(f"Column '{column_name}' not found in row")