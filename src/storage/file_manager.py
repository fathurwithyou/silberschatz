import os
from typing import Optional
from src.core.models import TableSchema
from src.storage.serializer import Serializer


class FileManager:
    
    def __init__(self, data_directory: str = "data"):
        self.data_directory = data_directory
        self.schema_directory = os.path.join(data_directory, "schemas")
        self.table_directory = os.path.join(data_directory, "tables")
        self.schema_directory = self.schema_directory 
        self.table_directory = self.table_directory
        self.serializer = Serializer()
        
        os.makedirs(self.schema_directory, exist_ok=True)
        os.makedirs(self.table_directory, exist_ok=True)
    
    def get_schema_path(self, table_name: str) -> str:
        return os.path.join(self.schema_directory, f"{table_name}.dat")
    
    def get_table_path(self, table_name: str) -> str:
        return os.path.join(self.table_directory, f"{table_name}.dat")
    
    def schema_exists(self, table_name: str) -> bool:
        return os.path.exists(self.get_schema_path(table_name))
    
    def save_schema(self, schema: TableSchema) -> None:
        schema_path = self.get_schema_path(schema.table_name)
        serialized_schema = self.serializer.serialize_schema(schema)
        
        with open(schema_path, 'wb') as f:
            f.write(serialized_schema)
    
    def load_schema(self, table_name: str) -> Optional[TableSchema]:
        schema_path = self.get_schema_path(table_name)
        
        if not os.path.exists(schema_path):
            return None
        
        with open(schema_path, 'rb') as f:
            serialized_schema = f.read()
        
        return self.serializer.deserialize_schema(serialized_schema)
    
    def delete_schema(self, table_name: str) -> None:
        schema_path = self.get_schema_path(table_name)
        
        if os.path.exists(schema_path):
            os.remove(schema_path)
    
    def create_table_file(self, table_name: str) -> None:
        table_path = self.get_table_path(table_name)
        open(table_path, 'wb').close()
    
    def delete_table_file(self, table_name: str) -> None:
        table_path = self.get_table_path(table_name)
        
        if os.path.exists(table_path):
            os.remove(table_path)
    
    def list_schema_files(self) -> list[str]:
        if not os.path.exists(self.schema_directory):
            return []
        
        schema_files = os.listdir(self.schema_directory)
        return [f.replace('.dat', '') for f in schema_files if f.endswith('.dat')]
    
    def validate_schema(self, schema: TableSchema) -> None:
        if not schema.table_name:
            raise ValueError("Table name cannot be empty")
        
        if not schema.columns:
            raise ValueError("Table must have at least one column")
        
        column_names = set()
        for col in schema.columns:
            if col.name in column_names:
                raise ValueError(f"Duplicate column name: {col.name}")
            column_names.add(col.name)
        
        if schema.primary_key and schema.primary_key not in column_names:
            raise ValueError(f"Primary key '{schema.primary_key}' not found in columns")
        
        for col in schema.columns:
            if col.foreign_key:
                ref_table = col.foreign_key.referenced_table
                if not self.schema_exists(ref_table):
                    raise ValueError(f"Referenced table '{ref_table}' does not exist")
                
                ref_schema = self.load_schema(ref_table)
                if ref_schema is None:
                    raise ValueError(f"Cannot load schema for referenced table '{ref_table}'")
                
                ref_col_names = {c.name for c in ref_schema.columns}
                
                if col.foreign_key.referenced_column not in ref_col_names:
                    raise ValueError(
                        f"Referenced column '{col.foreign_key.referenced_column}' "
                        f"not found in table '{ref_table}'"
                    )