import os
from typing import List, Optional
from src.core import IStorageManager
from src.core.models import (
    DataRetrieval, 
    DataWrite, 
    DataDeletion,
    Statistic, 
    TableSchema, 
    Rows
)
from src.storage.file_manager import FileManager


class StorageManager(IStorageManager):
    
    def __init__(self, data_directory: str = "data"):
        self.data_directory = data_directory
        self.file_manager = FileManager(f"src/{self.data_directory}")
    
    def read_block(self, data_retrieval: DataRetrieval) -> Rows:
        pass
    
    def write_block(self, data_write: DataWrite) -> int:
        pass
    
    def delete_block(self, data_deletion: DataDeletion) -> int:
        pass
    
    def get_stats(self, table_name: str) -> Statistic:
        pass
    
    def set_index(self, table: str, column: str, index_type: str) -> None:
        pass
    
    def drop_index(self, table: str, column: str) -> None:
        pass
    
    def has_index(self, table: str, column: str) -> bool:
        pass
    
    def create_table(self, schema: TableSchema) -> None:
        if self.file_manager.schema_exists(schema.table_name):
            raise ValueError(f"Table '{schema.table_name}' already exists")
        
        self.file_manager.validate_schema(schema)
        
        self.file_manager.save_schema(schema)
        self.file_manager.create_table_file(schema.table_name)
    
    def drop_table(self, table_name: str) -> None:
        if not self.file_manager.schema_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")
        
        self.file_manager.delete_schema(table_name)
        self.file_manager.delete_table_file(table_name)
    
    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        return self.file_manager.load_schema(table_name)
    
    def list_tables(self) -> List[str]:
        return self.file_manager.list_schema_files()