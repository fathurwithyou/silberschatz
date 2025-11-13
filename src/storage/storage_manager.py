# Implement IStorageManager interface
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


class StorageManager(IStorageManager):
    
    def __init__(self, data_directory: str = "data"):
        self.data_directory = data_directory
        pass
    
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
        pass
    
    def drop_table(self, table_name: str) -> None:
        pass
    
    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        pass
    
    def list_tables(self) -> List[str]:
        pass