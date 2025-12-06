from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from .models import (
    DataRetrieval, DataWrite, DataDeletion,
    Statistic, TableSchema, Rows
)

class IStorageManager(ABC):

    @abstractmethod
    def read_block(self, data_retrieval: DataRetrieval) -> Rows:
        raise NotImplementedError
    
    @abstractmethod
    def write_block(self, data_write: DataWrite) -> int:
        raise NotImplementedError
    
    @abstractmethod
    def delete_block(self, data_deletion: DataDeletion) -> int:
        raise NotImplementedError
    
    @abstractmethod
    def read_buffer(self, data_retrieval: DataRetrieval) -> Rows:
        raise NotImplementedError
    
    @abstractmethod
    def write_buffer(self, data_write: DataWrite) -> int:
        raise NotImplementedError
    
    @abstractmethod
    def delete_buffer(self, data_deletion: DataDeletion) -> int:
        raise NotImplementedError
    
    @abstractmethod
    def flush_buffer(self, table_name: Optional[str] = None) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def get_buffer_stats(self) -> Dict[str, Any]:
        raise NotImplementedError
    
    @abstractmethod
    def get_stats(self, table_name: str) -> Statistic:
        raise NotImplementedError
    
    @abstractmethod
    def set_index(self, table: str, column: str, index_type: str) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def drop_index(self, table: str, column: str) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def has_index(self, table: str, column: str) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def create_table(self, schema: TableSchema) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        raise NotImplementedError
    
    @abstractmethod
    def list_tables(self) -> List[str]:
        raise NotImplementedError