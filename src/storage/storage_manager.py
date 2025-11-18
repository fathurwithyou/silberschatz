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
from src.storage.ddl import DDLManager
from src.storage.dml import DMLManager
from src.storage.statistics import StatisticsManager


class StorageManager(IStorageManager):
    
    def __init__(self, data_directory: str = "data"):
        self.data_directory = data_directory
        self.ddl_manager = DDLManager(f"src/{self.data_directory}")
        self.dml_manager = DMLManager(f"src/{self.data_directory}")
        self.statistics_manager = StatisticsManager(f"src/{self.data_directory}")
    
    def read_block(self, data_retrieval: DataRetrieval) -> Rows:
        schema = self.ddl_manager.load_schema(data_retrieval.table_name)
        
        if schema is None:
            raise ValueError(f"Table '{data_retrieval.table_name}' does not exist")
        
        rows = self.dml_manager.load_all_rows(data_retrieval.table_name, schema)
        
        if data_retrieval.conditions:
            rows = self.dml_manager.apply_conditions(rows, data_retrieval.conditions)
        
        if data_retrieval.columns:
            rows = self.dml_manager.project_columns(rows, data_retrieval.columns)
        
        rows = self.dml_manager.apply_limit_offset(
            rows, 
            data_retrieval.limit, 
            data_retrieval.offset or 0
        )
        
        return rows
    
    def write_block(self, data_write: DataWrite) -> int:
        pass
    
    def delete_block(self, data_deletion: DataDeletion) -> int:
        pass
    
    def get_stats(self, table_name: str) -> Statistic:
        schema = self.ddl_manager.load_schema(table_name)
        if schema is None:
            raise ValueError(f"Table '{table_name}' does not exist")

        rows = self.dml_manager.load_all_rows(table_name, schema)
        
        # n_r : number of tuples (rows).
        n_r = self.statistics_manager.get_tuple_count(rows)

        # l_r : tuple size in bytes 
        l_r = self.statistics_manager.get_tuple_size(schema)

        # f_r : blocking factor (tuples per block)
        f_r = self.statistics_manager.get_blocking_factor(l_r)

        # b_r : number of blocks (pages)
        b_r = self.statistics_manager.get_number_of_blocks(n_r, f_r)

        # V(A,r) : number of distinct values for each column
        V = self.statistics_manager.calculate_distinct_values(rows, schema)

        # Opsional : min, max, null counts
        min_values, max_values = self.statistics_manager.calculate_min_max_values(rows, schema)
        null_counts = self.statistics_manager.calculate_null_counts(rows, schema)

        return Statistic(
            table_name=table_name,
            n_r=n_r,
            b_r=b_r,
            l_r=l_r,
            f_r=f_r,
            V=V,
            min_values=min_values,
            max_values=max_values,
            null_counts=null_counts
        )
    
    def set_index(self, table: str, column: str, index_type: str) -> None:
        pass
    
    def drop_index(self, table: str, column: str) -> None:
        pass
    
    def has_index(self, table: str, column: str) -> bool:
        pass
    
    def create_table(self, schema: TableSchema) -> None:
        if self.ddl_manager.schema_exists(schema.table_name):
            raise ValueError(f"Table '{schema.table_name}' already exists")

        self.ddl_manager.validate_schema(schema)

        self.ddl_manager.save_schema(schema)
        self.ddl_manager.create_table_file(schema.table_name)

    def drop_table(self, table_name: str) -> None:
        if not self.ddl_manager.schema_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")

        self.ddl_manager.delete_schema(table_name)
        self.ddl_manager.delete_table_file(table_name)

    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        return self.ddl_manager.load_schema(table_name)

    def list_tables(self) -> List[str]:
        return self.ddl_manager.list_schema_files()