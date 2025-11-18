import os
from typing import List, Dict, Any, Optional
from src.core import IStorageManager
from src.core.models import (
    DataRetrieval, 
    DataWrite, 
    DataDeletion,
    Statistic, 
    TableSchema,
    Rows,
    Condition 
)
from src.storage.ddl import DDLManager
from src.storage.dml import DMLManager


class StorageManager(IStorageManager):
    
    def __init__(self, data_directory: str = "data"):
        self.data_directory = data_directory
        self.ddl_manager = DDLManager(f"src/{self.data_directory}")
        self.dml_manager = DMLManager(f"src/{self.data_directory}")
    
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
        table = data_write.table_name
        schema = self.ddl_manager.load_schema(table) 
        pk_name = schema.primary_key

        if schema is None:
            raise ValueError(f"Table '{table}' does not exist")

        all_rows: Rows = self.dml_manager.load_all_rows(table, schema)

        # ======== INSERT ========
        if not data_write.is_update:
            new_row = dict(data_write.data) 
            new_row = self.dml_manager._cast_by_schema(new_row, schema)

            # Validasi PK unik
            if pk_name and pk_name in new_row:
                new_pk = new_row[pk_name]

                for r in all_rows.data:
                    if r.get(pk_name) == new_pk:
                        raise ValueError(f"Duplicate primary key '{pk_name}'={new_pk}")

            # Menambah row baru & simpan
            all_rows.data.append(new_row)
            all_rows.rows_count = len(all_rows.data)
            self.dml_manager.save_all_rows(table, all_rows, schema)
            return 1

        # ======== UPDATE ========
        conditions: List[Condition] = data_write.conditions

        updated_count = 0
        set_expr: Dict[str, Any] = dict(data_write.data)

        # Update in-memory
        for i, row in enumerate(all_rows.data):
            if self.dml_manager._matches(row, conditions):
                new_row = row.copy()

                for k, v in set_expr.items():
                    new_row[k] = v

                new_row = self.dml_manager._cast_by_schema(new_row, schema)

                if pk_name and (pk_name in set_expr):
                    new_pk = new_row[pk_name]
                    # agar tidak bentrok dengan row lain
                    for j, other in enumerate(all_rows.data):
                        if j != i and other.get(pk_name) == new_pk:
                            raise ValueError(f"UPDATE causes PK conflict '{pk_name}'={new_pk}")

                all_rows.data[i] = new_row
                updated_count += 1

        if updated_count > 0:
            all_rows.rows_count = len(all_rows.data)
            self.dml_manager.save_all_rows(table, all_rows, schema)

        return updated_count
    
    def delete_block(self, data_deletion: DataDeletion) -> int:
        table = data_deletion.table_name
        schema = self.ddl_manager.load_schema(table)

        if schema is None:
            raise ValueError(f"Table '{table}' does not exist")

        all_rows: Rows = self.dml_manager.load_all_rows(table, schema)

        conditions: List[Condition] = data_deletion.conditions

        before = len(all_rows.data)
        kept = [r for r in all_rows.data if not self.dml_manager._matches(r, conditions)]
        deleted = before - len(kept)

        if deleted > 0:
            new_rows = Rows(data=kept, rows_count=len(kept))
            self.dml_manager.save_all_rows(table, new_rows, schema)

        return deleted
    
    def get_stats(self, table_name: str) -> Statistic:
        pass
    
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