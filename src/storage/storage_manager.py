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
from src.storage.statistics import StatisticsManager
from src.storage.index import BPlusTreeIndex, BaseIndex
from src.storage.buffer_pool import BufferPool


class StorageManager(IStorageManager):
    
    def __init__(self, data_directory: str = "data", use_buffer: bool = True, buffer_size: int = 200):
        self.data_directory = data_directory
        self.use_buffer = use_buffer
        
        if use_buffer:
            self.buffer_pool = BufferPool(pool_size=buffer_size)
        else:
            self.buffer_pool = None
        
        self.ddl_manager = DDLManager(f"src/{self.data_directory}")
        self.dml_manager = DMLManager(f"src/{self.data_directory}", self.buffer_pool)
        self.statistics_manager = StatisticsManager(f"src/{self.data_directory}")
        self.indexes: Dict[tuple, BaseIndex] = {}
    
    def _write_page_to_disk(self, page_id: str, data: bytes) -> None:
        if page_id.startswith("table:"):
            tbl_name = page_id.split(":", 1)[1]
            table_path = self.dml_manager.get_table_path(tbl_name)
            with open(table_path, 'wb') as f:
                f.write(data)
    
    def read_block(self, data_retrieval: DataRetrieval) -> Rows:
        schema = self.ddl_manager.load_schema(data_retrieval.table_name)
        
        if schema is None:
            raise ValueError(f"Table '{data_retrieval.table_name}' does not exist")
        
        rows = None
        
        if data_retrieval.conditions:
            rows = self.dml_manager.try_read_with_index(
                data_retrieval.table_name,
                schema,
                data_retrieval.conditions,
                self.indexes,
                use_buffer=False
            )
        
        if rows is None:
            rows = self.dml_manager._load_from_disk(data_retrieval.table_name, schema)
            
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

        all_rows: Rows = self.dml_manager._load_from_disk(table, schema)

        if not data_write.is_update:
            new_row = dict(data_write.data)
            new_row = self.dml_manager._cast_by_schema(new_row, schema)

            if pk_name and pk_name in new_row:
                new_pk = new_row[pk_name]

                if self.has_index(table, pk_name):
                    index = self.indexes[(table, pk_name)]
                    existing_ids = index.search(new_pk)
                    if existing_ids:
                        raise ValueError(f"Duplicate primary key '{pk_name}'={new_pk}")
                else:
                    for r in all_rows.data:
                        if r.get(pk_name) == new_pk:
                            raise ValueError(f"Duplicate primary key '{pk_name}'={new_pk}")

            new_row_id = len(all_rows.data)
            all_rows.data.append(new_row)
            all_rows.rows_count = len(all_rows.data)
            
            for column_def in schema.columns:
                column = column_def.name
                if self.has_index(table, column):
                    index = self.indexes[(table, column)]
                    value = new_row.get(column)
                    if value is not None:
                        index.insert(value, new_row_id)
            
            self.dml_manager._save_to_disk(table, all_rows, schema)
            return 1

        conditions: List[Condition] = data_write.conditions
        updated_count = 0
        set_expr: Dict[str, Any] = dict(data_write.data)

        for i, row in enumerate(all_rows.data):
            if self.dml_manager._matches(row, conditions):
                old_row = row.copy()
                new_row = row.copy()

                for k, v in set_expr.items():
                    new_row[k] = v

                new_row = self.dml_manager._cast_by_schema(new_row, schema)

                if pk_name and (pk_name in set_expr):
                    new_pk = new_row[pk_name]
                    
                    if self.has_index(table, pk_name):
                        index = self.indexes[(table, pk_name)]
                        existing_ids = index.search(new_pk)
                        if existing_ids and i not in existing_ids:
                            raise ValueError(f"UPDATE causes PK conflict '{pk_name}'={new_pk}")
                    else:
                        for j, other in enumerate(all_rows.data):
                            if j != i and other.get(pk_name) == new_pk:
                                raise ValueError(f"UPDATE causes PK conflict '{pk_name}'={new_pk}")

                for column_def in schema.columns:
                    column = column_def.name
                    if self.has_index(table, column):
                        index = self.indexes[(table, column)]
                        old_value = old_row.get(column)
                        new_value = new_row.get(column)
                        
                        if old_value != new_value:
                            if old_value is not None:
                                index.delete(old_value, i)
                            if new_value is not None:
                                index.insert(new_value, i)
                
                all_rows.data[i] = new_row
                updated_count += 1

        if updated_count > 0:
            all_rows.rows_count = len(all_rows.data)
            self.dml_manager._save_to_disk(table, all_rows, schema)

        return updated_count
    
    def delete_block(self, data_deletion: DataDeletion) -> int:
        table = data_deletion.table_name
        schema = self.ddl_manager.load_schema(table)

        if schema is None:
            raise ValueError(f"Table '{table}' does not exist")

        all_rows: Rows = self.dml_manager._load_from_disk(table, schema)
        conditions: List[Condition] = data_deletion.conditions

        deleted_count = 0
        kept = []
        kept_with_old_id = []

        for i, row in enumerate(all_rows.data):
            if self.dml_manager._matches(row, conditions):
                for column_def in schema.columns:
                    column = column_def.name
                    if self.has_index(table, column):
                        index = self.indexes[(table, column)]
                        value = row.get(column)
                        if value is not None:
                            index.delete(value, i)

                deleted_count += 1
            else:
                kept.append(row)
                kept_with_old_id.append((i, row))

        if deleted_count > 0:
            for new_row_id, (old_row_id, row) in enumerate(kept_with_old_id):
                if old_row_id != new_row_id:
                    for column_def in schema.columns:
                        column = column_def.name
                        if self.has_index(table, column):
                            index = self.indexes[(table, column)]
                            value = row.get(column)
                            if value is not None:
                                index.delete(value, old_row_id)
                                index.insert(value, new_row_id)

            new_rows = Rows(data=kept, rows_count=len(kept))
            self.dml_manager._save_to_disk(table, new_rows, schema)

        return deleted_count
    
    def read_buffer(self, data_retrieval: DataRetrieval) -> Rows:
        schema = self.ddl_manager.load_schema(data_retrieval.table_name)
        
        if schema is None:
            raise ValueError(f"Table '{data_retrieval.table_name}' does not exist")
        
        rows = None
        
        if data_retrieval.conditions:
            rows = self.dml_manager.try_read_with_index(
                data_retrieval.table_name,
                schema,
                data_retrieval.conditions,
                self.indexes,
                use_buffer=True
            )
        
        if rows is None:
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
    
    def write_buffer(self, data_write: DataWrite) -> int:
        table = data_write.table_name
        schema = self.ddl_manager.load_schema(table)
        pk_name = schema.primary_key

        if schema is None:
            raise ValueError(f"Table '{table}' does not exist")

        all_rows: Rows = self.dml_manager.load_all_rows(table, schema)

        if not data_write.is_update:
            new_row = dict(data_write.data)
            new_row = self.dml_manager._cast_by_schema(new_row, schema)

            if pk_name and pk_name in new_row:
                new_pk = new_row[pk_name]

                if self.has_index(table, pk_name):
                    index = self.indexes[(table, pk_name)]
                    existing_ids = index.search(new_pk)
                    if existing_ids:
                        raise ValueError(f"Duplicate primary key '{pk_name}'={new_pk}")
                else:
                    for r in all_rows.data:
                        if r.get(pk_name) == new_pk:
                            raise ValueError(f"Duplicate primary key '{pk_name}'={new_pk}")

            new_row_id = len(all_rows.data)
            all_rows.data.append(new_row)
            all_rows.rows_count = len(all_rows.data)
            
            for column_def in schema.columns:
                column = column_def.name
                if self.has_index(table, column):
                    index = self.indexes[(table, column)]
                    value = new_row.get(column)
                    if value is not None:
                        index.insert(value, new_row_id)
            
            self.dml_manager.save_all_rows(table, all_rows, schema)
            return 1

        conditions: List[Condition] = data_write.conditions
        updated_count = 0
        set_expr: Dict[str, Any] = dict(data_write.data)

        for i, row in enumerate(all_rows.data):
            if self.dml_manager._matches(row, conditions):
                old_row = row.copy()
                new_row = row.copy()

                for k, v in set_expr.items():
                    new_row[k] = v

                new_row = self.dml_manager._cast_by_schema(new_row, schema)

                if pk_name and (pk_name in set_expr):
                    new_pk = new_row[pk_name]
                    
                    if self.has_index(table, pk_name):
                        index = self.indexes[(table, pk_name)]
                        existing_ids = index.search(new_pk)
                        if existing_ids and i not in existing_ids:
                            raise ValueError(f"UPDATE causes PK conflict '{pk_name}'={new_pk}")
                    else:
                        for j, other in enumerate(all_rows.data):
                            if j != i and other.get(pk_name) == new_pk:
                                raise ValueError(f"UPDATE causes PK conflict '{pk_name}'={new_pk}")

                for column_def in schema.columns:
                    column = column_def.name
                    if self.has_index(table, column):
                        index = self.indexes[(table, column)]
                        old_value = old_row.get(column)
                        new_value = new_row.get(column)
                        
                        if old_value != new_value:
                            if old_value is not None:
                                index.delete(old_value, i)
                            if new_value is not None:
                                index.insert(new_value, i)
                
                all_rows.data[i] = new_row
                updated_count += 1

        if updated_count > 0:
            all_rows.rows_count = len(all_rows.data)
            self.dml_manager.save_all_rows(table, all_rows, schema)

        return updated_count
    
    def delete_buffer(self, data_deletion: DataDeletion) -> int:
        table = data_deletion.table_name
        schema = self.ddl_manager.load_schema(table)

        if schema is None:
            raise ValueError(f"Table '{table}' does not exist")

        all_rows: Rows = self.dml_manager.load_all_rows(table, schema)
        conditions: List[Condition] = data_deletion.conditions

        deleted_count = 0
        kept = []
        kept_with_old_id = []

        for i, row in enumerate(all_rows.data):
            if self.dml_manager._matches(row, conditions):
                for column_def in schema.columns:
                    column = column_def.name
                    if self.has_index(table, column):
                        index = self.indexes[(table, column)]
                        value = row.get(column)
                        if value is not None:
                            index.delete(value, i)

                deleted_count += 1
            else:
                kept.append(row)
                kept_with_old_id.append((i, row))

        if deleted_count > 0:
            for new_row_id, (old_row_id, row) in enumerate(kept_with_old_id):
                if old_row_id != new_row_id:
                    for column_def in schema.columns:
                        column = column_def.name
                        if self.has_index(table, column):
                            index = self.indexes[(table, column)]
                            value = row.get(column)
                            if value is not None:
                                index.delete(value, old_row_id)
                                index.insert(value, new_row_id)

            new_rows = Rows(data=kept, rows_count=len(kept))
            self.dml_manager.save_all_rows(table, new_rows, schema)

        return deleted_count
    
    def flush_buffer(self, table_name: Optional[str] = None) -> None:
        if self.buffer_pool is None:
            return
        
        if table_name is not None:
            page_id = f"table:{table_name}"
            if page_id in self.buffer_pool.frames:
                self.buffer_pool.flush_page(page_id, lambda data: self._write_page_to_disk(page_id, data))
                del self.buffer_pool.frames[page_id]
        else:
            self.buffer_pool.flush_all(lambda page_id: lambda data: self._write_page_to_disk(page_id, data))
            self.buffer_pool.clear()
    
    def get_buffer_stats(self) -> Dict[str, Any]:
        if self.buffer_pool is None:
            return {"buffer_enabled": False}
        
        stats = self.buffer_pool.get_statistics()
        stats["buffer_enabled"] = True
        return stats
    
    def get_stats(self, table_name: str) -> Statistic:
        schema = self.ddl_manager.load_schema(table_name)
        if schema is None:
            raise ValueError(f"Table '{table_name}' does not exist")

        rows = self.dml_manager.load_all_rows(table_name, schema)
        
        n_r = self.statistics_manager.get_tuple_count(rows)
        l_r = self.statistics_manager.get_tuple_size(schema)
        f_r = self.statistics_manager.get_blocking_factor(l_r)
        b_r = self.statistics_manager.get_number_of_blocks(n_r, f_r)
        V = self.statistics_manager.calculate_distinct_values(rows, schema)
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
        schema = self.ddl_manager.load_schema(table)
        if schema is None:
            raise ValueError(f"Table '{table}' does not exist")

        column_exists = any(col.name == column for col in schema.columns)
        if not column_exists:
            raise ValueError(f"Column '{column}' does not exist in table '{table}'")

        if (table, column) in self.indexes:
            raise ValueError(f"Index already exists on {table}.{column}")

        index_type_lower = index_type.lower()
        if index_type_lower in ['b_plus_tree', 'btree', 'b+tree']:
            index = BPlusTreeIndex(table, column, f"src/{self.data_directory}")
        else:
            raise ValueError(f"Invalid index type '{index_type}'. Use 'b_plus_tree', 'btree', 'b+tree', or 'hash'")

        all_rows = self.dml_manager.load_all_rows(table, schema)
        for row_id, row in enumerate(all_rows.data):
            value = row.get(column)
            if value is not None:
                index.insert(value, row_id)
        
        index.save()
        self.indexes[(table, column)] = index
    
    def drop_index(self, table: str, column: str) -> None:
        if (table, column) not in self.indexes:
            raise ValueError(f"No index exists on {table}.{column}")

        index = self.indexes[(table, column)]
        index.destroy()

        del self.indexes[(table, column)]
    
    def has_index(self, table: str, column: str) -> bool:
        return (table, column) in self.indexes
    
    def create_table(self, schema: TableSchema) -> None:
        if self.ddl_manager.schema_exists(schema.table_name):
            raise ValueError(f"Table '{schema.table_name}' already exists")

        self.ddl_manager.validate_schema(schema)

        self.ddl_manager.save_schema(schema)
        self.ddl_manager.create_table_file(schema.table_name)

    def drop_table(self, table_name: str) -> None:
        if not self.ddl_manager.schema_exists(table_name):
            raise ValueError(f"Table '{table_name}' does not exist")

        indexes_to_drop = [(t, c) for (t, c) in self.indexes.keys() if t == table_name]
        for table, column in indexes_to_drop:
            self.drop_index(table, column)

        self.ddl_manager.delete_schema(table_name)
        self.ddl_manager.delete_table_file(table_name)

    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        return self.ddl_manager.load_schema(table_name)

    def list_tables(self) -> List[str]:
        return self.ddl_manager.list_schema_files()
    
    def update_table_schema(self, schema: TableSchema) -> None:
        if not self.ddl_manager.schema_exists(schema.table_name):
            raise ValueError(f"Table '{schema.table_name}' does not exist")

        self.ddl_manager.validate_schema(schema)
        self.ddl_manager.save_schema(schema)