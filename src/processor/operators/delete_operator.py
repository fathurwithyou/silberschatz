from typing import Dict, Any
from src.core import IConcurrencyControlManager, IStorageManager
from src.core.models import (
    DataDeletion, 
    Condition, 
    ComparisonOperator, 
    Rows
)

class DeleteOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager):
        self.ccm = ccm
        self.storage_manager = storage_manager

    def execute(self, rows: Rows) -> Rows:
        if len(rows.schema) != 1:
            raise ValueError("DeleteOperator only supports single table deletions.")
        
        schema = rows.schema[0]
        table_name = schema.table_name
        pk = schema.primary_key
        
        if pk is None:
            raise ValueError(f"Table '{table_name}' does not have a primary key. Safe delete cannot be performed.")

        deleted_count = 0

        # iterasi data
        for row in rows.data:
            # bersihkan nama kolom
            current_row = self._transform_col_name(row)
            
            if pk not in current_row:
                 raise ValueError(f"Primary key '{pk}' missing in row data.")

            pk_value = current_row[pk]
            # buat kondisi penghapusan berdasarkan primary key
            pk_condition = Condition(
                column=pk, 
                operator=ComparisonOperator.EQ, 
                value=pk_value
            )
            # buat request penghapusan
            delete_request = DataDeletion(
                table_name=table_name,
                conditions=[pk_condition]
            )

            count = self.storage_manager.delete_block(delete_request)
            deleted_count += count

        return Rows(
            schema=[], 
            data=[], 
            rows_count=deleted_count
        )

    def _transform_col_name(self, row: Dict[str, Any]) -> Dict[str, Any]:
        transformed = {}
        for key, value in row.items():
            if '.' in key:
                # ambil bagian setelah titik terakhir
                clean_key = key.split('.')[-1]
                transformed[clean_key] = value
            else:
                transformed[key] = value
        return transformed