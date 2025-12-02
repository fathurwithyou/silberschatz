from typing import Dict, Any
from src.core import IConcurrencyControlManager, IStorageManager, IFailureRecoveryManager
from src.core.models import (
    DataDeletion, 
    Condition, 
    ComparisonOperator, 
    Rows,
    Action,
    LogRecord,
    LogRecordType
)
from ..exceptions import AbortError

class DeleteOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager, frm: IFailureRecoveryManager):
        self.ccm = ccm
        self.storage_manager = storage_manager
        self.frm = frm

    def execute(self, rows: Rows, tx_id: int) -> Rows:
        if len(rows.schema) != 1:
            raise ValueError("DeleteOperator only supports single table deletions.")
        
        schema = rows.schema[0]
        table_name = schema.table_name
        pk = schema.primary_key
        
        if pk is None:
            raise ValueError(f"Table '{table_name}' does not have a primary key. Safe delete cannot be performed.")

        deleted_count = 0
        
        validate = self.ccm.validate_object(table_name, tx_id, Action.WRITE)
        if not validate.allowed:
            raise AbortError(tx_id, table_name, Action.WRITE, 
                           f"Write access denied by concurrency control manager")


        # iterasi data
        for row in rows.data:
            # bersihkan nama kolom
            current_row = self._transform_col_name(row)
            
            if pk not in current_row:
                 raise ValueError(f"Primary key '{pk}' missing in row data.")

            log_record = LogRecord(
                log_type=LogRecordType.CHANGE,
                transaction_id=tx_id,
                item_name=table_name,
                old_value=current_row,
                new_value=None,
                active_transactions=self.ccm.get_active_transactions()[1]
            )
            self.frm.write_log(log_record)
            
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