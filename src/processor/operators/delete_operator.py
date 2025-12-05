from typing import Dict, Any, List
from src.core import IConcurrencyControlManager, IStorageManager, IFailureRecoveryManager
from src.core.models import (
    DataDeletion,
    DataRetrieval,
    DataWrite, 
    Condition, 
    ComparisonOperator, 
    Rows,
    Action,
    LogRecord,
    LogRecordType,
    ForeignKeyAction
)
from ..exceptions import AbortError

class DeleteOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager, frm: IFailureRecoveryManager):
        self.ccm = ccm
        self.storage_manager = storage_manager
        self.frm = frm

    def execute(self, rows: Rows, tx_id: int) -> Rows:
        schema = None
        if isinstance(rows.schema, list):
            if not rows.schema:
                raise ValueError("Rows object has no schema.")
            schema = rows.schema[0] 
        else:
            schema = rows.schema

        if not hasattr(schema, 'table_name') or not hasattr(schema, 'primary_key'):
             raise ValueError("Invalid schema format in DeleteOperator.")
        
        table_name = schema.table_name
        pk = schema.primary_key
        
        if pk is None:
            raise ValueError(f"Table '{table_name}' does not have a primary key. Safe delete cannot be performed.")

        deleted_count = 0
        
        validate = self.ccm.validate_object(table_name, tx_id, Action.WRITE)
        if not validate.allowed:
            raise AbortError(tx_id, table_name, Action.WRITE, 
                           f"Write access denied by concurrency control manager")

        for row in rows.data:
            pk_value = row.get(pk)
            if pk_value is None:
                pk_value = row.get(f"{table_name}.{pk}")
            
            if pk_value is None:
                 raise ValueError(f"Primary key '{pk}' missing in row data.")
            
            self._apply_delete_fka(pk_value, table_name, pk, tx_id)
            log_record = LogRecord(
                log_type=LogRecordType.CHANGE,
                transaction_id=tx_id,
                item_name=table_name,
                old_value=row,
                new_value=None,
                active_transactions=self.ccm.get_active_transactions()[1]
            )
            self.frm.write_log(log_record)
            pk_condition = Condition(
                column=pk, 
                operator=ComparisonOperator.EQ, 
                value=pk_value
            )
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

    def _apply_delete_fka(self, pk_value : Any, table_name : str, pk_column : str, tx_id : int):
        # Ambil semua tabel untuk dicek FK
        tables = self.storage_manager.list_tables()
        if 'schema' in tables:
            tables.remove('schema')
            
        for t in tables:
            schema = self.storage_manager.get_table_schema(t)
            if schema is None:
                continue
            
            child_pk = schema.primary_key

            for col in schema.columns:
                if col.foreign_key is None: continue
                if col.foreign_key.referenced_table != table_name: continue
                if col.foreign_key.referenced_column != pk_column: continue


                if col.foreign_key.on_delete == ForeignKeyAction.RESTRICT or col.foreign_key.on_delete == ForeignKeyAction.NO_ACTION:
                    retrieval = DataRetrieval(
                        table_name=t,
                        columns=[col.name],
                        conditions=[
                            Condition(column=col.name, operator=ComparisonOperator.EQ, value=pk_value)
                        ]
                    )
                    rows = self.storage_manager.read_block(retrieval)
                    if rows.rows_count > 0:
                        raise ValueError(f"Integrity Error: Cannot delete '{table_name}' (id={pk_value}) because it is still referenced by table '{t}'.")

                elif col.foreign_key.on_delete == ForeignKeyAction.CASCADE:
                    # Ambil seluruh data anak
                    retrieval = DataRetrieval(
                        table_name=t,
                        columns=['*'],
                        conditions=[
                            Condition(column=col.name, operator=ComparisonOperator.EQ, value=pk_value)
                        ]
                    )
                    child_rows = self.storage_manager.read_block(retrieval)
                    
                    if child_rows.rows_count > 0:
                        if not child_rows.schema:
                            child_rows.schema = [schema]
                        sub_op = DeleteOperator(self.ccm, self.storage_manager, self.frm)
                        sub_op.execute(child_rows, tx_id)

                elif col.foreign_key.on_delete == ForeignKeyAction.SET_NULL:
                    retrieval = DataRetrieval(
                        table_name=t,
                        columns=['*'],
                        conditions=[
                            Condition(column=col.name, operator=ComparisonOperator.EQ, value=pk_value)
                        ]
                    )
                    rows = self.storage_manager.read_block(retrieval)
                    
                    if rows.rows_count > 0:
                        if child_pk is None:
                             raise ValueError(f"Table '{t}' must have a Primary Key to perform SET NULL action.")

                        for row in rows.data:
                            updated_row = row.copy()
                            
                            if col.name in updated_row:
                                updated_row[col.name] = None
                            
                            qualified_name = f"{t}.{col.name}"
                            if qualified_name in updated_row:
                                updated_row[qualified_name] = None

                            log_record = LogRecord(
                                log_type=LogRecordType.CHANGE,
                                transaction_id=tx_id,
                                item_name=t,
                                old_value=row,
                                new_value=updated_row,
                                active_transactions=self.ccm.get_active_transactions()[1]
                            )
                            self.frm.write_log(log_record)

                            child_pk_val = row.get(child_pk)
                            if child_pk_val is None:
                                child_pk_val = row.get(f"{t}.{child_pk}")


                            if child_pk_val is None:
                                for k, v in row.items():
                                    if k.endswith(f".{child_pk}"):
                                        child_pk_val = v
                                        break
                            
                            if child_pk_val is None:
                                raise ValueError(f"Could not find PK value for table '{t}' row during SET NULL.")

                            data_write = DataWrite(
                                table_name=t,
                                data=updated_row,
                                is_update=True,
                                conditions=[
                                    Condition(
                                        column=child_pk,
                                        operator=ComparisonOperator.EQ,
                                        value=child_pk_val
                                    )
                                ]
                            )
                            self.storage_manager.write_block(data_write)