from typing import Dict, Any
from src.core import IConcurrencyControlManager, IStorageManager, IFailureRecoveryManager
from src.core.models import (DataWrite, 
                             DataRetrieval,
                             TableSchema, 
                             ComparisonOperator, 
                             Condition, DataType, 
                             Rows, LogRecord, LogRecordType,
                             Action, ColumnDefinition,
                             ForeignKeyAction)
from ..exceptions import AbortError
from ..utils import get_column_from_schema, check_referential_integrity

class UpdateOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager, frm: IFailureRecoveryManager):
        self.ccm = ccm
        self.storage_manager = storage_manager
        self.frm = frm

    def execute(self, rows: Rows, set_clause: str, tx_id: int) -> Rows:
        if len(rows.schema) != 1:
            raise ValueError("UpdateOperator only supports single table updates.")
        
        table_name = rows.schema[0].table_name

        # ambil SET clause
        assignments = self._parse_assignment_string(set_clause)

        # ambil schema & PK
        schema = rows.schema[0]
        pk = schema.primary_key
        
        if pk is None:
            raise ValueError(f"Table '{table_name}' does not have a primary key.")

        # update per-row
        updated_count = 0
        
        # Validate with CCM before proceeding
        validate = self.ccm.validate_object(table_name, tx_id, Action.WRITE)
        if not validate.allowed:
            raise AbortError(tx_id, table_name, Action.WRITE, 
                           f"Write access denied by concurrency control manager")

        for row in rows.data:
            pk_with_table = f"{table_name}.{pk}"
            original_pk_value = row.get(pk_with_table) or row.get(pk)
            
            updated_row = self._apply_assignments(row, assignments, schema, tx_id)
            updated_row = self._transform_col_name(updated_row)
            
            # Log ke Failure Recovery Manager
            log_record = LogRecord(
                log_type=LogRecordType.CHANGE,
                transaction_id=tx_id,
                item_name=table_name,
                old_value=self._transform_col_name(row),
                new_value=updated_row,
                active_transactions=self.ccm.get_active_transactions()[1]
            )
            self.frm.write_log(log_record)
            
            # write ke storage
            data_write = DataWrite(
                table_name=table_name,
                data=updated_row,
                is_update=True,
                conditions=[Condition(pk, ComparisonOperator.EQ, original_pk_value)]
            )

            updated_count += self.storage_manager.write_buffer(data_write)

        return Rows(schema=[], 
                    data=[], 
                    rows_count=updated_count)

    def _parse_assignment_string(self, assignment_str: str) -> Dict[str, str]:
        assignments = {}

        parts = []
        cur = []
        in_quote = False
        q = None

        for c in assignment_str:
            if c in ('"', "'") and not in_quote:
                in_quote = True
                q = c
                cur.append(c)
            elif c == q and in_quote:
                in_quote = False
                q = None
                cur.append(c)
            elif c == ',' and not in_quote:
                parts.append(''.join(cur).strip())
                cur = []
            else:
                cur.append(c)

        if cur:
            parts.append(''.join(cur).strip())

        for p in parts:
            if '=' in p:
                col, val = p.split('=', 1)
                assignments[col.strip()] = val.strip()

        return assignments

    # apply assignment ke row lama
    def _apply_assignments(self, row: Dict[str, Any], assignments: Dict[str, str], schema: TableSchema, tx_id: int) -> Dict[str, Any]:
        updated = row.copy()
        table_name = schema.table_name
        pk_column = schema.primary_key
        
        for col, expr in assignments.items():
            qualified_col = col
            if ('.' not in col):
                qualified_col = f"{table_name}.{col}"
            updated[qualified_col] = self._parse_value(expr, col, schema)
            column = get_column_from_schema(schema, col)
            if (updated[qualified_col] is None) and (not column.nullable):
                raise ValueError(f"Column '{col}' cannot be set to NULL due to NOT NULL constraint.")
            
            if (updated[qualified_col] is None) and column.primary_key:
                raise ValueError(f"Column '{col}' cannot be set to NULL due to PRIMARY KEY constraint.")
            
            if column.foreign_key is not None:
                if not check_referential_integrity(updated[qualified_col], column, self.storage_manager):
                    raise ValueError(f"Referential integrity violation: value '{updated[qualified_col]}' for column '{col}' does not exist in referenced table '{column.foreign_key.referenced_table}'")
            
            if column.primary_key and pk_column is not None:
                new_pk_value = updated[qualified_col]
                old_pk_value = row.get(qualified_col)
                
                if new_pk_value != old_pk_value:
                    if self._check_pk_conflict(table_name, pk_column, new_pk_value):
                        raise ValueError(f"UPDATE causes PK conflict '{pk_column}'={new_pk_value}")
                    
                    self._apply_update_foreign_key_actions(
                        old_pk_value, new_pk_value, table_name, column, tx_id
                    )
        return updated
    
    def _apply_update_foreign_key_actions(self, old_value: Any, 
                                                new_value: Any, 
                                                table_name: str, 
                                                column: ColumnDefinition,
                                                tx_id: int):
        """
        Apply foreign key actions for UPDATE.
        old_value: nilai lama dari PK yang diupdate
        new_value: nilai baru dari PK yang diupdate
        table_name: nama tabel tempat PK diupdate
        column: definisi kolom PK yang diupdate
        tx_id: ID transaksi yang sedang berjalan
        """
        
        if column.primary_key and new_value is None:
            raise ValueError(f"Cannot set PRIMARY KEY column '{column.name}' to NULL")
        if not column.nullable and new_value is None:
            raise ValueError(f"Cannot set NOT NULL column '{column.name}' to NULL")
        if column.primary_key:
            if old_value != new_value:
                if self._check_pk_conflict(table_name, column.name, new_value):
                    raise ValueError(f"UPDATE causes PK conflict '{column.name}'={new_value}")
        
        tables = self.storage_manager.list_tables()
        
        for table in tables:
            schema = self.storage_manager.get_table_schema(table)
            if schema is None:
                continue
            for col in schema.columns:
                if col.foreign_key is None:
                    continue
                if col.foreign_key.referenced_table != table_name:
                    continue
                if col.foreign_key.referenced_column != column.name:
                    continue
                
                if col.foreign_key.on_update == ForeignKeyAction.CASCADE:
                    data_retrieval = DataRetrieval(
                        table_name=table,
                        columns=[col.name],
                        conditions=[Condition(column=col.name, operator=ComparisonOperator.EQ, value=old_value)]
                    )
                    result = self.storage_manager.read_buffer(data_retrieval)
                    
                    for row in result.data:
                        updated_row = row.copy()
                        updated_row[col.name] = new_value
                        
                        self._apply_update_foreign_key_actions(
                            row[col.name], new_value, table, col, tx_id
                        )
                        
                        log_record = LogRecord(
                            log_type=LogRecordType.CHANGE,
                            transaction_id=tx_id,
                            item_name=table,
                            old_value=row,
                            new_value=updated_row,
                            active_transactions=self.ccm.get_active_transactions()[1]
                        )
                        self.frm.write_log(log_record)
                        
                        data_write = DataWrite(
                            table_name=table,
                            data=updated_row,
                            is_update=True,
                            conditions=[Condition(column=col.name, operator=ComparisonOperator.EQ, value=old_value)]
                        )
                        self.storage_manager.write_buffer(data_write)
                
                elif col.foreign_key.on_update == ForeignKeyAction.SET_NULL:
                    data_retrieval = DataRetrieval(
                        table_name=table,
                        columns=[col.name],
                        conditions=[Condition(column=col.name, operator=ComparisonOperator.EQ, value=old_value)]
                    )
                    result = self.storage_manager.read_buffer(data_retrieval)
                    
                    
                    for row in result.data:
                        updated_row = row.copy()
                        updated_row[col.name] = None
                        
                        self._apply_update_foreign_key_actions(
                            row[col.name], None, table, col, tx_id
                        )
                        
                        log_record = LogRecord(
                                log_type=LogRecordType.CHANGE,
                                transaction_id=tx_id,
                                item_name=table,
                                old_value=row,
                                new_value=updated_row,
                                active_transactions=self.ccm.get_active_transactions()[1]
                            )
                        self.frm.write_log(log_record)
                        
                        data_write = DataWrite(
                            table_name=table,
                            data=updated_row,
                            is_update=True,
                            conditions=[Condition(column=col.name, operator=ComparisonOperator.EQ, value=old_value)]
                        )
                        self.storage_manager.write_buffer(data_write)
                elif col.foreign_key.on_update == ForeignKeyAction.RESTRICT or col.foreign_key.on_update == ForeignKeyAction.NO_ACTION:
                    data_retrieval = DataRetrieval(
                        table_name=table,
                        columns=[col.name],
                        conditions=[Condition(column=col.name, operator=ComparisonOperator.EQ, value=old_value)]
                    )
                    result = self.storage_manager.read_buffer(data_retrieval)
                    
                    if result.rows_count > 0:
                        raise ValueError(f"Referential integrity violation: cannot update value '{old_value}' in column '{column.name}' of table '{table_name}' because it is referenced in table '{table}'")
                
    
    def _check_pk_conflict(self, table_name: str, pk_column: str, new_pk_value: Any) -> bool:
        data_retrieval = DataRetrieval(
            table_name=table_name,
            columns=[pk_column],
            conditions=[Condition(pk_column, ComparisonOperator.EQ, new_pk_value)],
            limit=1
        )
        
        result = self.storage_manager.read_buffer(data_retrieval)
        
        return result.rows_count > 0
    
    def _transform_col_name(self, row: Dict[str, Any]):
        transformed = {}
        for key, value in row.items():
            if '.' in key:
                transformed[key.split('.')[-1]] = value
            else:
                transformed[key] = value
        return transformed

    #  parser tipe data value
    def _parse_value(self, value_expr: str, column_name: str, schema: TableSchema):

        value_expr = value_expr.strip()

        # cari tipe kolom
        col_type = None
        for col in schema.columns:
            if col.name == column_name:
                col_type = col.data_type
                break

        if col_type is None:
            raise ValueError(f"Column '{column_name}' not found")

        # null
        if value_expr.upper() == "NULL":
            return None

        # quoted literal
        if (value_expr.startswith("'") and value_expr.endswith("'")) or \
           (value_expr.startswith('"') and value_expr.endswith('"')):

            literal = value_expr[1:-1]

            if col_type == DataType.INTEGER:
                try:
                    return int(literal)
                except:
                    raise ValueError(f"Cannot convert '{literal}' to INTEGER")

            if col_type == DataType.FLOAT:
                try:
                    return float(literal)
                except:
                    raise ValueError(f"Cannot convert '{literal}' to FLOAT")

            return literal

        # unquoted numeric
        if col_type == DataType.INTEGER:
            try:
                return int(value_expr)
            except:
                raise ValueError(f"Cannot convert '{value_expr}' to INTEGER")

        if col_type == DataType.FLOAT:
            try:
                return float(value_expr)
            except:
                raise ValueError(f"Cannot convert '{value_expr}' to FLOAT")

        # VARCHAR / CHAR
        if col_type in (DataType.CHAR, DataType.VARCHAR):
            return value_expr

        return value_expr
