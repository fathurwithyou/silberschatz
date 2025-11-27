from typing import Dict, Any
from src.core import IConcurrencyControlManager, IStorageManager, IFailureRecoveryManager
from src.core.models import (DataWrite, TableSchema, ComparisonOperator, Condition, DataType, QueryTree, QueryNodeType, Rows, LogRecord, LogRecordType)


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

        for row in rows.data:

            # apply assignment seperti sebelumnya
            updated_row = self._apply_assignments(row, assignments, schema)
            updated_row = self._transform_col_name(updated_row)
            
            # Log ke Failure Recovery Manager
            log_record = LogRecord(
                log_type=LogRecordType.CHANGE,
                transaction_id=tx_id,
                item_name=table_name,
                old_value=self._transform_col_name(row),
                new_value=updated_row,
                active_transactions=None
            )
            self.frm.write_log(log_record)
            
            # write ke storage
            data_write = DataWrite(
                table_name=table_name,
                data=updated_row,
                is_update=True,
                conditions=[Condition(pk, ComparisonOperator.EQ, updated_row[pk])]
            )

            updated_count += self.storage_manager.write_block(data_write)

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
    def _apply_assignments(self, row, assignments, schema):
        updated = row.copy()
        for col, expr in assignments.items():
            updated[col] = self._parse_value(expr, col, schema)
        return updated
    
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
