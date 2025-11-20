from __future__ import annotations
from typing import Dict, Any, Optional, List
from src.core import IConcurrencyControlManager, IStorageManager
from src.core.models import (DataWrite, TableSchema, ComparisonOperator, Condition, DataType, QueryTree, QueryNodeType
)


class UpdateOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager):
        self.ccm = ccm
        self.storage_manager = storage_manager

    def execute(self, update_node: QueryTree, rows: List[Dict[str, Any]]):
        # ambil SET clause
        set_clause = update_node.value
        assignments = self._parse_assignment_string(set_clause)

        # ambil table name
        child = update_node.children[0]
        table_node = child
        while table_node.type != QueryNodeType.TABLE:
            table_node = table_node.children[0]

        table_name = table_node.value

        # ambil schema & PK
        schema = self.storage_manager.get_table_schema(table_name)
        pk = schema.primary_key

        # update per-row
        updated_count = 0

        for row in rows:

            # apply assignment seperti sebelumnya
            updated_row = self._apply_assignments(row, assignments, schema)

            # condition berbasis PK
            data_write = DataWrite(
                table_name=table_name,
                data=updated_row,
                is_update=True,
                conditions=[Condition(pk, ComparisonOperator.EQ, row[pk])]
            )

            updated_count += self.storage_manager.write_block(data_write)

        return {
            "updated_rows": updated_count
        }

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
