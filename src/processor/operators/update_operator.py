from __future__ import annotations

from typing import Dict, Any, Optional
from src.core import IConcurrencyControlManager, IStorageManager
from src.core.models import (DataWrite, TableSchema, ComparisonOperator, Condition, DataType)


class UpdateOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager):
        self.ccm = ccm
        self.storage_manager = storage_manager

    def execute(
        self,
        table_name: str,
        assignments: Dict[str, str],
        condition: Optional[str],
    ) -> int:

        schema = self.storage_manager.get_table_schema(table_name)
        if not schema:
            raise ValueError(f"Table '{table_name}' does not exist")

        # validate columns
        valid_cols = {c.name for c in schema.columns}
        for col in assignments:
            if col not in valid_cols:
                raise ValueError(f"Column '{col}' does not exist")

        # parse assignment values
        parsed = {}
        for col, expr in assignments.items():
            parsed[col] = self._parse_value(expr, col, schema)

        # IF condition string = None, conditions = None
        cond_list = None
        if condition:
            cond_list = [condition]  # sesuai harapan test

        # build DataWrite
        dw = DataWrite(
            table_name=table_name,
            data=parsed,
            is_update=True,
            conditions=cond_list
        )

        # ask storage to perform update
        return self.storage_manager.write_block(dw)

    def apply_per_row_update(
        self,
        table_name: str,
        rows: list[Dict[str, Any]],
        assignments: Dict[str, str]
    ) -> int:

        schema = self.storage_manager.get_table_schema(table_name)
        pk = schema.primary_key
        updated_count = 0

        for row in rows:
            new_row = self._apply_assignments(row, assignments, schema)

            data_write = DataWrite(
                table_name=table_name,
                data=new_row,
                is_update=True,
                conditions=[
                    Condition(pk, ComparisonOperator.EQ, row[pk], [schema])
                ]
            )

            updated_count += self.storage_manager.write_block(data_write)

        return updated_count

    def _apply_assignments(self, row, assignments, schema):
        updated = row.copy()
        for col, expr in assignments.items():
            updated[col] = self._parse_value(expr, col, schema)
        return updated

    def _parse_value(
        self,
        value_expr: str,
        column_name: str,
        schema: TableSchema
    ) -> object:

        value_expr = value_expr.strip()

        # determine column type
        col_type = None
        for col in schema.columns:
            if col.name == column_name:
                col_type = col.data_type
                break
        if col_type is None:
            raise ValueError(f"Column '{column_name}' not found in schema")

        # null handling
        if value_expr.upper() == "NULL":
            return None

        # quoted literal
        if (value_expr.startswith("'") and value_expr.endswith("'")) or \
           (value_expr.startswith('"') and value_expr.endswith('"')):
            literal = value_expr[1:-1]

            if col_type == DataType.INTEGER:
                try:
                    return int(literal)
                except ValueError:
                    raise ValueError(f"Cannot convert '{literal}' to INTEGER")

            if col_type == DataType.FLOAT:
                try:
                    return float(literal)
                except ValueError:
                    raise ValueError(f"Cannot convert '{literal}' to FLOAT")
            
            return literal

        # unquoted numeric
        if col_type == DataType.INTEGER:
            try:
                return int(value_expr)
            except ValueError:
                raise ValueError(f"Cannot convert '{value_expr}' to INTEGER")
        if col_type == DataType.FLOAT:
            try:
                return float(value_expr)
            except ValueError:
                raise ValueError(f"Cannot convert '{value_expr}' to FLOAT")

        # varchar / char unquoted
        if col_type in (DataType.CHAR, DataType.VARCHAR):
            return value_expr

        return value_expr
