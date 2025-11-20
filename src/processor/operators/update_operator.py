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

        # ambil skema tabel
        schema = self.storage_manager.get_table_schema(table_name)
        if not schema:
            raise ValueError(f"Table '{table_name}' does not exist")

        # validasi nama kolom
        valid_cols = {c.name for c in schema.columns}
        for col in assignments:
            if col not in valid_cols:
                raise ValueError(f"Column '{col}' does not exist")

        # parse assignment
        parsed_values = {}
        for col, expr in assignments.items():
            parsed_values[col] = self._parse_value(expr, col, schema)

        # kondisi dalam bentuk list string (atau None)
        cond_list = None
        if condition:
            cond_list = [condition]

        # build DataWrite
        dw = DataWrite(
            table_name=table_name,
            data=parsed_values,
            is_update=True,
            conditions=cond_list
        )

        # lakukan update
        return self.storage_manager.write_block(dw)

    def _parse_value(
        self,
        value_expr: str,
        column_name: str,
        schema: TableSchema
    ) -> object:

        value_expr = value_expr.strip()

        # cari tipe kolom
        col_type = None
        for col in schema.columns:
            if col.name == column_name:
                col_type = col.data_type
                break

        if col_type is None:
            raise ValueError(f"Column '{column_name}' not found in schema")

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

        # VARCHAR / CHAR
        if col_type in (DataType.CHAR, DataType.VARCHAR):
            return value_expr

        return value_expr
