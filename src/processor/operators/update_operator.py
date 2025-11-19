from __future__ import annotations

from typing import Dict, Optional

from src.core.models import TableSchema, DataWrite
from src.core import IConcurrencyControlManager, IStorageManager


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

        # ambil schema
        table_schema = self.storage_manager.get_table_schema(table_name)
        if not table_schema:
            raise ValueError(f"Table '{table_name}' does not exist")

        # validasi kolom
        valid_columns = {col.name for col in table_schema.columns}
        for c in assignments:
            if c not in valid_columns:
                raise ValueError(f"Column '{c}' does not exist")

        # parse dan konversi nilai assignment
        parsed_values = {}
        for col, expr in assignments.items():
            parsed_values[col] = self._parse_value(expr, col, table_schema)

        # bangun struktur DataWrite
        data_write = DataWrite(
            table_name=table_name,
            data=parsed_values,
            is_update=True,
            conditions=[condition] if condition else None
        )

        # pakai write_block untuk update
        updated_count = self.storage_manager.write_block(data_write)

        return updated_count
    
    def _apply_assignments(
        self,
        row: Dict[str, object],
        assignments: Dict[str, str],
        schema: TableSchema
    ) -> Dict[str, object]:
        updated_row = row.copy()
        
        for column_name, value_expr in assignments.items():
            # parse dan convert value
            new_value = self._parse_value(value_expr, column_name, schema)
            updated_row[column_name] = new_value
        
        return updated_row
        
    def _parse_value(
        self,
        value_expr: str,
        column_name: str,
        schema: TableSchema
    ) -> object:

        value_expr = value_expr.strip()

        # determine tipe kolom
        column_type = None
        for col in schema.columns:
            if col.name == column_name:
                column_type = col.data_type
                break

        if column_type is None:
            raise ValueError(f"Column '{column_name}' not found in schema")

        from src.core.models import DataType

        # NULL case
        if value_expr.upper() == "NULL":
            return None

        # deteksi quoted string
        is_quoted = (
            (value_expr.startswith("'") and value_expr.endswith("'")) or
            (value_expr.startswith('"') and value_expr.endswith('"'))
        )

        if is_quoted:
            literal = value_expr[1:-1]

            # Numeric column literal harus validasi sebagai number
            if column_type == DataType.INTEGER:
                try:
                    return int(literal)
                except ValueError:
                    raise ValueError(
                        f"Cannot convert '{literal}' to INTEGER for column '{column_name}'"
                    )

            if column_type == DataType.FLOAT:
                try:
                    return float(literal)
                except ValueError:
                    raise ValueError(
                        f"Cannot convert '{literal}' to FLOAT for column '{column_name}'"
                    )

            # CHAR/VARCHAR return literal as string
            return literal

        # Non-quoted numeric value
        if column_type == DataType.INTEGER:
            try:
                return int(value_expr)
            except ValueError:
                raise ValueError(
                    f"Cannot convert '{value_expr}' to INTEGER for column '{column_name}'"
                )

        if column_type == DataType.FLOAT:
            try:
                return float(value_expr)
            except ValueError:
                raise ValueError(
                    f"Cannot convert '{value_expr}' to FLOAT for column '{column_name}'"
                )

        # CHAR / VARCHAR tanpa quotes diperlakuin sebagai string
        if column_type in (DataType.CHAR, DataType.VARCHAR):
            return value_expr

        # Defaultnya return raw string
        return value_expr