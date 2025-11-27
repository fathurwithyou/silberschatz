from typing import Dict, Any, List
from src.core import IConcurrencyControlManager, IStorageManager
from src.core.models import (
    DataWrite, TableSchema, DataType, Rows, Condition, ComparisonOperator
)

class InsertOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager):
        self.ccm = ccm
        self.storage_manager = storage_manager

    def execute(self, rows: Rows, values: List[str]) -> Rows:

        if len(rows.schema) != 1:
            raise ValueError("InsertOperator only supports inserting into a single table.")

        schema = rows.schema[0]
        table_name = schema.table_name
        
        if len(values) != len(schema.columns):
            raise ValueError(
                f"INSERT expected {len(schema.columns)} values but got {len(values)}."
            )

        parsed_row = self._build_row(schema, values)
        parsed_row = self._transform_col_name(parsed_row)

        data_write = DataWrite(
            table_name=table_name,
            data=parsed_row,
            is_update=False,
            conditions=None
        )

        inserted = self.storage_manager.write_block(data_write)

        return Rows(schema=[], data=[], rows_count=inserted)

    def _build_row(self, schema: TableSchema, values: List[str]) -> Dict[str, Any]:
        new_row = {}

        for col, raw_val in zip(schema.columns, values):
            parsed = self._parse_value(raw_val.strip(), col.name, col.data_type)
            new_row[col.name] = parsed

        return new_row

    def _transform_col_name(self, row: Dict[str, Any]):
        transformed = {}
        for key, value in row.items():
            if '.' in key:
                transformed[key.split('.')[-1]] = value
            else:
                transformed[key] = value
        return transformed

    def _parse_value(self, token: str, col_name: str, col_type: DataType):

        # NULL literal
        if token.upper() == "NULL":
            return None

        # quoted literal
        if (token.startswith("'") and token.endswith("'")) or \
           (token.startswith('"') and token.endswith('"')):
            literal = token[1:-1]
            return self._convert_literal(literal, col_type)

        # unquoted
        return self._convert_literal(token, col_type)

    def _convert_literal(self, literal: str, col_type: DataType):
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

        if col_type in (DataType.CHAR, DataType.VARCHAR):
            return literal

        return literal
