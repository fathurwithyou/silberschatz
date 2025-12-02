from typing import Dict, Any, List
from src.core import IConcurrencyControlManager, IStorageManager, IFailureRecoveryManager
from src.core.models import (
    DataWrite, TableSchema, DataType, Rows, LogRecord, LogRecordType, Action
)
from ..exceptions import AbortError

class InsertOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager, frm: IFailureRecoveryManager):
        self.ccm = ccm
        self.storage_manager = storage_manager
        self.frm = frm

    def execute(self, table_name: str, values: str, tx_id: int) -> Rows:

        if len(table_name.split()) != 1:
            raise ValueError("InsertOperator only supports inserting into a single table.")

        schema = self.storage_manager.get_table_schema(table_name)
        if schema is None:
            raise ValueError(f"Table '{table_name}' does not exist.")

        parsed_values = self._parse_values(values, schema)
        parsed_row = self._build_row(schema, parsed_values)
        parsed_row = self._transform_col_name(parsed_row)

        validate = self.ccm.validate_object(table_name, tx_id, Action.WRITE)
        if not validate.allowed:
            raise AbortError(tx_id, table_name, Action.WRITE, 
                           f"Write access denied by concurrency control manager")

        log_record = LogRecord(
            log_type=LogRecordType.CHANGE,
            transaction_id=tx_id,
            item_name=table_name,
            old_value=None,
            new_value=parsed_row,
            active_transactions=self.ccm.get_active_transactions()[1]
        )
        self.frm.write_log(log_record)
    
        data_write = DataWrite(
            table_name=table_name,
            data=parsed_row,
            is_update=False,
            conditions=None
        )

        inserted = self.storage_manager.write_block(data_write)

        return Rows(schema=[], data=[], rows_count=inserted)
    
    def _parse_values(self, values_str: str, schema: TableSchema) -> List[str]:
        values_str = values_str.strip()
        
        first_close_paren = values_str.find(')')
        if first_close_paren == -1:
            raise ValueError("Invalid format: expected closing parenthesis")
        
        remaining_after_first = values_str[first_close_paren + 1:].strip()
        
        # Pattern: (columns) (values)
        if remaining_after_first.startswith('('):
            columns_part = values_str[:first_close_paren + 1]
            values_part = remaining_after_first
            
            # Parse columns
            columns_str = columns_part[1:-1] 
            specified_columns = [col.strip() for col in columns_str.split(',') if col.strip()]
            
            # Parse values
            values_str_inner = values_part[1:-1]
            specified_values = self._parse_value_list(values_str_inner)
            
            if len(specified_columns) != len(specified_values):
                raise ValueError(f"Number of columns ({len(specified_columns)}) doesn't match number of values ({len(specified_values)})")
            
            # Create column-value mapping
            column_value_map = dict(zip(specified_columns, specified_values))
            
            result = []
            for col in schema.columns:
                if col.name in column_value_map:
                    result.append(column_value_map[col.name])
                else:
                    if not col.nullable:
                        raise ValueError(f"Column '{col.name}' cannot be null")
                    result.append(None)
            
            return result
        
        # Pattern: (values)
        values_part = values_str
        
        if not (values_part.startswith('(') and values_part.endswith(')')):
            raise ValueError("Invalid format: values must be enclosed in parentheses")
        
        values_str_inner = values_part[1:-1] 
        values_list = self._parse_value_list(values_str_inner)
        
        result = []
        for i, col in enumerate(schema.columns):
            if i < len(values_list):
                result.append(values_list[i])
            else:
                if not col.nullable:
                    raise ValueError(f"Column '{col.name}' cannot be null")
                result.append(None)
        
        return result
    
    def _parse_value_list(self, values_str: str) -> List[str]:
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None
        i = 0
        
        while i < len(values_str):
            char = values_str[i]
            
            if not in_quotes:
                if char in ("'", '"'):
                    in_quotes = True
                    quote_char = char
                    current_value += char
                elif char == ',':
                    values.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            else:
                current_value += char
                if char == quote_char:
                    # Check if it's escaped
                    if i + 1 < len(values_str) and values_str[i + 1] == quote_char:
                        current_value += char
                        i += 1 
                    else:
                        in_quotes = False
                        quote_char = None
            
            i += 1
        
        if current_value.strip():
            values.append(current_value.strip())
        
        return values 

    def _build_row(self, schema: TableSchema, values: List[str]) -> Dict[str, Any]:
        new_row = {}

        for i, col in enumerate(schema.columns):
            raw_val = values[i]
            if raw_val is None:
                new_row[col.name] = None
                continue
            parsed = self._parse_value(raw_val.strip(), col.data_type)
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

    def _parse_value(self, token: str, col_type: DataType):

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
