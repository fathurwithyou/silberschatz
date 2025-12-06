from __future__ import annotations

from typing import TYPE_CHECKING, List, Tuple
from src.core.models import (
    ExecutionResult, ParsedQuery, QueryNodeType,
    TableSchema, ColumnDefinition, DataType, ForeignKeyConstraint, ForeignKeyAction
)
from datetime import datetime

if TYPE_CHECKING:
    from ..processor import QueryProcessor

class DDLHandler:
    """
    Menangani query DDL (Data Definition Language) 
    """
    def __init__(self, processor: QueryProcessor):
        self.processor = processor

    def handle(self, query: ParsedQuery) -> ExecutionResult:
        if query.tree.type == QueryNodeType.DROP_TABLE:
            return self._handle_drop_table(query)
        elif query.tree.type == QueryNodeType.CREATE_TABLE:
            return self._handle_create_table(query)
        raise SyntaxError("Unsupported DDL operation.")

    def _handle_drop_table(self, query: ParsedQuery) -> ExecutionResult:
        table_name, modifier = self._parse_drop_table_value(query.tree.value)
        drop_mode = (modifier or "RESTRICT").upper()
        tx_id = self.processor.transaction_id or 0

        if drop_mode == "CASCADE":
            updated_tables = self._remove_foreign_key_references(table_name)
            self.processor.storage.drop_table(table_name)
            dropped_tables = [table_name]

            message = f"DROP TABLE CASCADE completed. Table dropped: {table_name}"
            if updated_tables:
                message += ". Foreign key references removed from: " + \
                    ", ".join(updated_tables)
        else:
            dependents = self._find_dependent_tables(table_name)
            if dependents:
                dependent_list = ", ".join(dependents)
                raise ValueError(
                    f"Cannot drop table '{table_name}' because it is referenced by: {dependent_list}. "
                    "Use DROP TABLE ... CASCADE to drop dependent tables."
                )

            self.processor.storage.drop_table(table_name)
            dropped_tables = [table_name]
            message = f"Table '{table_name}' dropped."

        return ExecutionResult(
            transaction_id=tx_id,
            timestamp=datetime.now(),
            message=message,
            data=None,
            query=query.query
        )
    
    def _handle_create_table(self, query: ParsedQuery) -> ExecutionResult:
        tx_id = self.processor.transaction_id or 0
        value_str = query.tree.value
        
        table_name = value_str.split(" ", 1)[0]
        schema_str = value_str[len(table_name):].strip()
        
        table_schema = self._parse_create_table_schema(table_name, schema_str)
        
        if table_name in self.processor.storage.list_tables():
            raise ValueError(f"Table '{table_name}' already exists.")
        
        self._validate_foreign_keys(table_schema)
        
        self.processor.storage.create_table(table_schema)
        
        return ExecutionResult(
            transaction_id=tx_id,
            timestamp=datetime.now(),
            message=f"Table '{table_name}' created.",
            data=None,
            query=query.query
        )

    def _parse_create_table_schema(self, table_name: str, schema_str: str) -> TableSchema:
        """
        Expected format: (col1 TYPE [constraints], col2 TYPE [constraints], ...)
        """
        schema_str = schema_str.strip()
        if not schema_str.startswith('(') or not schema_str.endswith(')'):
            raise ValueError("CREATE TABLE schema must be enclosed in parentheses")
        
        schema_content = schema_str[1:-1].strip()
        
        column_definitions = self._split_column_definitions(schema_content)
        
        columns = []
        primary_key_columns = []
        
        for col_def in column_definitions:
            col_def = col_def.strip()
            if not col_def:
                continue
                
            # Parse individual column definition
            column, is_pk = self._parse_column_definition(col_def)
            columns.append(column)
            
            if is_pk:
                primary_key_columns.append(column.name)
        
        # Validate no duplicate column names
        seen_columns = set()
        for col in columns:
            if col.name in seen_columns:
                raise ValueError(f"Duplicate column name '{col.name}' in table '{table_name}'")
            seen_columns.add(col.name)
        
        # Set primary key (use first PRIMARY KEY column found, or None)
        primary_key = primary_key_columns[0] if primary_key_columns else None
        
        return TableSchema(
            table_name=table_name,
            columns=columns,
            primary_key=primary_key
        )
    
    def _split_column_definitions(self, content: str) -> List[str]:
        return [col_def.strip() for col_def in content.split(',') if col_def.strip()]
    
    def _parse_column_definition(self, col_def: str) -> Tuple[ColumnDefinition, bool]:
        parts = col_def.strip().split()
        if len(parts) < 2:
            raise ValueError(f"Invalid column definition: {col_def}")
        
        col_name = parts[0]
        col_type_str = parts[1].upper()
        
        data_type, max_length = self._parse_data_type(col_type_str)
        
        remaining_parts = parts[2:]
        nullable = True
        is_primary_key = False
        foreign_key = None
        
        i = 0
        while i < len(remaining_parts):
            part = remaining_parts[i].upper()
            
            if part == "PRIMARY" and i + 1 < len(remaining_parts) and remaining_parts[i + 1].upper() == "KEY":
                is_primary_key = True
                nullable = False
                i += 2
            elif part == "NOT" and i + 1 < len(remaining_parts) and remaining_parts[i + 1].upper() == "NULL":
                nullable = False
                i += 2
            elif part == "NULL":
                nullable = True
                i += 1
            elif part == "REFERENCES":
                # Parse foreign key: REFERENCES table_name(column_name) [ON DELETE action] [ON UPDATE action]
                if i + 1 >= len(remaining_parts):
                    raise ValueError(f"REFERENCES requires table(column) specification in: {col_def}")
                
                ref_spec = remaining_parts[i + 1]
                i += 2
                
                # Parse optional ON DELETE/UPDATE actions
                on_delete = ForeignKeyAction.RESTRICT
                on_update = ForeignKeyAction.RESTRICT
                
                while i < len(remaining_parts):
                    if (remaining_parts[i].upper() == "ON" and 
                        i + 2 < len(remaining_parts)):
                        
                        action_type = remaining_parts[i + 1].upper()
                        action_value = remaining_parts[i + 2].upper()
                        
                        if action_type == "DELETE":
                            on_delete = self._parse_foreign_key_action(action_value)
                            i += 3
                        elif action_type == "UPDATE":
                            on_update = self._parse_foreign_key_action(action_value)
                            i += 3
                        else:
                            i += 1
                    elif (remaining_parts[i].upper() == "ON" and 
                          i + 3 < len(remaining_parts) and
                          remaining_parts[i + 1].upper() == "DELETE" and
                          remaining_parts[i + 2].upper() == "SET" and
                          remaining_parts[i + 3].upper() == "NULL"):
                        # Handle "ON DELETE SET NULL"
                        on_delete = ForeignKeyAction.SET_NULL
                        i += 4
                    elif (remaining_parts[i].upper() == "ON" and 
                          i + 3 < len(remaining_parts) and
                          remaining_parts[i + 1].upper() == "UPDATE" and
                          remaining_parts[i + 2].upper() == "SET" and
                          remaining_parts[i + 3].upper() == "NULL"):
                        # Handle "ON UPDATE SET NULL"
                        on_update = ForeignKeyAction.SET_NULL
                        i += 4
                    elif (remaining_parts[i].upper() == "ON" and 
                          i + 3 < len(remaining_parts) and
                          remaining_parts[i + 1].upper() == "DELETE" and
                          remaining_parts[i + 2].upper() == "NO" and
                          remaining_parts[i + 3].upper() == "ACTION"):
                        # Handle "ON DELETE NO ACTION"
                        on_delete = ForeignKeyAction.NO_ACTION
                        i += 4
                    elif (remaining_parts[i].upper() == "ON" and 
                          i + 3 < len(remaining_parts) and
                          remaining_parts[i + 1].upper() == "UPDATE" and
                          remaining_parts[i + 2].upper() == "NO" and
                          remaining_parts[i + 3].upper() == "ACTION"):
                        # Handle "ON UPDATE NO ACTION"
                        on_update = ForeignKeyAction.NO_ACTION
                        i += 4
                    else:
                        i += 1
                
                foreign_key = self._parse_foreign_key_reference(ref_spec, on_delete, on_update)
            else:
                raise ValueError(f"Unknown constraint '{part}' in column definition: {col_def}")
        
        return ColumnDefinition(
            name=col_name,
            data_type=data_type,
            max_length=max_length,
            nullable=nullable,
            primary_key=is_primary_key,
            foreign_key=foreign_key
        ), is_primary_key
    
    def _parse_data_type(self, type_str: str) -> Tuple[DataType, int | None]:
        if '(' in type_str and ')' in type_str:
            type_name = type_str[:type_str.index('(')].upper()
            length_str = type_str[type_str.index('(') + 1:type_str.rindex(')')]
            try:
                max_length = int(length_str)
            except ValueError:
                raise ValueError(f"Invalid length specification in type: {type_str}")
        else:
            type_name = type_str.upper()
            max_length = None
        
        type_mapping = {
            'INTEGER': DataType.INTEGER,
            'INT': DataType.INTEGER,
            'FLOAT': DataType.FLOAT,
            'DOUBLE': DataType.FLOAT,
            'REAL': DataType.FLOAT,
            'CHAR': DataType.CHAR,
            'VARCHAR': DataType.VARCHAR,
        }
        
        if type_name not in type_mapping:
            raise ValueError(f"Unsupported data type: {type_name}")
        
        return type_mapping[type_name], max_length
    
    def _parse_foreign_key_reference(self, ref_spec: str, on_delete: ForeignKeyAction = ForeignKeyAction.RESTRICT, on_update: ForeignKeyAction = ForeignKeyAction.RESTRICT) -> ForeignKeyConstraint:
        if '(' not in ref_spec or ')' not in ref_spec:
            raise ValueError(f"Invalid foreign key reference format: {ref_spec}")
        
        table_name = ref_spec[:ref_spec.index('(')]
        column_name = ref_spec[ref_spec.index('(') + 1:ref_spec.rindex(')')]
        
        if not table_name or not column_name:
            raise ValueError(f"Invalid foreign key reference format: {ref_spec}")
        
        return ForeignKeyConstraint(
            referenced_table=table_name,
            referenced_column=column_name,
            on_delete=on_delete,
            on_update=on_update
        )

    def _parse_foreign_key_action(self, action_str: str) -> ForeignKeyAction:
        action_str = action_str.upper()
        
        if action_str == "CASCADE":
            return ForeignKeyAction.CASCADE
        elif action_str == "RESTRICT":
            return ForeignKeyAction.RESTRICT
        elif action_str == "SET":
            return ForeignKeyAction.SET_NULL
        elif action_str == "NO":
            return ForeignKeyAction.NO_ACTION
        else:
            raise ValueError(f"Unsupported foreign key action: {action_str}")
    
    def _validate_foreign_keys(self, table_schema: TableSchema) -> None:
        for column in table_schema.columns:
            if column.foreign_key is None:
                continue
            
            ref_table = column.foreign_key.referenced_table
            ref_column = column.foreign_key.referenced_column
            
            # Check if referenced table exists
            ref_schema = self.processor.storage.get_table_schema(ref_table)
            if ref_schema is None:
                raise ValueError(f"Referenced table '{ref_table}' does not exist")
            
            # Check if referenced column exists
            ref_columns = [c.name for c in ref_schema.columns]
            if ref_column not in ref_columns:
                raise ValueError(f"Referenced column '{ref_column}' does not exist in table '{ref_table}'")


    def _parse_drop_table_value(self, value: str) -> Tuple[str, str | None]:
        text = (value or "").strip()
        if not text:
            raise ValueError("DROP TABLE requires a table name")

        segments = text.rsplit(" ", 1)
        if len(segments) == 2 and segments[1].upper() in {"CASCADE", "RESTRICT"}:
            table = segments[0].strip()
            if not table:
                raise ValueError("DROP TABLE requires a valid table name before modifier")
            return table, segments[1].upper()

        return text, None

    def _find_dependent_tables(self, table_name: str) -> List[str]:
        dependents: List[str] = []
        for other_table in self.processor.storage.list_tables():
            if other_table == table_name:
                continue
            schema = self.processor.storage.get_table_schema(other_table)
            if schema is None:
                continue
            for column in schema.columns:
                fk = getattr(column, "foreign_key", None)
                if fk and fk.referenced_table == table_name:
                    dependents.append(other_table)
                    break

        return sorted(dependents)

    def _remove_foreign_key_references(self, table_name: str) -> List[str]:
        updated_tables: List[str] = []
        for dependent in self._find_dependent_tables(table_name):
            schema = self.processor.storage.get_table_schema(dependent)
            if schema is None:
                continue

            modified = False
            for column in schema.columns:
                fk = getattr(column, "foreign_key", None)
                if fk and fk.referenced_table == table_name:
                    column.foreign_key = None
                    modified = True

            if modified:
                self.processor.storage.update_table_schema(schema)
                updated_tables.append(dependent)

        return updated_tables
