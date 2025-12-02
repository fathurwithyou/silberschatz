from __future__ import annotations

from typing import TYPE_CHECKING, List, Set, Tuple
from src.core.models import ExecutionResult, ParsedQuery, QueryNodeType
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
            drop_order = self._collect_cascade_drop_order(table_name)
            dropped_tables: List[str] = []

            for name in drop_order:
                self.processor.storage.drop_table(name)
                dropped_tables.append(name)

            message = "DROP TABLE CASCADE completed. Tables dropped: " + \
                ", ".join(dropped_tables)
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
        table_def = query.tree.value 
        
        table_name = table_def["table_name"]
        columns = table_def["columns"]
        primary_keys = table_def.get("primary_keys", [])
        foreign_keys = table_def.get("foreign_keys", [])

        if table_name in self.processor.storage.list_tables():
            raise ValueError(f"Table '{table_name}' already exists.")

        seen = set()
        for col in columns:
            if col["name"] in seen:
                raise ValueError(f"Duplicate column name '{col['name']}' in table '{table_name}'.")
            seen.add(col["name"])

        valid_types = {"INTEGER", "FLOAT", "CHAR", "VARCHAR"}
        for col in columns:
            dtype = col["type"].upper()
            if dtype not in valid_types:
                raise ValueError(f"Unsupported data type '{dtype}' for column '{col['name']}'.")

        for pk in primary_keys:
            if pk not in seen:
                raise ValueError(f"Primary key column '{pk}' does not exist.")

        for fk in foreign_keys:
            col = fk["column"]
            ref_table = fk["ref_table"]
            ref_col = fk["ref_column"]

            if col not in seen:
                raise ValueError(f"Foreign key column '{col}' does not exist.")

            ref_schema = self.processor.storage.get_table_schema(ref_table)
            if ref_schema is None:
                raise ValueError(f"Referenced table '{ref_table}' does not exist.")

            ref_columns = [c.name for c in ref_schema.columns]
            if ref_col not in ref_columns:
                raise ValueError(f"Referenced column '{ref_col}' does not exist in '{ref_table}'.")

        table_schema = self.processor.storage.build_table_schema(
            table_name=table_name,
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys
        )

        self.processor.storage.create_table(table_schema)

        return ExecutionResult(
            transaction_id=tx_id,
            timestamp=datetime.now(),
            message=f"Table '{table_name}' created.",
            data=None,
            query=query.query
        )


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

    def _collect_cascade_drop_order(
        self,
        table_name: str,
        visited: Set[str] | None = None
    ) -> List[str]:
        if visited is None:
            visited = set()

        if table_name in visited:
            return []

        visited.add(table_name)
        order: List[str] = []

        for dependent in self._find_dependent_tables(table_name):
            order.extend(self._collect_cascade_drop_order(dependent, visited))

        order.append(table_name)
        return order
