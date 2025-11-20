from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Dict, List, Optional
import re

from src.core.models import Rows, TableSchema, ColumnDefinition
from ..utils import get_schema_from_table_name, get_column_value


@dataclass
class ProjectionItem:
    kind: str  # "wildcard", "table_wildcard", "column"
    value: Optional[str] = None
    alias: Optional[str] = None


class ProjectionOperator:
    def execute(self, rows: Rows, select_clause: Optional[str]) -> Rows:
        items = self._parse_projection_items(select_clause)
        if self._is_trivial_projection(items):
            return rows

        projected_data = [
            self._project_row(row, items, rows.schema or []) for row in rows.data
        ]
        new_schema = self._build_projected_schema(rows.schema or [], items)

        return Rows(
            data=projected_data,
            rows_count=len(projected_data),
            schema=new_schema,
        )

    def _parse_projection_items(
        self, select_clause: Optional[str]
    ) -> List[ProjectionItem]:
        """Convert a SELECT list into ProjectionItem objects."""
        if not select_clause:
            return [ProjectionItem(kind="wildcard")]

        split_items = self._split_select_list(select_clause)
        items: List[ProjectionItem] = []

        for token in split_items:
            stripped = token.strip()
            if not stripped:
                continue

            if stripped == "*":
                items.append(ProjectionItem(kind="wildcard"))
                continue

            if stripped.endswith(".*"):
                table_name = stripped[:-2].strip()
                if not table_name:
                    raise ValueError(
                        "Invalid projection token: '*' must reference a table"
                    )
                items.append(ProjectionItem(kind="table_wildcard", value=table_name))
                continue

            column_expr, alias = self._extract_alias(stripped)
            self._ensure_simple_column(column_expr)

            items.append(
                ProjectionItem(kind="column", value=column_expr.strip(), alias=alias)
            )

        if not items:
            return [ProjectionItem(kind="wildcard")]
        return items

    def _split_select_list(self, select_clause: str) -> List[str]:
        """
        Split a SELECT list while keeping parentheses contents intact.
        """
        parts: List[str] = []
        buffer: List[str] = []
        depth = 0
        in_single = False
        in_double = False

        for char in select_clause:
            if char == "'" and not in_double:
                in_single = not in_single
            elif char == '"' and not in_single:
                in_double = not in_double
            elif char == "(" and not in_single and not in_double:
                depth += 1
            elif char == ")" and not in_single and not in_double and depth > 0:
                depth -= 1

            if char == "," and depth == 0 and not in_single and not in_double:
                parts.append("".join(buffer).strip())
                buffer = []
            else:
                buffer.append(char)

        if buffer:
            parts.append("".join(buffer).strip())

        return [part for part in parts if part]

    def _extract_alias(self, token: str) -> tuple[str, Optional[str]]:
        alias_match = re.search(r"\s+AS\s+", token, flags=re.IGNORECASE)
        if alias_match:
            source = token[: alias_match.start()].strip()
            alias = token[alias_match.end() :].strip()
            return source, alias or None

        parts = token.rsplit(" ", 1)
        if len(parts) == 2:
            candidate_alias = parts[1].strip()
            if self._is_identifier(candidate_alias):
                source = parts[0].strip()
                return source, candidate_alias

        return token.strip(), None

    def _is_identifier(self, value: str) -> bool:
        return re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is not None

    def _ensure_simple_column(self, column: str) -> None:
        # Expressions (e.g., aggregates or arithmetic) are not implemented yet.
        if any(symbol in column for symbol in ("(", ")", "+", "-", "*", "/", "%")):
            raise NotImplementedError("Projection expressions are not supported yet")

    def _is_trivial_projection(self, items: List[ProjectionItem]) -> bool:
        return len(items) == 1 and items[0].kind == "wildcard"

    def _project_row(
        self,
        row: Dict[str, object],
        items: List[ProjectionItem],
        schemas: List[TableSchema],
    ) -> Dict[str, object]:
        """Apply projection items to a single row."""
        projected: Dict[str, object] = {}

        for item in items:
            if item.kind == "wildcard":
                self._append_all_tables(projected, row, schemas)
            elif item.kind == "table_wildcard" and item.value:
                self._append_single_table(projected, row, schemas, item.value)
            elif item.kind == "column" and item.value:
                column_def, table_name = self._get_column_definition(schemas, item.value)
                column_name = item.alias or f"{table_name}.{column_def.name}"
                projected[column_name] = get_column_value(row, item.value)

        return projected

    def _append_all_tables(
        self,
        target: Dict[str, object],
        row: Dict[str, object],
        schemas: List[TableSchema],
    ) -> None:
        if not schemas:
            target.update(row)
            return

        for schema in schemas:
            for column in schema.columns:
                target[f"{schema.table_name}.{column.name}"] = row.get(f"{schema.table_name}.{column.name}")

    def _append_single_table(
        self,
        target: Dict[str, object],
        row: Dict[str, object],
        schemas: List[TableSchema],
        table_name: str,
    ) -> None:
        schema = get_schema_from_table_name(schemas, table_name)
        for column in schema.columns:
            target[f"{table_name}.{column.name}"] = row.get(f"{table_name}.{column.name}")

    def _derive_output_name(self, column: str) -> str:
        if "." in column:
            return column.split(".", 1)[1]
        return column

    def _build_projected_schema(
        self, schemas: List[TableSchema], items: List[ProjectionItem]
    ) -> List[TableSchema]:
        if not schemas:
            return []

        if self._is_trivial_projection(items):
            return schemas

        table_order = [schema.table_name for schema in schemas]
        schema_lookup = {schema.table_name: schema for schema in schemas}
        result_templates: Dict[str, TableSchema] = {
            name: replace(schema_lookup[name], columns=[]) for name in table_order
        }
        included_tables: set[str] = set()

        def ensure_table(name: str) -> TableSchema:
            included_tables.add(name)
            return result_templates[name]

        for item in items:
            if item.kind == "wildcard":
                for schema in schemas:
                    target_schema = ensure_table(schema.table_name)
                    self._extend_columns(target_schema.columns, schema.columns)
            elif item.kind == "table_wildcard" and item.value:
                schema = get_schema_from_table_name(schemas, item.value)
                target_schema = ensure_table(schema.table_name)
                self._extend_columns(target_schema.columns, schema.columns)
            elif item.kind == "column" and item.value:
                column_def, table_name = self._get_column_definition(
                    schemas, item.value
                )
                output_name = item.alias or self._derive_output_name(item.value)
                new_column = replace(column_def, name=output_name, primary_key=False)
                target_schema = ensure_table(table_name)
                self._append_column(target_schema.columns, new_column)

        return [
            result_templates[name]
            for name in table_order
            if name in included_tables and result_templates[name].columns
        ]

    def _extend_columns(
        self, target: List[ColumnDefinition], source: List[ColumnDefinition]
    ) -> None:
        for column in source:
            self._append_column(target, column)

    def _append_column(
        self, target: List[ColumnDefinition], column: ColumnDefinition
    ) -> None:
        if any(existing.name == column.name for existing in target):
            return
        target.append(column)

    def _get_column_definition(
        self, schemas: List[TableSchema], column_name: str
    ) -> tuple[ColumnDefinition, str]:
        if "." in column_name:
            table_name, col_name = column_name.split(".", 1)
            schema = get_schema_from_table_name(schemas, table_name)
            for column in schema.columns:
                if column.name == col_name:
                    return column, schema.table_name
            raise ValueError(f"Column '{col_name}' not found in table '{table_name}'")

        matches: List[tuple[TableSchema, ColumnDefinition]] = []
        for schema in schemas:
            for column in schema.columns:
                if column.name == column_name:
                    matches.append((schema, column))

        if not matches:
            raise ValueError(f"Column '{column_name}' not found in projection source")
        if len(matches) > 1:
            raise ValueError(f"Ambiguous column '{column_name}' in projection")

        schema, column = matches[0]
        return column, schema.table_name
