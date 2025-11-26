from __future__ import annotations

from typing import Dict, List, Optional, Set

from src.core.models import Rows, TableSchema
from ..conditions import ConditionEvaluator

class JoinOperator:
    def __init__(self):
        pass
    
    def execute(
        self,
        outer_relation: Rows,
        inner_relation: Rows,
        conditions: Optional[str] = None,
        natural_shared_columns: Optional[Set[str]] = None,
    ) -> Rows:
        self._check_duplicate_alias(outer_relation.schema, inner_relation.schema)
        combined_schema = self._merge_schema(
            outer_relation.schema,
            inner_relation.schema,
            natural_shared_columns,
        )
        evaluation_schema = self._merge_schema(
            outer_relation.schema,
            inner_relation.schema,
            None,
        )
        condition_text = None
        evaluator = None
        if conditions and conditions.strip():
            condition_text = conditions.strip()
            evaluator = ConditionEvaluator(evaluation_schema)

        result_rows: List[Dict[str, object]] = []

        for outer_row in outer_relation.data:
            for inner_row in inner_relation.data:
                merged_row = self._merge_rows(
                    outer_row,
                    inner_row,
                    outer_relation.schema,
                    inner_relation.schema,
                )
                if evaluator and condition_text:
                    if evaluator.evaluate(condition_text, merged_row):
                        result_rows.append(
                            self._prune_duplicate_columns(
                                merged_row,
                                natural_shared_columns,
                                inner_relation.schema,
                            )
                        )
                else:
                    result_rows.append(
                        self._prune_duplicate_columns(
                            merged_row,
                            natural_shared_columns,
                            inner_relation.schema,
                        )
                    )

        return Rows(
            data=result_rows,
            rows_count=len(result_rows),
            schema=combined_schema,
        )

    def _merge_schema(
        self,
        outer_schema: Optional[List[TableSchema]],
        inner_schema: Optional[List[TableSchema]],
        skip_inner_columns: Optional[Set[str]] = None,
    ) -> List[TableSchema]:
        merged: List[TableSchema] = []
        if outer_schema:
            merged.extend(outer_schema)
        if inner_schema:
            for schema in inner_schema:
                if skip_inner_columns:
                    filtered_columns = [
                        column
                        for column in schema.columns
                        if column.name not in skip_inner_columns
                    ]
                else:
                    filtered_columns = list(schema.columns)

                merged.append(
                    TableSchema(
                        table_name=schema.table_name,
                        columns=filtered_columns,
                        primary_key=schema.primary_key,
                    )
                )
        return merged

    def _merge_rows(
        self,
        outer_row: Dict[str, object],
        inner_row: Dict[str, object],
        outer_schema: Optional[List[TableSchema]],
        inner_schema: Optional[List[TableSchema]],
    ) -> Dict[str, object]:
        merged: Dict[str, object] = {}
        self._inject_row(merged, outer_row, outer_schema)
        self._inject_row(merged, inner_row, inner_schema)
        return merged

    def _inject_row(
        self,
        target: Dict[str, object],
        row: Dict[str, object],
        schemas: Optional[List[TableSchema]],
    ) -> None:
        if not schemas:
            for key, value in row.items():
                if key not in target:
                    target[key] = value
            return

        for schema in schemas:
            table_name = schema.table_name or ""
            for column in schema.columns:
                base_key = column.name
                qualified_key = f"{table_name}.{base_key}" if table_name else base_key

                value = row.get(qualified_key)
                if value is None:
                    value = row.get(base_key)

                target[qualified_key] = value

    def _prune_duplicate_columns(
        self,
        row: Dict[str, object],
        skip_columns: Optional[Set[str]],
        inner_schema: Optional[List[TableSchema]],
    ) -> Dict[str, object]:
        if not skip_columns or not inner_schema:
            return row

        cleaned_row = dict(row)
        for schema in inner_schema:
            table_name = schema.table_name or ""
            for column in schema.columns:
                if column.name not in skip_columns:
                    continue
                qualified_key = f"{table_name}.{column.name}" if table_name else column.name
                cleaned_row.pop(qualified_key, None)
        return cleaned_row

    def _check_duplicate_alias(
        self,
        outer_schema: Optional[List[TableSchema]],
        inner_schema: Optional[List[TableSchema]],
    ) -> None:
        if not outer_schema or not inner_schema:
            return

        outer_aliases = {schema.table_name for schema in outer_schema if schema.table_name}
        for schema in inner_schema:
            if schema.table_name and schema.table_name in outer_aliases:
                raise ValueError(f"Duplicate table alias found: {schema.table_name}")
