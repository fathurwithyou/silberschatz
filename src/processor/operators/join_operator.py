from __future__ import annotations

from typing import Dict, List, Optional

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
    ) -> Rows:
        combined_schema = self._merge_schema(outer_relation.schema, inner_relation.schema)
        condition_text = None
        evaluator = None
        if conditions and conditions.strip():
            condition_text = conditions.strip()
            evaluator = ConditionEvaluator(combined_schema)

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
                        result_rows.append(merged_row)
                else:
                    result_rows.append(merged_row)

        return Rows(
            data=result_rows,
            rows_count=len(result_rows),
            schema=combined_schema,
        )

    def _merge_schema(
        self,
        outer_schema: Optional[List[TableSchema]],
        inner_schema: Optional[List[TableSchema]],
    ) -> List[TableSchema]:
        merged: List[TableSchema] = []
        if outer_schema:
            merged.extend(outer_schema)
        if inner_schema:
            merged.extend(inner_schema)
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

                if base_key not in target:
                    target[base_key] = value
                target[qualified_key] = value
