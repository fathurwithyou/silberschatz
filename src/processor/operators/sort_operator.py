from __future__ import annotations
from typing import List, Dict, Tuple, Any
from datetime import datetime
from src.core.models import Rows


class SortOperator:
    def execute(self, rows: Rows, order_by: str) -> Rows:
        if not order_by or not order_by.strip():
            return rows
        
        sort_keys = self._parse_order_by(order_by)
        sorted_data = sorted(
            rows.data,
            key=lambda row: self._build_sort_key(row, sort_keys)
        )

        return Rows(
            data=sorted_data,
            rows_count=len(sorted_data),
            schema=rows.schema,
        )

    def _parse_order_by(self, order_by: str) -> List[Tuple[str, str]]:
        parts = order_by.split(",")
        keys = []
        for part in parts:
            tokens = part.strip().split()
            col = tokens[0]
            direction = "ASC"
            if len(tokens) > 1 and tokens[1].upper() in ("ASC", "DESC"):
                direction = tokens[1].upper()
            keys.append((col, direction))
        return keys
    
    def _build_sort_key(self, row: Dict[str, object], sort_keys):
        key = []
        for col, direction in sort_keys:
            raw = self._resolve_column_value(row, col)
            norm = self._normalize_value(raw)
            key.append(self._apply_direction(norm, direction))
        return tuple(key)
    
    def _resolve_column_value(self, row: Dict[str, Any], col: str):
        # exact match (alias.col)
        if col in row:
            return row[col]

        # unqualified match (col)
        if "." in col:
            _, base = col.split(".", 1)
            if base in row:
                return row[base]

        # fallback
        low = col.lower()
        if low in row:
            return row[low]

        # fallback jika tidak ditemukan
        return None


    def _normalize_value(self, value: Any):
        if value is None:
            return (0, None)

        if isinstance(value, (int, float)):
            return (1, value)

        if isinstance(value, bool):
            return (2, int(value))

        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                return (3, dt.timestamp())
            except ValueError:
                pass
            return (4, value.lower()) 

        return (5, str(value))

    def _apply_direction(self, norm, direction: str):
        type_id, val = norm

        if direction == "ASC":
            return (type_id, val)
        
        if isinstance(val, (int, float)):
            return (type_id, -val)

        if val is None:
            return (type_id, val)

        if isinstance(val, str):
            return (type_id, "".join(chr(255 - ord(c)) for c in val))

        return (type_id, val)
