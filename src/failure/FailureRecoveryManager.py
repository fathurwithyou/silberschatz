from __future__ import annotations
from typing import Any, Dict, Optional, Literal
from pathlib import Path
import json
import time

class RecoverCriteria:
    __slots__ = ("_mode", "_value")

    def __init__(self, mode: Literal["timestamp", "transaction_id"], value: Any) -> None:
        if mode not in ("timestamp", "transaction_id"):
            raise ValueError("RecoverCriteria mode must be 'timestamp' or 'transaction_id'.")
        self._mode = mode
        self._value = value

    @staticmethod
    def from_timestamp(epoch_seconds: float) -> "RecoverCriteria":
        if not isinstance(epoch_seconds, (int, float)):
            raise TypeError("timestamp must be int or float")
        return RecoverCriteria("timestamp", float(epoch_seconds))

    @staticmethod
    def from_transaction(transaction_id: int) -> "RecoverCriteria":
        if not isinstance(transaction_id, int):
            raise TypeError("transaction_id must be an int")
        return RecoverCriteria("transaction_id", transaction_id)

    @property
    def is_timestamp(self) -> bool:
        return self._mode == "timestamp"

    @property
    def is_transaction(self) -> bool:
        return self._mode == "transaction_id"

    @property
    def value(self) -> Any:
        return self._value

    def match(self, entry_ts: float, entry_txn: int) -> bool:
        if self.is_timestamp:
            return float(entry_ts) >= float(self._value)
        return int(entry_txn) == int(self._value)

class FailureRecoveryManager:
    def __init__(
        self,
        log_path: str | Path = "wal.jsonl",
        *,
        buffer_max: int = 256,
        query_processor: Optional[object] = None,
        meta_path: Optional[str | Path] = None,
    ) -> None:
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.touch(exist_ok=True)

        self.meta_path = Path(meta_path) if meta_path else Path(str(self.log_path) + ".meta.json")
        if not self.meta_path.exists():
            self._write_meta({"last_checkpoint_line": 0, "created_at": time.time()})

        self._buffer_max = max(int(buffer_max), 1)
        self._buffer_size = 0  

        self._qp = query_processor  

    def _read_meta(self) -> Dict[str, Any]:
        try:
            return json.loads(self.meta_path.read_text(encoding="utf-8"))
        except Exception:
            return {"last_checkpoint_line": 0}

    def _write_meta(self, meta: Dict[str, Any]) -> None:
        self.meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    def write_log(self, *args, **kwargs):
        #TODO
        pass

    def save_checkpoint(self, *args, **kwargs):
        #TODO
        pass

    def recover(self, *args, **kwargs):
        #TODO
        pass