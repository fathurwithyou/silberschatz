from __future__ import annotations
from typing import Any , Dict, List , Optional
from pathlib import Path
import json , time
from core.failure_recovery_manager import IFailureRecoveryManager
from core.models.failure import LogRecord, LogRecordType, RecoverCriteria

class FailureRecoveryManager(IFailureRecoveryManager) :
    """
    inisialisasi path, meta sidecar, buffer config, dan hook query_processor.
    """

    def __init__(
        self,
        log_path: str | Path = "wal.jsonl",
        *,
        buffer_max: int = 256,
        query_processor: Optional[object] = None,
        meta_path: Optional[str | Path] = None,
    ) -> None:
        # path WAL & meta
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.touch(exist_ok=True)

        self.meta_path = Path(meta_path) if meta_path else Path(f"{self.log_path}.meta.json")
        if not self.meta_path.exists():
            self._write_meta({"last_checkpoint_line": 0, "created_at": time.time()})

        # Konfigurasi runtime
        self._buffer_max = max(int(buffer_max), 1)
        self._buffer_size = 0  
        self.buffer: List[LogRecord] = []

    # helper meta sidecar
    def _read_meta(self) -> Dict[str, Any]:
        try:
            return json.loads(self.meta_path.read_text(encoding="utf-8"))
        except Exception:
            return {"last_checkpoint_line": 0}

    def _write_meta(self, meta: Dict[str, Any]) -> None:
        self.meta_path.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def _flush_buffer_to_disk(self):
        if not self.buffer:
            return
        with self.log_path.open("a", encoding="utf-8") as f:
            for record in self.buffer:
                log_entry = record.to_dict()
                f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        self.buffer.clear()

    # TODO
    def write_log(self, info: LogRecord):
        if info:
            self.buffer.append(info)
            self._buffer_size += 1

        if (self._buffer_size >= self._buffer_max or (info and info.log_type in [LogRecordType.COMMIT, LogRecordType.ABORT])):
            self._flush_buffer_to_disk()
            self._buffer_size = 0


    def save_checkpoint(self, *args, **kwargs):
        pass

    def recover(self, *args, **kwargs):
        pass