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

    def recover(self, criteria: RecoverCriteria) -> List[str]:
        # Backward recovery berdasarkan criteria.
        # Returns: List of recovery actions performed
        if not self.log_path.exists():
            return []
        
        # Flush buffer dulu biar semua log masuk ke disk
        self._flush_buffer_to_disk()
        
        recovery_actions = []
        
        # Baca semua log entries dari file
        log_entries = []
        with self.log_path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                if line.strip():
                    try:
                        entry = json.loads(line.strip())
                        entry["line_number"] = line_num  # Tambah line number sebagai timestamp proxy
                        log_entries.append(entry)
                    except json.JSONDecodeError:
                        continue
        
        # Backward recovery: proses dari entry terakhir ke awal
        for entry in reversed(log_entries):
            # Ambil timestamp/id dari entry
            entry_timestamp = entry.get("line_number", 0)  # Gunakan line number sebagai timestamp
            entry_txn_id = entry.get("transaction_id", -1)
            
            # Cek apakah entry memenuhi criteria recovery
            if not criteria.match(entry_timestamp, entry_txn_id):
                break  # Berhenti jika criteria tidak terpenuhi
                
            # Lakukan undo operation berdasarkan log_type
            log_type = entry.get("log_type")
            
            if log_type == "CHANGE":
                # Undo perubahan data
                undo_action = self._undo_change(entry)
                if undo_action:
                    recovery_actions.append(undo_action)
                    
            elif log_type == "COMMIT":
                # Skip committed transactions (sudah persisten, tidak perlu di-undo)
                action = f"SKIP COMMIT for transaction {entry_txn_id}"
                recovery_actions.append(action)
                
            elif log_type == "ABORT":
                # Transaction sudah di-abort sebelumnya, skip
                action = f"SKIP ABORT for transaction {entry_txn_id}"
                recovery_actions.append(action)
                
            elif log_type == "BEGIN":
                # Catat bahwa transaction ini di-rollback
                action = f"ROLLBACK transaction {entry_txn_id}"
                recovery_actions.append(action)
        
        return recovery_actions

    def _undo_change(self, entry: Dict[str, Any]) -> str:
        # Undo satu perubahan dari log entry.
        # Untuk sekarang, hanya return description action yang akan dilakukan.
        # Nanti bisa diintegrasikan dengan query processor untuk eksekusi nyata.
        
        item_name = entry.get("item_name", "unknown")
        old_value = entry.get("old_value")
        new_value = entry.get("new_value")
        txn_id = entry.get("transaction_id", -1)
        
        if old_value is not None:
            # Return description dari undo action
            return f"RESTORE {item_name} FROM '{new_value}' TO '{old_value}' (txn: {txn_id})"
        else:
            # Jika old_value adalah None, berarti ini INSERT operation
            return f"DELETE {item_name} (undo INSERT, txn: {txn_id})"