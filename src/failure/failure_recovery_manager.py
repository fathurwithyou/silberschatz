from __future__ import annotations
from typing import Any , Dict , List , Optional
from pathlib import Path
import json , time
from src.core.failure_recovery_manager import IFailureRecoveryManager
from src.core.models.failure import LogRecord , LogRecordType , RecoverCriteria
from src.core.models.storage import DataWrite , Condition , ComparisonOperator

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
        storage_manager: Optional[object] = None,
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
        
        # Hook untuk integrasi dengan komponen lain
        self.query_processor = query_processor
        self.storage_manager = storage_manager

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

    def write_log(self, info: LogRecord):
        if info:
            self.buffer.append(info)
            self._buffer_size += 1

        if (self._buffer_size >= self._buffer_max or (info and info.log_type in [LogRecordType.COMMIT, LogRecordType.ABORT])):
            self._flush_buffer_to_disk()
            self._buffer_size = 0


    def save_checkpoint(self):
            """
            Menyimpan checkpoint ke dalam log.
            """
            # Step 1: Flush buffer WAL ke disk
            self._flush_buffer_to_disk()
            
            # Step 2: Baca metadata
            meta = self._read_meta()
            last_checkpoint_line = meta.get("last_checkpoint_line", 0)
            
            # Step 3: Identifikasi transaksi
            active_transactions = set()
            committed_transactions = set()
            log_entries = []
            
            current_line = 0
            if self.log_path.exists():
                with self.log_path.open("r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        current_line = line_num
                        if line_num <= last_checkpoint_line:
                            continue
                            
                        if line.strip():
                            try:
                                entry = json.loads(line.strip())
                                log_entries.append(entry)
                                
                                # Parse Enum name back to string for comparison if needed, 
                                # or compare string directly from JSON
                                log_type = entry.get("log_type") 
                                txn_id = entry.get("transaction_id")
                                
                                if log_type == "START":
                                    active_transactions.add(txn_id)
                                elif log_type == "COMMIT":
                                    committed_transactions.add(txn_id)
                                    active_transactions.discard(txn_id)
                                elif log_type == "ABORT":
                                    active_transactions.discard(txn_id)
                            except json.JSONDecodeError:
                                continue
            
            # Step 4: Apply committed changes ke physical storage
            if self.storage_manager:
                for entry in log_entries:
                    log_type = entry.get("log_type")
                    txn_id = entry.get("transaction_id")
                    
                    # Hanya apply changes dari committed transactions
                    if log_type == "CHANGE" and txn_id in committed_transactions:
                        try:
                            payload = entry.get("new_value")
                            
                            if isinstance(payload, dict):
                                table_name = payload.get("table")
                                operation = payload.get("operation")
                                conditions_raw = payload.get("conditions", [])
                                
                                # Reconstruct Conditions
                                conditions = []
                                for c in conditions_raw:
                                    if isinstance(c, dict):
                                        try:
                                            # Try to map operator string to Enum
                                            op_val = c.get("operator")
                                            # Handle if operator is stored as name (EQ) or value (=)
                                            if op_val in [e.value for e in ComparisonOperator]:
                                                op_enum = ComparisonOperator(op_val)
                                            elif op_val in [e.name for e in ComparisonOperator]:
                                                op_enum = ComparisonOperator[op_val]
                                            else:
                                                # Default or skip if unknown
                                                continue
                                                
                                            conditions.append(Condition(
                                                column=c.get("column"), 
                                                operator=op_enum, 
                                                value=c.get("value")
                                            ))
                                        except Exception:
                                            continue
                                
                                is_update = (operation == "UPDATE")
                                
                                # Construct data dictionary
                                if is_update:
                                    col = payload.get("column")
                                    val = payload.get("actual_value")
                                    # If column is specified, it's a single column update
                                    data = {col: val} if col else val
                                else:
                                    # INSERT: actual_value should be the row dict
                                    data = payload.get("actual_value")
                                
                                data_write_obj = DataWrite(
                                    table_name=table_name,
                                    data=data,
                                    is_update=is_update,
                                    conditions=conditions
                                )
                                self.storage_manager.write_block(data_write_obj)
                        except Exception as e:
                            print(f"Error applying log to storage: {e}")
            
            # Step 5: (Skipped - Storage Manager has no public flush method in spec)
            
            # Step 6: Buat dan tulis checkpoint log entry
            # UPDATED: Now using your LogRecord definition correctly
            checkpoint_record = LogRecord(
                log_type=LogRecordType.CHECKPOINT,
                transaction_id=-1,
                item_name=None, # Checkpoint doesn't need an item name
                old_value=None,
                new_value=None,
                active_transactions=list(active_transactions) # Pass directly!
            )
            
            with self.log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(checkpoint_record.to_dict(), ensure_ascii=False) + "\n")
                current_line += 1
            
            # Step 7: Update metadata
            meta["last_checkpoint_line"] = current_line
            meta["last_checkpoint_time"] = time.time()
            meta["active_transactions_at_checkpoint"] = list(active_transactions)
            self._write_meta(meta)
            
            self._buffer_size = 0

    def recover(self, criteria: RecoverCriteria) -> List[str]:
        # Backward recovery berdasarkan criteria.
        # Returns: List of recovery actions performed
        if not self.log_path.exists():
            return []
        
        # Flush buffer dulu biar semua log masuk ke disk
        self._flush_buffer_to_disk()
        
        # Baca checkpoint info untuk optimasi
        meta = self._read_meta()
        last_checkpoint_line = meta.get("last_checkpoint_line", 0)
        active_txns_at_checkpoint = set(meta.get("active_transactions_at_checkpoint", []))
        
        recovery_actions = []
        
        # Baca log entries HANYA dari checkpoint terakhir (optimasi)
        log_entries = []
        with self.log_path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                if line_num < last_checkpoint_line:  # SKIP entries sebelum checkpoint
                    continue
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
            if (criteria.is_transaction) :
                # Mode transaction: skip entry yang tidak cocok, tapi jangan menghentikan scanning.
                if (not criteria.match(entry_timestamp , entry_txn_id)) :
                    continue    # Lanjut scanning
            else :
                # Mode timestamp: kalau tidak match (timestamp < cutoff), berhenti karena entry sebelumnya lebih lama.
                if (not criteria.match(entry_timestamp , entry_txn_id)) :
                    break    # Berhenti jika criteria tidak terpenuhi
                
            # Lakukan undo operation berdasarkan log_type
            log_type = entry.get("log_type")
            
            if log_type == "CHANGE":
                # Undo perubahan data
                undo_action = self._undo_change(entry, active_txns_at_checkpoint)
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

    def _undo_change(self, entry: Dict[str, Any], active_txns_at_checkpoint: set = None) -> str:
        # Undo satu perubahan dari log entry.
        # Untuk sekarang, hanya return description action yang akan dilakukan.
        # Nanti bisa diintegrasikan dengan storage manager untuk eksekusi nyata.
        
        txn_id = entry.get("transaction_id", -1)
        
        # Cek apakah transaksi ini sudah committed di checkpoint terakhir
        if active_txns_at_checkpoint and txn_id not in active_txns_at_checkpoint and txn_id != -1:
            return f"SKIP CHANGE for committed transaction {txn_id}"
        
        item_name = entry.get("item_name", "unknown")
        old_value = entry.get("old_value")
        new_value = entry.get("new_value")

        #cek apakah DDL atau tidak (create/drop table)
        if isinstance(new_value, dict) and "operation" in new_value:
            operation = new_value.get("operation")
            table_name = new_value.get("table", "unknown")
            
            if operation == "CREATE_TABLE":
                # Undo CREATE TABLE = DROP TABLE
                return f"DROP TABLE {table_name} (undo CREATE, txn: {txn_id})"
            
            elif operation == "DROP_TABLE":
                # Undo DROP TABLE = CREATE TABLE dengan schema lama
                schema = old_value.get("schema") if isinstance(old_value, dict) else None
                return f"CREATE TABLE {table_name} WITH SCHEMA {schema} (undo DROP, txn: {txn_id})"
        
        #kalo update, insert, delete
        if old_value is not None:
            # Return description dari undo action
            return f"RESTORE {item_name} FROM '{new_value}' TO '{old_value}' (txn: {txn_id})"
        else:
            # Jika old_value adalah None, berarti ini INSERT operation
            return f"DELETE {item_name} (undo INSERT, txn: {txn_id})"