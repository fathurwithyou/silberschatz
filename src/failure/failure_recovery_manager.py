from __future__ import annotations
from typing import Any , Dict , List , Optional
from pathlib import Path
import json , time
from src.core.failure_recovery_manager import IFailureRecoveryManager
from src.core.models.failure import LogRecord , LogRecordType , RecoverCriteria
from src.core.models.storage import DataWrite , Condition , ComparisonOperator, DataDeletion, TableSchema, ColumnDefinition

class FailureRecoveryManager(IFailureRecoveryManager) :
    """
    inisialisasi path, meta sidecar, buffer config, dan hook query_processor.
    """

    def __init__(
        self,
        log_path: str | Path = "wal.jsonl",
        *,
        buffer_max: int = 256,
        checkpoint_interval: int = 60,
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
        self.checkpoint_interval = checkpoint_interval
        self._buffer_size = 0  
        self.buffer: List[LogRecord] = []
        self.active_transactions = set()
        self.last_checkpoint_time = time.time()
        self.lines_written_since_last_checkpoint = 0
        
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
        
        self.lines_written_since_last_checkpoint += len(self.buffer)
        self.buffer.clear()
        self._buffer_size = 0


    def write_log(self, info: LogRecord):
        if info:
            # Update active transactions
            if info.log_type == LogRecordType.START:
                self.active_transactions.add(info.transaction_id)
            elif info.log_type in (LogRecordType.COMMIT, LogRecordType.ABORT):
                self.active_transactions.discard(info.transaction_id)

            self.buffer.append(info)
            self._buffer_size += 1

        # Check auto-checkpoint conditions
        should_checkpoint = False
        if self._buffer_size >= self._buffer_max:
             should_checkpoint = True
        elif (time.time() - self.last_checkpoint_time) >= self.checkpoint_interval:
             should_checkpoint = True

        if should_checkpoint:
            self.save_checkpoint()
        elif (info and info.log_type in [LogRecordType.COMMIT, LogRecordType.ABORT]):
            self._flush_buffer_to_disk()


    def save_checkpoint(self):
            """
            Menyimpan checkpoint ke dalam log.
            """
            # Step 1: Apply changes from buffer to storage
            if self.storage_manager:
                for record in self.buffer:
                    if record.log_type == LogRecordType.CHANGE:
                        self._apply_change(record)

            # Step 2: Flush buffer WAL ke disk
            self._flush_buffer_to_disk()
            
            # Step 3: Baca metadata
            meta = self._read_meta()
            last_checkpoint_line = meta.get("last_checkpoint_line", 0)
            
            # Step 4: Buat dan tulis checkpoint log entry
            checkpoint_record = LogRecord(
                log_type=LogRecordType.CHECKPOINT,
                transaction_id=-1,
                item_name=None,
                old_value=None,
                new_value=None,
                active_transactions=list(self.active_transactions)
            )
            
            with self.log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(checkpoint_record.to_dict(), ensure_ascii=False) + "\n")
            
            # Update counters
            self.lines_written_since_last_checkpoint += 1
            current_line = last_checkpoint_line + self.lines_written_since_last_checkpoint
            
            # Step 5: Update metadata
            meta["last_checkpoint_line"] = current_line
            meta["last_checkpoint_time"] = time.time()
            meta["active_transactions_at_checkpoint"] = list(self.active_transactions)
            self._write_meta(meta)
            
            # Reset counter for next checkpoint
            self.lines_written_since_last_checkpoint = 0
            self.last_checkpoint_time = time.time()

    def _apply_change(self, record: LogRecord):
        if not self.storage_manager:
            return

        new_value = record.new_value
        old_value = record.old_value
        
        # 1. DDL: CREATE/DROP TABLE
        if isinstance(new_value, dict) and "operation" in new_value:
            operation = new_value.get("operation")
            table_name = new_value.get("table")
            
            if operation == "CREATE_TABLE":
                schema_dict = new_value.get("schema")
                if schema_dict:
                    try:
                        columns = [ColumnDefinition(**c) for c in schema_dict.get("columns", [])]
                        schema = TableSchema(table_name=schema_dict["table_name"], columns=columns, primary_key=schema_dict.get("primary_key"))
                        self.storage_manager.create_table(schema)
                    except Exception:
                        pass 
            
            elif operation == "DROP_TABLE":
                try:
                    self.storage_manager.drop_table(table_name)
                except Exception:
                    pass
            return

        # 2. DML
        payload = new_value if isinstance(new_value, dict) and "table" in new_value else None
        if not payload:
            return

        table = payload.get("table")
        data = payload.get("data")
        conditions_dicts = payload.get("conditions", [])
        
        conditions = []
        for c in conditions_dicts:
            try:
                op_val = c.get("operator")
                if op_val in [e.value for e in ComparisonOperator]:
                    op_enum = ComparisonOperator(op_val)
                elif op_val in [e.name for e in ComparisonOperator]:
                    op_enum = ComparisonOperator[op_val]
                else:
                    continue
                conditions.append(Condition(column=c.get("column"), operator=op_enum, value=c.get("value")))
            except Exception:
                continue

        try:
            # INSERT
            if old_value is None:
                dw = DataWrite(table_name=table, data=data, is_update=False)
                self.storage_manager.write_block(dw)
            
            # UPDATE
            elif old_value is not None and data is not None:
                dw = DataWrite(table_name=table, data=data, is_update=True, conditions=conditions)
                self.storage_manager.write_block(dw)
            
            # DELETE
            elif old_value is not None and data is None:
                dd = DataDeletion(table_name=table, conditions=conditions)
                self.storage_manager.delete_block(dd)
        except Exception:
            pass

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
        # Kalau storage_manager ada, maka undo ke storage, kalau tidak hanya return description action
        txn_id = entry.get("transaction_id", -1)
        
        # Cek apakah transaksi ini sudah committed di checkpoint terakhir
        if active_txns_at_checkpoint and txn_id not in active_txns_at_checkpoint and txn_id != -1:
            return f"SKIP CHANGE for committed transaction {txn_id}"
        
        item_name = entry.get("item_name", "unknown")
        old_value = entry.get("old_value")
        new_value = entry.get("new_value")

        # 1) CREATE TABLE, DROP TABLE
        if isinstance(new_value, dict) and "operation" in new_value:
            operation = new_value.get("operation")
            table_name = new_value.get("table", "unknown")
            
            if operation == "CREATE_TABLE":
                # Undo CREATE TABLE = DROP TABLE
                if self.storage_manager and hasattr(self.storage_manager, "drop_table"):
                    try:
                        self.storage_manager.drop_table(table_name)
                        return f"DROPPED TABLE {table_name} (undo CREATE, txn: {txn_id})"
                    except Exception as e:
                        return f"FAILED DROP TABLE {table_name} (undo CREATE, txn: {txn_id}): {e}"
                return f"DROP TABLE {table_name} (undo CREATE, txn: {txn_id})"
            
            elif operation == "DROP_TABLE":
                # Undo DROP TABLE = CREATE TABLE dengan schema lama
                schema_obj = old_value.get("schema") if isinstance(old_value, dict) else None
                if self.storage_manager and hasattr(self.storage_manager, "create_table") and schema_obj:
                    try:
                        self.storage_manager.create_table(schema_obj)
                        return f"CREATED TABLE {table_name} (undo DROP, txn: {txn_id})"
                    except Exception as e:
                        return f"FAILED CREATE TABLE {table_name} (undo DROP, txn: {txn_id}): {e}"
                return f"CREATE TABLE {table_name} WITH SCHEMA {schema_obj} (undo DROP, txn: {txn_id})"
        
        # UPDATE, INSERT, DELETE
        payload = new_value if isinstance(new_value, dict) and "table" in new_value else None
        table = None
        
        if payload:
            table = payload.get("table")
        elif isinstance(item_name, str) and "." in item_name:
            # Parse table dari item_name kayak "Employee.salary"
            table = item_name.split(".", 1)[0]
        
        if old_value is not None:
            # UPDATE atau DELETE
            if payload and table and self.storage_manager:
                # Reconstruct conditions from payload
                conditions = []
                for c in payload.get("conditions", []):
                    try:
                        op_val = c.get("operator")
                        if op_val in [e.value for e in ComparisonOperator]:
                            op_enum = ComparisonOperator(op_val)
                        elif op_val in [e.name for e in ComparisonOperator]:
                            op_enum = ComparisonOperator[op_val]
                        else:
                            continue
                        conditions.append(Condition(column=c.get("column"), operator=op_enum, value=c.get("value")))
                    except Exception:
                        continue
                
                # Data yg direstore = old_value
                data_to_write = old_value if isinstance(old_value, dict) else {}
                try:
                    dw = DataWrite(table_name=table, data=data_to_write, is_update=True, conditions=conditions or None)
                    affected = self.storage_manager.write_block(dw)
                    return f"RESTORED {table} affected={affected} (txn: {txn_id})"
                except Exception as e:
                    return f"FAILED RESTORE {table} (txn: {txn_id}): {e}"
            
            # Fallback: return description
            return f"RESTORE {item_name} FROM '{new_value}' TO '{old_value}' (txn: {txn_id})"
        
        else:
            # old_value None ->INSERT, undo nya DELETE
            if payload and table and self.storage_manager:
                actual = payload.get("actual_value")
                conditions = []
                
                if isinstance(actual, dict):
                    for col, val in actual.items():
                        try:
                            conditions.append(Condition(column=col, operator=ComparisonOperator.EQ, value=val))
                        except Exception:
                            continue
                
                if conditions:
                    try:
                        from src.core.models.storage import DataDeletion
                        data_del = DataDeletion(table_name=table, conditions=conditions)
                        affected = self.storage_manager.delete_block(data_del)
                        return f"DELETED {table} affected={affected} (undo INSERT, txn: {txn_id})"
                    except Exception as e:
                        return f"FAILED DELETE {table} (undo INSERT, txn: {txn_id}): {e}"
            
            # Fallback: return description
            return f"DELETE {item_name} (undo INSERT, txn: {txn_id})"