import sys , os , json
from pathlib import Path
sys.path.insert(0 , os.path.abspath(os.path.join(os.path.dirname(__file__) , ".." , "..")))
from src.failure.failure_recovery_manager import FailureRecoveryManager
from src.core.models.failure import LogRecord , LogRecordType , RecoverCriteria

class TestFailureRecoveryManagerServices :
    """
    Unit Tests src/failure/failure_recovery_manager.py :
    1. Initialization
        1a. Initialization With Create Logs And Meta Files
        1b. Initialization With Custom Meta Path
    2. Buffer Configuration
        2a. Buffer Configuration And Minimum
        2b. Buffer Max Clamped To Minimum One
    3. Meta Read / Write
        3a. Read & Write Meta Valid
        3b. Read & Write Meta Fallback On Invalid JSON
        3c. Read & Write Meta When File Missing Returns Default
    4. Flush Buffer To Disk
        4a. Flush Buffer To Disk Noop When Empty
        4b. Flush Buffer To Disk Appends After Existing Content
    5. Write Log
        5a. Write Log Buffers Until Threshold Then Flush
        5b. Write Log Flush On Commit Even If Buffer Not Full
    6. Save Checkpoint
        6a. Save Checkpoint Updates Last Checkpoint Line
        6b. Save Checkpoint Stores Active Transactions At Checkpoint
        6c. Save Checkpoint Applies Committed Changes To Storage Manager
    7. Recover
        7a. Recover Uses Criteria.Match For Each Log Entry
        7b. Recover With Non Existing Transaction
        7c. Recover Skips Entries Before Last Checkpoint
        7d. Recover Respects Active Transactions At Checkpoint
        7e. Recover With Timestamp Criteria Cuts Off Older Entries
        7f. Recover With Timestamp Criteria When Cutoff After All Entries
        7c. Recover Skips Entries Before Last Checkpoint
    """

    def test_init_1a(self , tmp_path : Path) -> None :
        """
        1a. Initialization With Create Logs And Meta Files
        - Membuat file log_path (WAL), file meta sidecar log_path + '.meta.json', serta meta awal punya last_checkpoint_line = 0.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 4)
        assert (manager)
        assert (log_path.exists())
        meta_path = Path(f"{log_path}.meta.json")
        assert (meta_path.exists())
        meta = json.loads(meta_path.read_text(encoding = "UTF-8"))
        assert (meta.get("last_checkpoint_line") == 0)
        assert ("created_at" in meta)

    def test_init_1b(self , tmp_path : Path) -> None :
        """
        1b. Initialization With Custom Meta Path
        - Jika meta_path disediakan, gunakan itu sebagai path file meta sidecar.
        """
        log_path = tmp_path / "wal.jsonl"
        custom_meta = tmp_path / "my_meta.json"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 4 , meta_path = custom_meta)
        assert (manager)
        assert (manager.meta_path == custom_meta)
        assert (custom_meta.exists())
        default_meta = Path(f"{log_path}.meta.json")
        assert (not default_meta.exists())

    def test_buffer_configuration_2a(self , tmp_path : Path) -> None :
        """
        2a. Buffer Configuration And Minimum
        - Value buffer_max minimal 1.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 2)
        assert (manager._buffer_max >= 1)
    
    def test_buffer_configuration_2b(self , tmp_path : Path) -> None :
        """
        2b. Buffer Max Clamped To Minimum One
        - Jika buffer_max diberikan 0 atau nilai negatif, internal _buffer_max harus di-clamp menjadi 1.
        """
        log_path = tmp_path / "wal.jsonl"
        manager_zero = FailureRecoveryManager(log_path = log_path , buffer_max = 0)
        assert (manager_zero._buffer_max == 1)
        manager_negative = FailureRecoveryManager(log_path = log_path , buffer_max = -10)
        assert (manager_negative._buffer_max == 1)

    def test_meta_read_write_3a(self , tmp_path : Path) -> None :
        """
        3a. Read & Write Meta Valid
        - Fungsi _read_meta() harus dapat membaca meta yang valid dengan benar.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 2)
        meta_path = manager.meta_path
        meta_data = {"last_checkpoint_line" : 10 , "created_at" : 1.35 , "note" : "ok"}
        meta_path.write_text(json.dumps(meta_data , ensure_ascii = False , indent = 2) , encoding = "UTF-8")
        loaded = manager._read_meta()
        assert (loaded == meta_data)

    def test_meta_read_write_3b(self , tmp_path : Path) -> None :
        """
        3b. Read & Write Meta Fallback On Invalid JSON
        - Jika file meta Invalid JSON, _read_meta() harus melakukan fallback ke minimal last_checkpoint_line = 0.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 2)
        assert (manager)
        meta_path = manager.meta_path
        meta_path.write_text("Invalid_JSON" , encoding = "UTF-8")
        loaded = manager._read_meta()
        assert (loaded.get("last_checkpoint_line") == 0)

    def test_meta_read_write_3c(self , tmp_path : Path) -> None :
        """
        3c. Read & Write Meta When File Missing Returns Default
        - Jika file meta hilang, _read_meta() harus melakukan fallback ke default.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 2)
        meta_path = manager.meta_path
        if (meta_path.exists()) :
            meta_path.unlink()
        loaded = manager._read_meta()
        assert (loaded.get("last_checkpoint_line") == 0)

    def test_flush_buffer_4a(self , tmp_path : Path) -> None :
        """
        4a. Flush Buffer To Disk Noop When Empty
        - Fungsi _flush_buffer_to_disk() tidak mengubah file jika buffer kosong.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 2)
        log_path.write_text("Initialize\n" , encoding = "UTF-8")
        manager._flush_buffer_to_disk()
        content = log_path.read_text(encoding = "UTF-8")
        assert (content == "Initialize\n")

    def test_flush_buffer_4b(self , tmp_path : Path) -> None :
        """
        4b. Flush Buffer To Disk Appends After Existing Content
        - Fungsi _flush_buffer_to_disk() harus melakukan append log baru, bukan overwrite isi lama.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 4)
        log_path.write_text("Old_Log\n" , encoding = "UTF-8")
        record = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.name" , old_value = "Alice" , new_value = "Bob" , active_transactions = [1])
        manager.buffer.append(record)
        manager._flush_buffer_to_disk()
        content = log_path.read_text(encoding = "UTF-8").strip().splitlines()
        assert (content[0] == "Old_Log")
        assert (len(content) == 2)
        new_entry = json.loads(content[1])
        assert (new_entry["log_type"] == "CHANGE")
        assert (new_entry["transaction_id"] == 1)
        assert (new_entry["item_name"] == "Employee.name")

    def test_write_log_5a(self , tmp_path : Path) -> None :
        """
        5a. Write Log Buffers Until Threshold Then Flush
        - Menambah record ke buffer.
        - Sebelum buffer_size mencapai buffer_max, file WAL masih kosong.
        - Saat buffer_size >= buffer_max, flush ke disk dan buffer dikosongkan.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 2)
        record1 = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.salary" , old_value = 10_000 , new_value = 20_000 , active_transactions = [1])
        record2 = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 2 , item_name = "Employee.age" , old_value = 20 , new_value = 21 , active_transactions = [2])
        assert (log_path.exists())
        assert (log_path.read_text(encoding = "UTF-8") == "")
        manager.write_log(record1)
        assert (len(manager.buffer) == 1)
        assert (manager._buffer_size == 1)
        assert (log_path.read_text(encoding = "UTF-8") == "")
        manager.write_log(record2)
        assert (len(manager.buffer) == 0)
        assert (manager._buffer_size == 0)
        content = log_path.read_text(encoding = "UTF-8").strip()
        lines = content.splitlines()
        assert (len(lines) == 2)
        first_entry = json.loads(lines[0])
        second_entry = json.loads(lines[1])
        assert (first_entry["log_type"] == "CHANGE")
        assert (first_entry["transaction_id"] == 1)
        assert (second_entry["log_type"] == "CHANGE")
        assert (second_entry["transaction_id"] == 2)

    def test_write_log_5b(self , tmp_path : Path) -> None :
        """
        5b. Write Log Flush On Commit Even If Buffer Not Full
        - Jika ada COMMIT/ABORT, harus flush buffer meskipun buffer_size < buffer_max.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 2)
        record_start = LogRecord(log_type = LogRecordType.START , transaction_id = 1 , item_name = None , old_value = None , new_value = None , active_transactions = [1])
        record_commit = LogRecord(log_type = LogRecordType.COMMIT , transaction_id = 1 , item_name = None , old_value = None , new_value = None , active_transactions = [1])
        manager.write_log(record_start)
        assert (len(manager.buffer) == 1)
        assert (manager._buffer_size == 1)
        assert (log_path.read_text(encoding = "UTF-8") == "")
        manager.write_log(record_commit)
        assert (len(manager.buffer) == 0)
        assert (manager._buffer_size == 0)
        content = log_path.read_text(encoding = "UTF-8").strip()
        lines = content.splitlines()
        assert (len(lines) == 2)
        types = [json.loads(line)["log_type"] for line in lines]
        assert (types == ["START" , "COMMIT"])

    def test_save_checkpoint_6a(self , tmp_path : Path) -> None :
        """
        6a. Save Checkpoint Updates Last Checkpoint Line
        - Fungsi save_checkpoint() harus melakukan update meta["last_checkpoint_line"] sesuai dengan jumlah baris log di file WAL.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 4)
        record1 = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.salary" , old_value = 10_000 , new_value = 20_000 , active_transactions = [1])
        record2 = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 2 , item_name = "Employee.age" , old_value = 20 , new_value = 21 , active_transactions = [2])
        record3 = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.bonus" , old_value = 0 , new_value = 10 , active_transactions = [1])
        manager.write_log(record1)
        manager.write_log(record2)
        manager.write_log(record3)
        manager._flush_buffer_to_disk()
        meta_before = manager._read_meta()
        assert (meta_before.get("last_checkpoint_line") == 0)
        manager.save_checkpoint()
        meta_after = manager._read_meta()
        log_content = log_path.read_text(encoding = "UTF-8").strip()
        if (not log_content) :
            assert (meta_after.get("last_checkpoint_line") == 0)
        else :
            assert (meta_after.get("last_checkpoint_line") == len(log_content.splitlines()))
    
    def test_save_checkpoint_6b(self , tmp_path : Path) -> None :
        """
        6b. Save Checkpoint Stores Active Transactions At Checkpoint
        - Save checkpoint harus menyimpan daftar transaksi yang masih aktif pada saat checkpoint ke meta["active_transactions_at_checkpoint"], serta menuliskan entry checkpoint di WAL dengan field tersebut.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 8)
        record_start_t1 = LogRecord(log_type = LogRecordType.START , transaction_id = 1 , item_name = None , old_value = None , new_value = None , active_transactions = [1])
        record_change_t1 = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.salary" , old_value = 10_000 , new_value = 20_000 , active_transactions = [1])
        record_start_t2 = LogRecord(log_type = LogRecordType.START , transaction_id = 2 , item_name = None , old_value = None , new_value = None , active_transactions = [2])
        record_change_t2 = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 2 , item_name = "Employee.age" , old_value = 20 , new_value = 21 , active_transactions = [2])
        record_commit_t2 = LogRecord(log_type = LogRecordType.COMMIT , transaction_id = 2 , item_name = None , old_value = None , new_value = None , active_transactions = [2])
        for (entry) in (record_start_t1 , record_change_t1 , record_start_t2 , record_change_t2 , record_commit_t2) :
            manager.write_log(entry)
        manager._flush_buffer_to_disk()
        manager.save_checkpoint()
        meta = manager._read_meta()
        active_at_checkpoint = set(meta.get("active_transactions_at_checkpoint" , []))
        assert (active_at_checkpoint == {1})
        log_lines = log_path.read_text(encoding = "UTF-8").strip().splitlines()
        assert (len(log_lines) >= 1)
        checkpoint_entry = json.loads(log_lines[-1])
        assert (checkpoint_entry.get("log_type") == "CHECKPOINT")
        assert (set(checkpoint_entry.get("active_transactions" , [])) == {1})

    def test_save_checkpoint_6c(self , tmp_path : Path) -> None :
        """
        6c. Save Checkpoint Applies Committed Changes To Storage Manager
        - Fungsi save_checkpoint() harus memanggil storage_manager.write_block() untuk setiap CHANGE dari transaksi yang sudah COMMIT setelah checkpoint terakhir.
        """

        class DummyStorageManager :
            def __init__(self) -> None :
                self.calls = []

            def write_block(self , data_write) -> None :
                self.calls.append(data_write)

        log_path = tmp_path / "wal.jsonl"
        storage_manager = DummyStorageManager()
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 4 , storage_manager = storage_manager)
        payload_update = {
            "table" : "Employee" ,
            "operation" : "UPDATE" ,
            "column" : "salary" ,
            "actual_value" : 20_000 ,
            "conditions" : [{"column" : "id" , "operator" : "EQ" , "value" : 1}]
        }
        record_start = LogRecord(log_type = LogRecordType.START , transaction_id = 1 , item_name = None , old_value = None , new_value = None , active_transactions = [1])
        record_change_committed = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.salary" , old_value = 10_000 , new_value = payload_update , active_transactions = [1])
        record_commit = LogRecord(log_type = LogRecordType.COMMIT , transaction_id = 1 , item_name = None , old_value = None , new_value = None , active_transactions = [1])
        payload_uncommitted = {
            "table" : "Employee" ,
            "operation" : "UPDATE" ,
            "column" : "age" ,
            "actual_value" : 30 ,
            "conditions" : [{"column" : "id" , "operator" : "EQ" , "value" : 2}]
        }
        record_change_uncommitted = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 2 , item_name = "Employee.age" , old_value = 20 , new_value = payload_uncommitted , active_transactions = [2])
        manager.write_log(record_start)
        manager.write_log(record_change_committed)
        manager.write_log(record_commit)
        manager.write_log(record_change_uncommitted)
        manager.save_checkpoint()
        assert (len(storage_manager.calls) == 1)
        data_write = storage_manager.calls[0]
        assert (getattr(data_write , "table_name") == "Employee")
        assert (getattr(data_write , "is_update") is True)
        data_payload = getattr(data_write , "data")
        assert (isinstance(data_payload , dict))
        assert (data_payload.get("salary") == 20_000)
        conditions = getattr(data_write , "conditions")
        assert (isinstance(conditions , list))
        assert (len(conditions) == 1)
        condition0 = conditions[0]
        assert (getattr(condition0 , "column") == "id")
        assert (hasattr(condition0 , "operator"))

    def test_recover_7a(self , tmp_path : Path) -> None :
        """
        7a. Recover Uses Criteria.Match For Each Log Entry
        - Recover(criteria) harus membaca semua baris log dan memanggil criteria.match(...) untuk setiap entry.
        - Hasil return cukup dicek bertipe list (minimum contract).
        """
        log_path = tmp_path / "wal.jsonl"
        entries = [{"log_type" : "CHANGE" , "transaction_id" : 1 , "item_name" : "Employee.salary" , "old_value" : 10_000 , "new_value" : 20_000 , "active_transactions" : [1]} , {"log_type" : "CHANGE" , "transaction_id" : 2 , "item_name" : "Employee.age" , "old_value" : 20 , "new_value" : 21 , "active_transactions" : [2]} , {"log_type" : "CHANGE" , "transaction_id" : 1 , "item_name" : "Employee.bonus" , "old_value" : 0 , "new_value" : 10 , "active_transactions" : [1]}]
        log_path.parent.mkdir(parents = True , exist_ok = True)
        with (log_path.open("w" , encoding = "UTF-8")) as f :
            for (entry) in (entries) :
                f.write(json.dumps(entry , ensure_ascii = False) + "\n")
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 2)
        criteria_transaction1 = RecoverCriteria.from_transaction(1)
        result_transaction1 = manager.recover(criteria_transaction1)
        assert (isinstance(result_transaction1 , list))
        criteria_transaction2 = RecoverCriteria.from_transaction(2)
        result_transaction2 = manager.recover(criteria_transaction2)
        assert (isinstance(result_transaction2 , list))
        assert (len(result_transaction1) >= len(result_transaction2))
        assert (len(result_transaction1) == 2)
        assert (len(result_transaction2) == 1)

    def test_recover_7b(self , tmp_path : Path) -> None :
        """
        7b. Recover With Non Existing Transaction
        - Jika criteria transaction_id tidak ada di log, recover() tetap mengembalikan list tetapi isinya seharusnya kosong (tidak ada entry yang diproses).
        """
        log_path = tmp_path / "wal.jsonl"
        entries = [{"log_type" : "CHANGE" , "transaction_id" : 1 , "item_name" : "Employee.salary" , "old_value" : 10_000 , "new_value" : 20_000 , "active_transactions" : [1]} , {"log_type" : "CHANGE" , "transaction_id" : 2 , "item_name" : "Employee.age" , "old_value" : 20 , "new_value" : 21 , "active_transactions" : [2]} , {"log_type" : "CHANGE" , "transaction_id" : 1 , "item_name" : "Employee.bonus" , "old_value" : 0 , "new_value" : 10 , "active_transactions" : [1]}]
        log_path.parent.mkdir(parents = True , exist_ok = True)
        with (log_path.open("w" , encoding = "UTF-8")) as f :
            for (entry) in (entries) :
                f.write(json.dumps(entry , ensure_ascii = False) + "\n")
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 2)
        criteria_not_found = RecoverCriteria.from_transaction(3)
        result = manager.recover(criteria_not_found)
        assert (isinstance(result , list))
        assert (len(result) == 0)
    
    def test_recover_7c(self , tmp_path : Path) -> None :
        """
        7c. Recover Skips Entries Before Last Checkpoint
        - Setelah ada checkpoint, recover() hanya memproses log mulai dari last_checkpoint_line.
        - Entry sebelum checkpoint tidak boleh mempengaruhi hasil recover.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 4)
        before_entries = [LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.salary" , old_value = 10_000 , new_value = 20_000 , active_transactions = [1]) , LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 2 , item_name = "Employee.age" , old_value = 20 , new_value = 21 , active_transactions = [2]) , LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.bonus" , old_value = 0 , new_value = 10 , active_transactions = [1])]
        for (entry) in (before_entries) :
            manager.write_log(entry)
        manager._flush_buffer_to_disk()
        manager.save_checkpoint()
        meta = manager._read_meta()
        last_checkpoint_line = meta.get("last_checkpoint_line")
        assert (last_checkpoint_line is not None)
        assert (last_checkpoint_line >= 1)
        after_entries = [LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.salary.after_checkpoint" , old_value = 20_000 , new_value = 15_000 , active_transactions = [1]) , LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 3 , item_name = "Employee.level" , old_value = "junior" , new_value = "senior" , active_transactions = [3])]
        for (entry) in (after_entries) :
            manager.write_log(entry)
        manager._flush_buffer_to_disk()
        criteria_transaction1 = RecoverCriteria.from_transaction(1)
        result_transaction1 = manager.recover(criteria_transaction1)
        assert (isinstance(result_transaction1 , list))
        assert (len(result_transaction1) == 1)
        criteria_transaction2 = RecoverCriteria.from_transaction(2)
        result_transaction2 = manager.recover(criteria_transaction2)
        assert (isinstance(result_transaction2 , list))
        assert (len(result_transaction2) == 0)

    def test_recover_7d(self , tmp_path : Path) -> None :
        """
        7d. Recover Respects Active Transactions At Checkpoint
        - Transaction yang sudah commit sebelum checkpoint dianggap "committed" dan perubahannya di-skip.
        - Transaction yang masih aktif pada saat checkpoint akan di-undo / di-restore ketika recover() dijalankan.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 8)
        record_start_t1 = LogRecord(log_type = LogRecordType.START , transaction_id = 1 , item_name = None , old_value = None , new_value = None , active_transactions = [1])
        record_change_t1 = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.salary" , old_value = 10_000 , new_value = 20_000 , active_transactions = [1])
        record_commit_t1 = LogRecord(log_type = LogRecordType.COMMIT , transaction_id = 1 , item_name = None , old_value = None , new_value = None , active_transactions = [1])
        record_start_t2 = LogRecord(log_type = LogRecordType.START , transaction_id = 2 , item_name = None , old_value = None , new_value = None , active_transactions = [2])
        record_change_t2 = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 2 , item_name = "Employee.age" , old_value = 20 , new_value = 21 , active_transactions = [2])
        for (entry) in (record_start_t1 , record_change_t1 , record_commit_t1 , record_start_t2 , record_change_t2) :
            manager.write_log(entry)
        manager._flush_buffer_to_disk()
        manager.save_checkpoint()
        meta = manager._read_meta()
        active_at_checkpoint = set(meta.get("active_transactions_at_checkpoint" , []))
        assert (1 not in active_at_checkpoint)
        assert (2 in active_at_checkpoint)
        record_change_t1_after = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.salary" , old_value = 20_000 , new_value = 30_000 , active_transactions = [1])
        record_change_t2_after = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 2 , item_name = "Employee.age" , old_value = 21 , new_value = 22 , active_transactions = [2])
        manager.write_log(record_change_t1_after)
        manager.write_log(record_change_t2_after)
        manager._flush_buffer_to_disk()
        criteria_transaction1 = RecoverCriteria.from_transaction(1)
        result_transaction1 = manager.recover(criteria_transaction1)
        assert (isinstance(result_transaction1 , list))
        assert (len(result_transaction1) == 1)
        assert (result_transaction1[0].startswith("SKIP CHANGE for committed transaction 1"))
        criteria_transaction2 = RecoverCriteria.from_transaction(2)
        result_transaction2 = manager.recover(criteria_transaction2)
        assert (isinstance(result_transaction2 , list))
        assert (len(result_transaction2) == 1)
        assert ("RESTORE" in result_transaction2[0])
        assert ("(txn: 2)" in result_transaction2[0])

    def test_recover_7e(self , tmp_path : Path) -> None :
        """
        7e. Recover With Timestamp Criteria Cuts Off Older Entries
        - Dengan kriteria timestamp, recover() hanya memproses entry yang timestamp-nya >= cutoff dan berhenti ketika menemukan entry di bawah cutoff.
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 1)
        for (i) in range(1 , 6) :
            record = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = f"Employee.salary#{i}" , old_value = 10_000 + (i - 1) * 1_000 , new_value = 11_000 + (i - 1) * 1_000 , active_transactions = [1])
            manager.write_log(record)
        content_lines = log_path.read_text(encoding = "UTF-8").strip().splitlines()
        assert (len(content_lines) == 5)
        criteria_ts = RecoverCriteria.from_timestamp(3)
        result = manager.recover(criteria_ts)
        assert (isinstance(result , list))
        assert (len(result) == 3)
        assert (all(action.startswith("RESTORE") for action in result))

    def test_recover_7f(self , tmp_path : Path) -> None :
        """
        7f. Recover With Timestamp Criteria When Cutoff After All Entries
        - Jika cutoff timestamp lebih besar dari semua timestamp log, recover() tidak memproses entry apa pun (langsung berhenti di entry pertama).
        """
        log_path = tmp_path / "wal.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 1)
        for (i) in range(1 , 4) :
            record = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = f"Employee.bonus#{i}" , old_value = i * 100 , new_value = i * 200 , active_transactions = [1])
            manager.write_log(record)
        content_lines = log_path.read_text(encoding = "UTF-8").strip().splitlines()
        assert (len(content_lines) == 3)
        criteria_ts = RecoverCriteria.from_timestamp(10)
        result = manager.recover(criteria_ts)
        assert (isinstance(result , list))
        assert (len(result) == 0)

    def test_recover_7g(self , tmp_path : Path) -> None :
        """
        7g. Recover Handles DDL Create & Drop Table
        - Fungsi _undo_change() harus mengembalikan aksi yang sesuai untuk DDL, CREATE_TABLE -> DROP TABLE atau DROP_TABLE -> CREATE TABLE.
        """
        log_path = tmp_path / "wal_ddl.jsonl"
        manager = FailureRecoveryManager(log_path = log_path , buffer_max = 1)
        record_create = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "DDL.Employee" , old_value = None , new_value = {"operation" : "CREATE_TABLE" , "table" : "Employee"} , active_transactions = [1])
        old_schema = {"schema" : {"id" : "INT" , "name" : "TEXT"}}
        record_drop = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 2 , item_name = "DDL.Department" , old_value = old_schema , new_value = {"operation" : "DROP_TABLE" , "table" : "Department"} , active_transactions = [2])
        manager.write_log(record_create)
        manager.write_log(record_drop)
        criteria_transaction1 = RecoverCriteria.from_transaction(1)
        result_transaction1 = manager.recover(criteria_transaction1)
        assert (isinstance(result_transaction1 , list))
        assert (len(result_transaction1) == 1)
        assert (result_transaction1[0].startswith("DROP TABLE Employee"))
        assert ("(undo CREATE, txn: 1)" in result_transaction1[0])
        criteria_transaction2 = RecoverCriteria.from_transaction(2)
        result_transaction2 = manager.recover(criteria_transaction2)
        assert (isinstance(result_transaction2 , list))
        assert (len(result_transaction2) == 1)
        assert (result_transaction2[0].startswith("CREATE TABLE Department WITH SCHEMA"))
        assert ("(undo DROP, txn: 2)" in result_transaction2[0])