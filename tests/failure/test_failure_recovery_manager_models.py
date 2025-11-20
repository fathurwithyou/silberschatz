import sys , os , pytest
sys.path.insert(0 , os.path.abspath(os.path.join(os.path.dirname(__file__) , ".." , "..")))
from src.core.models.failure import LogRecordType , LogRecord , RecoverCriteria

class TestFailureRecoveryManagerModels :
    """
    Unit Tests src/core/models/failure.py :
    1. LogRecord
        1a. LogRecord With Basic Change
        1b. LogRecord With Basic Non-Change & Non-Checkpoint
        1c. LogRecord With Basic Change With No Active Transactions
        1d. LogRecord With Basic Checkpoint
    2. RecoverCriteria
        2a. RecoverCriteria From Timestamp Valid
        2b. RecoverCriteria From Timestamp Invalid Type
        2c. RecoverCriteria From Transaction Valid
        2d. RecoverCriteria From Transaction Invalid Type
        2e. RecoverCriteria Invalid Mode In Constructor
        2f. RecoverCriteria Flag Consistency
        2g. RecoverCriteria Match Timestamp Mode
        2h. RecoverCriteria Match Transaction Mode
    """

    def test_log_record_1a(self) -> None :
        """
        1a. LogRecord With Basic Change
        - LogRecordType CHANGE harus dapat memetakan field dengan benar.
        """
        record = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Employee.salary" , old_value = 10_000 , new_value = 20_000 , active_transactions = [1 , 2 , 3])
        data = record.to_dict()
        assert (data["log_type"] == "CHANGE")
        assert (data["transaction_id"] == 1)
        assert (data["item_name"] == "Employee.salary")
        assert (data["old_value"] == 10_000)
        assert (data["new_value"] == 20_000)
        assert (data["active_transactions"] == [1 , 2 , 3])

    def test_log_record_1b(self) -> None :
        """
        1b. LogRecord With Basic Non-Change
        - LogRecordType START/COMMIT/ABORT harus dapat memetakan field dengan benar dan mengizinkan item_name serta old_value dan new_value None.
        """
        record_start = LogRecord(
            log_type = LogRecordType.START ,
            transaction_id = 1 ,
            item_name = None ,
            old_value = None ,
            new_value = None ,
            active_transactions = [1]
        )
        data_start = record_start.to_dict()
        assert (data_start["log_type"] == "START")
        assert (data_start["transaction_id"] == 1)
        assert (data_start["item_name"] is None)
        assert (data_start["old_value"] is None)
        assert (data_start["new_value"] is None)
        assert (data_start["active_transactions"] == [1])
        record_commit = LogRecord(
            log_type = LogRecordType.COMMIT ,
            transaction_id = 1 ,
            item_name = None ,
            old_value = None ,
            new_value = None ,
            active_transactions = [1]
        )
        data_commit = record_commit.to_dict()
        assert (data_commit["log_type"] == "COMMIT")
        assert (data_commit["transaction_id"] == 1)
        assert (data_commit["item_name"] is None)
        assert (data_commit["old_value"] is None)
        assert (data_commit["new_value"] is None)
        assert (data_commit["active_transactions"] == [1])
        record_abort = LogRecord(
            log_type = LogRecordType.ABORT ,
            transaction_id = 1 ,
            item_name = None ,
            old_value = None ,
            new_value = None ,
            active_transactions = [1]
        )
        data_abort = record_abort.to_dict()
        assert (data_abort["log_type"] == "ABORT")
        assert (data_abort["transaction_id"] == 1)
        assert (data_abort["item_name"] is None)
        assert (data_abort["old_value"] is None)
        assert (data_abort["new_value"] is None)
        assert (data_abort["active_transactions"] == [1])

    def test_log_record_1c(self) -> None :
        """
        1c. LogRecord With No Active Transactions
        - LogRecordType CHANGE dengan active_transactions None harus dipetakan dengan benar.
        """
        record = LogRecord(log_type = LogRecordType.CHANGE , transaction_id = 1 , item_name = "Table.col" , old_value = 10_000 , new_value = 20_000 , active_transactions = None)
        data = record.to_dict()
        assert (data["log_type"] == "CHANGE")
        assert (data["active_transactions"] is None)
    
    def test_log_record_1d(self) :
        """
        1d. LogRecord With Basic Checkpoint
        - LogRecordType CHECKPOINT harus memetakan field dengan benar dan mengizinkan item_name serta old_value dan new_value None.
        """
        record = LogRecord(log_type = LogRecordType.CHECKPOINT , transaction_id = 1 , item_name = None , old_value = None , new_value = None , active_transactions = [10 , 20 , 30])
        data = record.to_dict()
        assert (data["log_type"] == "CHECKPOINT")
        assert (data["transaction_id"] == 1)
        assert (data["item_name"] is None)
        assert (data["old_value"] is None)
        assert (data["new_value"] is None)
        assert (data["active_transactions"] == [10 , 20 , 30])

    def test_recover_criteria_2a(self) -> None :
        """
        2a. RecoverCriteria From Timestamp Valid
        - RecoverCriteria.from_timestamp menerima tipe data numerik float dan set mode timestamp.
        """
        criteria = RecoverCriteria.from_timestamp(1.35)
        assert (criteria.is_timestamp)
        assert (not criteria.is_transaction)
        assert (isinstance(criteria.value , float))
        assert (criteria.value == pytest.approx(1.35))

    def test_recover_criteria_2b(self) -> None :
        """
        2b. RecoverCriteria From Timestamp Invalid Type
        - RecoverCriteria.from_timestamp harus raise TypeError untuk tipe non-numerik-float.
        """
        with (pytest.raises(TypeError)) :
            RecoverCriteria.from_timestamp("Timestamp Invalid Type")

    def test_recover_criteria_2c(self) -> None :
        """
        2c. RecoverCriteria From Transaction Valid
        - RecoverCriteria.from_transaction menerima tipe data numerik integer dan set mode transaction.
        """
        criteria = RecoverCriteria.from_transaction(135)
        assert (criteria.is_transaction)
        assert (not criteria.is_timestamp)
        assert (criteria.value == 135)

    def test_recover_criteria_2d(self) -> None :
        """
        2d. RecoverCriteria From Transaction Invalid Type
        - RecoverCriteria.from_transaction harus raise TypeError untuk tipe non-numerik-integer.
        """
        with (pytest.raises(TypeError)) :
            RecoverCriteria.from_transaction(1.35)
            RecoverCriteria.from_transaction("Transaction Invalid Type")

    def test_recover_criteria_2e(self) -> None :
        """
        2e. RecoverCriteria Invalid Mode In Constructor
        - RecoverCriteria constructor harus raise ValueError untuk mode selain "timestamp" atau "transaction_id".
        """
        with (pytest.raises(ValueError)) :
            RecoverCriteria("Invalid_Mode" , 1)

    def test_recover_criteria_2f(self) -> None :
        """
        2f. RecoverCriteria Flag Consistency
        - Value boolean is_timestamp dan is_transaction harus konsisten dengan mode yang dipilih.
        """
        criteria_timestamp = RecoverCriteria.from_timestamp(1.35)
        criteria_transaction = RecoverCriteria.from_transaction(135)
        assert (criteria_timestamp.is_timestamp is True)
        assert (criteria_timestamp.is_transaction is False)
        assert (criteria_transaction.is_transaction is True)
        assert (criteria_transaction.is_timestamp is False)

    @pytest.mark.parametrize("cutoff_ts , entry_ts , expected" , [(100.0 , 50.0 , False) , (100.0 , 100.0 , True) , (100.0 , 150.0 , True)])
    def test_recover_criteria_2g(self , cutoff_ts , entry_ts , expected) :
        """
        2g. RecoverCriteria Match Timestamp Mode
        - Fungsi match() True jika entry_ts >= cutoff_ts.
        """
        criteria = RecoverCriteria.from_timestamp(cutoff_ts)
        assert (criteria.is_timestamp)
        assert (criteria.match(entry_ts , 999) is expected)

    @pytest.mark.parametrize("target_txn , entry_txn , expected" , [(2 , 2 , True) , (2 , 1 , False) , (3 , 3 , True) , (3 , 4 , False)])
    def test_recover_criteria_2h(self , target_txn , entry_txn , expected) -> None :
        """
        2h. RecoverCriteria Match Transaction Mode
        - Fungsi match() True jika entry_txn == target_txn.
        """
        criteria = RecoverCriteria.from_transaction(target_txn)
        assert (criteria.is_transaction)
        assert (criteria.match(0.0 , entry_txn) is expected)