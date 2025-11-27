from dataclasses import dataclass
from typing import Dict, Set
from src.core.concurrency_manager import IConcurrencyControlManager
from src.core.models.action import Action
from src.core.models.response import Response
from src.core.models.transaction_state import TransactionState

# Untuk menyimpan timestamp setiap data objek
@dataclass
class ObjectTimestamp:
    read_timestamp: float = 0.0
    write_timestamp: float = 0.0
    readers: Set[int] = None
    
    def __post_init__(self):
        if self.readers is None:
            self.readers = set()

# Untuk menyimpan informasi transaksi
@dataclass
class TransactionInfo:
    transaction_id: int
    timestamp: float
    status: str = TransactionState.ACTIVE
    accessed_objects: Set[str] = None

    def __post_init__(self):
        if self.accessed_objects == None:
            self.accessed_objects = set()

class TimestampBasedConcurrencyControl(IConcurrencyControlManager):
    def __init__(self):
        self._transaction_counter: int = 0
        self._transactions: Dict[int, TransactionInfo] = {}         # map transaction_id ke transaction info
        self._object_timestamps: Dict[str, ObjectTimestamp] = {}    # map object_id ke object timestamp
        self._logical_clock: float = 0.0

    def begin_transaction(self) -> int:
        self._transaction_counter += 1
        transaction_id = self._transaction_counter

        timestamp = self._get_next_timestamp()

        transaction_info = TransactionInfo(
            transaction_id = transaction_id,
            timestamp = timestamp,
            status = TransactionState.ACTIVE
        )

        self._transactions[transaction_id] = transaction_info

        return transaction_id

    def end_transaction(self, transaction_id: int) -> Response:
        if transaction_id not in self._transactions:
            return Response(allowed=False, transaction_id=transaction_id)
        
        transaction = self._transactions[transaction_id]

        if transaction.status == TransactionState.ACTIVE:
            transaction.status = TransactionState.COMMITTED

        del self._transactions[transaction_id]
        return Response(allowed=True, transaction_id=transaction_id)

    def log_object(self, table: str, transaction_id: int) -> None:
        if transaction_id not in self._transactions:
            raise ValueError(f"Transaction {transaction_id} not found!")

        object_id = table

        transaction = self._transactions[transaction_id]
        transaction.accessed_objects.add(object_id)

        if object_id not in self._object_timestamps:
            self._object_timestamps[object_id] = ObjectTimestamp()


    def validate_object(self, table: str, transaction_id: int, action: Action) -> Response:
        if transaction_id not in self._transactions:
            return Response(allowed = False, transaction_id = transaction_id)
        
        transaction = self._transactions[transaction_id]

        if transaction.status == TransactionState.ABORTED:
            return Response(allowed = False, transaction_id = transaction_id)
        object_id = table

        if object_id not in self._object_timestamps:
            self._object_timestamps[object_id] = ObjectTimestamp()

        obj_ts = self._object_timestamps[object_id]
        ts_transaction = transaction.timestamp

        if action == Action.READ:
            if ts_transaction >= obj_ts.write_timestamp:
                obj_ts.read_timestamp = max(obj_ts.read_timestamp, ts_transaction)
                obj_ts.readers.add(transaction_id)
                return Response(allowed = True, transaction_id = transaction_id)
            else:
                self._abort_transaction(transaction_id)
                return Response(allowed = False, transaction_id = transaction_id)
            
        elif action == Action.WRITE:
            if ts_transaction < obj_ts.read_timestamp:
                self._abort_transaction(transaction_id)
                return Response(allowed = False, transaction_id = transaction_id)            
            elif ts_transaction >= obj_ts.write_timestamp:
                obj_ts.write_timestamp = ts_transaction
                return Response(allowed = True, transaction_id = transaction_id)
            else:
                if transaction_id not in obj_ts.readers:
                    return Response(allowed = True, transaction_id = transaction_id)
                else:
                    self._abort_transaction(transaction_id)
                    return Response(allowed = False, transaction_id = transaction_id)
        
        return Response(allowed = False, transaction_id = transaction_id)

    def get_active_transactions(self) -> tuple[int, list[int]]:
        active_transactions = [
            tid for tid, tx in self._transactions.items()
            if tx.status == TransactionState.ACTIVE
        ]
        return len(active_transactions), active_transactions
    
    # Helper

    def _get_next_timestamp(self) -> float:
        self._logical_clock += 1
        return self._logical_clock
    
    def _abort_transaction(self, transaction_id: int) -> None:
        if transaction_id in self._transactions:
            self._transactions[transaction_id].status = TransactionState.ABORTED
    
