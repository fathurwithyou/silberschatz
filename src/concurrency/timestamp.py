from src.core.concurrency_manager import IConcurrencyControlManager
from dataclasses import dataclass
from typing import Dict, Set
from src.core.models.action import Action
from src.core.models.response import Response
from src.core.models.result import Rows
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

    def log_object(self, row: Rows, transaction_id: int) -> None:
        if transaction_id not in self._transactions:
            raise ValueError(f"Transaction {transaction_id} not found!")
        
        object_id = self._generate_object_id(row)

        transaction = self._transactions[transaction_id]
        transaction.accessed_objects.add(object_id)

        if object_id not in self._object_timestamps:
            self._object_timestamps[object_id] = ObjectTimestamp()


    def validate_object(self, row: Rows, transaction_id: int, action: Action) -> Response:
        if transaction_id not in self._transactions:
            return Response(allowed = False, transaction_id = transaction_id)
        
        transaction = self._transactions[transaction_id]

        if transaction.status == TransactionState.ABORTED:
            return Response(allowed = False, transaction_id = transaction_id)
        
        object_id = self._generate_object_id(row)

        if object_id not in self._object_timestamps:
            self._object_timestamps[object_id] = ObjectTimestamp()

        obj_ts = self._object_timestamps[object_id]
        ts_transaction = transaction.timestamp

        if action == Action.READ:
            if ts_transaction < obj_ts.write_timestamp:
                self._abort_transaction(transaction_id)
                return Response(allowed = False, transaction_id = transaction_id)
            
            obj_ts.read_timestamp = max(obj_ts.read_timestamp, ts_transaction)
            obj_ts.readers.add(transaction_id)
            return Response(allowed = True, transaction_id = transaction_id)
            
        elif action == Action.WRITE:
            if ts_transaction < obj_ts.read_timestamp:
                self._abort_transaction(transaction_id)
                return Response(allowed = False, transaction_id = transaction_id)
            
            if ts_transaction < obj_ts.write_timestamp:
                self._abort_transaction(transaction_id)
                return Response(allowed = False, transaction_id = transaction_id)
            
            obj_ts.write_timestamp = ts_transaction
            return Response(allowed = True, transaction_id = transaction_id)
        
        return Response(allowed = False, transaction_id = transaction_id)
    
    # Helper

    def _get_next_timestamp(self) -> float:
        self._logical_clock += 1
        return self._logical_clock
    
    def _generate_object_id(self, row: Rows) -> str:
        if hasattr(row, 'data') and row.data:
            first_row = row.data[0] if isinstance(row.data, list) and row.data else row.data

            if isinstance(first_row, dict):
                if 'id' in first_row:
                    return f"object_{first_row['id']}"
                return f"object_{hash(str(sorted(first_row.items())))}"
            
        return f"object_{hash(str(row))}"
        
    def _abort_transaction(self, transaction_id: int) -> None:
        if transaction_id in self._transactions:
            self._transactions[transaction_id].status = TransactionState.ABORTED