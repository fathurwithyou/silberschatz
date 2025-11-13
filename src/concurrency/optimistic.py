from src.core.concurrency_manager import IConcurrencyControlManager
from dataclasses import dataclass, field
from typing import Dict, Set, List
from src.core.models.action import Action
from src.core.models.response import Response
from src.core.models.result import Rows
import time

# Untuk menyimpan timestamp setiap data objek
@dataclass
class OCCTransactionInfo:
    transaction_id: int
    start_timestamp: int
    finish_timestamp: int = float('inf')
    status: str = "active"
    read_set: Set[str] = field(default_factory=set)
    write_set: Set[str] = field(default_factory=set)

class OptimisticConcurrencyControl(IConcurrencyControlManager):
    def __init__(self):
        self._transaction_counter: int = 0
        self._logical_clock: int = 0        
        self._active_transactions: Dict[int, OCCTransactionInfo] = {}        
        self._committed_history: List[OCCTransactionInfo] = []

    def begin_transaction(self) -> int:
        self._transaction_counter += 1
        transaction_id = self._transaction_counter        
        start_ts = self._get_next_clock()
        transaction_info = OCCTransactionInfo(
            transaction_id=transaction_id,
            start_timestamp=start_ts
        )
        self._active_transactions[transaction_id] = transaction_info
        return transaction_id

    def end_transaction(self, transaction_id: int) -> None:
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found!")
        transaction = self._active_transactions[transaction_id]
        if transaction.status != "active":
            return

        is_valid = self._validate(transaction)

        if is_valid: #commit
            transaction.finish_timestamp = self._get_next_clock()
            transaction.status = "committed"            
            self._committed_history.append(transaction)            
            del self._active_transactions[transaction_id]
            print(f"Transaction {transaction_id} COMMITTED successfully.")
        else: #abort
            transaction.status = "aborted"
            print(f"Transaction {transaction_id} ABORTED due to conflict.")

    def log_object(self, row: Rows, transaction_id: int) -> None:
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found!")        
        transaction = self._active_transactions[transaction_id]
        if transaction.status != "active":
            return
        object_id = self._generate_object_id(row)
        transaction.read_set.add(object_id)

    def validate_object(self, row: Rows, transaction_id: int, action: Action) -> Response:
        if transaction_id not in self._active_transactions:
            return Response(allowed=False, transaction_id=transaction_id)
        transaction = self._active_transactions[transaction_id]
        if transaction.status == "aborted":
            return Response(allowed=False, transaction_id=transaction_id)
        object_id = self._generate_object_id(row)
        if action == Action.READ:
            transaction.read_set.add(object_id)
        elif action == Action.WRITE:
            transaction.write_set.add(object_id)
        return Response(allowed=True, transaction_id=transaction_id)

    # Helper
    def _validate(self, transaction: OCCTransactionInfo) -> bool:
        for committed_txn in self._committed_history:
            if committed_txn.finish_timestamp > transaction.start_timestamp:
                common_objects = committed_txn.write_set.intersection(transaction.read_set)
                if common_objects:
                    return False        
        return True

    def _get_next_clock(self) -> int:
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
        return f"object_unknown"

if __name__ == "__main__":
    print("Optimistic Concurrency Control Module Loaded")