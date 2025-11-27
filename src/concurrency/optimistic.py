from dataclasses import dataclass, field
from typing import Dict, Set, List
from src.core.concurrency_manager import IConcurrencyControlManager
from src.core.models.action import Action
from src.core.models.response import Response
from src.core.models.transaction_state import TransactionState

# Untuk menyimpan timestamp setiap data objek
@dataclass
class OCCTransactionInfo:
    transaction_id: int
    start_timestamp: int
    finish_timestamp: int = float('inf')
    status: str = TransactionState.ACTIVE
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

    def end_transaction(self, transaction_id: int) -> Response:
        if transaction_id not in self._active_transactions:
            return Response(allowed=False, transaction_id=transaction_id)
        transaction = self._active_transactions[transaction_id]
        if transaction.status != TransactionState.ACTIVE:
            return Response(allowed=False, transaction_id=transaction_id)

        is_valid = self._validate(transaction)

        if is_valid: #commit
            transaction.finish_timestamp = self._get_next_clock()
            transaction.status = TransactionState.COMMITTED           
            self._committed_history.append(transaction)            
            del self._active_transactions[transaction_id]
            print(f"Transaction {transaction_id} COMMITTED successfully.")
            return Response(allowed=True, transaction_id=transaction_id)
        else: #abort
            transaction.status = TransactionState.ABORTED
            print(f"Transaction {transaction_id} ABORTED due to conflict.")
            return Response(allowed=False, transaction_id=transaction_id)

    def log_object(self, table: str, transaction_id: int) -> None:
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found!")        
        transaction = self._active_transactions[transaction_id]
        if transaction.status != TransactionState.ACTIVE:
            return
        transaction.read_set.add(table)

    def validate_object(self, table: str, transaction_id: int, action: Action) -> Response:
        if transaction_id not in self._active_transactions:
            return Response(allowed=False, transaction_id=transaction_id)
        transaction = self._active_transactions[transaction_id]
        if transaction.status == TransactionState.ABORTED:
            return Response(allowed=False, transaction_id=transaction_id)
        if action == Action.READ:
            transaction.read_set.add(table)
        elif action == Action.WRITE:
            transaction.write_set.add(table)
        return Response(allowed=True, transaction_id=transaction_id)

    def get_active_transactions(self) -> tuple[int, list[int]]:
        active_transactions = [
            tid for tid, tx in self._active_transactions.items()
            if tx.status == TransactionState.ACTIVE
        ]
        return len(active_transactions), active_transactions

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

if __name__ == "__main__":
    print("Optimistic Concurrency Control Module Loaded")
