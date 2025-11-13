from typing import List, Optional
from src.core.concurrency_manager import IConcurrencyControlManager
from .two_phase_locking import TwoPhaseLocking
from .timestamp import TimestampBasedConcurrencyControl
from .optimistic import OptimisticConcurrencyControl
from .snapshot import SnapshotIsolation
from src.core.models.query import Rows
from src.core.models.action import Action
from src.core.models.response import Response

class ConcurrencyControlManager(IConcurrencyControlManager):
    '''
    Algorithm available: 
    - '2PL' (Two-Phase Locking)
    - 'Timestamp' (Timestamp-Based Concurrency Control)
    - 'OCC' (Optimistic Concurrency Control)
    - 'Snapshot' (Snapshot Isolation Multi-Version Concurrency Control)
    '''
    def __init__(self,
                 algorithm: str = 'Timestamp'):
        match algorithm:
            case '2PL':
                self._cc_manager = TwoPhaseLocking()
            case 'Timestamp':
                self._cc_manager = TimestampBasedConcurrencyControl()
            case 'OCC':
                self._cc_manager = OptimisticConcurrencyControl()
            case 'Snapshot':
                self._cc_manager = SnapshotIsolation()
            case _:
                raise ValueError(f"Unsupported concurrency control algorithm: {algorithm}")
    
    def switch_algorithm(self, 
                         algorithm: str):
        match algorithm:
            case '2PL':
                self._cc_manager = TwoPhaseLocking()
            case 'Timestamp':
                self._cc_manager = TimestampBasedConcurrencyControl()
            case 'OCC':
                self._cc_manager = OptimisticConcurrencyControl()
            case 'Snapshot':
                self._cc_manager = SnapshotIsolation()
            case _:
                raise ValueError(f"Unsupported concurrency control algorithm: {algorithm}")
        print(f"Switched to {algorithm} concurrency control.")

    def begin_transaction(self) -> int:
        return self.cc_manager.begin_transaction()

    def log_object(self, row: Rows, transaction_id: int) -> None:
        self.cc_manager.log_object(row, transaction_id)

    def validate_object(self, row: Rows, transaction_id: int, action: Action) -> Response:
        return self.cc_manager.validate_object(row, transaction_id, action)

    def end_transaction(self, transaction_id: int):
        self.cc_manager.end_transaction(transaction_id)
