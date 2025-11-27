from src.core.concurrency_manager import IConcurrencyControlManager
from src.core.models.action import Action
from src.core.models.response import Response
from src.core.models.result import Rows
from src.core.models.transaction_state import TransactionState
from .lock_type import LockType
import threading
class TwoPhaseLocking(IConcurrencyControlManager):
    def __init__(self):
        self.lock_table = {}    
        self.active_transactions = {}  
        self.transaction_counter = 0
        self.lock = threading.Lock()
        self.waiting_queue = []
        self.transaction_timestamps = []

    def begin_transaction(self) -> int:
        with self.lock:
            self.transaction_counter += 1
            tid = self.transaction_counter

            self.active_transactions[tid] = {
                "state": TransactionState.ACTIVE,
                "locked_items": set()
            }
            self.transaction_timestamps.append(tid)
        return tid

    def log_object(self, row: Rows, transaction_id: int):
        with self.lock:
            if transaction_id not in self.active_transactions:
                return Response(False, transaction_id)

    def validate_object(self, row: Rows, transaction_id: int, action: Action) -> Response:
        with self.lock:
            if transaction_id not in self.active_transactions:
                return Response(False, transaction_id)

            tx = self.active_transactions[transaction_id]

            if tx["state"] == TransactionState.ABORTED:
                return Response(False, transaction_id)

            all_success = True
            acquired = []

            for item in row.data:
                item_id = hash(item)

                if action == Action.READ:
                    ok = self._acquire_shared_lock(transaction_id, item_id)
                else:
                    ok = self._acquire_exclusive_lock(transaction_id, item_id)

                if not ok:
                    all_success = False
                    break
                acquired.append(item_id)

            if not all_success:
                for item_id in acquired:
                    self._drop_lock(transaction_id, item_id)

            return Response(all_success, transaction_id)
    
    def end_transaction(self, tid):
        with self.lock:
            if tid not in self.active_transactions:
                return Response(False, tid)

        tx = self.active_transactions[tid]

        self._handle_queue()

        if tx["state"] == TransactionState.ABORTED:
            print(f"Transaction {tid} aborted before commit.")
        else:
            tx["state"] = TransactionState.COMMITTED

        with self.lock:
            self._release_all_transaction_locks(tid)
            del self.active_transactions[tid]

    # wound-wait    
    def _is_older(self, t1, t2):
        return self.transaction_timestamps.index(t1) < self.transaction_timestamps.index(t2)

    def _has_conflict(self, requesting_tid, item_id, mode):
        if item_id not in self.lock_table:
            return None

        lock_info = self.lock_table[item_id]
        lock_type = lock_info["type"]

        if lock_type == LockType.EXCLUSIVE:
            holder = lock_info["holders"]
            if holder != requesting_tid:
                return holder
        if mode == LockType.EXCLUSIVE and lock_type == LockType.SHARED:
            holders = lock_info["holders"]
            if not (holders == {requesting_tid}):
                return list(holders)[0]

        return None

    def _apply_wound_wait(self, requester_tid, item_id, holder_tid):
        if self._is_older(requester_tid, holder_tid):
            self._abort_transaction(holder_tid)
            return True

        self.waiting_queue.append({
            "transaction": requester_tid,
            "record_id": item_id
        })
        return False

    def _abort_transaction(self, tid):
        if tid not in self.active_transactions:
            return

        tx = self.active_transactions[tid]
        tx["state"] = TransactionState.ABORTED

        self._release_all_transaction_locks(tid)

        self.waiting_queue = [
            w for w in self.waiting_queue if w["transaction"] != tid
        ]

        return Response(False, tid)

    def _acquire_shared_lock(self, tid, item_id):
        conflict = self._has_conflict(tid, item_id, LockType.SHARED)

        if conflict:
            return self._apply_wound_wait(tid, item_id, conflict)

        if item_id in self.lock_table:
            lock_info = self.lock_table[item_id]
            if lock_info["type"] == LockType.EXCLUSIVE and lock_info["holders"] == {tid}:
                return True

            if lock_info["type"] == LockType.SHARED:
                lock_info["holders"].add(tid)
                self.active_transactions[tid]["locked_items"].add(item_id)
                return True

        self.lock_table[item_id] = {
            "type": LockType.SHARED,
            "holders": {tid}
        }
        self.active_transactions[tid]["locked_items"].add(item_id)
        return True

    def _acquire_exclusive_lock(self, tid, item_id):
        conflict = self._has_conflict(tid, item_id, LockType.EXCLUSIVE)

        if conflict:
            return self._apply_wound_wait(tid, item_id, conflict)

        if item_id in self.lock_table:
            lock_info = self.lock_table[item_id]
            if lock_info["type"] == LockType.EXCLUSIVE and lock_info["holders"] == tid:
                return True

            if lock_info["type"] == LockType.SHARED and lock_info["holders"] == {tid}:
                self.lock_table[item_id] = {
                    "type": LockType.EXCLUSIVE,
                    "holders": tid
                }
                return True

        self.lock_table[item_id] = {
            "type": LockType.EXCLUSIVE,
            "holders": tid
        }
        self.active_transactions[tid]["locked_items"].add(item_id)

        return True

    def _drop_lock(self, tid, item_id):
        if item_id not in self.lock_table:
            return

        lock_info = self.lock_table[item_id]

        if lock_info["type"] == LockType.SHARED:
            lock_info["holders"].discard(tid)
            if not lock_info["holders"]:
                del self.lock_table[item_id]
        elif lock_info["type"] == LockType.EXCLUSIVE:
            if lock_info["holders"] == tid:
                del self.lock_table[item_id]

    def _release_all_transaction_locks(self, tid):
        for item_id in list(self.lock_table.keys()):
            lock_info = self.lock_table[item_id]

            if lock_info["type"] == LockType.EXCLUSIVE and lock_info["holders"] == tid:
                del self.lock_table[item_id]
            elif lock_info["type"] == LockType.SHARED and tid in lock_info["holders"]:
                lock_info["holders"].discard(tid)
                if not lock_info["holders"]:
                    del self.lock_table[item_id]

    def _handle_queue(self):
        new_queue = []
        for entry in self.waiting_queue:
            item = entry["record_id"]
            if item not in self.lock_table:
                continue
            new_queue.append(entry)

        self.waiting_queue = new_queue