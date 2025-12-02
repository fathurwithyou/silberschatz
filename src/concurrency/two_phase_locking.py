import threading
from src.core.models.result import Rows
from src.core.concurrency_manager import IConcurrencyControlManager
from src.core.models.action import Action
from src.core.models.response import Response
from src.core.models.transaction_state import TransactionState
from .lock_type import LockType

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

    def log_object(self, table: str, transaction_id: int):
        with self.lock:
            if transaction_id not in self.active_transactions:
                return Response(False, transaction_id)

    def validate_object(self, table: str, transaction_id: int, action: Action) -> Response:
        with self.lock:
            if transaction_id not in self.active_transactions:
                return Response(False, transaction_id)
            
            tx = self.active_transactions[transaction_id]

            if tx["state"] == TransactionState.ABORTED:
                return Response(False, transaction_id)

            item_id = table

            if action == Action.READ:
                ok = self._acquire_shared_lock(transaction_id, item_id)
            else:
                ok = self._acquire_exclusive_lock(transaction_id, item_id)

            if not ok:
                self._drop_lock(transaction_id, item_id)

            return Response(ok, transaction_id)
    
    def end_transaction(self, tid):
        with self.lock: 
            if tid not in self.active_transactions:
                return Response(False, tid)

            tx = self.active_transactions[tid]
            allowed = True
            if tx["state"] == TransactionState.ABORTED:
                allowed = False
            else:
                tx["state"] = TransactionState.COMMITTED

            self._release_all_transaction_locks(tid)
            
            del self.active_transactions[tid]
            self._handle_queue()
            return Response(allowed, tid)

    def get_active_transactions(self) -> tuple[int, list[int]]:
        with self.lock:
            active_transactions = [
                tid for tid, tx in self.active_transactions.items()
                if tx["state"] == TransactionState.ACTIVE
            ]
        return len(active_transactions), active_transactions

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

    def _apply_wound_wait(self, requester_tid, item_id, holder_tid, mode):
        if self._is_older(requester_tid, holder_tid):
            self._abort_transaction(holder_tid)
            return True

        already_queued = False
        for entry in self.waiting_queue:
            if entry["transaction"] == requester_tid and entry["record_id"] == item_id:
                already_queued = True
                break
        
        if not already_queued:
            self.waiting_queue.append({
                "transaction": requester_tid,
                "record_id": item_id,
                "mode": mode
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
            return self._apply_wound_wait(tid, item_id, conflict, LockType.SHARED)

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
            return self._apply_wound_wait(tid, item_id, conflict, LockType.EXCLUSIVE)

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
        if not self.waiting_queue:
            return
        remaining_queue = []
        for entry in self.waiting_queue:
            tid = entry["transaction"]
            item_id = entry["record_id"]
            mode = entry["mode"]
            
            if tid not in self.active_transactions or self.active_transactions[tid]["state"] == TransactionState.ABORTED:
                continue

            success = False
            if mode == LockType.SHARED:
                success = self._acquire_shared_lock(tid, item_id)
            else:
                success = self._acquire_exclusive_lock(tid, item_id)
            
            if success:
                continue
            else:
                remaining_queue.append(entry)

        self.waiting_queue = remaining_queue

    def _generate_object_id(self, row: Rows) -> str:
        if hasattr(row, 'data') and row.data:
            first_row = row.data[0] if isinstance(row.data, list) and row.data else row.data

            if isinstance(first_row, dict):
                if 'id' in first_row:
                    return f"object_{first_row['id']}"
                return f"object_{hash(str(sorted(first_row.items())))}"
            
        return f"object_{hash(str(row))}"
