from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
import threading

from src.core.concurrency_manager import IConcurrencyControlManager
from src.core.models.action import Action
from src.core.models.response import Response
from src.core.models.result import Rows
from src.core.models.transaction_state import TransactionState


@dataclass
class VersionEntry:
    data: Any
    commit_ts: int
    created_by: int


@dataclass
class SnapshotTransaction:
    transaction_id: int
    start_ts: int
    state: TransactionState = TransactionState.ACTIVE
    read_set: Set[str] = field(default_factory=set)
    write_set: Set[str] = field(default_factory=set)
    local_writes: Dict[str, Any] = field(default_factory=dict)


class SnapshotIsolation(IConcurrencyControlManager):
    """
    Multi-Version Concurrency Control with Snapshot Isolation semantics.
    - Readers never block writers and vice versa.
    - Commit-time write locks (sorted acquisition) implement first-committer-wins
      and avoid deadlocks.
    """

    def __init__(self):
        self._tx_counter: int = 0
        self._logical_clock: int = 0
        self._transactions: Dict[int, SnapshotTransaction] = {}
        self._versions: Dict[str, List[VersionEntry]] = {}

        self._lock_table: Dict[str, int] = {}
        self._mutex = threading.Lock()
        self._lock_cv = threading.Condition(self._mutex)

    def begin_transaction(self) -> int:
        with self._mutex:
            self._tx_counter += 1
            tid = self._tx_counter
            start_ts = self._next_timestamp()
            self._transactions[tid] = SnapshotTransaction(
                transaction_id=tid,
                start_ts=start_ts,
            )
            return tid

    def end_transaction(self, transaction_id: int) -> Response:
        with self._mutex:
            tx = self._transactions.get(transaction_id)
            if not tx:
                return Response(False, transaction_id)

            if tx.state == TransactionState.ABORTED:
                self._cleanup_transaction(transaction_id)
                return Response(False, transaction_id)

        row_ids = sorted(tx.write_set)

        # Commit-time locks in a global order prevent cycles.
        self._acquire_write_locks(transaction_id, row_ids)
        try:
            if self._has_write_conflict(tx):
                self._abort_transaction(transaction_id)
                return Response(False, transaction_id)

            with self._mutex:
                commit_ts = self._next_timestamp()
            with self._mutex:
                for row_id in row_ids:
                    data = tx.local_writes.get(row_id)
                    entry = VersionEntry(data=data, commit_ts=commit_ts, created_by=transaction_id)
                    self._versions.setdefault(row_id, []).append(entry)

                tx.state = TransactionState.COMMITTED
                self._transactions.pop(transaction_id, None)

            self._garbage_collect()
            return Response(True, transaction_id)
        finally:
            self._release_write_locks(transaction_id, row_ids)

    def log_object(self, row: Rows, transaction_id: int):
        entries = self._extract_row_entries(row)
        with self._mutex:
            tx = self._transactions.get(transaction_id)
            if not tx:
                return Response(False, transaction_id)

            for row_id, record in entries:
                if row_id not in self._versions:
                    # Bootstrap an initial committed version at timestamp 0.
                    self._versions[row_id] = [VersionEntry(data=record, commit_ts=0, created_by=0)]
        return Response(True, transaction_id)

    def validate_object(self, row: Rows, transaction_id: int, action: Action) -> Response:
        entries = self._extract_row_entries(row)

        with self._mutex:
            tx = self._transactions.get(transaction_id)
            if not tx or tx.state == TransactionState.ABORTED:
                return Response(False, transaction_id)

            if action == Action.READ:
                for row_id, _ in entries:
                    tx.read_set.add(row_id)
                    # Ensure a visible version exists; readers do not take locks.
                    self._ensure_visible_version(row_id, tx)
                return Response(True, transaction_id)

            if action == Action.WRITE:
                for row_id, record in entries:
                    tx.write_set.add(row_id)
                    tx.local_writes[row_id] = record
                    self._ensure_visible_version(row_id, tx)
                return Response(True, transaction_id)

        return Response(False, transaction_id)

    # Internal helpers

    def _next_timestamp(self) -> int:
        self._logical_clock += 1
        return self._logical_clock

    def _extract_row_entries(self, rows: Rows) -> List[Tuple[str, Any]]:
        entries: List[Tuple[str, Any]] = []
        if hasattr(rows, "data") and rows.data:
            for item in rows.data:
                entries.append((self._generate_object_id(item), item))
        return entries

    def _generate_object_id(self, record: Any) -> str:
        if isinstance(record, dict):
            if "id" in record:
                return f"object_{record['id']}"
            return f"object_{hash(str(sorted(record.items())))}"
        return f"object_{hash(str(record))}"

    def _ensure_visible_version(self, row_id: str, tx: SnapshotTransaction) -> Optional[VersionEntry]:
        """
        Return the latest committed version visible to the transaction.
        """
        versions = self._versions.get(row_id, [])
        if not versions:
            placeholder = VersionEntry(data=None, commit_ts=0, created_by=0)
            self._versions[row_id] = [placeholder]
            versions = self._versions[row_id]
        # Prioritize the transaction's own uncommitted write.
        if row_id in tx.local_writes:
            return VersionEntry(data=tx.local_writes[row_id], commit_ts=tx.start_ts, created_by=tx.transaction_id)

        # Find the newest version with commit_ts <= start_ts.
        visible = None
        for version in reversed(versions):
            if version.commit_ts <= tx.start_ts:
                visible = version
                break

        if not visible and versions:
            visible = versions[0]
        return visible

    def _has_write_conflict(self, tx: SnapshotTransaction) -> bool:
        with self._mutex:
            for row_id in tx.write_set:
                latest = self._get_latest_committed_version(row_id)
                if latest and latest.commit_ts > tx.start_ts and latest.created_by != tx.transaction_id:
                    return True
        return False

    def _get_latest_committed_version(self, row_id: str) -> Optional[VersionEntry]:
        versions = self._versions.get(row_id)
        if not versions:
            return None
        return versions[-1]

    def _abort_transaction(self, transaction_id: int) -> None:
        with self._mutex:
            tx = self._transactions.get(transaction_id)
            if not tx:
                return
            tx.state = TransactionState.ABORTED
            self._transactions.pop(transaction_id, None)
        self._garbage_collect()

    def _cleanup_transaction(self, transaction_id: int) -> None:
        with self._mutex:
            self._transactions.pop(transaction_id, None)
        self._garbage_collect()

    def _acquire_write_locks(self, transaction_id: int, row_ids: List[str]) -> None:
        with self._lock_cv:
            for row_id in row_ids:
                while row_id in self._lock_table and self._lock_table[row_id] != transaction_id:
                    self._lock_cv.wait()
                self._lock_table[row_id] = transaction_id

    def _release_write_locks(self, transaction_id: int, row_ids: List[str]) -> None:
        with self._lock_cv:
            for row_id in row_ids:
                if self._lock_table.get(row_id) == transaction_id:
                    del self._lock_table[row_id]
            self._lock_cv.notify_all()

    def _garbage_collect(self) -> None:
        with self._mutex:
            active_starts = [
                tx.start_ts for tx in self._transactions.values()
                if tx.state == TransactionState.ACTIVE
            ]
            min_active_start = min(active_starts) if active_starts else float("inf")

            for row_id, versions in list(self._versions.items()):
                versions.sort(key=lambda v: v.commit_ts)
                # Drop obsolete versions; keep at least the newest version.
                while len(versions) > 1 and versions[1].commit_ts <= min_active_start:
                    versions.pop(0)
                if not versions:
                    del self._versions[row_id]


if __name__ == "__main__":
    print("Snapshot Isolation Module")
