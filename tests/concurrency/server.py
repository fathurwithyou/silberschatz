import os
import socket
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.concurrency import ConcurrencyControlManager
from src.core.models import Action


@dataclass
class Operation:
    """
    A single operation in the schedule.
    """
    action: str
    tid: int
    data: Optional[int] = None


@dataclass
class TransactionStream:
    """
    Tracks operations in the order they appear so we can apply concurrency control fairly.
    """
    ordered_ops: List[Operation]

    @classmethod
    def from_lines(cls, lines: List[str]) -> "TransactionStream":
        ops: List[Operation] = []
        for raw in lines:
            line = raw.strip()
            if not line:
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) == 3:
                action, tid, data = parts
                ops.append(Operation(action=action, tid=int(tid), data=int(data)))
            elif len(parts) == 2:
                action, tid = parts
                ops.append(Operation(action=action, tid=int(tid)))
            else:
                raise ValueError(f"Invalid transaction line: {line}")
        return cls(ordered_ops=ops)


def _table_name(item: int) -> str:
    # Use a stable identifier for the accessed table.
    return f"table_{item}"


def apply_concurrency_control(stream: TransactionStream, algorithm: str = "Snapshot") -> List[Tuple[int, str, bool]]:
    """
    Apply the selected concurrency control algorithm over the provided schedule.
    Returns a log of (tid, operation, allowed) for assertions.
    """
    ccm = ConcurrencyControlManager(algorithm=algorithm)
    active: Dict[int, int] = {}
    results: List[Tuple[int, str, bool]] = []

    for op in stream.ordered_ops:
        if op.action.upper() == "C":
            tx = active.pop(op.tid, None)
            if tx is None:
                results.append((op.tid, "COMMIT", False))
                continue
            response = ccm.end_transaction(tx)
            results.append((op.tid, "COMMIT", bool(response.allowed)))
            continue

        if op.tid not in active:
            active[op.tid] = ccm.begin_transaction()

        tx_id = active[op.tid]
        table = _table_name(op.data if op.data is not None else op.tid)
        ccm.log_object(table, tx_id)

        action = Action.READ if op.action.upper() == "R" else Action.WRITE
        response = ccm.validate_object(table, tx_id, action)
        results.append((op.tid, op.action.upper(), response.allowed))

        if not response.allowed:
            ccm.end_transaction(tx_id)
            active.pop(op.tid, None)

    # Abort any lingering active tx to clean state.
    for tx in active.values():
        ccm.end_transaction(tx)

    return results


def serve():
    """
    Lightweight server to drive CC manually.
    Send newline-separated operations like 'R,1,10' over the socket.
    """
    host: str = "127.0.0.1"
    port: int = 5000

    server_socket = socket.socket()
    server_socket.bind((host, port))
    server_socket.listen(1)
    print("Server is listening at port", port)

    conn, address = server_socket.accept()
    print(f"Connection from: {address}")

    while True:
        data = conn.recv(4096).decode()
        print(f"Received data:\n{data}")

        stream = TransactionStream.from_lines(data.splitlines())
        results = apply_concurrency_control(stream, algorithm="Snapshot")
        for tid, action, allowed in results:
            print(f"T{tid} {action}: {'ALLOWED' if allowed else 'ABORTED'}")

if __name__ == "__main__":
    serve()
