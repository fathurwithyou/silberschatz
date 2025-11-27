from enum import Enum

class TransactionState(Enum):
    ACTIVE = "Active"
    ABORTED = "Aborted"
    COMMITTED = "Committed"