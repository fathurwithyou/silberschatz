from enum import Enum, auto
from typing import Any, Dict, List , Literal
from typing import Union

class LogRecordType(Enum):
    START = auto() # Begin Transaction
    COMMIT = auto() 
    ABORT = auto()
    CHANGE = auto() # Update, Insert, Delete, Create/Drop Table
    CHECKPOINT = auto()

class LogRecord:
    def __init__(
        self,
        log_type: LogRecordType,
        transaction_id: int,
        item_name: Union[str, None],
        old_value: Union[Any, None],
        new_value: Union[Any, None],
        active_transactions: Union[List[int], None]
    ):
        self.log_type = log_type
        self.transaction_id = transaction_id
        self.item_name = item_name
        self.old_value = old_value
        self.new_value = new_value
        self.active_transactions = active_transactions

    def to_dict(self) -> Dict[str, Any]:
        return {
            "log_type": self.log_type.name,
            "transaction_id": self.transaction_id,
            "item_name": self.item_name,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "active_transactions": self.active_transactions
        }
    
class RecoverCriteria :
    """
    - timestamp (undo semua entry dengan ts >= cutoff)
    - transaction_id (undo semua entry milik txn tersebut)
    """
    __slots__ = ("_mode", "_value")

    def __init__(self , mode : Literal["timestamp", "transaction_id"] , value : Any) -> None :
        if (mode not in ("timestamp" , "transaction_id")) :
            raise ValueError("mode must be 'timestamp' or 'transaction_id'")
        self._mode = mode
        self._value = value

    @staticmethod
    def from_timestamp(epoch_seconds : float) -> "RecoverCriteria" :
        if (not isinstance(epoch_seconds , (int , float))) :
            raise TypeError("timestamp must be int or float")
        return RecoverCriteria("timestamp" , float(epoch_seconds))

    @staticmethod
    def from_transaction(transaction_id : int) -> "RecoverCriteria" :
        if (not isinstance(transaction_id , int)) :
            raise TypeError("transaction_id must be an int")
        return RecoverCriteria("transaction_id" , transaction_id)

    @property
    def is_timestamp(self) -> bool :
        return self._mode == "timestamp"

    @property
    def is_transaction(self) -> bool :
        return self._mode == "transaction_id"

    @property
    def value(self) -> Any :
        return self._value

    def match(self , entry_ts : float , entry_txn : int) -> bool :
        """Utility untuk dipakai recover(): ts>=cutoff atau txn==id"""
        if self.is_timestamp :
            return float(entry_ts) >= float(self._value)
        return int(entry_txn) == int(self._value)