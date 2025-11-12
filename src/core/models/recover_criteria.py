from typing import Any , Literal

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