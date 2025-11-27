from typing import Optional
from src.core.models import Action


class AbortError(Exception):
    """
    Exception raised when a transaction needs to be aborted due to concurrency control.
    """
    
    def __init__(self, transaction_id: int, table_name: str, action: Action, message: Optional[str] = None):
        self.transaction_id = transaction_id
        self.table_name = table_name
        self.action = action
        
        if message is None:
            message = f"Transaction {transaction_id} aborted during {action} operation on {table_name}"
        
        super().__init__(message)
        
    def __str__(self):
        return f"AbortError(tx_id={self.transaction_id}, table={self.table_name}, action={self.action}): {super().__str__()}"