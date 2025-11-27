from typing import TYPE_CHECKING
from src.core.models import ExecutionResult, ParsedQuery, QueryNodeType, LogRecord, LogRecordType
from datetime import datetime

if TYPE_CHECKING:
    from ..processor import QueryProcessor

class TCLHandler:
    """
    Menangani validasi sintaks dan eksekusi query TCL (Transaction Control Language).
    """
    def __init__(self, processor: QueryProcessor):
        self.processor = processor
        
    def handle(self, query: ParsedQuery) -> ExecutionResult:
        if query.tree.type == QueryNodeType.BEGIN_TRANSACTION:
            self.processor.transaction_id = self.processor.ccm.begin_transaction()
            
            self.processor.frm.write_log(LogRecord(
                log_type=LogRecordType.START,
                transaction_id=self.processor.transaction_id,
                item_name=None,
                old_value=None,
                new_value=None,
                active_transactions=None
            ))
            
            return ExecutionResult(transaction_id=self.processor.transaction_id, 
                                   message="BEGIN TRANSACTION successful.", 
                                   data=None, 
                                   timestamp=datetime.now(), 
                                   query=query.query)
            
        elif query.tree.type == QueryNodeType.COMMIT: 
            tx_id = self.processor.transaction_id
            if not tx_id:
                raise Exception("No active transaction to commit.")
            
            self.processor.frm.write_log(LogRecord(
                log_type=LogRecordType.COMMIT,
                transaction_id=tx_id,
                item_name=None,
                old_value=None,
                new_value=None,
                active_transactions=None
            ))

            self.processor.ccm.end_transaction(tx_id)
            self.processor.transaction_id = None
            
            return ExecutionResult(transaction_id=tx_id, 
                                   message="COMMIT successful.", 
                                   data=None, 
                                   timestamp=datetime.now(), 
                                   query=query.query)
            
        
        raise SyntaxError("Unsupported TCL operation.")
