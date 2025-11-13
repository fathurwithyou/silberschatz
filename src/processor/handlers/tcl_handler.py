from ..processor import QueryProcessor
from core.models import ExecutionResult, ParsedQuery, QueryNodeType
from datetime import datetime

class TCLHandler:
    """
    Menangani validasi sintaks dan eksekusi query TCL (Transaction Control Language).
    """
    def __init__(self, processor: QueryProcessor):
        self.processor = processor
        
    def handle(self, query: ParsedQuery) -> ExecutionResult:
        if query.tree.type == QueryNodeType.BEGIN_TRANSACTION:
            self.processor.transaction_id = self.processor.ccm.begin_transaction()
            
            return ExecutionResult(transaction_id=self.processor.transaction_id, 
                                   message="BEGIN TRANSACTION successful.", 
                                   data=None, 
                                   timestamp=datetime.now(), 
                                   query=query.query)
            
        elif query.tree.type == QueryNodeType.COMMIT: 
            tx_id = self.processor.transaction_id
            if not tx_id:
                raise Exception("No active transaction to commit.")
            
            self.processor.ccm.end_transaction(tx_id)
            self.processor.transaction_id = None
            
            return ExecutionResult(transaction_id=tx_id, 
                                   message="COMMIT successful.", 
                                   data=None, 
                                   timestamp=datetime.now(), 
                                   query=query.query)
            
        
        raise SyntaxError("Unsupported TCL operation.")
