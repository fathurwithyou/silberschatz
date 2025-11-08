from ..processor import QueryProcessor
from core.models import ExecutionResult

class TCLHandler:
    """
    Menangani validasi sintaks dan eksekusi query TCL (Transaction Control Language).
    """
    def __init__(self, processor: QueryProcessor):
        self.processor = processor

    def handle_begin(self, tokens) -> ExecutionResult:
        """
        Handle perintah 'BEGIN TRANSACTION'
        """
        if not (len(tokens) == 2 and tokens[1] == 'TRANSACTION'):
            raise SyntaxError("Invalid syntax. Expected 'BEGIN TRANSACTION'.")
        
        if self.processor.transaction_id:
            raise Exception("Transaction already active.")
            
        # Panggil CCM
        # tx_id = self.processor.ccm.begin_transaction()
        # self.processor.transaction_id = tx_id
        
        # return ExecutionResult(transaction_id=tx_id, message="BEGIN TRANSACTION successful.")
        raise NotImplementedError

    def handle_commit(self, tokens) -> ExecutionResult:
        """
        Handle perintah 'COMMIT'
        """
        if len(tokens) > 1:
             raise SyntaxError("Invalid syntax. Expected 'COMMIT'.")
             
        tx_id = self.processor.transaction_id
        if not tx_id:
            raise Exception("No active transaction to commit.")
        
        # Panggil CCM
        # self.processor.ccm.end_transaction(tx_id)
        # self.processor.transaction_id = None
        
        # return ExecutionResult(transaction_id=tx_id, message="COMMIT successful.")
        raise NotImplementedError

    def handle_abort(self, tokens) -> ExecutionResult:
        """
        Handle perintah 'ABORT'
        """
        if len(tokens) > 1:
             raise SyntaxError("Invalid syntax. Expected 'ABORT'.")
        
        tx_id = self.processor.transaction_id
        if not tx_id:
            raise Exception("No active transaction to abort.")
            
        # Panggil CCM
        # self.processor.ccm.end_transaction(tx_id)
        # self.processor.transaction_id = None
        
        # return ExecutionResult(transaction_id=tx_id, message="ABORT successful.")
        raise NotImplementedError