from typing import TYPE_CHECKING
from src.core.models import ExecutionResult, ParsedQuery, QueryNodeType, LogRecord, LogRecordType, RecoverCriteria
from datetime import datetime
from ..exceptions import AbortError

if TYPE_CHECKING:
    from ..processor import QueryProcessor
    
class DMLHandler:
    """
    Menangani query DML (Data Manipulation Language) 
    Optimasi -> Eksekusi -> Logging
    """
    def __init__(self, processor: QueryProcessor):
        self.processor = processor

    def handle(self, query: ParsedQuery) -> ExecutionResult:
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                return self._execute_query(query)
            except AbortError as abort_error:
                print(f"Transaction {abort_error.transaction_id} aborted (attempt {attempt + 1}/{max_retries}): {abort_error}")
                
                recovery_criteria = RecoverCriteria.from_transaction(abort_error.transaction_id)
                self.processor.frm.recover(recovery_criteria)
                
                if attempt == max_retries - 1:
                    raise RuntimeError(f"Transaction failed after {max_retries} attempts due to concurrency conflicts") from abort_error
                
                print(f"Retrying transaction (attempt {attempt + 2}/{max_retries})...")
        
        raise RuntimeError("Unexpected error in transaction handling")
    
    def _execute_query(self, query: ParsedQuery) -> ExecutionResult:
        """
        Execute a single DML query attempt.
        """
        is_implicit = False
        tx_id = self.processor.transaction_id
        
        if not tx_id:
            tx_id = self.processor.ccm.begin_transaction()
            is_implicit = True
            # Logging awal transaksi
            self.processor.frm.write_log(LogRecord(
                log_type=LogRecordType.START,
                transaction_id=tx_id,
                item_name=None,
                old_value=None,
                new_value=None,
                active_transactions=None
            ))
        
        try:
            rows = self.processor.execute(query.tree, tx_id)
            
            result = ExecutionResult(
                transaction_id=tx_id,
                data=rows,
                message="Query executed successfully.",
                query=query.query,
                timestamp=datetime.now()
            )
            
            if query.tree.type == QueryNodeType.UPDATE:
                result.message = "update successful"
            elif query.tree.type == QueryNodeType.DELETE:
                result.message = "delete successful"            
            # self.processor.frm.write_log(result)
            
            if is_implicit:
                self.processor.frm.write_log(LogRecord(
                    log_type=LogRecordType.COMMIT,
                    transaction_id=tx_id,
                    item_name=None,
                    old_value=None,
                    new_value=None,
                    active_transactions=None
                ))
                
                self.processor.ccm.end_transaction(tx_id) # Commit
                
            return result

        except AbortError:
            if is_implicit:
                self.processor.frm.write_log(LogRecord(
                    log_type=LogRecordType.ABORT,
                    transaction_id=tx_id,
                    item_name=None,
                    old_value=None,
                    new_value=None,
                    active_transactions=None
                ))
                
                self.processor.ccm.end_transaction(tx_id) # Abort
            
            raise
            
        except Exception as e:
            if is_implicit:
                self.processor.frm.write_log(LogRecord(
                    log_type=LogRecordType.ABORT,
                    transaction_id=tx_id,
                    item_name=None,
                    old_value=None,
                    new_value=None,
                    active_transactions=None
                ))
                
                self.processor.ccm.end_transaction(tx_id) # Abort
            
            raise e