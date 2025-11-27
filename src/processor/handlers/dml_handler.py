from typing import TYPE_CHECKING
from src.core.models import ExecutionResult, ParsedQuery, QueryNodeType, LogRecord, LogRecordType
from datetime import datetime

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