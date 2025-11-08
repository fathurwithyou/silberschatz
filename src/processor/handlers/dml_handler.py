from ..processor import QueryProcessor
from core.models import ExecutionResult

class DMLHandler:
    """
    Menangani query DML (Data Manipulation Language) 
    Optimasi -> Eksekusi -> Logging
    """
    def __init__(self, processor: QueryProcessor):
        self.processor = processor

    def handle(self, query_str: str) -> ExecutionResult:
        is_implicit = False
        tx_id = self.processor.transaction_id
        
        if not tx_id:
            # tx_id = self.processor.ccm.begin_transaction()
            is_implicit = True
        
        try:
            parsed_query = self.processor.optimizer.parse_query(query_str)
            
            query_plan = self.processor.optimizer.optimize_query(parsed_query)
            
            # rows = self.processor.execute(query_plan.tree, tx_id)
            
            # result = ExecutionResult(
            #     transaction_id=tx_id,
            #     data=rows.data,
            #     rows_count=rows.rows_count,
            #     message="Query executed successfully.",
            #     query=query_str
            # )
            
            # self.processor.frm.write_log(result)
            
            # if is_implicit:
            #     self.processor.ccm.end_transaction(tx_id) # Commit
                
            # return result
            raise NotImplementedError

        except Exception as e:
            # if is_implicit:
            #     self.processor.ccm.end_transaction(tx_id) # Abort
            
            raise e