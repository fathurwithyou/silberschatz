from core import IQueryProcessor, IQueryOptimizer
from core.models import ExecutionResult, Rows, QueryTree
from .handlers import TCLHandler, DMLHandler
from .operators import ScanOperator
from typing import Optional
import re

"""
Kelas utama untuk memproses query yang diterima dari user.
"""    
class QueryProcessor(IQueryProcessor):
    
    def __init__(self, 
                 optimizer: IQueryOptimizer,
                #  ccm: IConcurrencyControlManager,
                #  frm: IFailureRecoveryManager,
                #  storage: IStorageManager
                 ):
        
        self.optimizer = optimizer
        
        self.transaction_id: Optional[int] = None
        
        # handler untuk berbagai jenis query (TCL, DML, atau DDL kalo mau kerja bonus)
        self.dml_handler = DMLHandler(self)
        self.tcl_handler = TCLHandler(self)
        # self.ddl_handler = DDLHandler(self)
        
        # operator untuk berbagai operasi (scan, join, selection, dsb)
        self.scan_operator = ScanOperator()
        # self.join_operator = JoinOperator()
        # dst

    def execute_query(self, query: str) -> ExecutionResult:
        """
        Eksekusi query yang diterima dari user.
        """
        try: 
            return self._route_query(query)
        except Exception as e:
            raise e
            # return ExecutionResult(message=str(e), transaction_id=None, data=None, timestamp=None, query=query)
        

    def _route_query(self, query_str: str):
        """
        Membaca query dan memanggil handler yang sesuai.
        """
        if not query_str or query_str.isspace():
            raise ValueError("Query cannot be empty.")

        # Bersihkan spasi dan pecah jadi token
        tokens = re.split(r'\s+', query_str.strip().upper())
        first_token = tokens[0]

        # Handle DML (Data Manipulation Language)
        if first_token in ('SELECT', 'UPDATE', 'INSERT', 'DELETE'):
            return self.dml_handler.handle(query_str)

        # Handle TCL (Transaction Control Language)
        elif first_token == 'BEGIN':
            return self.tcl_handler.handle_begin(tokens)
        elif first_token == 'COMMIT':
            return self.tcl_handler.handle_commit(tokens)
        elif first_token == 'ABORT':
            return self.tcl_handler.handle_abort(tokens)
        else:
            raise SyntaxError(f"Unrecognized command: {first_token}")
        
        
    def execute(self, node: QueryTree, tx_id: int) -> Rows:
        """
        Eksekusi query secara rekursif berdasarkan pohon query yang sudah di-parse.
        """
        
        if node.type == 'TABLE_SCAN':
            return self.scan_operator.execute(node.value, tx_id)
        # elif node.type == 'JOIN':
        #     return self.join_operator.execute(node.children, tx_id)
        # dst
        
        raise NotImplementedError