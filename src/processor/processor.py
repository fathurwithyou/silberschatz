from core import IQueryProcessor, IQueryOptimizer, IStorageManager, CCManager, IFailureRecoveryManager
from core.models import ExecutionResult, Rows, QueryTree, ParsedQuery, QueryNodeType
from .handlers import TCLHandler, DMLHandler, DDLHandler
from .operators import ScanOperator
from typing import Optional

"""
Kelas utama untuk memproses query yang diterima dari user.
"""    
class QueryProcessor(IQueryProcessor):
    
    def __init__(self, 
                 optimizer: IQueryOptimizer,
                 ccm: CCManager,
                 frm: IFailureRecoveryManager,
                 storage: IStorageManager
                 ):
        
        self.optimizer = optimizer
        self.ccm = ccm
        self.frm = frm
        self.storage = storage
        
        self.transaction_id: Optional[int] = None
        
        # handler untuk berbagai jenis query (TCL, DML, atau DDL kalo mau kerja bonus)
        self.dml_handler = DMLHandler(self)
        self.tcl_handler = TCLHandler(self)
        self.ddl_handler = DDLHandler(self)
        
        # operator untuk berbagai operasi (scan, join, selection, dsb)
        self.scan_operator = ScanOperator()
        # self.join_operator = JoinOperator()
        # dst

    def execute_query(self, query: str) -> ExecutionResult:
        """
        Eksekusi query yang diterima dari user.
        """
        try: 
            # validated_query = self.validator.validate(query)
            parsed_query = self.optimizer.parse_query(query)
            return self._route_query(parsed_query)
        except Exception as e:
            raise e
            # return ExecutionResult(message=str(e), transaction_id=None, data=None, timestamp=None, query=query)
        

    def _route_query(self, query: ParsedQuery):
        """
        Membaca query dan memanggil handler yang sesuai.
        """
        query_type = self._get_query_type(query.tree)
        if query_type == "DML":
            return self.dml_handler.handle(query)
        elif query_type == "TCL":
            return self.tcl_handler.handle(query)
        else:
            return self.ddl_handler.handle(query)
        
        
    def execute(self, node: QueryTree, tx_id: int) -> Rows:
        """
        Eksekusi query secara rekursif berdasarkan pohon query yang sudah di-parse.
        """
        
        if node.type == QueryNodeType.TABLE:
            return self.scan_operator.execute(node.value, tx_id)
        # elif node.type == 'JOIN':
        #     return self.join_operator.execute(node.children, tx_id)
        # dst
        
        raise NotImplementedError
    
    def _get_query_type(self, query_tree: QueryTree) -> str:
        """
        Mengembalikan tipe query berdasarkan pohon query.
        """
        
        ddl_type = [QueryNodeType.CREATE_TABLE, QueryNodeType.DROP_TABLE]
        tcl_type = [QueryNodeType.BEGIN_TRANSACTION, QueryNodeType.COMMIT]
        if query_tree.type in ddl_type:
            return "DDL"
        elif query_tree.type in tcl_type:
            return "TCL"
        else:
            return "DML"