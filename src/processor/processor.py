from core import IQueryProcessor, IQueryOptimizer, IStorageManager, CCManager, IFailureRecoveryManager
from core.models import ExecutionResult, Rows, QueryTree, ParsedQuery, QueryNodeType
from .handlers import TCLHandler, DMLHandler, DDLHandler
from .operators import ScanOperator
from .validators import SyntaxValidator
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
        
        # validator untuk syntax SQL
        self.validator = SyntaxValidator()
        
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
        validated_query = self.validator.validate(query)
        if not validated_query.is_valid:
            error_msg = f"syntax error: {validated_query.error_message}\n"
            if validated_query.error_position:
                line, col = validated_query.error_position
                error_msg += f"LINE {line}: {query}\n"
                if query:
                    pointer = ' ' * (col + 6) + '^'
                    error_msg += pointer + '\n'
            raise SyntaxError(f"ERROR: {error_msg}")
        
        parsed_query = self.optimizer.parse_query(query)
        return self._route_query(parsed_query)
        

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