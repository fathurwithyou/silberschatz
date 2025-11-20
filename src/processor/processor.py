from src.core import IQueryProcessor, IQueryOptimizer, IStorageManager, IConcurrencyControlManager, IFailureRecoveryManager
from src.core.models import ExecutionResult, Rows, QueryTree, ParsedQuery, QueryNodeType
from .handlers import TCLHandler, DMLHandler, DDLHandler
from .operators import (
    ScanOperator,
    SelectionOperator,
    ProjectionOperator,
    JoinOperator,
    UpdateOperator,
    SortOperator,
)
from .validators import SyntaxValidator
from typing import Optional
import re

"""
Kelas utama untuk memproses query yang diterima dari user.
"""    
class QueryProcessor(IQueryProcessor):
    
    def __init__(self, 
                 optimizer: IQueryOptimizer,
                 ccm: IConcurrencyControlManager,
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
        self.scan_operator = ScanOperator(self.ccm, self.storage)
        self.selection_operator = SelectionOperator()
        self.projection_operator = ProjectionOperator()
        self.join_operator = JoinOperator()
        self.update_operator = UpdateOperator(self.ccm, self.storage) 
        self.sort_operator = SortOperator()
        # dst

    def execute_query(self, query: str) -> ExecutionResult:
        """
        Eksekusi query yang diterima dari user.
        """
        validated_query = self.validator.validate(query)
        if not validated_query.is_valid:
            error_msg = f"{validated_query.error_message}\n"
            if validated_query.error_position:
                line, col = validated_query.error_position
                error_msg += f"LINE {line}: {query}\n"
                if query:
                    pointer = ' ' * (col + 6) + '^'
                    error_msg += pointer
            raise SyntaxError(f"{error_msg}")
        
        query = re.sub(r'\s+', ' ', query.strip()).strip()
        
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
        
        elif node.type == QueryNodeType.SELECTION:
            rows = self.execute(node.children[0], tx_id)
            return self.selection_operator.execute(rows, node.value)
        
        elif node.type == QueryNodeType.PROJECTION:
            rows = self.execute(node.children[0], tx_id)
            return self.projection_operator.execute(rows, node.value)
        
        elif node.type in {
            QueryNodeType.JOIN,
            QueryNodeType.NATURAL_JOIN,
            QueryNodeType.CARTESIAN_PRODUCT,
            QueryNodeType.THETA_JOIN,
        }:
            left = self.execute(node.children[0], tx_id)
            right = self.execute(node.children[1], tx_id)
            condition, natural_shared_columns = self._build_join_condition(node, left, right)
            return self.join_operator.execute(left, right, condition, natural_shared_columns)
        
        elif node.type == QueryNodeType.UPDATE:
            target_rows = self.execute(node.children[0], tx_id)
            return self.update_operator.execute(target_rows, node.value)

        elif node.type == QueryNodeType.ORDER_BY:
            rows = self.execute(node.children[0], tx_id)
            return self.sort_operator.execute(rows, node.value)        
        
        raise ValueError(f"Unknown query type: {node.type}")
    
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

    def _build_join_condition(
        self, node: QueryTree, left: Rows, right: Rows
    ) -> tuple[str | None, set[str] | None]:
        if node.type == QueryNodeType.JOIN or node.type == QueryNodeType.THETA_JOIN:
            return node.value or None, None
        if node.type == QueryNodeType.CARTESIAN_PRODUCT:
            return None, None
        if node.type == QueryNodeType.NATURAL_JOIN:
            clauses = []
            shared_columns: set[str] = set()
            for left_schema in left.schema or []:
                for right_schema in right.schema or []:
                    left_cols = {col.name for col in left_schema.columns}
                    right_cols = {col.name for col in right_schema.columns}
                    shared = left_cols & right_cols
                    for column in shared:
                        shared_columns.add(column)
                        left_ref = (
                            f"{left_schema.table_name}.{column}"
                            if left_schema.table_name
                            else column
                        )
                        right_ref = (
                            f"{right_schema.table_name}.{column}"
                            if right_schema.table_name
                            else column
                        )
                        clauses.append(f"{left_ref} = {right_ref}")
            condition_text = " AND ".join(clauses) if clauses else None
            return condition_text, shared_columns or None
        return node.value or None, None
