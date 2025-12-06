from src.core import IQueryProcessor, IQueryOptimizer, IStorageManager, IConcurrencyControlManager, IFailureRecoveryManager
from src.core.models import ExecutionResult, Rows, QueryTree, ParsedQuery, QueryNodeType
from .handlers import TCLHandler, DMLHandler, DDLHandler, QueryTypeEnum
from .operators import (
    ScanOperator,
    SelectionOperator,
    ProjectionOperator,
    JoinOperator,
    UpdateOperator,
    SortOperator,
    DeleteOperator,
    InsertOperator
)
from .validators import SyntaxValidator
from typing import Optional
from datetime import datetime
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
        self.update_operator = UpdateOperator(self.ccm, self.storage, self.frm) 
        self.sort_operator = SortOperator()
        self.delete_operator = DeleteOperator(self.ccm, self.storage, self.frm)
        self.insert_operator = InsertOperator(self.ccm, self.storage, self.frm)
        # dst

    def execute_query(self, query: str) -> ExecutionResult:
        """
        Eksekusi query yang diterima dari user.
        """
        # Handle meta commands first, before validation
        meta_result = self._handle_meta_commands(query.strip())
        if meta_result is not None:
            return meta_result
        
        validated_query = self.validator.validate(query)
        if not validated_query.is_valid:
            error_msg = f"{validated_query.error_message}\n"
            if validated_query.error_position:
                line, col = validated_query.error_position
                query_lines = query.splitlines()
                if 0 < line <= len(query_lines):
                    error_line = query_lines[line - 1]
                    error_msg += f"LINE {line}: {error_line}\n"
                    pointer = ' ' * (col + 6) + '^'
                    error_msg += pointer
                else:
                    error_msg += f"LINE {line}: {query}\n"
                    pointer = ' ' * (col + 6) + '^'
                    error_msg += pointer
            raise SyntaxError(f"{error_msg}")
        
        query = re.sub(r'\s+', ' ', query.strip()).strip()
        
        parsed_query = self.optimizer.parse_query(query)
        optimized_query = self.optimizer.optimize_query(parsed_query)
        return self._route_query(optimized_query)
        

    def _route_query(self, query: ParsedQuery):
        """
        Membaca query dan memanggil handler yang sesuai.
        """
        query_type = self._get_query_type(query.tree)
        if query_type == QueryTypeEnum.DML:
            return self.dml_handler.handle(query)
        elif query_type == QueryTypeEnum.TCL:
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
            return self.update_operator.execute(target_rows, node.value, tx_id)

        elif node.type == QueryNodeType.ORDER_BY:
            rows = self.execute(node.children[0], tx_id)
            return self.sort_operator.execute(rows, node.value)

        elif node.type == QueryNodeType.DELETE:
            target_rows = self.execute(node.children[0], tx_id)
            return self.delete_operator.execute(target_rows, tx_id)  

        elif node.type == QueryNodeType.INSERT:
            return self.insert_operator.execute(
                node.children[0].value,
                node.value,
                tx_id
            )   
        elif node.type == QueryNodeType.LIMIT:
            rows = self.execute(node.children[0], tx_id)
            try:
                limit = int(node.value)
            except ValueError:
                raise ValueError(f"Invalid LIMIT value: {node.value}")
            return Rows(
                data=rows.data[:limit] if limit is not None else rows.data,
                rows_count=min(rows.rows_count, limit) if limit is not None else rows.rows_count
            )

        raise ValueError(f"Unknown query type: {node.type}")
    
    def _get_query_type(self, query_tree: QueryTree) -> QueryTypeEnum:
        """
        Mengembalikan tipe query berdasarkan pohon query.
        """
        
        ddl_type = [QueryNodeType.CREATE_TABLE, QueryNodeType.DROP_TABLE]
        tcl_type = [QueryNodeType.BEGIN_TRANSACTION, QueryNodeType.COMMIT, QueryNodeType.ABORT]
        if query_tree.type in ddl_type:
            return QueryTypeEnum.DDL
        elif query_tree.type in tcl_type:
            return QueryTypeEnum.TCL
        else:
            return QueryTypeEnum.DML

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
    
    def _handle_meta_commands(self, query: str) -> Optional[ExecutionResult]:
        """
        Handle PostgreSQL-style meta commands (\\dt, \\d {table_name})
        """
        query = query.strip()
        
        if not query.startswith('\\'):
            return None
        
        # Handle \dt - list all tables
        if query == '\\dt':
            return self._handle_list_tables()
        
        # Handle \d {table_name} - describe table
        if query.startswith('\\d '):
            table_name = query[3:].strip()
            if table_name:
                return self._handle_describe_table(table_name)
        
        # Handle \d alone - list all tables (same as \dt)
        if query == '\\d':
            return self._handle_list_tables()
        
        return None
    
    def _handle_list_tables(self) -> ExecutionResult:
        try:
            tables = self.storage.list_tables()
            
            # Create result data
            table_data = []
            for table in tables:
                table_info = {
                    'Name': table,
                }
                table_data.append(table_info)
            
            rows = Rows(data=table_data, rows_count=len(table_data))
            
            message = f"List of relations"
            if len(tables) == 0:
                message = "No tables found."
            
            return ExecutionResult(
                transaction_id=self.transaction_id or 0,
                timestamp=datetime.now(),
                message=message,
                data=rows,
                query='\\dt'
            )
        except Exception as e:
            return ExecutionResult(
                transaction_id=self.transaction_id or 0,
                timestamp=datetime.now(),
                message=f"Error listing tables: {str(e)}",
                data=None,
                query='\\dt'
            )
    
    def _handle_describe_table(self, table_name: str) -> ExecutionResult:
        try:
            schema = self.storage.get_table_schema(table_name)
            
            if schema is None:
                return ExecutionResult(
                    transaction_id=self.transaction_id or 0,
                    timestamp=datetime.now(),
                    message=f"Table '{table_name}' does not exist.",
                    data=None,
                    query=f'\\d {table_name}'
                )
            
            # Create column information
            column_data = []
            for column in schema.columns:
                column_info = {
                    'Column': column.name,
                    'Type': column.data_type.name.lower() + (f"({column.max_length})" if column.max_length else ""),
                    'Nullable': 'YES' if column.nullable else 'NO',
                }
                
                # Add primary key indicator
                if schema.primary_key == column.name:
                    column_info['Key'] = 'PK'
                else:
                    column_info['Key'] = ''
                
                # Add foreign key information
                if column.foreign_key:
                    fk = column.foreign_key
                    fk_info = f"{fk.referenced_table}({fk.referenced_column})"
                    if fk.on_delete.value != "restrict" or fk.on_update.value != "restrict":
                        actions = []
                        if fk.on_delete.value != "restrict":
                            actions.append(f"ON DELETE {fk.on_delete.value.upper()}")
                        if fk.on_update.value != "restrict":
                            actions.append(f"ON UPDATE {fk.on_update.value.upper()}")
                        fk_info += f" [{', '.join(actions)}]"
                    column_info['Foreign Key'] = fk_info
                else:
                    column_info['Foreign Key'] = ''
                    
                column_data.append(column_info)
            
            rows = Rows(data=column_data, rows_count=len(column_data))
            
            message = f"Table '{table_name}'"
            
            return ExecutionResult(
                transaction_id=self.transaction_id or 0,
                timestamp=datetime.now(),
                message=message,
                data=rows,
                query=f'\\d {table_name}'
            )
        except Exception as e:
            return ExecutionResult(
                transaction_id=self.transaction_id or 0,
                timestamp=datetime.now(),
                message=f"Error describing table '{table_name}': {str(e)}",
                data=None,
                query=f'\\d {table_name}'
            )
