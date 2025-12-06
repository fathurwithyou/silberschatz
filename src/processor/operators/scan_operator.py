from src.core.models.result import Rows
from src.core import IConcurrencyControlManager, IStorageManager
from src.core.models import DataRetrieval, Action, QueryTree, Condition, TableSchema
from ..exceptions import AbortError
from ..conditions import ConditionParser
from typing import List, Dict, Any, Optional

class ScanOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager):
        self.ccm = ccm
        self.storage_manager = storage_manager

    def execute(self, table_name: str, tx_id: int, parent_node: Optional[QueryTree] = None) -> Rows:
        table_name, table_alias = self._parse_table_name_and_alias(table_name)
        table_schema = self.storage_manager.get_table_schema(table_name)

        if table_name not in self.storage_manager.list_tables() or table_schema is None:
            raise ValueError(f"Table '{table_name}' does not exist")
        table_schema.table_name = table_alias
        
        validate = self.ccm.validate_object(table_name, tx_id, Action.READ)
        if not validate.allowed:
            raise AbortError(tx_id, table_name, Action.READ, "Read access denied by concurrency control manager")
        
        conditions = None
        if parent_node:
            conditions = self._try_index_selection(parent_node, table_schema)
        
        data_retrieval = DataRetrieval(
            table_name=table_name,
            columns=['*'],
            conditions=conditions
        )
        
        rows = self.storage_manager.read_buffer(data_retrieval)
        rows.schema = [table_schema]
        rows.data = self._transform_rows(rows.data, table_alias)
        
        return rows
    
    def _parse_table_name_and_alias(self, table_name: str) -> tuple[str, str]:
        names = table_name.split()
        if len(names) == 3:
            return names[0], names[2]
        
        return names[0], names[0]
    
    def _transform_rows(self, rows: List[Dict[str, Any]], table_alias: str) -> List[Dict[str, Any]]:
        transformed_data = []
        for row in rows:
            transformed_row = {}
            for key, value in row.items():
                qualified_key = f"{table_alias}.{key}"
                transformed_row[qualified_key] = value
            transformed_data.append(transformed_row)
        
        return transformed_data
    
    def _try_index_selection(self, node: QueryTree, schema: TableSchema):
        condition_parser = ConditionParser.get_instance([schema])
        condition = condition_parser.parse(node.value)
        
        check = condition.check_valid()
        if check == (None, None, None):
            raise ValueError("Invalid condition in WHERE clause")
        left, operator, right = check
        
        condition = Condition(left, operator, right)
        return [condition]