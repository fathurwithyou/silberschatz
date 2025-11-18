from src.core.models.result import Rows
from src.core import IConcurrencyControlManager, IStorageManager
from src.core.models import DataRetrieval


class ScanOperator:
    def __init__(self, ccm: IConcurrencyControlManager, storage_manager: IStorageManager):
        self.ccm = ccm
        self.storage_manager = storage_manager

    def execute(self, table_name: str, tx_id: int) -> Rows:
        table_name, table_alias = self._parse_table_name_and_alias(table_name)
        table_schema = self.storage_manager.get_table_schema(table_name)
        if not table_schema:
            raise ValueError(f"Table '{table_name}' does not exist")
        table_schema.table_name = table_alias
        
        # self.ccm.validate_object(table_name, tx_id)
        
        data_retrieval = DataRetrieval(
            table_name=table_name,
            columns=['*']
        )
        rows = self.storage_manager.read_block(data_retrieval)
        rows.schema = [table_schema]
        
        return rows
    
    def _parse_table_name_and_alias(self, table_name: str) -> tuple[str, str]:
        names = table_name.split()
        if len(names) == 3:
            return names[0], names[2]
        
        return names[0], names[0]