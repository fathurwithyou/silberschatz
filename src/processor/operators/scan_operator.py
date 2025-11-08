from core.models.result import Rows


class ScanOperator:
    def __init__(self):
        pass

    def execute(self, table_name: str, tx_id: int) -> Rows:
        # 1. Minta Izin ke Concurrency Control Manager
        # 2. Minta Data ke Storage Manager
        raise NotImplementedError