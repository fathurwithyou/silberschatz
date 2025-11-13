from src.core.concurrency_manager import IConcurrencyControlManager

class SnapshotIsolation(IConcurrencyControlManager):
    def __init__(self):
        pass

    def begin_transaction(self, transaction):
        pass
    
    def end_transaction(self, transaction):
        pass

    def log_object(self, row, transaction):
        pass

    def validate_object(self, row, transaction, action):
        pass
        
    