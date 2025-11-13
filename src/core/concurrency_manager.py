from abc import ABC, abstractmethod
from models import Action, Response, Row

class CCManager(ABC):
    @abstractmethod
    def log_object(self, row: Row, transaction_id: int):
        """
        Logs an object that is being accessed by the transaction.
        """
        pass

    @abstractmethod
    def begin_transaction(self) -> int:
        """
        Starts a new transaction and returns the transaction ID.
        """
        pass

    @abstractmethod
    def end_transaction(self, transaction_id: int):
        """
        Ends a transaction, commits or aborts it based on its state.
        """
        pass

    @abstractmethod
    def validate_object(self, row: Row, transaction_id: int, action: Action) -> Response:
        """
        Validates whether the transaction can perform a certain action (READ or WRITE) on an object.
        """
        pass
