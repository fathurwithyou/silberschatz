from abc import ABC, abstractmethod
from .models import Action, Response

class IConcurrencyControlManager(ABC):
    @abstractmethod
    def begin_transaction(self) -> int:
        """
        Starts a new transaction and returns the transaction ID.
        """
        pass

    @abstractmethod
    def end_transaction(self, transaction_id: int) -> Response:
        """
        Ends a transaction, commits or aborts it based on its state.
        """
        pass

    @abstractmethod
    def log_object(self, table: str, transaction_id: int) -> None:
        """
        Logs an object (table) that is being accessed by the transaction.
        """
        pass

    @abstractmethod
    def validate_object(self, table: str, transaction_id: int, action: Action) -> Response:
        """
        Validates whether the transaction can perform a certain action (READ or WRITE) on a table.
        """
        pass

    @abstractmethod
    def get_active_transactions(self) -> tuple[int, list[int]]:
        """
        Returns a tuple containing the number of active transactions and their IDs.
        """
        pass
