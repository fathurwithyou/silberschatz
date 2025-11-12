from __future__ import annotations
from abc import ABC , abstractmethod
from .models import LogRecord, RecoverCriteria

class IFailureRecoveryManager(ABC) :
    @abstractmethod
    def write_log(self , info : LogRecord) -> None :
        """Append satu entry ke write-ahead log."""
        raise NotImplementedError

    @abstractmethod
    def save_checkpoint(self , *args , **kwargs) -> None :
        """Simpan checkpoint."""
        raise NotImplementedError

    @abstractmethod
    def recover(self , criteria : RecoverCriteria) -> list[str] :
        """
        Backward recovery berdasarkan kriteria.
        """
        raise NotImplementedError