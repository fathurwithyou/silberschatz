from abc import ABC, abstractmethod
from typing import Any, List, Optional
import pickle
import os


class BaseIndex(ABC):
    def __init__(self, table_name: str, column_name: str, data_directory: str = "data"):
        self.table_name = table_name
        self.column_name = column_name
        self.data_directory = data_directory
        self.index_file = self._get_index_file_path()

    def _get_index_file_path(self) -> str:
        index_dir = os.path.join(self.data_directory, "indexes", self.table_name)
        os.makedirs(index_dir, exist_ok=True)
        return os.path.join(index_dir, f"{self.column_name}_{self.get_index_type()}.idx")

    @abstractmethod
    def get_index_type(self) -> str:
        """Return tipe index (b_plus_tree atau hash)"""
        pass

    @abstractmethod
    def insert(self, key: Any, row_id: int) -> None:
        """
        Insert key-value pair ke dalam index

        Args:
            key: Nilai dari kolom yang di-index
            row_id: ID/posisi dari row dalam table file
        """
        pass

    @abstractmethod
    def search(self, key: Any) -> List[int]:
        """
        Search untuk key tertentu dalam index

        Args:
            key: Nilai yang dicari

        Returns:
            List of row IDs yang memiliki key tersebut
        """
        pass

    @abstractmethod
    def delete(self, key: Any, row_id: int) -> None:
        """
        Delete key-value pair dari index

        Args:
            key: Nilai dari kolom yang di-index
            row_id: ID/posisi dari row dalam table file
        """
        pass

    @abstractmethod
    def range_search(self, start_key: Any, end_key: Any) -> List[int]:
        """
        Search untuk range of keys (hanya efektif untuk B+ Tree)

        Args:
            start_key: Nilai awal range
            end_key: Nilai akhir range

        Returns:
            List of row IDs dalam range tersebut
        """
        pass

    def save(self) -> None:
        """Save index structure ke disk"""
        with open(self.index_file, 'wb') as f:
            pickle.dump(self._get_state(), f)

    def load(self) -> None:
        """Load index structure dari disk"""
        if os.path.exists(self.index_file):
            with open(self.index_file, 'rb') as f:
                state = pickle.load(f)
                self._set_state(state)

    @abstractmethod
    def _get_state(self) -> dict:
        """Return state yang akan di-pickle"""
        pass

    @abstractmethod
    def _set_state(self, state: dict) -> None:
        """Restore state dari pickle"""
        pass

    def destroy(self) -> None:
        if os.path.exists(self.index_file):
            os.remove(self.index_file)
