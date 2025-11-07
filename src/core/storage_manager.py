from abc import ABC, abstractmethod
from typing import List, Optional
from .models import (
    DataRetrieval, DataWrite, DataDeletion,
    Statistic, TableSchema, Row
)

class IStorageManager(ABC):

    # ============================================
    # DML Operations
    # ============================================
    
    @abstractmethod
    def read_block(self, data_retrieval: DataRetrieval) -> List[Row]:
        """
        Membaca data dari storage
        Args:
            data_retrieval: Spesifikasi data yang diminta
        Returns:
            List of rows yang match dengan criteria
        """
        raise NotImplementedError
    
    @abstractmethod
    def write_block(self, data_write: DataWrite) -> int:
        """
        Menulis data (INSERT atau UPDATE) ke storage
        Args:
            data_write: Data yang akan ditulis
        Returns:
            Jumlah rows yang terpengaruh
        """
        raise NotImplementedError
    
    @abstractmethod
    def delete_block(self, data_deletion: DataDeletion) -> int:
        """
        Menghapus data dari storage
        Args:
            data_deletion: data yang akan dihapus
        Returns:
            Jumlah rows yang terhapus
        """
        raise NotImplementedError
    
    # ============================================
    # Statistics (untuk Integrasi Query Optimizer)
    # ============================================
    
    @abstractmethod
    def get_stats(self, table_name: str) -> Statistic:
        """
        Mengembalikan statistics untuk cost estimation
        Args:
            table_name: Nama tabel
        Returns:
            Statistic object dengan n_r, b_r, l_r, f_r, V(A,r)
        """
        raise NotImplementedError
    
    # ============================================
    # Indexing
    # ============================================
    
    @abstractmethod
    def set_index(self, table: str, column: str, index_type: str) -> None:
        """
        Membuat index pada kolom tertentu
        Args:
            table: Nama tabel
            column: Nama kolom
            index_type: 'b_plus_tree' atau 'hash'
        """
        raise NotImplementedError
    
    @abstractmethod
    def drop_index(self, table: str, column: str) -> None:
        """Drop index dari kolom"""
        raise NotImplementedError
    
    @abstractmethod
    def has_index(self, table: str, column: str) -> bool:
        """Check apakah kolom memiliki index"""
        raise NotImplementedError
    
    # ============================================
    # DDL Operations (Bonus)
    # ============================================
    
    @abstractmethod
    def create_table(self, schema: TableSchema) -> None:
        """
        CREATE TABLE
        """
        raise NotImplementedError
    
    @abstractmethod
    def drop_table(self, table_name: str) -> None:
        """
        DELETE TABLE
        """
        raise NotImplementedError
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """Get schema definition untuk tabel"""
        raise NotImplementedError
    
    @abstractmethod
    def list_tables(self) -> List[str]:
        """List semua tabel yang ada"""
        raise NotImplementedError