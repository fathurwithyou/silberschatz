from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
from enum import Enum


# ============================================
# Enums
# ============================================

class DataType(Enum):
    INTEGER = "integer"
    FLOAT = "float"
    CHAR = "char"
    VARCHAR = "varchar"


class IndexType(Enum):
    B_PLUS_TREE = "b_plus_tree"
    HASH = "hash"


class ComparisonOperator(Enum):
    EQ = "="      
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="

# ============================================
# Data Models untuk DML
# ============================================

@dataclass
class Condition:
    """
    Merepresentasikan condition dalam klausa WHERE
    Contoh: WHERE age > 25 -> Condition(column="age", operator=GT, value=25)
    """
    column: str
    operator: ComparisonOperator
    value: Any


@dataclass
class DataRetrieval:
    """
    Request untuk membaca data dari storage
    Dikirim oleh Query Processor ke StorageManager.read_block()
    """
    table_name: str
    columns: List[str]  # ['*'] untuk semua kolom
    conditions: Optional[List[Condition]] = None  # WHERE clauses
    limit: Optional[int] = None
    offset: Optional[int] = 0


@dataclass
class DataWrite:
    """
    Request untuk menulis data (INSERT atau UPDATE)
    Dikirim oleh Query Processor ke StorageManager.write_block()
    """
    table_name: str
    data: Dict[str, Any]  # {column_name: value}
    is_update: bool = False
    conditions: Optional[List[Condition]] = None  # WHERE clauses


@dataclass
class DataDeletion:
    """
    Request untuk menghapus data
    Dikirim oleh Query Processor ke StorageManager.delete_block()
    """
    table_name: str
    conditions: List[Condition]  # WHERE clauses


# ============================================
# Statistics Models (untuk Integrasi Query Optimizer)
# ============================================

@dataclass
class Statistic:
    """
    Statistics untuk satu tabel
    Dikirim ke Query Optimizer untuk cost estimation
    """
    table_name: str
    n_r: int        # Number of tuples (rows)
    b_r: int        # Number of blocks (pages)
    l_r: int        # Tuple size in bytes
    f_r: int        # Blocking factor (tuples per block)
    
    # Column statistics
    V: Dict[str, int]  # V(A,r)
    
    min_values: Optional[Dict[str, Any]] = None
    max_values: Optional[Dict[str, Any]] = None
    null_counts: Optional[Dict[str, int]] = None


# ============================================
# Schema Models untuk DDL
# ============================================

@dataclass
class ColumnDefinition:
    """Column definition untuk CREATE TABLE"""
    name: str
    data_type: DataType
    max_length: Optional[int] = None  # Untuk CHAR/VARCHAR
    nullable: bool = True
    primary_key: bool = False


@dataclass
class TableSchema:
    table_name: str
    columns: List[ColumnDefinition]
    primary_key: Optional[str] = None