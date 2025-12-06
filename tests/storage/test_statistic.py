import pytest
import os
import shutil
import sys 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.storage.storage_manager import StorageManager
from src.core.models import (
    TableSchema,
    ColumnDefinition,
    DataType,
    Rows
)


@pytest.fixture(scope="function")
def test_data_dir():
    test_dir = "src/data_test"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    yield test_dir
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


@pytest.fixture(scope="function")
def storage(test_data_dir):
    return StorageManager("data_test")


@pytest.fixture(scope="function")
def employees_table(storage):
    schema = TableSchema(
        table_name="employees",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="age", data_type=DataType.INTEGER),
            ColumnDefinition(name="salary", data_type=DataType.FLOAT),
        ],
        primary_key="id"
    )
    
    storage.create_table(schema)
    
    test_rows = Rows(
        data=[
            {"id": 1, "name": "Alice", "age": 30, "salary": 75000.0},
            {"id": 2, "name": "Bob", "age": 25, "salary": 60000.0},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 90000.0},
            {"id": 4, "name": "Diana", "age": 28, "salary": 70000.0},
            {"id": 5, "name": "Eve", "age": 32, "salary": 85000.0},
        ],
        rows_count=5
    )
    
    storage.dml_manager.save_all_rows("employees", test_rows, schema)
    
    return storage


@pytest.fixture(scope="function")
def products_table(storage):
    schema = TableSchema(
        table_name="products",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50, nullable=True),
            ColumnDefinition(name="price", data_type=DataType.FLOAT, nullable=True),
        ],
        primary_key="id"
    )
    
    storage.create_table(schema)
    
    test_rows = Rows(
        data=[
            {"id": 1, "name": "Laptop", "price": 1000.0},
            {"id": 2, "name": None, "price": 200.0},
            {"id": 3, "name": "Mouse", "price": None},
            {"id": 4, "name": None, "price": None},
        ],
        rows_count=4
    )
    
    storage.dml_manager.save_all_rows("products", test_rows, schema)
    
    return storage


@pytest.fixture(scope="function")
def product_desc_table(storage):
    text_schema = TableSchema(
        table_name="product_desc",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.VARCHAR, max_length=10, primary_key=True),
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="description", data_type=DataType.CHAR, max_length=100),
        ],
        primary_key="id"
    )
    
    storage.create_table(text_schema)
    
    test_rows = Rows(
        data=[
            {"id": "A1", "name": "Laptop", "description": "Mobile computer"},
            {"id": "A2", "name": "Mouse", "description": "Looks like a mouse"},
        ],
        rows_count=2
    )
    
    storage.dml_manager.save_all_rows("product_desc", test_rows, text_schema)
    
    return storage


@pytest.fixture(scope="function")
def empty_table(storage):
    empty_schema = TableSchema(
        table_name="empty_table",
        columns=[
            ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
            ColumnDefinition(name="value", data_type=DataType.FLOAT),
        ],
        primary_key="id"
    )
    
    storage.create_table(empty_schema)
    
    return storage


class TestStatistics:
    
    def test_get_stats_basic(self, employees_table):
        stats = employees_table.get_stats("employees")
        
        assert stats.table_name == "employees"
        assert stats.n_r == 5
        assert stats.l_r > 0
        assert stats.f_r > 0
        assert stats.b_r > 0
        assert stats.V["id"] == 5
        assert stats.V["name"] == 5
        assert stats.V["age"] == 5
        assert stats.V["salary"] == 5
    
    def test_get_stats_min_max_values(self, employees_table):
        schema = employees_table.get_table_schema("employees")
        rows = employees_table.dml_manager.load_all_rows("employees", schema)
        stats = employees_table.get_stats("employees")
        
        assert stats.min_values is not None
        assert stats.max_values is not None
        assert stats.min_values.get("age") == 25
        assert stats.max_values.get("age") == 35
        assert stats.min_values.get("salary") == 60000.0
        assert stats.max_values.get("salary") == 90000.0
        assert "name" not in stats.min_values
        assert "name" not in stats.max_values
    
    def test_get_stats_no_nulls(self, employees_table):
        schema = employees_table.get_table_schema("employees")
        rows = employees_table.dml_manager.load_all_rows("employees", schema)
        stats = employees_table.get_stats("employees")
        
        assert stats.null_counts is None
    
    def test_get_stats_with_nulls(self, products_table):
        stats = products_table.get_stats("products")
        
        assert stats.null_counts is not None
        assert stats.null_counts.get("name") == 2
        assert stats.null_counts.get("price") == 2
        assert "id" not in stats.null_counts
    
    def test_get_stats_no_numeric_columns(self, product_desc_table):
        stats = product_desc_table.get_stats("product_desc")
        
        assert stats.n_r == 2
        assert stats.min_values is None
        assert stats.max_values is None
    
    def test_get_stats_empty_table(self, empty_table):
        rows = empty_table.dml_manager.load_all_rows("empty_table", empty_table.get_table_schema("empty_table"))
        stats = empty_table.get_stats("empty_table")
        
        assert stats.n_r == 0
        assert stats.b_r == 0
        assert all(v == 0 for v in stats.V.values())
        assert stats.min_values is None
        assert stats.max_values is None
        assert stats.null_counts is None
    
    def test_get_stats_nonexistent_table(self, storage):
        with pytest.raises(ValueError, match="does not exist"):
            storage.get_stats("nonexistent")