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
from src.storage.index.b_plus_tree_index import BPlusTreeIndex


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
            ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=100),
            ColumnDefinition(name="age", data_type=DataType.INTEGER),
            ColumnDefinition(name="department", data_type=DataType.VARCHAR, max_length=50),
            ColumnDefinition(name="salary", data_type=DataType.FLOAT),
        ],
        primary_key="id"
    )
    
    storage.create_table(schema)
    
    employees_data = Rows(
        data=[
            {"id": 1, "name": "Alice", "age": 30, "department": "Engineering", "salary": 75000.0},
            {"id": 2, "name": "Bob", "age": 25, "department": "Sales", "salary": 60000.0},
            {"id": 3, "name": "Charlie", "age": 35, "department": "Engineering", "salary": 90000.0},
            {"id": 4, "name": "Diana", "age": 28, "department": "HR", "salary": 70000.0},
            {"id": 5, "name": "Eve", "age": 32, "department": "Engineering", "salary": 85000.0},
            {"id": 6, "name": "Frank", "age": 27, "department": "Sales", "salary": 65000.0},
            {"id": 7, "name": "Grace", "age": 31, "department": "HR", "salary": 72000.0},
            {"id": 8, "name": "Henry", "age": 29, "department": "Engineering", "salary": 78000.0},
            {"id": 9, "name": "Ivy", "age": 26, "department": "Sales", "salary": 62000.0},
            {"id": 10, "name": "Jack", "age": 33, "department": "Engineering", "salary": 88000.0},
        ],
        rows_count=10
    )
    
    storage.dml_manager.save_all_rows("employees", employees_data, schema)
    
    return storage, schema, employees_data


class TestBPlusTreeIndex:
    
    def test_create_index_basic(self, employees_table):
        storage, schema, _ = employees_table
        
        storage.set_index("employees", "age", "BTREE")
        
        has_index = storage.has_index("employees", "age")
        
        assert has_index
    
    def test_populate_and_search_index(self, employees_table):
        storage, schema, employees_data = employees_table
        
        storage.set_index("employees", "age", "BTREE")
        index = storage.indexes[("employees", "age")]
        
        rows = storage.dml_manager.load_all_rows("employees", schema)
        for i, row in enumerate(rows.data):
            age = row.get("age")
            if age is not None:
                index.insert(age, i)
        
        result_30 = index.search(30)
        result_25 = index.search(25)
        result_99 = index.search(99)
        
        assert len(result_30) > 0
        assert len(result_25) > 0
        assert len(result_99) == 0
    
    def test_range_search(self, employees_table):
        storage, schema, _ = employees_table
        
        storage.set_index("employees", "age", "BTREE")
        index = storage.indexes[("employees", "age")]
        
        rows = storage.dml_manager.load_all_rows("employees", schema)
        for i, row in enumerate(rows.data):
            age = row.get("age")
            if age is not None:
                index.insert(age, i)
        
        result_range = index.range_search(27, 31)
        
        all_in_range = all(27 <= rows.data[i]['age'] <= 31 for i in result_range)
        
        assert len(result_range) > 0
        assert all_in_range
    
    def test_index_on_department(self, employees_table):
        storage, schema, _ = employees_table
        
        storage.set_index("employees", "department", "BTREE")
        index = storage.indexes[("employees", "department")]
        
        rows = storage.dml_manager.load_all_rows("employees", schema)
        for i, row in enumerate(rows.data):
            dept = row.get("department")
            if dept is not None:
                index.insert(dept, i)
        
        result_eng = index.search("Engineering")
        result_sales = index.search("Sales")
        result_hr = index.search("HR")
        
        assert len(result_eng) > 0
        assert len(result_sales) > 0
        assert len(result_hr) > 0
    
    def test_multiple_indexes(self, employees_table):
        storage, schema, _ = employees_table
        
        storage.set_index("employees", "age", "BTREE")
        storage.set_index("employees", "department", "BTREE")
        
        has_age = storage.has_index("employees", "age")
        has_dept = storage.has_index("employees", "department")
        has_salary = storage.has_index("employees", "salary")
        
        assert has_age
        assert has_dept
        assert not has_salary
    
    def test_drop_index(self, employees_table):
        storage, schema, _ = employees_table
        
        storage.set_index("employees", "age", "BTREE")
        
        has_before = storage.has_index("employees", "age")
        
        storage.drop_index("employees", "age")
        
        has_after = storage.has_index("employees", "age")
        
        assert has_before
        assert not has_after
    
    def test_duplicate_department_values(self, employees_table):
        storage, schema, _ = employees_table
        
        storage.set_index("employees", "department", "BTREE")
        index = storage.indexes[("employees", "department")]
        
        rows = storage.dml_manager.load_all_rows("employees", schema)
        for i, row in enumerate(rows.data):
            dept = row.get("department")
            if dept is not None:
                index.insert(dept, i)
        
        result = index.search("Engineering")
        
        assert len(result) > 1
    
    def test_index_persistence(self, employees_table):
        storage, schema, _ = employees_table
        index1 = BPlusTreeIndex("employees", "age", "src/data_test")
        
        rows = storage.dml_manager.load_all_rows("employees", schema)
        for i, row in enumerate(rows.data):
            age = row.get("age")
            if age is not None:
                index1.insert(age, i)
        
        index1.save()
        result1 = index1.search(30)
        index2 = BPlusTreeIndex("employees", "age", "src/data_test")
        result2 = index2.search(30)
        
        assert result1 == result2
        assert len(result2) > 0
    
    def test_large_range_search(self, employees_table):
        storage, schema, _ = employees_table
        
        storage.set_index("employees", "age", "BTREE")
        index = storage.indexes[("employees", "age")]
        
        rows = storage.dml_manager.load_all_rows("employees", schema)
        for i, row in enumerate(rows.data):
            age = row.get("age")
            if age is not None:
                index.insert(age, i)
        
        result_all = index.range_search(20, 40)
        
        assert len(result_all) == 20
    
    def test_error_duplicate_index(self, employees_table):
        storage, schema, _ = employees_table
        
        storage.set_index("employees", "age", "BTREE")
        
        with pytest.raises(ValueError, match="Index already exists"):
            storage.set_index("employees", "age", "BTREE")